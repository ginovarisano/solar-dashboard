"""
NILM (Non-Intrusive Load Monitoring) Engine — v1.5 "Load Events"

We used to try to identify specific appliances ("that's your dishwasher")
by matching power signatures across reboots. With 1Hz power-only data,
that's fundamentally unreliable — a 1500W coffee maker and a 1500W heater
look identical to us. So we stopped guessing.

What we ship now: raw load events. When something turns on, we log it.
When it turns off, we close the event with its duration and average power.
The user labels what it was. The dashboard offers to apply a label to
similar past events when one is set.

How it works
------------
1. Every ~5s, we receive a load_power reading from MQTT.
2. We smooth across the last few samples to suppress noise.
3. If the smoothed level steps UP by >= EDGE_THRESHOLD, we open a pending
   event (timestamp, baseline, step magnitude).
4. If it steps DOWN, we close the closest matching pending event by
   power magnitude and write the full row: started_at, ended_at, power_w,
   duration_s, category, confidence.
5. Categories are coarse buckets inferred from the magnitude (and a
   weak inrush hint from comparing on/off step sizes). They're a hint,
   not a claim — the user_label is the source of truth.

Public surface
--------------
- startup(db_path)               — call once at server boot
- store_load_sample(...)         — feed in a raw reading every ~5s
- detect_edge(...)               — runs after store_load_sample, may emit an event
- get_recent_events(...)         — for the dashboard event feed
- get_today_category_breakdown(...)
- label_event(...)               — set user_label on one event
- find_similar_unlabeled(...)    — events near the same wattage as a labeled one
- apply_label_to_events(...)     — bulk-label by ids
- cleanup_old_data(...)
"""

import sqlite3
import time
from datetime import datetime, timedelta
from collections import deque

# --- Tunables (live-overridable from settings) ---
EDGE_THRESHOLD = 30        # Min watts of change to count as an event
SMOOTHING_WINDOW = 3       # Samples averaged for noise reduction
DEBOUNCE_SECONDS = 10      # Cooldown between detected events
INVERTER_IDLE_LOAD = 70    # Inverter baseline draw (subtracted to keep small loads honest)

# Hard caps
MAX_PENDING_AGE_HOURS = 6   # Auto-close events that never see an off step

# --- In-memory state ---
_recent_loads = deque(maxlen=SMOOTHING_WINDOW)
_stable_level = None
_last_event_time = 0
# Each pending entry: {"id": int, "started_at": str, "step_up_w": float}
_pending_events = []


def _smooth(values):
    if not values:
        return 0
    return sum(values) / len(values)


def _categorize(power_w, duration_s, step_up_w, step_down_w):
    """Infer a coarse category from the event's shape.

    With 1Hz power-only data we can't reliably tell a 1500W coffee maker
    from a 1500W space heater — these are buckets, not claims. The user
    can override via user_label.
    """
    p = abs(power_w) if power_w else 0
    # Inrush hint: motors/compressors pull more current at startup than
    # steady state, so the on-step often reads larger than the off-step.
    has_inrush = False
    if step_up_w and step_down_w and step_down_w > 50:
        has_inrush = step_up_w > step_down_w * 1.4

    if p < 200:
        return "electronics"
    if has_inrush and p < 1500:
        return "motor"
    if p >= 1500:
        return "heating"
    return "resistive"


def _confidence(step_up_w, step_down_w, duration_s):
    """How confident are we that power_w is right?

    High when on/off steps agree. Low when only one side seen, or they
    disagree wildly.
    """
    if not step_up_w or not step_down_w:
        return 0.4  # Open or orphan — only one side observed
    bigger = max(step_up_w, step_down_w)
    smaller = min(step_up_w, step_down_w)
    if bigger == 0:
        return 0.4
    agreement = smaller / bigger  # 1.0 = identical, smaller = more variance
    # 0.5 floor + up to 0.5 from agreement
    return round(min(1.0, 0.5 + agreement * 0.5), 2)


def startup(db_path):
    """Clear stale pending state on server boot.

    We can't know what was running before a reboot, so any open event
    in the DB gets ended_at = started_at + 1s, confidence floored.
    """
    global _pending_events
    _pending_events = []

    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    # Close any open events from a previous run with a stale marker.
    c.execute(
        "UPDATE load_events "
        "SET ended_at = started_at, duration_s = 0, confidence = 0.2 "
        "WHERE ended_at IS NULL"
    )
    closed = c.rowcount
    conn.commit()
    conn.close()

    if closed > 0:
        print(f"NILM: closed {closed} stale open events from previous run")


def store_load_sample(db_path, load_total, load_l1, load_l2):
    """Record a sample and return the smoothed value.

    Called every ~5s from the MQTT handler. Keeps a rolling buffer for
    smoothing and persists the raw reading for replay/cleanup.
    """
    _recent_loads.append(load_total)
    smoothed = _smooth(_recent_loads)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute(
        "INSERT OR REPLACE INTO load_samples (timestamp, load_total, load_l1, load_l2, smoothed_total) "
        "VALUES (?, ?, ?, ?, ?)",
        (now, load_total, load_l1 or 0, load_l2 or 0, round(smoothed, 1))
    )
    conn.commit()
    conn.close()
    return smoothed


def detect_edge(db_path, smoothed_total, load_l1=None, load_l2=None, override_timestamp=None):
    """Detect on/off steps and emit completed load events.

    Returns one of:
      None                                          — no event this tick
      {"kind": "open",  "id": int, ...}             — a load just turned on
      {"kind": "closed","id": int, ...}             — a previously-on load just turned off
    """
    global _stable_level, _last_event_time, _pending_events

    if _stable_level is None:
        _stable_level = smoothed_total
        return None

    raw_current = _recent_loads[-1] if _recent_loads else smoothed_total
    delta = raw_current - _stable_level

    # Stable: drift baseline slowly toward the smoothed level
    if abs(delta) < EDGE_THRESHOLD:
        _stable_level = _stable_level * 0.95 + smoothed_total * 0.05
        return None

    # Debounce
    if override_timestamp:
        try:
            now_epoch = datetime.strptime(override_timestamp, "%Y-%m-%d %H:%M:%S").timestamp()
        except (ValueError, TypeError):
            now_epoch = time.time()
    else:
        now_epoch = time.time()
    if now_epoch - _last_event_time < DEBOUNCE_SECONDS:
        return None
    _last_event_time = now_epoch

    timestamp = override_timestamp or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    step_w = round(abs(delta), 1)
    _stable_level = raw_current

    if delta > 0:
        # Step UP — open a new event
        category = _categorize(step_w, None, step_w, 0)
        confidence = _confidence(step_w, None, None)
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        c.execute(
            "INSERT INTO load_events "
            "(started_at, ended_at, power_w, duration_s, category, user_label, confidence) "
            "VALUES (?, NULL, ?, NULL, ?, NULL, ?)",
            (timestamp, step_w, category, confidence)
        )
        event_id = c.lastrowid
        conn.commit()
        conn.close()

        _pending_events.append({
            "id": event_id,
            "started_at": timestamp,
            "step_up_w": step_w,
        })
        return {
            "kind": "open",
            "id": event_id,
            "started_at": timestamp,
            "power_w": step_w,
            "category": category,
            "confidence": confidence,
        }

    # Step DOWN — close the closest matching pending event
    if not _pending_events:
        # Orphan off — log a zero-duration event so we don't lose it
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        c.execute(
            "INSERT INTO load_events "
            "(started_at, ended_at, power_w, duration_s, category, user_label, confidence) "
            "VALUES (?, ?, ?, 0, ?, NULL, 0.3)",
            (timestamp, timestamp, step_w, _categorize(step_w, 0, 0, step_w))
        )
        event_id = c.lastrowid
        conn.commit()
        conn.close()
        return {"kind": "closed", "id": event_id, "orphan": True, "power_w": step_w}

    # Find the pending event closest in magnitude to this off-step
    best_idx = 0
    best_diff = float("inf")
    for i, p in enumerate(_pending_events):
        diff = abs(p["step_up_w"] - step_w)
        if diff < best_diff:
            best_diff = diff
            best_idx = i
    pending = _pending_events.pop(best_idx)

    try:
        on_dt = datetime.strptime(pending["started_at"], "%Y-%m-%d %H:%M:%S")
        off_dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
        duration_s = max(0, int((off_dt - on_dt).total_seconds()))
    except (ValueError, TypeError):
        duration_s = 0

    # Average the two step sizes — that's our best read on the load's actual draw
    avg_power = round((pending["step_up_w"] + step_w) / 2, 1)
    category = _categorize(avg_power, duration_s, pending["step_up_w"], step_w)
    confidence = _confidence(pending["step_up_w"], step_w, duration_s)

    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute(
        "UPDATE load_events SET ended_at = ?, duration_s = ?, power_w = ?, "
        "category = ?, confidence = ? WHERE id = ?",
        (timestamp, duration_s, avg_power, category, confidence, pending["id"])
    )
    conn.commit()
    conn.close()

    if not override_timestamp:
        mins = duration_s // 60
        secs = duration_s % 60
        print(f"NILM: load event closed — {avg_power}W for {mins}m{secs}s ({category})")

    return {
        "kind": "closed",
        "id": pending["id"],
        "started_at": pending["started_at"],
        "ended_at": timestamp,
        "power_w": avg_power,
        "duration_s": duration_s,
        "category": category,
        "confidence": confidence,
    }


def get_recent_events(db_path, limit=50):
    """Return recent load events, newest first.

    Open events (ended_at IS NULL) are surfaced too so the UI can show
    "currently running" entries.
    """
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute(
        "SELECT id, started_at, ended_at, power_w, duration_s, category, user_label, confidence "
        "FROM load_events ORDER BY started_at DESC LIMIT ?",
        (limit,)
    )
    rows = c.fetchall()
    conn.close()
    return [{
        "id": r[0],
        "started_at": r[1],
        "ended_at": r[2],
        "power_w": r[3],
        "duration_s": r[4],
        "category": r[5] or "unknown",
        "user_label": r[6],
        "confidence": r[7],
    } for r in rows]


def get_today_category_breakdown(db_path):
    """Return today's energy split by category as a list of {category, energy_wh, count}."""
    today = datetime.now().strftime("%Y-%m-%d")
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute(
        "SELECT category, COUNT(*), "
        "       SUM(COALESCE(power_w, 0) * COALESCE(duration_s, 0) / 3600.0) "
        "FROM load_events "
        "WHERE started_at >= ? AND duration_s > 0 "
        "GROUP BY category",
        (today + " 00:00:00",)
    )
    rows = c.fetchall()
    conn.close()
    return [{
        "category": r[0] or "unknown",
        "count": r[1],
        "energy_wh": round(r[2] or 0, 1),
    } for r in rows]


def label_event(db_path, event_id, label):
    """Set user_label on a single event. Pass label='' (or None) to clear."""
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute(
        "UPDATE load_events SET user_label = ? WHERE id = ?",
        (label or None, event_id)
    )
    changed = c.rowcount
    conn.commit()
    conn.close()
    return changed > 0


def find_similar_unlabeled(db_path, event_id, tolerance_pct=0.15):
    """Find unlabeled past events within ±tolerance_pct of the given event's power.

    Used after a user labels an event so the UI can offer "apply this
    label to N similar past events?" — never auto-applies.
    """
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("SELECT power_w FROM load_events WHERE id = ?", (event_id,))
    row = c.fetchone()
    if not row or row[0] is None:
        conn.close()
        return []
    target = row[0]
    low = target * (1 - tolerance_pct)
    high = target * (1 + tolerance_pct)
    c.execute(
        "SELECT id, started_at, ended_at, power_w, duration_s, category, confidence "
        "FROM load_events "
        "WHERE id != ? AND (user_label IS NULL OR user_label = '') "
        "  AND power_w BETWEEN ? AND ? "
        "ORDER BY started_at DESC LIMIT 50",
        (event_id, low, high)
    )
    rows = c.fetchall()
    conn.close()
    return [{
        "id": r[0], "started_at": r[1], "ended_at": r[2], "power_w": r[3],
        "duration_s": r[4], "category": r[5] or "unknown", "confidence": r[6],
    } for r in rows]


def apply_label_to_events(db_path, event_ids, label):
    """Bulk-set user_label on the given event ids."""
    if not event_ids:
        return 0
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    placeholders = ",".join("?" * len(event_ids))
    c.execute(
        f"UPDATE load_events SET user_label = ? WHERE id IN ({placeholders})",
        [label] + list(event_ids)
    )
    changed = c.rowcount
    conn.commit()
    conn.close()
    return changed


def cleanup_old_data(db_path):
    """Trim long tails so the DB doesn't grow forever.

    - load_samples: keep 7 days
    - load_events:  keep 60 days
    Also auto-close pending events older than MAX_PENDING_AGE_HOURS so we
    don't carry zombies forever.
    """
    global _pending_events
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    samples_cutoff = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
    c.execute("DELETE FROM load_samples WHERE timestamp < ?", (samples_cutoff,))
    deleted_samples = c.rowcount

    events_cutoff = (datetime.now() - timedelta(days=60)).strftime("%Y-%m-%d %H:%M:%S")
    c.execute("DELETE FROM load_events WHERE started_at < ?", (events_cutoff,))
    deleted_events = c.rowcount

    # Auto-close zombies
    zombie_cutoff_dt = datetime.now() - timedelta(hours=MAX_PENDING_AGE_HOURS)
    zombie_cutoff = zombie_cutoff_dt.strftime("%Y-%m-%d %H:%M:%S")
    c.execute(
        "UPDATE load_events "
        "SET ended_at = ?, "
        "    duration_s = CAST((julianday(?) - julianday(started_at)) * 86400 AS INTEGER), "
        "    confidence = 0.3 "
        "WHERE ended_at IS NULL AND started_at < ?",
        (zombie_cutoff, zombie_cutoff, zombie_cutoff)
    )
    closed_zombies = c.rowcount
    conn.commit()
    conn.close()

    # Drop in-memory pending entries that match closed zombies
    _pending_events = [
        p for p in _pending_events
        if p["started_at"] >= zombie_cutoff
    ]

    if deleted_samples or deleted_events or closed_zombies:
        msg = f"NILM cleanup: removed {deleted_samples} samples, {deleted_events} old events"
        if closed_zombies:
            msg += f", auto-closed {closed_zombies} zombie events"
        print(msg)
