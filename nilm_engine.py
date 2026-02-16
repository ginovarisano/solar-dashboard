"""
NILM (Non-Intrusive Load Monitoring) Engine
Detects individual appliances from total load power signal.

How it works:
1. Stores load samples every ~5 seconds in SQLite
2. Smooths the signal with a moving average (last 5 readings)
3. Detects "edges" — sudden jumps or drops in power (>30W)
4. Groups similar edges into "signatures" (e.g., all ~150W jumps = fridge)
5. Tracks which appliances are currently on and their usage over time
"""

import sqlite3
import time
from datetime import datetime, timedelta
from collections import deque

# --- Constants ---
EDGE_THRESHOLD = 20        # Minimum watt change to count as an appliance event (increased from 15)
SMOOTHING_WINDOW = 3       # Number of samples for moving average (3 × 5s = 15s)
DEBOUNCE_SECONDS = 10      # Ignore rapid fluctuations within this window (increased from 8)
SIGNATURE_TOLERANCE = 0.30 # 30% variation allowed when matching signatures (increased from 25%)
INVERTER_IDLE_LOAD = 70    # Watts drawn by the inverter when nothing else is on
MAX_PAIRING_HOURS = 12     # Maximum hours to look back when pairing on/off events

# --- In-memory state ---
# Rolling buffer of recent load readings for smoothing
_recent_loads = deque(maxlen=SMOOTHING_WINDOW)
_stable_level = None       # Last stable power level (only updates when load settles)
_last_event_time = 0       # Timestamp of last detected event (for debounce)

# Per-leg buffers for L1/L2 detection
_recent_l1 = deque(maxlen=SMOOTHING_WINDOW)
_recent_l2 = deque(maxlen=SMOOTHING_WINDOW)


def _smooth(values):
    """Calculate the average of a deque of numbers (moving average).
    This smooths out noise so we only detect real step changes."""
    if not values:
        return 0
    return sum(values) / len(values)


def _suggest_label(power):
    """Suggest a default label based on power range.
    These are rough guesses — the user can rename them later."""
    p = abs(power)
    if p < 50:
        return "Small Device"
    elif p < 200:
        return "Medium Device"
    elif p < 800:
        return "Appliance"
    elif p < 2000:
        return "Heater/Cooler"
    else:
        return "Major Appliance"


def _suggest_icon(power):
    """Suggest an emoji icon based on power range."""
    p = abs(power)
    if p < 50:
        return "\U0001F50C"   # plug
    elif p < 200:
        return "\u2744\uFE0F" # snowflake (fridge)
    elif p < 800:
        return "\U0001F4FA"   # TV
    elif p < 2000:
        return "\U0001F525"   # fire (heater)
    else:
        return "\u26A1"       # lightning


def _suggest_color(power):
    """Suggest a display color based on power range."""
    p = abs(power)
    if p < 50:
        return "#60a5fa"   # light blue
    elif p < 200:
        return "#22c55e"   # green
    elif p < 800:
        return "#f59e0b"   # amber
    elif p < 2000:
        return "#ef4444"   # red
    else:
        return "#a855f7"   # purple


def startup(db_path):
    """Call once at server startup.

    Clears stale 'active' states from the previous session (we can't
    know what's still on after a reboot) and logs how many saved
    signatures were found. Signatures persist in SQLite — they do NOT
    need to be relearned after a reboot.
    """
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    # Clear stale active states — we don't know what's on after restart
    c.execute("UPDATE appliance_signatures SET is_active = 0, active_count = 0")

    # Mark any unpaired on-events as stale (set a very long duration)
    c.execute(
        "UPDATE load_events SET duration = -1 "
        "WHERE event_type = 'on' AND duration IS NULL"
    )

    # Count saved signatures
    c.execute("SELECT COUNT(*) FROM appliance_signatures")
    sig_count = c.fetchone()[0]

    conn.commit()
    conn.close()

    if sig_count > 0:
        print(f"NILM: Loaded {sig_count} saved appliance signatures from database")
    else:
        print("NILM: No saved signatures yet — detection will learn as appliances turn on/off")

    return sig_count


def store_load_sample(db_path, load_total, load_l1, load_l2):
    """Store a load reading and return the smoothed value.

    Called every ~5 seconds from the MQTT handler.
    Adds the reading to our rolling buffer and saves to SQLite.

    Returns the smoothed (averaged) total load value.
    """
    _recent_loads.append(load_total)
    _recent_l1.append(load_l1 if load_l1 is not None else 0)
    _recent_l2.append(load_l2 if load_l2 is not None else 0)

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


def detect_edge(db_path, smoothed_total, load_l1, load_l2, override_timestamp=None):
    """Check if the load just jumped or dropped significantly.

    Compares the latest RAW load reading against a stable baseline.
    The smoothed value is used to drift the baseline slowly, but
    we use the actual raw readings (from the deque) to measure the
    true delta — this gives accurate wattage for detected events.

    override_timestamp: used during reanalyze to use the sample's own
    timestamp instead of wall-clock time.

    Returns a dict describing the event, or None if no edge detected.
    """
    global _stable_level, _last_event_time

    # Need at least a few readings
    if _stable_level is None:
        _stable_level = smoothed_total
        return None

    # Use the most recent raw reading for accurate delta measurement
    # (smoothed_total lags behind during transitions)
    raw_current = _recent_loads[-1] if _recent_loads else smoothed_total
    delta = raw_current - _stable_level

    # If the change is small, the load is stable — drift the baseline slowly
    # Reduced drift rate from 0.1 to 0.05 to prevent phantom events
    if abs(delta) < EDGE_THRESHOLD:
        _stable_level = _stable_level * 0.95 + smoothed_total * 0.05
        return None

    # Debounce: ignore events too close together
    if override_timestamp:
        try:
            now = datetime.strptime(override_timestamp, "%Y-%m-%d %H:%M:%S").timestamp()
        except (ValueError, TypeError):
            now = time.time()
    else:
        now = time.time()

    if now - _last_event_time < DEBOUNCE_SECONDS:
        return None
    _last_event_time = now

    event_type = "on" if delta > 0 else "off"
    power_delta = round(abs(delta), 1)

    # Update stable level to current raw value (the new baseline)
    _stable_level = raw_current

    timestamp = override_timestamp or datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Determine which leg
    l1_now = load_l1 or 0
    l2_now = load_l2 or 0
    if abs(l1_now - l2_now) < 20:
        leg = "both"
    elif l1_now > l2_now:
        leg = "L1"
    else:
        leg = "L2"

    # Confidence score — higher power deltas are more confident
    confidence = min(1.0, 0.5 + power_delta / 400)
    confidence = round(max(0.5, confidence), 2)

    # Find or create a matching signature
    signature_id = find_matching_signature(db_path, power_delta, leg)
    if signature_id:
        update_signature(db_path, signature_id, power_delta)
    else:
        signature_id = create_signature(db_path, power_delta, leg)

    # Calculate duration for "off" events
    duration = None
    if event_type == "off":
        duration = pair_on_off_event(db_path, signature_id, timestamp, power_delta)

    # Store the event
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute(
        "INSERT INTO load_events (timestamp, event_type, power_delta, leg, duration, signature_id, confidence) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (timestamp, event_type, power_delta, leg, duration, signature_id, confidence)
    )
    conn.commit()
    conn.close()

    # Update signature active state
    _update_signature_active_state(db_path, signature_id, event_type, timestamp, power_delta)

    # Update daily stats if off event with duration
    if event_type == "off" and duration:
        _update_daily_stats(db_path, signature_id, power_delta, duration)

    label = _get_signature_label(db_path, signature_id)

    # Debug logging (only for non-reanalyze events to avoid spam)
    if not override_timestamp:
        duration_str = f", ran for {duration}s" if duration else ""
        print(f"NILM: {label} turned {event_type} ({power_delta}W{duration_str}) [confidence: {confidence}]")

    return {
        "event_type": event_type,
        "power_delta": power_delta,
        "leg": leg,
        "duration": duration,
        "signature_id": signature_id,
        "confidence": confidence,
        "label": label,
        "timestamp": timestamp,
    }


def find_matching_signature(db_path, power_delta, leg):
    """Find an existing signature that matches this power delta.

    A match means the power is within SIGNATURE_TOLERANCE (15%) of the
    signature's average power. This is how we recognize "the fridge"
    turning on again — it always draws about the same wattage.

    Returns the signature ID, or None if no match found.
    """
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("SELECT id, power_avg FROM appliance_signatures")
    rows = c.fetchall()
    conn.close()

    best_id = None
    best_diff = float("inf")

    for sig_id, avg_power in rows:
        diff = abs(power_delta - avg_power)
        tolerance = avg_power * SIGNATURE_TOLERANCE
        if diff <= tolerance and diff < best_diff:
            best_id = sig_id
            best_diff = diff

    return best_id


def create_signature(db_path, power_delta, leg):
    """Create a new appliance signature when no existing one matches.

    This happens the first time we see a new power level — we create
    a new "unknown" appliance and auto-suggest a label based on wattage.

    Returns the new signature ID.
    """
    label = _suggest_label(power_delta)
    icon = _suggest_icon(power_delta)
    color = _suggest_color(power_delta)

    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute(
        "INSERT INTO appliance_signatures "
        "(power_avg, power_min, power_max, leg_pattern, event_count, user_label, icon, color, "
        "is_active, last_on_time, avg_duration, daily_cycles) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, NULL, 0, 0)",
        (power_delta, power_delta, power_delta, leg, 1, label, icon, color)
    )
    sig_id = c.lastrowid
    conn.commit()
    conn.close()

    return sig_id


def update_signature(db_path, signature_id, power_delta):
    """Update a signature's stats when we see another matching event.

    Updates the running average, min, max, and event count.
    This makes the signature more accurate over time.
    """
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute(
        "SELECT power_avg, power_min, power_max, event_count FROM appliance_signatures WHERE id = ?",
        (signature_id,)
    )
    row = c.fetchone()
    if not row:
        conn.close()
        return

    old_avg, old_min, old_max, count = row
    new_count = count + 1
    # Running average: weighted update
    new_avg = round(((old_avg * count) + power_delta) / new_count, 1)
    new_min = min(old_min, power_delta)
    new_max = max(old_max, power_delta)

    c.execute(
        "UPDATE appliance_signatures SET power_avg = ?, power_min = ?, power_max = ?, event_count = ? "
        "WHERE id = ?",
        (new_avg, new_min, new_max, new_count, signature_id)
    )
    conn.commit()
    conn.close()


def pair_on_off_event(db_path, signature_id, off_timestamp, off_power=0):
    """Find the matching 'on' event for an 'off' event.

    Searches all unpaired 'on' events and picks the one with the
    closest power match. Uses FIFO for ties (oldest first).
    This is more reliable than requiring the same signature ID,
    since on/off wattages often differ slightly.

    Only considers on-events within the last MAX_PAIRING_HOURS to
    avoid pairing unrelated events.

    Returns the duration in seconds, or None if no match found.
    """
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    # Get the off event's power for matching
    if off_power <= 0:
        c.execute("SELECT power_avg FROM appliance_signatures WHERE id = ?", (signature_id,))
        sig_row = c.fetchone()
        off_power = sig_row[0] if sig_row else 0

    # Calculate time cutoff for pairing (only look back MAX_PAIRING_HOURS)
    try:
        off_dt = datetime.strptime(off_timestamp, "%Y-%m-%d %H:%M:%S")
        cutoff_dt = off_dt - timedelta(hours=MAX_PAIRING_HOURS)
        cutoff_timestamp = cutoff_dt.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        # Fallback if timestamp parsing fails
        cutoff_timestamp = (datetime.now() - timedelta(hours=MAX_PAIRING_HOURS)).strftime("%Y-%m-%d %H:%M:%S")

    # Find all unpaired on-events with their power info (within time window)
    c.execute(
        "SELECT e.id, e.timestamp, e.power_delta, s.power_avg "
        "FROM load_events e "
        "JOIN appliance_signatures s ON e.signature_id = s.id "
        "WHERE e.event_type = 'on' AND e.duration IS NULL AND e.timestamp >= ? "
        "ORDER BY e.timestamp ASC",
        (cutoff_timestamp,)
    )
    candidates = c.fetchall()

    # Find the best match by power similarity
    row = None
    best_diff = float("inf")
    for cand_id, cand_ts, cand_delta, cand_avg in candidates:
        # Compare using both the event's actual delta and the signature avg
        diff = min(abs(off_power - cand_delta), abs(off_power - cand_avg))
        if diff < best_diff:
            best_diff = diff
            row = (cand_id, cand_ts)

    # Only accept if reasonably close (within 40% or 40W, whichever is larger)
    # Tightened from 50% to reduce false pairings
    if row and best_diff > max(off_power * 0.4, 40):
        row = None

    if not row:
        conn.close()
        return None

    on_id, on_timestamp = row
    try:
        on_dt = datetime.strptime(on_timestamp, "%Y-%m-%d %H:%M:%S")
        off_dt = datetime.strptime(off_timestamp, "%Y-%m-%d %H:%M:%S")
        duration = int((off_dt - on_dt).total_seconds())
    except (ValueError, TypeError):
        conn.close()
        return None

    # Mark the on-event with this duration too
    c.execute("UPDATE load_events SET duration = ? WHERE id = ?", (duration, on_id))
    conn.commit()
    conn.close()

    # Update average duration on the signature
    _update_avg_duration(db_path, signature_id, duration)

    return duration


def _update_signature_active_state(db_path, signature_id, event_type, timestamp, power_delta=0):
    """Mark a signature as active (on) or inactive (off).

    Uses instance counting so identical appliances (e.g. two same lamps)
    show as "Lamp x2" instead of resetting the timer.

    For OFF events, also checks other active signatures with similar
    power in case the on/off wattages differ slightly.
    """
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    if event_type == "on":
        # Get current active count
        c.execute("SELECT active_count FROM appliance_signatures WHERE id = ?", (signature_id,))
        row = c.fetchone()
        current_count = row[0] if row and row[0] else 0
        new_count = current_count + 1
        c.execute(
            "UPDATE appliance_signatures SET is_active = 1, active_count = ?, last_on_time = ? WHERE id = ?",
            (new_count, timestamp, signature_id)
        )
    else:
        # Try to decrement the matched signature first
        decremented = False
        c.execute("SELECT active_count FROM appliance_signatures WHERE id = ? AND is_active = 1", (signature_id,))
        row = c.fetchone()
        if row and row[0] and row[0] > 0:
            new_count = row[0] - 1
            if new_count <= 0:
                c.execute(
                    "UPDATE appliance_signatures SET is_active = 0, active_count = 0 WHERE id = ?",
                    (signature_id,)
                )
            else:
                c.execute(
                    "UPDATE appliance_signatures SET active_count = ? WHERE id = ?",
                    (new_count, signature_id)
                )
            decremented = True

        # If we didn't decrement the matched signature (it wasn't active),
        # look for any active signature with similar power and decrement that
        if not decremented and power_delta > 0:
            c.execute(
                "SELECT id, power_avg, active_count FROM appliance_signatures WHERE is_active = 1"
            )
            for other_id, other_power, other_count in c.fetchall():
                diff = abs(power_delta - other_power)
                tolerance = max(other_power * SIGNATURE_TOLERANCE, EDGE_THRESHOLD)
                if diff <= tolerance:
                    new_count = (other_count or 1) - 1
                    if new_count <= 0:
                        c.execute(
                            "UPDATE appliance_signatures SET is_active = 0, active_count = 0 WHERE id = ?",
                            (other_id,)
                        )
                    else:
                        c.execute(
                            "UPDATE appliance_signatures SET active_count = ? WHERE id = ?",
                            (new_count, other_id)
                        )
                    break  # Only decrement one matching signature
    conn.commit()
    conn.close()


def _update_avg_duration(db_path, signature_id, new_duration):
    """Update the running average duration for a signature."""
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute(
        "SELECT avg_duration, event_count FROM appliance_signatures WHERE id = ?",
        (signature_id,)
    )
    row = c.fetchone()
    if row:
        old_avg, count = row
        # Use event_count / 2 since each cycle has on+off
        cycles = max(1, count // 2)
        new_avg = round(((old_avg * (cycles - 1)) + new_duration) / cycles, 1)
        c.execute(
            "UPDATE appliance_signatures SET avg_duration = ? WHERE id = ?",
            (new_avg, signature_id)
        )
    conn.commit()
    conn.close()


def _update_daily_stats(db_path, signature_id, power_delta, duration):
    """Update daily per-appliance stats after an off event."""
    today = datetime.now().strftime("%Y-%m-%d")
    # Energy = power * time, convert to kWh
    energy_kwh = round((power_delta * duration) / 3600 / 1000, 4)

    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute(
        "SELECT cycles, total_duration, energy_kwh FROM appliance_daily_stats "
        "WHERE date = ? AND signature_id = ?",
        (today, signature_id)
    )
    row = c.fetchone()
    if row:
        c.execute(
            "UPDATE appliance_daily_stats SET cycles = cycles + 1, "
            "total_duration = total_duration + ?, energy_kwh = energy_kwh + ? "
            "WHERE date = ? AND signature_id = ?",
            (duration, energy_kwh, today, signature_id)
        )
    else:
        c.execute(
            "INSERT INTO appliance_daily_stats (date, signature_id, cycles, total_duration, energy_kwh) "
            "VALUES (?, ?, 1, ?, ?)",
            (today, signature_id, duration, energy_kwh)
        )
    conn.commit()
    conn.close()

    # Update daily_cycles on the signature
    _update_daily_cycles(db_path, signature_id)


def _update_daily_cycles(db_path, signature_id):
    """Calculate and update the average daily cycles for a signature."""
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute(
        "SELECT AVG(cycles) FROM appliance_daily_stats WHERE signature_id = ?",
        (signature_id,)
    )
    row = c.fetchone()
    if row and row[0]:
        c.execute(
            "UPDATE appliance_signatures SET daily_cycles = ? WHERE id = ?",
            (round(row[0], 1), signature_id)
        )
    conn.commit()
    conn.close()


def _get_signature_label(db_path, signature_id):
    """Get the user-facing label for a signature."""
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("SELECT user_label FROM appliance_signatures WHERE id = ?", (signature_id,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else "Unknown"


def get_active_appliances(db_path, current_load=None):
    """Return list of currently-on appliances with their duration.

    Looks at unpaired "on" events (duration IS NULL) from the last
    4 hours. Events older than 4 hours are auto-expired (marked stale).

    If current_load is provided (the real inverter load reading),
    cross-checks the total active power against it. If the active
    list claims more power than the inverter is actually drawing,
    it prunes the most suspect entries (oldest first).
    """
    now = datetime.now()
    stale_cutoff = (now - timedelta(hours=4)).strftime("%Y-%m-%d %H:%M:%S")

    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    # Auto-expire old unpaired events (missed off-events)
    c.execute(
        "UPDATE load_events SET duration = -1 "
        "WHERE event_type = 'on' AND duration IS NULL AND timestamp < ?",
        (stale_cutoff,)
    )
    conn.commit()

    # Find active (recent unpaired) on-events
    c.execute(
        "SELECT e.id, e.timestamp, e.power_delta, e.signature_id, "
        "s.user_label, s.icon, s.color, s.power_avg "
        "FROM load_events e "
        "JOIN appliance_signatures s ON e.signature_id = s.id "
        "WHERE e.event_type = 'on' AND e.duration IS NULL "
        "ORDER BY e.timestamp ASC"
    )
    rows = c.fetchall()

    # Build the active list
    active = []
    for row in rows:
        event_id, on_time, power_delta, sig_id, label, icon, color, power_avg = row
        duration = 0
        try:
            on_dt = datetime.strptime(on_time, "%Y-%m-%d %H:%M:%S")
            duration = int((now - on_dt).total_seconds())
        except (ValueError, TypeError):
            pass
        active.append({
            "id": sig_id,
            "event_id": event_id,
            "power": power_avg,
            "label": label,
            "icon": icon,
            "color": color,
            "duration": duration,
        })

    # Cross-check against actual inverter load
    if current_load is not None and len(active) > 0:
        # Subtract inverter idle to get appliance-only power
        appliance_load = max(0, current_load - INVERTER_IDLE_LOAD)
        active_total = sum(a["power"] for a in active)

        # If active list claims significantly more than inverter shows,
        # prune oldest entries until it makes sense
        # Increased margin from 30% to 50% + 50W to reduce false pruning
        # This accounts for measurement variance and appliances with variable loads
        if active_total > appliance_load * 1.5 + 50:
            # Remove oldest entries first (most likely to be stale)
            while len(active) > 0 and active_total > appliance_load * 1.5 + 50:
                removed = active.pop(0)  # oldest entry
                active_total -= removed["power"]
                # Mark the event as stale in the DB
                c.execute(
                    "UPDATE load_events SET duration = -1 WHERE id = ?",
                    (removed["event_id"],)
                )
            conn.commit()

    conn.close()
    return active


def get_recent_events(db_path, hours=24):
    """Get recent on/off events for the event feed."""
    since = (datetime.now() - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute(
        "SELECT e.id, e.timestamp, e.event_type, e.power_delta, e.leg, e.duration, "
        "e.signature_id, e.confidence, s.user_label, s.icon, s.color "
        "FROM load_events e "
        "LEFT JOIN appliance_signatures s ON e.signature_id = s.id "
        "WHERE e.timestamp >= ? "
        "ORDER BY e.timestamp DESC LIMIT 100",
        (since,)
    )
    rows = c.fetchall()
    conn.close()

    return [{
        "id": r[0], "timestamp": r[1], "event_type": r[2], "power_delta": r[3],
        "leg": r[4], "duration": r[5], "signature_id": r[6], "confidence": r[7],
        "label": r[8] or "Unknown", "icon": r[9] or "\U0001F50C", "color": r[10] or "#6b7280",
    } for r in rows]


def get_all_signatures(db_path):
    """Get all known appliance signatures."""
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute(
        "SELECT id, power_avg, power_min, power_max, leg_pattern, event_count, "
        "user_label, icon, color, is_active, last_on_time, avg_duration, daily_cycles "
        "FROM appliance_signatures ORDER BY event_count DESC"
    )
    rows = c.fetchall()
    conn.close()

    return [{
        "id": r[0], "power_avg": r[1], "power_min": r[2], "power_max": r[3],
        "leg_pattern": r[4], "event_count": r[5], "user_label": r[6],
        "icon": r[7], "color": r[8], "is_active": bool(r[9]),
        "last_on_time": r[10], "avg_duration": r[11], "daily_cycles": r[12],
    } for r in rows]


def update_signature_label(db_path, signature_id, label=None, icon=None, color=None):
    """Update user-facing label, icon, and/or color for a signature."""
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    if label is not None:
        c.execute("UPDATE appliance_signatures SET user_label = ? WHERE id = ?", (label, signature_id))
    if icon is not None:
        c.execute("UPDATE appliance_signatures SET icon = ? WHERE id = ?", (icon, signature_id))
    if color is not None:
        c.execute("UPDATE appliance_signatures SET color = ? WHERE id = ?", (color, signature_id))
    conn.commit()
    conn.close()


def merge_signatures(db_path, keep_id, merge_id):
    """Merge two signatures into one.

    Moves all events from merge_id to keep_id, combines stats,
    then deletes merge_id. Useful when the same appliance created
    two slightly different signatures.
    """
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    # Get both signatures
    c.execute("SELECT power_avg, event_count FROM appliance_signatures WHERE id = ?", (keep_id,))
    keep = c.fetchone()
    c.execute("SELECT power_avg, event_count FROM appliance_signatures WHERE id = ?", (merge_id,))
    merge = c.fetchone()

    if not keep or not merge:
        conn.close()
        return False

    # Weighted average of power
    total_count = keep[1] + merge[1]
    new_avg = round((keep[0] * keep[1] + merge[0] * merge[1]) / total_count, 1) if total_count > 0 else keep[0]

    # Move events to keep_id
    c.execute("UPDATE load_events SET signature_id = ? WHERE signature_id = ?", (keep_id, merge_id))

    # Move daily stats
    c.execute("UPDATE appliance_daily_stats SET signature_id = ? WHERE signature_id = ?", (keep_id, merge_id))

    # Update keep signature
    c.execute(
        "UPDATE appliance_signatures SET power_avg = ?, event_count = ? WHERE id = ?",
        (new_avg, total_count, keep_id)
    )

    # Delete merged signature
    c.execute("DELETE FROM appliance_signatures WHERE id = ?", (merge_id,))

    conn.commit()
    conn.close()
    return True


def get_daily_stats(db_path, signature_id, days=30):
    """Get daily usage pattern for one appliance over the last N days."""
    since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute(
        "SELECT date, cycles, total_duration, energy_kwh "
        "FROM appliance_daily_stats "
        "WHERE signature_id = ? AND date >= ? "
        "ORDER BY date DESC",
        (signature_id, since)
    )
    rows = c.fetchall()
    conn.close()

    return [{
        "date": r[0], "cycles": r[1],
        "total_duration": r[2], "energy_kwh": r[3],
    } for r in rows]


def _save_user_labels(db_path):
    """Save user-customized labels/icons/colors before reanalyze.

    Returns a dict mapping power_avg (rounded) to the user's
    customizations, so they can be restored after new signatures
    are created.
    """
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("SELECT power_avg, user_label, icon, color FROM appliance_signatures")
    rows = c.fetchall()
    conn.close()

    saved = {}
    for power_avg, label, icon, color in rows:
        # Use rounded power as the key so it matches new signatures
        key = round(power_avg)
        saved[key] = {"label": label, "icon": icon, "color": color}
    return saved


def _restore_user_labels(db_path, saved_labels):
    """Restore user-customized labels after reanalyze.

    Matches saved labels to new signatures by closest power.
    Only restores if the label was user-modified (not an auto-label).
    """
    auto_labels = {"Small Device", "Medium Device", "Appliance", "Heater/Cooler", "Major Appliance"}

    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("SELECT id, power_avg, user_label FROM appliance_signatures")
    new_sigs = c.fetchall()

    restored = 0
    for sig_id, power_avg, current_label in new_sigs:
        key = round(power_avg)
        # Look for a saved label within 25% of this signature's power
        best_match = None
        best_diff = float("inf")
        for saved_power, customization in saved_labels.items():
            diff = abs(key - saved_power)
            tolerance = max(key * SIGNATURE_TOLERANCE, EDGE_THRESHOLD)
            if diff <= tolerance and diff < best_diff:
                # Only restore if the saved label was user-customized
                if customization["label"] not in auto_labels:
                    best_match = customization
                    best_diff = diff

        if best_match:
            c.execute(
                "UPDATE appliance_signatures SET user_label = ?, icon = ?, color = ? WHERE id = ?",
                (best_match["label"], best_match["icon"], best_match["color"], sig_id)
            )
            restored += 1

    conn.commit()
    conn.close()
    if restored > 0:
        print(f"NILM: Restored {restored} custom appliance names from previous session")


def reanalyze(db_path):
    """Replay all stored load_samples through the edge detector.

    Preserves user-customized appliance names/icons/colors and
    restores them after rebuilding signatures.

    Returns a summary dict with counts of what was found.
    """
    global _recent_loads, _recent_l1, _recent_l2, _stable_level, _last_event_time

    # Save user's custom labels before wiping
    saved_labels = _save_user_labels(db_path)

    # 1. Clear old detections (but keep the raw samples)
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("DELETE FROM load_events")
    c.execute("DELETE FROM appliance_signatures")
    c.execute("DELETE FROM appliance_daily_stats")
    conn.commit()

    # 2. Read all stored samples in chronological order
    c.execute(
        "SELECT timestamp, load_total, load_l1, load_l2 "
        "FROM load_samples ORDER BY timestamp ASC"
    )
    samples = c.fetchall()
    conn.close()

    if not samples:
        return {"samples": 0, "events": 0, "signatures": 0}

    # 3. Reset in-memory state so we start fresh
    _recent_loads = deque(maxlen=SMOOTHING_WINDOW)
    _recent_l1 = deque(maxlen=SMOOTHING_WINDOW)
    _recent_l2 = deque(maxlen=SMOOTHING_WINDOW)
    _stable_level = None
    _last_event_time = 0

    # 4. Replay each sample through the detector
    #    Pass the sample's own timestamp so debounce works correctly
    events_found = 0
    for ts, load_total, load_l1, load_l2 in samples:
        # Build up the smoothing buffer
        _recent_loads.append(load_total)
        _recent_l1.append(load_l1 or 0)
        _recent_l2.append(load_l2 or 0)
        smoothed = _smooth(_recent_loads)

        # Run edge detection with the sample's timestamp for proper debounce
        event = detect_edge(db_path, smoothed, load_l1, load_l2, override_timestamp=ts)
        if event:
            events_found += 1

    # 5. After replay, mark all signatures as inactive
    #    (we can't know current state from history alone)
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("UPDATE appliance_signatures SET is_active = 0, active_count = 0")
    sig_count = c.execute("SELECT COUNT(*) FROM appliance_signatures").fetchone()[0]
    conn.commit()
    conn.close()

    # Restore user's custom labels
    _restore_user_labels(db_path, saved_labels)

    print(f"NILM reanalyze: {len(samples)} samples -> {events_found} events, {sig_count} signatures")

    return {
        "samples": len(samples),
        "events": events_found,
        "signatures": sig_count,
    }


def reanalyze_from_influx(db_path, sa_query_fn):
    """Pull raw load power history from Solar Assistant's InfluxDB
    and run edge detection on it.

    sa_query_fn: the app.sa_query function, passed in so we don't
    need to import app (avoids circular imports).

    This gives us ~10 days of ~10-second data to analyze, much more
    than the 7-day local samples table.
    """
    global _recent_loads, _recent_l1, _recent_l2, _stable_level, _last_event_time

    print("NILM: Pulling load power history from Solar Assistant...")

    # Query raw load power — InfluxDB stores ~10 days at ~10s intervals
    # Pull in batches by day to avoid timeouts
    all_samples = []
    results = sa_query_fn('SELECT "combined" FROM "Load power" WHERE time > now() - 10d ORDER BY time ASC')
    if not results:
        print("NILM: No data from Solar Assistant")
        return {"samples": 0, "events": 0, "signatures": 0, "error": "No data from Solar Assistant"}

    for r in results:
        for s in r.get("series", []):
            for v in s.get("values", []):
                ts_raw = v[0]  # e.g. "2026-02-04T00:44:01Z"
                power = v[1]
                if power is None:
                    continue
                # Convert ISO timestamp to our format
                try:
                    # Handle both "Z" and "+00:00" suffixes
                    ts_clean = ts_raw.replace("Z", "").replace("+00:00", "")
                    # Parse and convert to local time
                    from datetime import timezone
                    utc_dt = datetime.strptime(ts_clean[:19], "%Y-%m-%dT%H:%M:%S")
                    utc_dt = utc_dt.replace(tzinfo=timezone.utc)
                    local_dt = utc_dt.astimezone()
                    ts = local_dt.strftime("%Y-%m-%d %H:%M:%S")
                except (ValueError, TypeError):
                    continue
                all_samples.append((ts, float(power)))

    if not all_samples:
        print("NILM: No valid samples parsed")
        return {"samples": 0, "events": 0, "signatures": 0, "error": "No valid samples"}

    print(f"NILM: Got {len(all_samples)} samples from {all_samples[0][0]} to {all_samples[-1][0]}")

    # Save user's custom labels before wiping
    saved_labels = _save_user_labels(db_path)

    # Clear existing detections
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("DELETE FROM load_events")
    c.execute("DELETE FROM appliance_signatures")
    c.execute("DELETE FROM appliance_daily_stats")
    conn.commit()
    conn.close()

    # Reset in-memory state
    _recent_loads = deque(maxlen=SMOOTHING_WINDOW)
    _recent_l1 = deque(maxlen=SMOOTHING_WINDOW)
    _recent_l2 = deque(maxlen=SMOOTHING_WINDOW)
    _stable_level = None
    _last_event_time = 0

    # Replay all samples through detector
    # Note: we don't have L1/L2 split from InfluxDB, so pass None
    events_found = 0
    for ts, load_total in all_samples:
        _recent_loads.append(load_total)
        smoothed = _smooth(_recent_loads)
        event = detect_edge(db_path, smoothed, None, None, override_timestamp=ts)
        if event:
            events_found += 1

    # Mark all signatures as inactive after replay
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("UPDATE appliance_signatures SET is_active = 0, active_count = 0")
    sig_count = c.execute("SELECT COUNT(*) FROM appliance_signatures").fetchone()[0]
    event_count = c.execute("SELECT COUNT(*) FROM load_events").fetchone()[0]
    conn.commit()
    conn.close()

    # Restore user's custom labels
    _restore_user_labels(db_path, saved_labels)

    print(f"NILM reanalyze (InfluxDB): {len(all_samples)} samples -> {events_found} events, {sig_count} signatures")

    return {
        "samples": len(all_samples),
        "events": events_found,
        "signatures": sig_count,
        "source": "Solar Assistant InfluxDB",
        "date_range": f"{all_samples[0][0]} to {all_samples[-1][0]}",
    }


def cleanup_old_data(db_path):
    """Delete old data to keep the database from growing forever.

    - Load samples: keep 7 days (high-frequency data)
    - Load events: keep 30 days
    """
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    samples_cutoff = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
    c.execute("DELETE FROM load_samples WHERE timestamp < ?", (samples_cutoff,))
    deleted_samples = c.rowcount

    events_cutoff = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")
    c.execute("DELETE FROM load_events WHERE timestamp < ?", (events_cutoff,))
    deleted_events = c.rowcount

    conn.commit()
    conn.close()

    if deleted_samples > 0 or deleted_events > 0:
        print(f"NILM cleanup: removed {deleted_samples} samples, {deleted_events} events")
