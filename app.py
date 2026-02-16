"""
Solar Dashboard Server
Connects to Solar Assistant via MQTT and serves a live web dashboard.
Open http://localhost:5050 in your browser to see it.
"""

import sys
sys.stdout.reconfigure(line_buffering=True)

from flask import Flask, render_template, jsonify, request
from flask_socketio import SocketIO
import paho.mqtt.client as mqtt
from datetime import datetime, timedelta
import json
import threading
import sqlite3
import os
import urllib.request
import urllib.parse
import time as _time
import nilm_engine

# --- PyInstaller support ---
# When packaged as an executable, PyInstaller extracts files to a temp folder.
# _BUNDLE_DIR = where bundled files (templates, etc.) live
# _APP_DIR    = where the executable (or script) lives (for database, etc.)
if getattr(sys, 'frozen', False):
    _BUNDLE_DIR = sys._MEIPASS
    _APP_DIR = os.path.dirname(sys.executable)
else:
    _BUNDLE_DIR = os.path.dirname(os.path.abspath(__file__))
    _APP_DIR = _BUNDLE_DIR

# --- Server start time (for uptime counter) ---
SERVER_START = _time.time()

# --- Configuration (loaded from DB settings table) ---
SA_DB = "solar_assistant"

# Session cookie for SA (auto-login on first use)
_sa_cookie = None

# Global MQTT client reference (for reconnection)
_mqtt_client = None

# --- Settings cache (populated from DB on startup) ---
_settings_cache = {}

# Default settings — seeded into DB on first run
_SETTINGS_DEFAULTS = {
    "mqtt_broker": "",
    "mqtt_port": "1883",
    "mqtt_user": "",
    "mqtt_pass": "",
    "sa_host": "",
    "location_lat": "",
    "location_lon": "",
    "location_name": "",
    "timezone": "America/New_York",
    "electricity_rate": "0.30",
    "battery_capacity_wh": "0",
    "inverter_idle_load": "70",
    "nilm_edge_threshold": "15",
    "nilm_debounce": "8",
    "nilm_signature_tolerance": "0.25",
    "nilm_smoothing_window": "3",
    "web_port": "5050",
    "setup_completed": "false",
}


def get_setting(key, default=None):
    """Get a single setting value from the cache."""
    return _settings_cache.get(key, _SETTINGS_DEFAULTS.get(key, default))


def get_setting_float(key, default=0):
    """Get a setting as a float."""
    try:
        return float(get_setting(key, str(default)))
    except (ValueError, TypeError):
        return default


def get_setting_int(key, default=0):
    """Get a setting as an integer."""
    try:
        return int(float(get_setting(key, str(default))))
    except (ValueError, TypeError):
        return default


def get_all_settings():
    """Return all settings as a dict."""
    result = dict(_SETTINGS_DEFAULTS)
    result.update(_settings_cache)
    return result


def set_setting(key, value):
    """Save a single setting to DB and update cache."""
    _settings_cache[key] = str(value)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, str(value)))
    conn.commit()
    conn.close()


def load_settings_cache():
    """Load all settings from DB into the in-memory cache."""
    global _settings_cache
    try:
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute("SELECT key, value FROM settings").fetchall()
        conn.close()
        _settings_cache = {r[0]: r[1] for r in rows}
    except Exception:
        _settings_cache = {}


def apply_nilm_settings():
    """Push current settings to nilm_engine module-level constants."""
    nilm_engine.EDGE_THRESHOLD = get_setting_float("nilm_edge_threshold", 15)
    nilm_engine.DEBOUNCE_SECONDS = get_setting_float("nilm_debounce", 8)
    nilm_engine.SIGNATURE_TOLERANCE = get_setting_float("nilm_signature_tolerance", 0.25)
    nilm_engine.SMOOTHING_WINDOW = get_setting_int("nilm_smoothing_window", 3)
    nilm_engine.INVERTER_IDLE_LOAD = get_setting_float("inverter_idle_load", 70)


def _sa_grafana_url():
    """Build the SA Grafana URL from current settings."""
    host = get_setting("sa_host", "")
    return f"http://{host}/grafana/api/datasources/proxy/1/query"

# Database file lives next to the executable (not in the temp bundle)
DB_PATH = os.path.join(_APP_DIR, "solar_history.db")

# --- Setup Flask + SocketIO ---
app = Flask(__name__, template_folder=os.path.join(_BUNDLE_DIR, "templates"))
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

# Store the latest values so new browser connections get current data
latest_data = {}
# Store recent history for charts (last 120 readings per metric)
history = {
    "pv_power": [],
    "load_power": [],
    "battery_soc": [],
    "battery_power": [],
}
MAX_HISTORY = 120

# Track session energy
daily_tracker = {
    "date": datetime.now().strftime("%Y-%m-%d"),
    "pv_energy_start": None,
    "load_energy_start": None,
}

# Latest weather data (updated every 10 minutes)
latest_weather = {}

# Map MQTT topics to friendly names
TOPIC_MAP = {
    "inverter_1/pv_power/state": "pv_power",
    "inverter_1/pv_power_1/state": "pv_power_1",
    "inverter_1/pv_power_2/state": "pv_power_2",
    "inverter_1/pv_voltage_1/state": "pv_voltage_1",
    "inverter_1/pv_voltage_2/state": "pv_voltage_2",
    "inverter_1/pv_current_1/state": "pv_current_1",
    "inverter_1/load_power/state": "load_power",
    "inverter_1/load_power_1/state": "load_power_1",
    "inverter_1/load_power_2/state": "load_power_2",
    "inverter_1/load_percentage/state": "load_percentage",
    "inverter_1/load_apparent_power/state": "load_apparent_power",
    "inverter_1/battery_voltage/state": "battery_voltage",
    "inverter_1/battery_current/state": "battery_current",
    "inverter_1/grid_power/state": "grid_power",
    "inverter_1/grid_voltage/state": "grid_voltage",
    "inverter_1/grid_frequency/state": "grid_frequency",
    "inverter_1/temperature/state": "inverter_temp",
    "inverter_1/device_mode/state": "device_mode",
    "inverter_1/ac_output_frequency/state": "ac_frequency",
    "inverter_1/output_source_priority/state": "output_priority",
    "total/battery_state_of_charge/state": "battery_soc",
    "total/battery_power/state": "battery_power",
    "total/bus_voltage/state": "bus_voltage",
    "total/pv_energy/state": "pv_energy_total",
    "total/load_energy/state": "load_energy_total",
    "total/grid_energy_in/state": "grid_energy_in",
    "total/grid_energy_out/state": "grid_energy_out",
    "total/battery_energy_in/state": "battery_energy_in",
    "total/battery_energy_out/state": "battery_energy_out",
}

# WMO weather code descriptions
WMO_CODES = {
    0: ("Clear sky", "sun"),
    1: ("Mainly clear", "sun"),
    2: ("Partly cloudy", "cloud-sun"),
    3: ("Overcast", "cloud"),
    45: ("Fog", "fog"),
    48: ("Icy fog", "fog"),
    51: ("Light drizzle", "drizzle"),
    53: ("Drizzle", "drizzle"),
    55: ("Heavy drizzle", "drizzle"),
    61: ("Light rain", "rain"),
    63: ("Rain", "rain"),
    65: ("Heavy rain", "rain"),
    66: ("Freezing rain", "rain"),
    67: ("Heavy freezing rain", "rain"),
    71: ("Light snow", "snow"),
    73: ("Snow", "snow"),
    75: ("Heavy snow", "snow"),
    77: ("Snow grains", "snow"),
    80: ("Light showers", "rain"),
    81: ("Showers", "rain"),
    82: ("Heavy showers", "rain"),
    85: ("Light snow showers", "snow"),
    86: ("Snow showers", "snow"),
    95: ("Thunderstorm", "storm"),
    96: ("Thunderstorm w/ hail", "storm"),
    99: ("Thunderstorm w/ heavy hail", "storm"),
}


# =====================================================
#  Solar Assistant InfluxDB Queries (via Grafana proxy)
# =====================================================

def sa_login():
    """Get an authenticated session cookie from Solar Assistant."""
    global _sa_cookie
    import http.cookiejar
    cj = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
    # Just hit the root — SA auto-redirects through /sign_in and sets session cookie
    try:
        opener.open(f"http://{get_setting('sa_host', '')}/", timeout=10)
    except Exception:
        pass
    # Extract the cookie string
    cookies = "; ".join(f"{c.name}={c.value}" for c in cj)
    if cookies:
        _sa_cookie = cookies
        print(f"Solar Assistant session established")
    return _sa_cookie


def sa_query(influx_query):
    """Run an InfluxDB query against Solar Assistant's Grafana proxy."""
    global _sa_cookie
    if not _sa_cookie:
        sa_login()
    if not _sa_cookie:
        return None

    encoded = urllib.parse.quote(influx_query)
    url = f"{_sa_grafana_url()}?db={SA_DB}&q={encoded}"
    req = urllib.request.Request(url)
    req.add_header("Cookie", _sa_cookie)

    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
        return data.get("results", [])
    except Exception as e:
        print(f"SA query error: {e}")
        # Try re-login once
        _sa_cookie = None
        sa_login()
        if _sa_cookie:
            req2 = urllib.request.Request(url)
            req2.add_header("Cookie", _sa_cookie)
            try:
                with urllib.request.urlopen(req2, timeout=15) as resp:
                    return json.loads(resp.read().decode()).get("results", [])
            except Exception as e2:
                print(f"SA query retry error: {e2}")
        return None


def sa_get_daily_history(days=30):
    """Get daily energy from Solar Assistant's InfluxDB."""
    q = (
        f'SELECT sum("combined") FROM "PV power hourly" WHERE time >= now() - {days}d GROUP BY time(1d) fill(0);'
        f'SELECT sum("combined") FROM "Load power hourly" WHERE time >= now() - {days}d GROUP BY time(1d) fill(0);'
        f'SELECT sum("combined") FROM "Grid power in hourly" WHERE time >= now() - {days}d GROUP BY time(1d) fill(0);'
        f'SELECT sum("combined") FROM "Grid power out hourly" WHERE time >= now() - {days}d GROUP BY time(1d) fill(0)'
    )
    results = sa_query(q)
    if not results or len(results) < 4:
        return None

    def extract(result_idx):
        series = results[result_idx].get("series", [{}])
        if not series:
            return {}
        return {v[0][:10]: v[1] for v in series[0].get("values", []) if v[1]}

    pv = extract(0)
    load = extract(1)
    grid_in = extract(2)
    grid_out = extract(3)

    all_dates = sorted(set(pv.keys()) | set(load.keys()), reverse=True)
    # Filter out dates with no real data
    today = datetime.now().strftime("%Y-%m-%d")
    rows = []
    for d in all_dates:
        pv_wh = pv.get(d, 0)
        load_wh = load.get(d, 0)
        if pv_wh == 0 and load_wh == 0:
            continue
        rows.append({
            "date": d,
            "pv": round(pv_wh / 1000, 2),
            "load": round(load_wh / 1000, 2),
            "grid_in": round(grid_in.get(d, 0) / 1000, 2),
            "grid_out": round(grid_out.get(d, 0) / 1000, 2),
        })

    # Add weather data from local DB
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    for row in rows:
        c.execute("SELECT high_f, low_f, cloud_cover_avg, uv_index_max, sunshine_hours, conditions FROM daily_weather WHERE date = ?", (row["date"],))
        wx = c.fetchone()
        if wx:
            row["high_f"] = wx[0]
            row["low_f"] = wx[1]
            row["cloud_cover"] = wx[2]
            row["uv_max"] = wx[3]
            row["sunshine_hrs"] = wx[4]
            row["conditions"] = wx[5]
        else:
            row["conditions"] = None
    conn.close()

    return rows


def sa_get_monthly_history():
    """Aggregate SA daily data into monthly totals."""
    daily = sa_get_daily_history(365)
    if not daily:
        return None
    months = {}
    for row in daily:
        m = row["date"][:7]
        if m not in months:
            months[m] = {"month": m, "pv": 0, "load": 0, "grid_in": 0, "grid_out": 0, "days": 0}
        months[m]["pv"] += row["pv"]
        months[m]["load"] += row["load"]
        months[m]["grid_in"] += row["grid_in"]
        months[m]["grid_out"] += row["grid_out"]
        months[m]["days"] += 1
    result = sorted(months.values(), key=lambda x: x["month"], reverse=True)
    for r in result:
        r["pv"] = round(r["pv"], 2)
        r["load"] = round(r["load"], 2)
        r["grid_in"] = round(r["grid_in"], 2)
        r["grid_out"] = round(r["grid_out"], 2)
    return result


def sa_get_yearly_history():
    """Aggregate SA daily data into yearly totals."""
    daily = sa_get_daily_history(3650)
    if not daily:
        return None
    years = {}
    for row in daily:
        y = row["date"][:4]
        if y not in years:
            years[y] = {"year": y, "pv": 0, "load": 0, "grid_in": 0, "grid_out": 0, "days": 0}
        years[y]["pv"] += row["pv"]
        years[y]["load"] += row["load"]
        years[y]["grid_in"] += row["grid_in"]
        years[y]["grid_out"] += row["grid_out"]
        years[y]["days"] += 1
    result = sorted(years.values(), key=lambda x: x["year"], reverse=True)
    for r in result:
        r["pv"] = round(r["pv"], 2)
        r["load"] = round(r["load"], 2)
        r["grid_in"] = round(r["grid_in"], 2)
        r["grid_out"] = round(r["grid_out"], 2)
    return result


def sa_get_all_time_stats():
    """Calculate all-time stats from SA's InfluxDB."""
    daily = sa_get_daily_history(3650)
    if not daily:
        return None

    total_pv = sum(r["pv"] for r in daily)
    total_load = sum(r["load"] for r in daily)
    total_grid_in = sum(r["grid_in"] for r in daily)
    total_days = len(daily)
    first_date = daily[-1]["date"] if daily else ""
    last_date = daily[0]["date"] if daily else ""

    # Query total battery discharge (energy out) from InfluxDB
    total_bat_out_kwh = 0
    bat_q = 'SELECT sum("combined") FROM "Battery power out hourly" WHERE time > now() - 3650d'
    bat_results = sa_query(bat_q)
    if bat_results:
        bat_series = bat_results[0].get("series", [{}])
        if bat_series:
            for v in bat_series[0].get("values", []):
                if v[1] and v[1] > 0:
                    total_bat_out_kwh += v[1] / 1000

    rate = get_setting_float("electricity_rate", 0.30)
    co2_lbs = round(total_pv * 0.855, 1)
    money_saved = round(total_pv * rate, 2)
    bat_money_saved = round(total_bat_out_kwh * rate, 2)
    iphones = round(total_pv / 0.012)
    ev_miles = round(total_pv / 0.3, 1)
    led_hours = round(total_pv / 0.01)
    laptops = round(total_pv / 0.06)
    gallons_gas = round(total_pv / 33.7, 1)
    laundry = round(total_pv / 2.3)
    showers = round(total_pv / 1.7)
    tree_days = round(co2_lbs / 0.131) if co2_lbs > 0 else 0
    netflix_hours = round(total_pv / 0.1)
    pizzas = round(total_pv / 2)
    avg_daily = round(total_pv / total_days, 2) if total_days > 0 else 0
    self_sufficiency = round((1 - total_grid_in / total_load) * 100, 1) if total_load > 0 else 100

    return {
        "total_pv": round(total_pv, 2),
        "total_load": round(total_load, 2),
        "total_bat_out_kwh": round(total_bat_out_kwh, 2),
        "bat_money_saved": bat_money_saved,
        "total_days": total_days,
        "first_date": first_date,
        "last_date": last_date,
        "avg_daily_pv": avg_daily,
        "self_sufficiency": self_sufficiency,
        "co2_lbs": co2_lbs,
        "money_saved": money_saved,
        "iphones": iphones,
        "ev_miles": ev_miles,
        "led_hours": led_hours,
        "laptops": laptops,
        "gallons_gas": gallons_gas,
        "laundry": laundry,
        "showers": showers,
        "tree_days": tree_days,
        "netflix_hours": netflix_hours,
        "pizzas": pizzas,
    }


# =====================================================
#  Weather
# =====================================================

def get_solar_factor():
    """Calculate average kWh produced per sunshine hour from recent history.
    This lets us estimate future production based on forecasted sunshine."""
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        # Get days with both production and sunshine data from the last 14 days
        c.execute("""
            SELECT ROUND(e.pv_last - e.pv_first, 2) as pv_kwh,
                   w.sunshine_hours
            FROM daily_energy e
            JOIN daily_weather w ON e.date = w.date
            WHERE w.sunshine_hours > 0.5
              AND (e.pv_last - e.pv_first) > 0.5
            ORDER BY e.date DESC
            LIMIT 14
        """)
        rows = c.fetchall()
        conn.close()
        if len(rows) >= 3:
            total_kwh = sum(r[0] for r in rows)
            total_sun = sum(r[1] for r in rows)
            return round(total_kwh / total_sun, 2) if total_sun > 0 else None
        return None
    except Exception:
        return None


def fetch_weather():
    """Fetch current weather from Open-Meteo (free, no API key needed)."""
    global latest_weather
    try:
        lat = get_setting("location_lat", "")
        lon = get_setting("location_lon", "")
        tz = get_setting("timezone", "America/New_York")
        url = (
            f"https://api.open-meteo.com/v1/forecast?"
            f"latitude={lat}&longitude={lon}"
            f"&current=temperature_2m,relative_humidity_2m,apparent_temperature,"
            f"cloud_cover,weather_code,wind_speed_10m,uv_index"
            f"&daily=temperature_2m_max,temperature_2m_min,sunshine_duration,"
            f"uv_index_max,weather_code,sunrise,sunset,"
            f"precipitation_probability_max,wind_speed_10m_max,precipitation_sum"
            f"&hourly=weather_code,temperature_2m,cloud_cover,precipitation_probability,"
            f"wind_speed_10m"
            f"&temperature_unit=fahrenheit&wind_speed_unit=mph"
            f"&timezone={tz}&forecast_days=2"
        )
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())

        current = data.get("current", {})
        daily = data.get("daily", {})

        weather_code = current.get("weather_code", 0)
        desc, icon_type = WMO_CODES.get(weather_code, ("Unknown", "cloud"))

        # Build per-day forecast list (today + tomorrow)
        daily_codes = daily.get("weather_code", [])
        daily_highs = daily.get("temperature_2m_max", [])
        daily_lows = daily.get("temperature_2m_min", [])
        daily_precip = daily.get("precipitation_probability_max", [])
        daily_wind = daily.get("wind_speed_10m_max", [])
        daily_uv = daily.get("uv_index_max", [])
        daily_sunshine = daily.get("sunshine_duration", [])
        daily_sunrise = daily.get("sunrise", [])
        daily_sunset = daily.get("sunset", [])
        daily_precip_sum = daily.get("precipitation_sum", [])

        # Hourly data for day/night condition splits
        hourly = data.get("hourly", {})
        hourly_times = hourly.get("time", [])
        hourly_codes = hourly.get("weather_code", [])
        hourly_temps = hourly.get("temperature_2m", [])
        hourly_clouds = hourly.get("cloud_cover", [])
        hourly_precip_prob = hourly.get("precipitation_probability", [])
        hourly_wind = hourly.get("wind_speed_10m", [])

        # Get historical kWh-per-sunshine-hour for production estimates
        kwh_per_sun_hr = get_solar_factor()

        def most_common_code(codes):
            """Return the most frequent weather code from a list."""
            if not codes:
                return 0
            from collections import Counter
            return Counter(codes).most_common(1)[0][0]

        forecast_days = []
        for i in range(min(2, len(daily_codes))):
            d_desc, d_icon = WMO_CODES.get(daily_codes[i], ("Unknown", "cloud"))
            sun_hrs = round(daily_sunshine[i] / 3600, 1) if i < len(daily_sunshine) else 0

            rise = daily_sunrise[i] if i < len(daily_sunrise) else None
            sset = daily_sunset[i] if i < len(daily_sunset) else None

            # Calculate daylight hours
            daylight_hrs = None
            rise_dt = None
            sset_dt = None
            if rise and sset:
                rise_dt = datetime.fromisoformat(rise)
                sset_dt = datetime.fromisoformat(sset)
                daylight_hrs = round((sset_dt - rise_dt).total_seconds() / 3600, 1)

            # Split hourly data into daytime and nighttime
            day_codes = []
            day_temps = []
            day_clouds = []
            day_precip = []
            day_winds = []
            night_codes = []
            night_temps = []
            night_clouds = []
            night_precip = []
            night_winds = []

            if rise_dt and sset_dt:
                for h_idx, h_time_str in enumerate(hourly_times):
                    h_time = datetime.fromisoformat(h_time_str)
                    # Only look at hours belonging to this calendar day
                    if h_time.date() != rise_dt.date():
                        continue
                    h_code = hourly_codes[h_idx] if h_idx < len(hourly_codes) else 0
                    h_temp = hourly_temps[h_idx] if h_idx < len(hourly_temps) else None
                    h_cloud = hourly_clouds[h_idx] if h_idx < len(hourly_clouds) else None
                    h_prec = hourly_precip_prob[h_idx] if h_idx < len(hourly_precip_prob) else None
                    h_wind = hourly_wind[h_idx] if h_idx < len(hourly_wind) else None

                    if rise_dt <= h_time < sset_dt:
                        day_codes.append(h_code)
                        if h_temp is not None: day_temps.append(h_temp)
                        if h_cloud is not None: day_clouds.append(h_cloud)
                        if h_prec is not None: day_precip.append(h_prec)
                        if h_wind is not None: day_winds.append(h_wind)
                    else:
                        night_codes.append(h_code)
                        if h_temp is not None: night_temps.append(h_temp)
                        if h_cloud is not None: night_clouds.append(h_cloud)
                        if h_prec is not None: night_precip.append(h_prec)
                        if h_wind is not None: night_winds.append(h_wind)

            day_wmo = most_common_code(day_codes)
            night_wmo = most_common_code(night_codes)
            day_desc_str, day_icon_str = WMO_CODES.get(day_wmo, ("Unknown", "cloud"))
            night_desc_str, night_icon_str = WMO_CODES.get(night_wmo, ("Unknown", "cloud"))

            # Solar percentage and 1–10 rating
            solar_pct = round(sun_hrs / daylight_hrs * 100) if daylight_hrs and daylight_hrs > 0 else 0
            solar_score = max(1, min(10, round(solar_pct / 10))) if daylight_hrs else 1

            # Estimated kWh production
            est_kwh = round(kwh_per_sun_hr * sun_hrs, 1) if kwh_per_sun_hr and sun_hrs else None

            forecast_days.append({
                "high_f": daily_highs[i] if i < len(daily_highs) else None,
                "low_f": daily_lows[i] if i < len(daily_lows) else None,
                "weather_code": daily_codes[i],
                "description": d_desc,
                "icon_type": d_icon,
                "precip_chance": daily_precip[i] if i < len(daily_precip) else None,
                "precip_sum": daily_precip_sum[i] if i < len(daily_precip_sum) else None,
                "wind_max_mph": daily_wind[i] if i < len(daily_wind) else None,
                "uv_max": daily_uv[i] if i < len(daily_uv) else None,
                "sunshine_hours": sun_hrs,
                "sunrise": rise,
                "sunset": sset,
                "daylight_hours": daylight_hrs,
                "solar_percent": solar_pct,
                "solar_score": solar_score,
                "estimated_kwh": est_kwh,
                # Daytime detail
                "day_desc": day_desc_str,
                "day_icon": day_icon_str,
                "day_high": round(max(day_temps)) if day_temps else None,
                "day_cloud_avg": round(sum(day_clouds) / len(day_clouds)) if day_clouds else None,
                "day_precip_max": max(day_precip) if day_precip else None,
                "day_wind_avg": round(sum(day_winds) / len(day_winds), 1) if day_winds else None,
                # Nighttime detail
                "night_desc": night_desc_str,
                "night_icon": night_icon_str,
                "night_low": round(min(night_temps)) if night_temps else None,
                "night_cloud_avg": round(sum(night_clouds) / len(night_clouds)) if night_clouds else None,
                "night_precip_max": max(night_precip) if night_precip else None,
                "night_wind_avg": round(sum(night_winds) / len(night_winds), 1) if night_winds else None,
            })

        latest_weather = {
            "location": get_setting("location_name", ""),
            "temp_f": current.get("temperature_2m"),
            "feels_like_f": current.get("apparent_temperature"),
            "humidity": current.get("relative_humidity_2m"),
            "cloud_cover": current.get("cloud_cover"),
            "wind_mph": current.get("wind_speed_10m"),
            "uv_index": current.get("uv_index"),
            "weather_code": weather_code,
            "description": desc,
            "icon_type": icon_type,
            "high_f": daily.get("temperature_2m_max", [None])[0],
            "low_f": daily.get("temperature_2m_min", [None])[0],
            "sunshine_hours": round(daily.get("sunshine_duration", [0])[0] / 3600, 1),
            "uv_max": daily.get("uv_index_max", [None])[0],
            "sunrise": daily.get("sunrise", [None])[0],
            "sunset": daily.get("sunset", [None])[0],
            "fetched_at": datetime.now().strftime("%H:%M:%S"),
            "forecast": forecast_days,
        }

        # Log weather to database
        record_weather(
            datetime.now().strftime("%Y-%m-%d"),
            latest_weather["high_f"],
            latest_weather["low_f"],
            latest_weather["cloud_cover"],
            latest_weather["uv_max"],
            latest_weather["sunshine_hours"],
            desc,
        )

        # Push to connected browsers
        socketio.emit("weather_update", latest_weather)
        print(f"Weather updated: {desc}, {latest_weather['temp_f']}°F, "
              f"clouds {latest_weather['cloud_cover']}%, UV {latest_weather['uv_index']}")

    except Exception as e:
        print(f"Weather fetch error: {e}")


def weather_loop():
    """Fetch weather every 10 minutes."""
    import time
    while True:
        fetch_weather()
        time.sleep(600)  # 10 minutes


# =====================================================
#  SQLite Database for Historical Tracking
# =====================================================

def init_db():
    """Create the database tables if they don't exist."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS daily_energy (
            date        TEXT PRIMARY KEY,
            pv_first    REAL,
            pv_last     REAL,
            load_first  REAL,
            load_last   REAL,
            grid_in_first  REAL,
            grid_in_last   REAL,
            grid_out_first REAL,
            grid_out_last  REAL
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS daily_weather (
            date            TEXT PRIMARY KEY,
            high_f          REAL,
            low_f           REAL,
            cloud_cover_avg REAL,
            uv_index_max    REAL,
            sunshine_hours  REAL,
            conditions      TEXT
        )
    """)
    # --- NILM tables (appliance detection) ---
    c.execute("""
        CREATE TABLE IF NOT EXISTS load_samples (
            timestamp    TEXT PRIMARY KEY,
            load_total   REAL,
            load_l1      REAL,
            load_l2      REAL,
            smoothed_total REAL
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS load_events (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp    TEXT,
            event_type   TEXT,
            power_delta  REAL,
            leg          TEXT,
            duration     INTEGER,
            signature_id INTEGER,
            confidence   REAL
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS appliance_signatures (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            power_avg    REAL,
            power_min    REAL,
            power_max    REAL,
            leg_pattern  TEXT,
            event_count  INTEGER,
            user_label   TEXT,
            icon         TEXT,
            color        TEXT,
            is_active    INTEGER DEFAULT 0,
            active_count INTEGER DEFAULT 0,
            last_on_time TEXT,
            avg_duration REAL DEFAULT 0,
            daily_cycles REAL DEFAULT 0
        )
    """)
    # Add active_count column if upgrading from older schema
    try:
        c.execute("ALTER TABLE appliance_signatures ADD COLUMN active_count INTEGER DEFAULT 0")
    except Exception:
        pass  # Column already exists
    c.execute("""
        CREATE TABLE IF NOT EXISTS appliance_daily_stats (
            date         TEXT,
            signature_id INTEGER,
            cycles       INTEGER,
            total_duration INTEGER,
            energy_kwh   REAL,
            PRIMARY KEY (date, signature_id)
        )
    """)
    # --- Settings table ---
    c.execute("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)")
    # Seed defaults (won't overwrite existing values)
    for key, val in _SETTINGS_DEFAULTS.items():
        c.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (key, val))
    conn.commit()
    conn.close()
    print(f"Database ready: {DB_PATH}")


def record_weather(date_str, high_f, low_f, cloud_cover, uv_max, sunshine_hrs, conditions):
    """Record daily weather summary."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        INSERT OR REPLACE INTO daily_weather
        (date, high_f, low_f, cloud_cover_avg, uv_index_max, sunshine_hours, conditions)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (date_str, high_f, low_f, cloud_cover, uv_max, sunshine_hrs, conditions))
    conn.commit()
    conn.close()


def record_energy(date_str, pv_total, load_total, grid_in, grid_out):
    """Record a cumulative energy snapshot. Updates first/last for the day."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT pv_first FROM daily_energy WHERE date = ?", (date_str,))
    row = c.fetchone()
    if row is None:
        c.execute("""
            INSERT INTO daily_energy
            (date, pv_first, pv_last, load_first, load_last,
             grid_in_first, grid_in_last, grid_out_first, grid_out_last)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (date_str, pv_total, pv_total, load_total, load_total,
              grid_in, grid_in, grid_out, grid_out))
    else:
        c.execute("""
            UPDATE daily_energy
            SET pv_last = ?, load_last = ?, grid_in_last = ?, grid_out_last = ?
            WHERE date = ?
        """, (pv_total, load_total, grid_in, grid_out, date_str))
    conn.commit()
    conn.close()


def get_daily_history(limit=30):
    """Get daily production/consumption with weather for the last N days."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        SELECT e.date,
               ROUND(e.pv_last - e.pv_first, 2) as pv_kwh,
               ROUND(e.load_last - e.load_first, 2) as load_kwh,
               ROUND(e.grid_in_last - e.grid_in_first, 2) as grid_in_kwh,
               ROUND(e.grid_out_last - e.grid_out_first, 2) as grid_out_kwh,
               w.high_f, w.low_f, w.cloud_cover_avg,
               w.uv_index_max, w.sunshine_hours, w.conditions
        FROM daily_energy e
        LEFT JOIN daily_weather w ON e.date = w.date
        ORDER BY e.date DESC
        LIMIT ?
    """, (limit,))
    rows = c.fetchall()
    conn.close()
    return [{"date": r[0], "pv": r[1], "load": r[2],
             "grid_in": r[3], "grid_out": r[4],
             "high_f": r[5], "low_f": r[6], "cloud_cover": r[7],
             "uv_max": r[8], "sunshine_hrs": r[9], "conditions": r[10]}
            for r in rows]


def get_monthly_history(limit=12):
    """Aggregate daily data into monthly totals."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        SELECT SUBSTR(date, 1, 7) as month,
               ROUND(SUM(pv_last - pv_first), 2) as pv_kwh,
               ROUND(SUM(load_last - load_first), 2) as load_kwh,
               ROUND(SUM(grid_in_last - grid_in_first), 2) as grid_in_kwh,
               ROUND(SUM(grid_out_last - grid_out_first), 2) as grid_out_kwh,
               COUNT(*) as days
        FROM daily_energy
        GROUP BY month
        ORDER BY month DESC
        LIMIT ?
    """, (limit,))
    rows = c.fetchall()
    conn.close()
    return [{"month": r[0], "pv": r[1], "load": r[2],
             "grid_in": r[3], "grid_out": r[4], "days": r[5]} for r in rows]


def get_yearly_history():
    """Aggregate daily data into yearly totals."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        SELECT SUBSTR(date, 1, 4) as year,
               ROUND(SUM(pv_last - pv_first), 2) as pv_kwh,
               ROUND(SUM(load_last - load_first), 2) as load_kwh,
               ROUND(SUM(grid_in_last - grid_in_first), 2) as grid_in_kwh,
               ROUND(SUM(grid_out_last - grid_out_first), 2) as grid_out_kwh,
               COUNT(*) as days
        FROM daily_energy
        GROUP BY year
        ORDER BY year DESC
    """)
    rows = c.fetchall()
    conn.close()
    return [{"year": r[0], "pv": r[1], "load": r[2],
             "grid_in": r[3], "grid_out": r[4], "days": r[5]} for r in rows]


# =====================================================
#  MQTT Callbacks
# =====================================================

def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        print("MQTT connected to Solar Assistant!")
        client.subscribe("solar_assistant/#")
    else:
        print(f"MQTT connection failed: {reason_code}")


_cumulative = {
    "pv_energy_total": None,
    "load_energy_total": None,
    "grid_energy_in": None,
    "grid_energy_out": None,
}
_last_db_write = None
_last_nilm_sample = 0      # Timestamp of last NILM sample (throttle to ~5s)
_last_nilm_cleanup = None  # Last time we ran NILM data cleanup


def on_message(client, userdata, message):
    global _last_db_write

    topic = message.topic.replace("solar_assistant/", "")
    value = message.payload.decode("utf-8")

    friendly = TOPIC_MAP.get(topic, None)
    if friendly is None:
        return

    try:
        value = float(value)
    except ValueError:
        pass

    latest_data[friendly] = value
    latest_data["last_update"] = datetime.now().strftime("%H:%M:%S")

    today = datetime.now().strftime("%Y-%m-%d")
    if daily_tracker["date"] != today:
        daily_tracker["date"] = today

    if friendly in _cumulative:
        _cumulative[friendly] = value

    now = datetime.now()
    if all(v is not None for v in _cumulative.values()):
        if _last_db_write is None or (now - _last_db_write).seconds >= 60:
            _last_db_write = now
            record_energy(
                today,
                _cumulative["pv_energy_total"],
                _cumulative["load_energy_total"],
                _cumulative["grid_energy_in"],
                _cumulative["grid_energy_out"],
            )

    if friendly in history and isinstance(value, float):
        now_str = now.strftime("%H:%M:%S")
        history[friendly].append({"time": now_str, "value": value})
        if len(history[friendly]) > MAX_HISTORY:
            history[friendly] = history[friendly][-MAX_HISTORY:]

    # --- NILM: store load samples and detect appliance events ---
    if friendly == "load_power" and isinstance(value, float):
        global _last_nilm_sample, _last_nilm_cleanup
        now_ts = _time.time()
        # Only sample every ~5 seconds to avoid overwhelming the DB
        if now_ts - _last_nilm_sample >= 5:
            _last_nilm_sample = now_ts
            load_l1 = latest_data.get("load_power_1")
            load_l2 = latest_data.get("load_power_2")
            try:
                smoothed = nilm_engine.store_load_sample(DB_PATH, value, load_l1, load_l2)
                event = nilm_engine.detect_edge(DB_PATH, smoothed, load_l1, load_l2)
                if event:
                    print(f"NILM: {event['label']} {event['event_type']} ({event['power_delta']}W)")
                    socketio.emit("nilm_event", event)
            except Exception as e:
                print(f"NILM error: {e}")

            # Cleanup old NILM data once per hour
            if _last_nilm_cleanup is None or (now - _last_nilm_cleanup).seconds >= 3600:
                _last_nilm_cleanup = now
                try:
                    nilm_engine.cleanup_old_data(DB_PATH)
                except Exception as e:
                    print(f"NILM cleanup error: {e}")

    socketio.emit("solar_update", {
        "key": friendly,
        "value": value,
        "all": latest_data,
        "history": history,
    })


# =====================================================
#  Web Routes
# =====================================================

@app.route("/")
def dashboard():
    if get_setting("setup_completed") != "true":
        return render_template("setup.html", settings=get_all_settings())
    return render_template("dashboard.html")


@app.route("/setup")
def setup_page():
    """Always show the setup wizard (for re-running it from the dashboard)."""
    return render_template("setup.html", settings=get_all_settings())


@app.route("/api/data")
def api_data():
    return jsonify({"current": latest_data, "history": history})


@app.route("/api/uptime")
def api_uptime():
    return jsonify({"start": SERVER_START})


@app.route("/api/chart24h")
def api_chart24h():
    """Get 24 hours of PV, load, and battery power at 5-minute intervals from InfluxDB.
    Optional query param: days_ago (0=today, 1=yesterday, etc.)"""
    tz = get_setting("timezone", "America/New_York")
    days_ago = int(request.args.get("days_ago", 0))

    from zoneinfo import ZoneInfo
    local_tz = ZoneInfo(tz)
    now_local = datetime.now(local_tz)
    target_date = now_local - timedelta(days=days_ago)
    # Start/end of the target day in local time, converted to UTC for InfluxDB
    day_start = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end = day_start + timedelta(days=1)
    start_utc = day_start.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_utc = day_end.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%SZ")

    q = (
        f'SELECT mean("combined") FROM "PV power" WHERE time >= \'{start_utc}\' AND time < \'{end_utc}\' GROUP BY time(5m) fill(null);'
        f'SELECT mean("combined") FROM "Load power" WHERE time >= \'{start_utc}\' AND time < \'{end_utc}\' GROUP BY time(5m) fill(null);'
        f'SELECT mean("combined") FROM "Battery power" WHERE time >= \'{start_utc}\' AND time < \'{end_utc}\' GROUP BY time(5m) fill(null)'
    )
    results = sa_query(q)
    if not results or len(results) < 3:
        return jsonify({"error": "No data available"}), 503

    def extract_series(idx):
        series = results[idx].get("series", [{}])
        if not series:
            return [], []
        labels = []
        values = []
        for v in series[0].get("values", []):
            if v[1] is None:
                continue
            t = datetime.fromisoformat(v[0].replace("Z", "+00:00")).astimezone(local_tz)
            labels.append(t.strftime("%H:%M"))
            values.append(round(v[1], 1))
        return labels, values

    pv_labels, pv_vals = extract_series(0)
    _, load_vals = extract_series(1)
    _, bat_vals = extract_series(2)

    return jsonify({
        "labels": pv_labels,
        "pv": pv_vals,
        "load": load_vals,
        "battery": [abs(v) for v in bat_vals],
        "date": target_date.strftime("%Y-%m-%d"),
        "date_label": "Today" if days_ago == 0 else "Yesterday" if days_ago == 1 else target_date.strftime("%a %b %-d"),
    })


@app.route("/api/weather")
def api_weather():
    return jsonify(latest_weather)


@app.route("/api/today")
def api_today():
    """Get today's energy totals from Solar Assistant (midnight EST reset)."""
    from zoneinfo import ZoneInfo
    tz = get_setting("timezone", "America/New_York")
    now_local = datetime.now(ZoneInfo(tz))
    midnight_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    midnight_utc = midnight_local.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%SZ")

    q = (
        f'SELECT sum("combined") FROM "PV power hourly" WHERE time >= \'{midnight_utc}\';'
        f'SELECT sum("combined") FROM "Load power hourly" WHERE time >= \'{midnight_utc}\';'
        f'SELECT max("combined") FROM "PV power" WHERE time >= \'{midnight_utc}\''
    )
    results = sa_query(q)
    if results and len(results) >= 2:
        pv_wh = 0
        load_wh = 0
        pv_peak = 0
        pv_series = results[0].get("series", [{}])
        load_series = results[1].get("series", [{}])
        if pv_series:
            for v in pv_series[0].get("values", []):
                if v[1] and v[1] > 0:
                    pv_wh += v[1]
        if load_series:
            for v in load_series[0].get("values", []):
                if v[1] and v[1] > 0:
                    load_wh += v[1]
        if len(results) >= 3:
            peak_series = results[2].get("series", [{}])
            if peak_series:
                vals = peak_series[0].get("values", [])
                if vals and vals[0][1]:
                    pv_peak = round(vals[0][1])
        return jsonify({
            "pv_today_kwh": round(pv_wh / 1000, 2),
            "load_today_kwh": round(load_wh / 1000, 2),
            "pv_peak_w": pv_peak,
        })
    return jsonify({"pv_today_kwh": None, "load_today_kwh": None, "pv_peak_w": None})


@app.route("/api/history/daily")
def api_history_daily():
    data = sa_get_daily_history(30)
    if data is not None:
        return jsonify(data)
    return jsonify(get_daily_history(30))


@app.route("/api/history/monthly")
def api_history_monthly():
    data = sa_get_monthly_history()
    if data is not None:
        return jsonify(data)
    return jsonify(get_monthly_history(12))


@app.route("/api/history/yearly")
def api_history_yearly():
    data = sa_get_yearly_history()
    if data is not None:
        return jsonify(data)
    return jsonify(get_yearly_history())


@app.route("/api/stats")
def api_stats():
    """Calculate fun all-time stats — pull from SA first, fallback to local DB."""
    data = sa_get_all_time_stats()
    if data is not None:
        return jsonify(data)

    # Fallback to local SQLite
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        SELECT
            ROUND(SUM(pv_last - pv_first), 2),
            ROUND(SUM(load_last - load_first), 2),
            ROUND(SUM(grid_in_last - grid_in_first), 2),
            COUNT(*),
            MIN(date),
            MAX(date)
        FROM daily_energy
    """)
    row = c.fetchone()
    conn.close()

    total_pv = row[0] or 0
    total_load = row[1] or 0
    total_grid_in = row[2] or 0
    total_days = row[3] or 0
    first_date = row[4] or ""
    last_date = row[5] or ""

    rate = get_setting_float("electricity_rate", 0.30)
    co2_lbs = round(total_pv * 0.855, 1)
    money_saved = round(total_pv * rate, 2)
    iphones = round(total_pv / 0.012)
    ev_miles = round(total_pv / 0.3, 1)
    led_hours = round(total_pv / 0.01)
    laptops = round(total_pv / 0.06)
    gallons_gas = round(total_pv / 33.7, 1)
    laundry = round(total_pv / 2.3)
    showers = round(total_pv / 1.7)
    tree_days = round(co2_lbs / 0.131) if co2_lbs > 0 else 0
    netflix_hours = round(total_pv / 0.1)
    pizzas = round(total_pv / 2)
    avg_daily = round(total_pv / total_days, 2) if total_days > 0 else 0
    self_sufficiency = round((1 - total_grid_in / total_load) * 100, 1) if total_load > 0 else 100

    return jsonify({
        "total_pv": total_pv,
        "total_load": total_load,
        "total_days": total_days,
        "first_date": first_date,
        "last_date": last_date,
        "avg_daily_pv": avg_daily,
        "self_sufficiency": self_sufficiency,
        "co2_lbs": co2_lbs,
        "money_saved": money_saved,
        "iphones": iphones,
        "ev_miles": ev_miles,
        "led_hours": led_hours,
        "laptops": laptops,
        "gallons_gas": gallons_gas,
        "laundry": laundry,
        "showers": showers,
        "tree_days": tree_days,
        "netflix_hours": netflix_hours,
        "pizzas": pizzas,
    })


# =====================================================
#  NILM (Appliance Detection) API Routes
# =====================================================

@app.route("/api/nilm/events")
def api_nilm_events():
    """Get recent appliance on/off events (last 24 hours)."""
    events = nilm_engine.get_recent_events(DB_PATH, hours=24)
    return jsonify(events)


@app.route("/api/nilm/signatures")
def api_nilm_signatures():
    """Get all known appliance signatures."""
    sigs = nilm_engine.get_all_signatures(DB_PATH)
    return jsonify(sigs)


@app.route("/api/nilm/active")
def api_nilm_active():
    """Get currently active (on) appliances.

    Passes the current inverter load so the engine can cross-check
    and prune stale entries that don't match reality.
    """
    current_load = latest_data.get("load_power")
    active = nilm_engine.get_active_appliances(DB_PATH, current_load=current_load)
    return jsonify(active)


@app.route("/api/nilm/signature/<int:sig_id>", methods=["PUT"])
def api_nilm_update_signature(sig_id):
    """Update a signature's label, icon, or color."""
    data = request.get_json()
    if not data:
        return jsonify({"error": "No data provided"}), 400
    nilm_engine.update_signature_label(
        DB_PATH, sig_id,
        label=data.get("label"),
        icon=data.get("icon"),
        color=data.get("color"),
    )
    return jsonify({"ok": True})


@app.route("/api/nilm/signature/<int:keep_id>/merge", methods=["POST"])
def api_nilm_merge_signature(keep_id):
    """Merge another signature into this one."""
    data = request.get_json()
    if not data or "merge_id" not in data:
        return jsonify({"error": "merge_id required"}), 400
    ok = nilm_engine.merge_signatures(DB_PATH, keep_id, data["merge_id"])
    if ok:
        return jsonify({"ok": True})
    return jsonify({"error": "Signature not found"}), 404


@app.route("/api/nilm/daily/<int:sig_id>")
def api_nilm_daily(sig_id):
    """Get daily usage pattern for one appliance (last 30 days)."""
    stats = nilm_engine.get_daily_stats(DB_PATH, sig_id, days=30)
    return jsonify(stats)


@app.route("/api/nilm/reanalyze", methods=["POST"])
def api_nilm_reanalyze():
    """Re-run edge detection on historical load data.

    Pulls ~10 days of raw load power from Solar Assistant's InfluxDB
    (~87k samples at 10-second intervals) and replays them through
    the edge detector to discover appliance signatures.
    """
    # Try InfluxDB first (much more data), fall back to local samples
    result = nilm_engine.reanalyze_from_influx(DB_PATH, sa_query)
    if result.get("error"):
        # Fallback to local samples if SA not available
        result = nilm_engine.reanalyze(DB_PATH)
    return jsonify(result)


# --- MQTT connection (supports reconnection) ---
def start_mqtt():
    """Connect to MQTT broker using current settings."""
    global _mqtt_client
    broker = get_setting("mqtt_broker", "")
    port = get_setting_int("mqtt_port", 1883)
    user = get_setting("mqtt_user", "")
    passwd = get_setting("mqtt_pass", "")

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    if user:
        client.username_pw_set(user, passwd)
    client.on_connect = on_connect
    client.on_message = on_message
    try:
        client.connect(broker, port)
    except Exception as e:
        print(f"MQTT connect error: {e}")
    _mqtt_client = client
    client.loop_forever()


def reconnect_mqtt():
    """Disconnect old MQTT client and start a new one in a background thread."""
    global _mqtt_client
    if _mqtt_client:
        try:
            _mqtt_client.disconnect()
        except Exception:
            pass
        _mqtt_client = None
    mqtt_thread = threading.Thread(target=start_mqtt, daemon=True)
    mqtt_thread.start()


# =====================================================
#  Settings API
# =====================================================

@app.route("/api/settings", methods=["GET"])
def api_settings_get():
    """Return all settings as JSON."""
    return jsonify(get_all_settings())


@app.route("/api/settings", methods=["POST"])
def api_settings_save():
    """Save settings, reload cache, reconnect MQTT if changed, apply NILM."""
    data = request.get_json()
    if not data:
        return jsonify({"error": "No data provided"}), 400

    # Check if MQTT settings changed (to know if we need to reconnect)
    old_mqtt = (get_setting("mqtt_broker"), get_setting("mqtt_port"),
                get_setting("mqtt_user"), get_setting("mqtt_pass"))

    # Save each setting
    for key, value in data.items():
        if key in _SETTINGS_DEFAULTS:
            set_setting(key, value)

    # Reload cache
    load_settings_cache()

    # Reconnect MQTT if connection settings changed
    new_mqtt = (get_setting("mqtt_broker"), get_setting("mqtt_port"),
                get_setting("mqtt_user"), get_setting("mqtt_pass"))
    if new_mqtt != old_mqtt:
        print("MQTT settings changed — reconnecting...")
        reconnect_mqtt()

    # Push NILM settings to the engine
    apply_nilm_settings()

    # Re-login to SA if host changed
    global _sa_cookie
    _sa_cookie = None

    print("Settings saved successfully")
    return jsonify({"ok": True})


@app.route("/api/setup/complete", methods=["POST"])
def api_setup_complete():
    """Save initial settings from the setup wizard and start services."""
    data = request.get_json()
    if not data:
        return jsonify({"error": "No data provided"}), 400

    # Save each setting from the wizard
    for key, value in data.items():
        if key in _SETTINGS_DEFAULTS:
            set_setting(key, value)

    # Mark setup as done
    set_setting("setup_completed", "true")

    # Reload cache so all threads see the new values
    load_settings_cache()
    apply_nilm_settings()

    # Start MQTT and weather threads now that we have valid settings
    print("Setup complete — starting services...")
    sa_login()
    mqtt_thread = threading.Thread(target=start_mqtt, daemon=True)
    mqtt_thread.start()
    weather_thread = threading.Thread(target=weather_loop, daemon=True)
    weather_thread.start()

    print("Setup wizard finished successfully")
    return jsonify({"ok": True})


@app.route("/api/settings/rate")
def api_settings_rate():
    """Return just the rate and battery capacity for the JS dollar counters."""
    return jsonify({
        "rate": get_setting_float("electricity_rate", 0.30),
        "battery_capacity_wh": get_setting_int("battery_capacity_wh", 0),
    })


# =====================================================
#  Startup
# =====================================================

if __name__ == "__main__":
    init_db()
    load_settings_cache()
    apply_nilm_settings()
    nilm_engine.startup(DB_PATH)

    web_port = get_setting_int("web_port", 5050)

    print("=" * 50)
    print("  Solar Dashboard")
    print(f"  Open http://localhost:{web_port} in your browser")
    print("=" * 50)
    print()

    # Only start MQTT/weather if setup is already done.
    # If not, the setup wizard will start them after the user finishes.
    if get_setting("setup_completed") == "true":
        sa_login()
        mqtt_thread = threading.Thread(target=start_mqtt, daemon=True)
        mqtt_thread.start()
        weather_thread = threading.Thread(target=weather_loop, daemon=True)
        weather_thread.start()
    else:
        print("First-run setup required — open the URL above to get started.")

    socketio.run(app, host="0.0.0.0", port=web_port, allow_unsafe_werkzeug=True)
