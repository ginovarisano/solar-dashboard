# Solar Dashboard

A real-time web dashboard for [Solar Assistant](https://solar-assistant.io/) users. Monitor your solar production, battery status, grid usage, weather conditions, and more — all from a single page in your browser.

[![Buy Me a Coffee](https://img.shields.io/badge/Buy%20Me%20a%20Coffee-support-yellow?logo=buymeacoffee)](https://buymeacoffee.com/ginovarisano)

**Free and open source** — if you find it useful, consider [buying me a coffee](https://buymeacoffee.com/ginovarisano)!

## Features

- **Live monitoring** — PV power, load, battery SOC, grid power updated in real time via MQTT
- **24-hour power charts** — Interactive charts showing PV, load, and battery power throughout the day
- **Energy history** — Daily, monthly, and yearly production/consumption tracking
- **Weather integration** — Current conditions, forecast, sunshine hours, and solar production estimates (via Open-Meteo, no API key needed)
- **Appliance detection (NILM)** — Automatically identifies appliances from your total load signal using edge detection. Learns and remembers signatures across reboots.
- **Fun stats** — All-time solar production with equivalents (CO2 saved, EV miles, iPhone charges, etc.)
- **Setup wizard** — First-run wizard walks you through configuration — no file editing needed

## Requirements

- **Solar Assistant** running on your network with MQTT enabled
- **Python 3.9+**

## Quick Start

1. **Clone or download** this repository:
   ```bash
   git clone https://github.com/ginovarisano/solar-dashboard.git
   cd solar-dashboard
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Run the dashboard:**
   ```bash
   python3 app.py
   ```

4. **Open your browser** to `http://localhost:5050`

On first run, a **setup wizard** will appear asking for:
- Your Solar Assistant IP address (for MQTT and InfluxDB data)
- MQTT credentials (if you've set them in Solar Assistant)
- Your location (for weather forecasts)
- Your electricity rate (for savings calculations)

After completing the wizard, the dashboard will connect to Solar Assistant and start displaying live data.

## Configuration

All settings are stored in a local SQLite database (`solar_history.db`, created automatically on first run). You can change settings at any time by clicking the gear icon in the dashboard or visiting `http://localhost:5050/setup`.

### Key Settings

| Setting | Description |
|---|---|
| MQTT Broker IP | Your Solar Assistant device's IP address |
| MQTT Port | Default: 1883 |
| MQTT User/Pass | Only needed if you've set MQTT authentication in Solar Assistant |
| SA Host | Same as MQTT broker IP (used for pulling historical data from Solar Assistant's InfluxDB) |
| Location | Latitude/longitude for weather forecasts |
| Timezone | Your local timezone (e.g. `America/New_York`) |
| Electricity Rate | Your $/kWh rate for savings calculations |
| Web Port | Dashboard port (default: 5050) |

### NILM (Appliance Detection) Settings

The dashboard includes built-in appliance detection that identifies devices from your total load signal. These settings are tunable from the settings page:

| Setting | Default | Description |
|---|---|---|
| Edge Threshold | 15W | Minimum power change to detect an on/off event |
| Debounce | 8s | Cooldown between events to avoid false triggers |
| Signature Tolerance | 25% | How closely a power change must match a known signature |
| Smoothing Window | 3 | Number of samples to average for noise reduction |
| Inverter Idle Load | 70W | Your inverter's baseline power draw (subtracted from detection) |

## MQTT Topics

The dashboard subscribes to `solar_assistant/#` and maps standard Solar Assistant topics automatically. It works with the default topic structure that Solar Assistant publishes — no extra configuration needed.

## Debug Tool

A standalone MQTT listener is included for troubleshooting:

```bash
# Listen to all Solar Assistant MQTT messages
python3 solar_listener.py 192.168.1.100

# With authentication
python3 solar_listener.py 192.168.1.100 username password
```

## Project Structure

```
solar-dashboard/
  app.py               — Main Flask server (web routes, MQTT, weather, settings)
  nilm_engine.py       — Appliance detection engine (edge detection, signature matching)
  solar_listener.py    — Standalone MQTT debug listener
  requirements.txt     — Python dependencies
  templates/
    dashboard.html     — Main dashboard UI
    setup.html         — First-run setup wizard
  solar_history.db     — SQLite database (created on first run, not included in repo)
```

## License

MIT
