# Solar Dashboard

A real-time web dashboard for [Solar Assistant](https://solar-assistant.io/) users. Monitor your solar production, battery status, grid usage, weather conditions, and more — all from a single page in your browser.

[![Buy Me a Coffee](https://img.shields.io/badge/Buy%20Me%20a%20Coffee-support-yellow?logo=buymeacoffee)](https://buymeacoffee.com/ginovarisano)

**Free and open source** — if you find it useful, consider [buying me a coffee](https://buymeacoffee.com/ginovarisano)!

## Features

- **Live monitoring** — PV power, load, battery SOC, grid power updated in real time via MQTT
- **At-a-glance gauges** — Color-graduated radial gauges for battery SOC and PV output (red → yellow → green) so you can read system status from across the room
- **Layout modes** — Standard layout for full detail, or **Big Clock mode** that puts a giant clock + weather + power flow front-and-center for wall-mounted tablets
- **Installable PWA** — Add to Home Screen on iOS/Android or "install" on desktop Chrome to run as a standalone app with no browser chrome
- **24-hour power charts** — Interactive charts showing PV, load, and battery power throughout the day
- **Energy history** — Daily, monthly, and yearly production/consumption tracking
- **Weather integration** — Current conditions, forecast, sunshine hours, and solar production estimates (via Open-Meteo, no API key needed)
- **Multi-inverter support** — Automatically detects and aggregates data from multiple inverters in parallel (e.g. 3x EG4-6000XP). No configuration needed — single-inverter setups work identically.
- **Appliance detection (NILM)** — Automatically identifies appliances from your total load signal using edge detection. Learns and remembers signatures across reboots.
- **Fun stats** — All-time solar production with equivalents (CO2 saved, EV miles, iPhone charges, etc.)
- **Customizable display** — Draggable cards, font size adjustments, and 12 color themes (lock/unlock the layout from the gear menu)
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
- **Solar Assistant IP address** — this is the same IP you use to open Solar Assistant in your browser (e.g. `192.168.1.100`). You can find it in your router's device list — it usually shows up as "solar-assistant". This is used for both MQTT and InfluxDB data.
- **MQTT credentials** — only needed if you've set a username/password in Solar Assistant under Settings → MQTT. If you haven't changed this, leave it blank.
- **Your location** — for weather forecasts
- **Your electricity rate** — for savings calculations

After completing the wizard, the dashboard will connect to Solar Assistant and start displaying live data.

### Access from your phone

The dashboard is mobile-friendly. To view it on your phone or tablet, make sure the device is on the same Wi-Fi network as the computer running the dashboard, then open:

```
http://<COMPUTER_IP>:5050
```

Replace `<COMPUTER_IP>` with the local IP address of the computer running the dashboard (e.g. `192.168.1.50`). You can find this in your computer's network settings or router device list.

### Install as an app (PWA)

The dashboard is a Progressive Web App, so you can install it like a native app on phones, tablets, and desktops. After install, it launches in its own window with no browser chrome — ideal for wall-mounted tablets.

- **iOS (Safari):** Tap the Share icon → **Add to Home Screen**
- **Android (Chrome):** Tap the three-dot menu → **Install app** (or **Add to Home Screen**)
- **Desktop (Chrome/Edge):** Click the install icon in the address bar, or three-dot menu → **Install Solar Dashboard**

Each installed instance remembers its own layout and theme via localStorage, so you can give different tablets different presets (e.g. wall tablet on Big Clock, desk tablet on Standard).

### Wall-mount mode (Big Clock)

For tablets mounted on walls or in shared rooms where you want to read system status at a glance:

1. Open the dashboard, click the gear icon, choose **Display → Layout → Big Clock**
2. The layout collapses to: top stats row + giant clock with weather + Solar Forecast + power flow diagram
3. Color-graduated gauges on the battery and PV cards make status readable from 6+ feet away
4. The choice persists per-device via localStorage

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
| Expected Max Solar Output (kW) | Your array's nameplate capacity. Used to scale the radial PV gauge (default: 10kW). Set this to your actual peak — e.g. 16 for a 16kW system — so the gauge reflects "how much of your potential are you producing right now". |
| Battery Capacity (kWh) | Total usable battery capacity. Used for time-to-full / time-to-empty estimates. Set to 0 to auto-detect from observed swings. |

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

For systems with multiple inverters, power fields (`pv_power`, `load_power`, `grid_power`, `load_apparent_power`, `battery_current`) are automatically summed across all `inverter_N/` topics. Non-additive fields like voltage, frequency, and temperature use inverter 1's values. Energy totals come from Solar Assistant's pre-aggregated `total/` topics.

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
  static/
    manifest.json      — PWA manifest (app name, icon, display mode)
    sw.js              — Service worker (offline shell + cache)
    icon.svg           — App icon used by the PWA install prompt
  solar_history.db     — SQLite database (created on first run, not included in repo)
```

## License

MIT
