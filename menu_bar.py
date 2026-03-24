"""
Solar Dashboard — macOS Menu Bar Indicator
Shows a green or red sun icon in the menu bar depending on
whether the solar dashboard (port 5050) is running.
"""

import os
import signal
import socket
import subprocess
import webbrowser

import rumps

# --- Configuration ---
DASHBOARD_PORT = 5050
CHECK_INTERVAL = 7          # seconds between status checks
DASHBOARD_URL = f"http://localhost:{DASHBOARD_PORT}"
APP_SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "app.py")
VENV_PYTHON = os.path.join(os.path.dirname(os.path.abspath(__file__)), "venv", "bin", "python3")

ICON_RUNNING = "\u2600\ufe0f"    # sun (dashboard is ON)
ICON_STOPPED = "\U0001f311"      # new moon / dark circle (dashboard is OFF)


def is_dashboard_running():
    """Try connecting to the dashboard's port. Returns True if something is listening."""
    try:
        with socket.create_connection(("127.0.0.1", DASHBOARD_PORT), timeout=2):
            return True
    except (ConnectionRefusedError, OSError):
        return False


def find_dashboard_pid():
    """Find the PID of a running app.py process (if any)."""
    try:
        result = subprocess.run(
            ["pgrep", "-f", f"python.*{os.path.basename(APP_SCRIPT)}"],
            capture_output=True, text=True
        )
        pids = result.stdout.strip().split("\n")
        # Return the first valid PID (exclude our own process)
        for pid_str in pids:
            if pid_str and int(pid_str) != os.getpid():
                return int(pid_str)
    except Exception:
        pass
    return None


class SolarDashboardMenuBar(rumps.App):
    def __init__(self):
        super().__init__(ICON_STOPPED)

        # Menu items
        self.status_item = rumps.MenuItem("Dashboard: Checking...")
        self.status_item.set_callback(None)  # not clickable, info-only

        self.menu = [
            self.status_item,
            None,                                   # separator
            rumps.MenuItem("Open Dashboard", callback=self.open_dashboard),
            None,                                   # separator
            rumps.MenuItem("Start Dashboard", callback=self.start_dashboard),
            rumps.MenuItem("Stop Dashboard", callback=self.stop_dashboard),
        ]

        # Kick off the repeating timer
        self.timer = rumps.Timer(self.check_status, CHECK_INTERVAL)
        self.timer.start()

        # Also check immediately on launch
        self.check_status(None)

    # --- Periodic status check ---
    def check_status(self, _sender):
        running = is_dashboard_running()
        self.title = ICON_RUNNING if running else ICON_STOPPED
        self.status_item.title = "Dashboard: Running" if running else "Dashboard: Stopped"

    # --- Menu actions ---
    def open_dashboard(self, _sender):
        webbrowser.open(DASHBOARD_URL)

    def start_dashboard(self, _sender):
        if is_dashboard_running():
            rumps.notification("Solar Dashboard", "", "Dashboard is already running.")
            return

        try:
            # Launch the dashboard in the background using the venv Python
            subprocess.Popen(
                [VENV_PYTHON, APP_SCRIPT],
                cwd=os.path.dirname(APP_SCRIPT),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            rumps.notification("Solar Dashboard", "", "Dashboard is starting...")
            # Refresh status after a short delay
            rumps.Timer(self.check_status, 3).start()
        except Exception as e:
            rumps.notification("Solar Dashboard", "Error", str(e))

    def stop_dashboard(self, _sender):
        pid = find_dashboard_pid()
        if pid is None:
            rumps.notification("Solar Dashboard", "", "No running dashboard found.")
            return

        try:
            # Send SIGINT (Ctrl+C) for graceful shutdown
            os.kill(pid, signal.SIGINT)
            rumps.notification("Solar Dashboard", "", "Stopping dashboard (sent Ctrl+C)...")
        except ProcessLookupError:
            rumps.notification("Solar Dashboard", "", "Dashboard process already exited.")
        except Exception as e:
            rumps.notification("Solar Dashboard", "Error", str(e))


if __name__ == "__main__":
    SolarDashboardMenuBar().run()
