"""
Solar Assistant MQTT Listener
Connects to your Solar Assistant and prints live solar data.

MQTT is like a radio station - Solar Assistant "broadcasts" your solar data
on different channels (called "topics"), and this script "tunes in" to listen.

Usage:
    python3 solar_listener.py <broker_ip> [username] [password]

Examples:
    python3 solar_listener.py 192.168.1.100
    python3 solar_listener.py 192.168.1.100 myuser mypass
"""

import sys
import paho.mqtt.client as mqtt  # The library that lets us connect to MQTT

# Force Python to print immediately (not buffer output)
sys.stdout.reconfigure(line_buffering=True)

# --- Parse command-line arguments ---
if len(sys.argv) < 2:
    print("Usage: python3 solar_listener.py <broker_ip> [username] [password]")
    print("  broker_ip  — IP address of your Solar Assistant (e.g. 192.168.1.100)")
    print("  username   — MQTT username (optional)")
    print("  password   — MQTT password (optional)")
    sys.exit(1)

BROKER_IP = sys.argv[1]
BROKER_PORT = 1883  # Default MQTT port
USERNAME = sys.argv[2] if len(sys.argv) > 2 else None
PASSWORD = sys.argv[3] if len(sys.argv) > 3 else None

# --- What happens when we successfully connect ---
def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        print("Connected to Solar Assistant!")
        print("Waiting for data... (this may take a few seconds)\n")
        # Subscribe to ALL Solar Assistant topics (channels)
        # The "#" means "everything" — like tuning into all radio stations at once
        client.subscribe("solar_assistant/#")
    else:
        print(f"Connection failed with code: {reason_code}")
        print("Check your username, password, and IP address.")

# --- What happens when we receive a message ---
def on_message(client, userdata, message):
    # message.topic = the "channel" name (e.g. "solar_assistant/battery/state_of_charge")
    # message.payload = the actual value (e.g. "85")
    topic = message.topic
    value = message.payload.decode("utf-8")  # Convert from bytes to readable text

    # Make the topic easier to read by removing the prefix
    short_topic = topic.replace("solar_assistant/", "")

    print(f"  {short_topic:50s} = {value}")

# --- Main program ---
print("=" * 60)
print("  Solar Assistant Live Data Listener")
print("=" * 60)
print(f"Connecting to {BROKER_IP}...")
print()

# Create the MQTT client (our "radio receiver")
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
if USERNAME:
    client.username_pw_set(USERNAME, PASSWORD or "")

# Tell it what to do when events happen
client.on_connect = on_connect
client.on_message = on_message

# Try to connect
try:
    client.connect(BROKER_IP, BROKER_PORT)
    # This runs forever, listening for messages. Press Ctrl+C to stop.
    client.loop_forever()
except KeyboardInterrupt:
    print("\n\nStopped listening. Goodbye!")
    client.disconnect()
except ConnectionRefusedError:
    print("Could not connect! Make sure:")
    print("  1. Solar Assistant is running on your Raspberry Pi")
    print("  2. MQTT is enabled in Solar Assistant settings")
    print(f"  3. The IP address {BROKER_IP} is correct")
except Exception as e:
    print(f"Error: {e}")
