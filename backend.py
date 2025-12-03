import json
import paho.mqtt.client as mqtt
import firebase_admin
from firebase_admin import credentials, db
from datetime import datetime
import os

# ---------------------------
# Firebase Setup
# ---------------------------

# Make sure you download your service account JSON
SERVICE_ACCOUNT_FILE = "path/to/serviceAccountKey.json"  # <-- Replace with your file
DATABASE_URL = "https://project-kazu-fd451-default-rtdb.firebaseio.com/"  # <-- Replace with your Firebase Realtime DB URL

if not os.path.exists(SERVICE_ACCOUNT_FILE):
    raise FileNotFoundError(f"Firebase service account file not found: {SERVICE_ACCOUNT_FILE}")

cred = credentials.Certificate(SERVICE_ACCOUNT_FILE)
firebase_admin.initialize_app(cred, {
    'databaseURL': DATABASE_URL
})

# ---------------------------
# MQTT Broker Settings
# ---------------------------
BROKER = "broker.hivemq.com"
PORT = 1883
USERNAME = "esp32_kazu"  # Replace if your broker needs auth
PASSWORD = "ESP32_kazu"

# List of devices to subscribe
devices = ['device1234']  # Add more devices as needed

# ---------------------------
# MQTT Callbacks
# ---------------------------
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("✅ Connected to MQTT Broker!")
        subscribe_devices(client, devices)
    else:
        print(f"❌ Failed to connect, return code {rc}")

def subscribe_devices(client, devices):
    for device in devices:
        topic = f"pets_live/{device}/alert"
        client.subscribe(topic, qos=0)
        print(f"🔔 Subscribed to {topic}")

def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode()
        print(f"Received message from {msg.topic}: {payload}")

        # Convert payload to dict
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            data = {'message': payload}

        # Timestamp-based key
        now = datetime.now()
        formatted_date = now.strftime("%Y%m%d_%H%M%S")

        # Extract deviceId from topic
        topic_parts = msg.topic.split('/')
        device_id = topic_parts[1] if len(topic_parts) > 1 else "unknown_device"

        # Push alert to Firebase under timestamp key
        alert_ref = db.reference(f"alert/{device_id}/{formatted_date}")
        alert_ref.set({
            'message': data.get('message', ''),
            'createdAt': now.isoformat()
        })

        # Update notification count for this device
        notif_ref = db.reference(f"alert/notificationCount")
        current_count = notif_ref.get() or 0
        notif_ref.set(current_count + 1)

        print(f"✅ Alert saved for {device_id} at {formatted_date}")
    except Exception as e:
        print(f"❌ Error processing message: {e}")

# ---------------------------
# Connect to MQTT Broker
# ---------------------------
client = mqtt.Client()
client.username_pw_set(USERNAME, PASSWORD)
client.on_connect = on_connect
client.on_message = on_message

client.connect(BROKER, PORT, 60)
client.loop_forever()
