# ---------------------------
# 1. Imports & Setup
# ---------------------------
from datetime import datetime
import json
import threading

import firebase_admin
from firebase_admin import credentials, db, firestore
from flask import Flask, jsonify
from flask_cors import CORS
import paho.mqtt.client as mqtt


# ---------------------------
# 2. Firebase Initialization
# ---------------------------
SERVICE_ACCOUNT_FILE = "serviceAccountKey.json"
DATABASE_URL = "https://project-kazu-fd451-default-rtdb.firebaseio.com/"

cred = credentials.Certificate(SERVICE_ACCOUNT_FILE)
firebase_admin.initialize_app(cred, {'databaseURL': DATABASE_URL})

# Firestore client
firestore_db = firestore.client()


# ---------------------------
# 3. MQTT Setup
# ---------------------------
BROKER = "broker.hivemq.com"
PORT = 1883
USERNAME = "esp32_kazu"
PASSWORD = "ESP32_kazu"

mqtt_client = mqtt.Client()
mqtt_client.username_pw_set(USERNAME, PASSWORD)

# Global user devices storage
user_devices = {}  # {user_id: ['device1', 'device2']}


# ---------------------------
# 4. MQTT Callbacks
# ---------------------------
def on_connect(client, userdata, flags, rc):
    print("✅ Connected to MQTT Broker!" if rc == 0 else f"❌ MQTT connection failed: {rc}")


def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode()

        # Try decode JSON
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            data = {'message': payload}

        now = datetime.now()
        formatted_date = now.strftime("%Y%m%d_%H%M%S")

        device_id = msg.topic.split('/')[1]

        # Save alert in Firebase RTDB
        ref = db.reference(f"alert/{device_id}/{formatted_date}")
        ref.set({
            'message': data.get('message', ''),
            'createdAt': now.isoformat()
        })

        # Update notification count
        notif_ref = db.reference("alert/notificationCount")
        current_count = notif_ref.get() or 0
        notif_ref.set(current_count + 1)

        print(f"🔔 Alert saved for {device_id} at {formatted_date}")

    except Exception as e:
        print(f"❌ Error processing message: {e}")


mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message


# ---------------------------
# 5. Helper Functions
# ---------------------------
def subscribe_devices(devices):
    for device in devices:
        topic = f"pets_live/{device}/alert"
        mqtt_client.subscribe(topic, qos=0)
        print(f"🔔 Subscribed to {topic}")


def fetch_devices_for_user(user_id):
    try:
        print(f"Fetching devices for user {user_id}")
        doc_ref = firestore_db.collection('users').document(user_id)
        doc = doc_ref.get()

        if not doc.exists:
            return []

        data = doc.to_dict()

        if isinstance(data.get('devices'), list):
            return data['devices']

        elif isinstance(data.get('devices'), dict):
            return list(data['devices'].keys())

        return []

    except Exception as e:
        print(f"❌ Error fetching devices for user {user_id}: {e}")
        return []


# ---------------------------
# 6. Flask API
# ---------------------------

app = Flask(__name__)
CORS(app)

# Health check route
@app.route('/', methods=['GET'])
def home():
    return jsonify({
        "message": "🐾 Kazu server running!",
        "service": "MQTT + Firebase Bridge",
        "status": "ok"
    }), 200


# Get user's devices and subscribe to topics
@app.route('/user/<user_id>', methods=['GET'])
def get_user_devices(user_id):
    devices = fetch_devices_for_user(user_id)

    user_devices[user_id] = devices
    subscribe_devices(devices)

    return jsonify({'devices': devices}), 200


# ---------------------------
# 7. Run MQTT in Background
# ---------------------------
def start_mqtt():
    mqtt_client.connect(BROKER, PORT, 60)
    mqtt_client.loop_forever()


mqtt_thread = threading.Thread(target=start_mqtt)
mqtt_thread.daemon = True
mqtt_thread.start()


# ---------------------------
# 8. Run Flask Server
# ---------------------------
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
