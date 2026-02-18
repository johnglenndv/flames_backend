import os
import json
import joblib
import numpy as np
import pandas as pd
import paho.mqtt.client as mqtt
import mysql.connector
import requests  # For API notification
from datetime import datetime
from mysql.connector import Error

# --- 1. LOAD AI ASSETS ---
try:
    model = joblib.load('fire_model.pkl')
    scaler = joblib.load('scaler.pkl')
    class_names = joblib.load('classes.pkl')
    print("AI Model loaded successfully.")
except Exception as e:
    print(f"AI Loading Warning: {e}. Check if .pkl files exist.")

# --- CONFIGURATION (Preserved) ---
BROKER = os.getenv("HIVEMQ_HOST")
PORT   = 8883
USER   = os.getenv("HIVEMQ_USER")
PASS   = os.getenv("HIVEMQ_PASS")
TOPIC  = "lora/uplink"

DB_CONFIG = {
    "host": os.getenv("MYSQLHOST"),
    "port": int(os.getenv("MYSQLPORT", 3306)),
    "user": os.getenv("MYSQLUSER"),
    "password": os.getenv("MYSQLPASSWORD"),
    "database": os.getenv("MYSQLDATABASE"),
}

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("MQTT connected")
        client.subscribe(TOPIC)
    else:
        print(f"MQTT connect failed rc={rc}")

def on_message(client, userdata, msg):
    try:
        raw = msg.payload.decode()
        data = json.loads(raw)
        print("Received:", data)

        wrapper = data
        payload = wrapper.get("payload", {})
        node    = payload.get("node")
        if not node:
            print("No node → skip")
            return

        # 2. Extract Data (All original fields preserved)
        ph_timestamp = wrapper.get("received_at")
        temp  = payload.get("temp")
        hum   = payload.get("hum")
        flame = payload.get("flame", 0)
        smoke = payload.get("smoke", 0)
        lat   = payload.get("lat")
        lon   = payload.get("lon")
        rssi  = wrapper.get("rssi")
        snr   = wrapper.get("snr")

        # 3. AI INFERENCE
        # Creating a DataFrame ensures the scaler gets the correct feature names
        input_data = pd.DataFrame([[smoke, temp, flame, hum]], 
                                 columns=['smoke', 'temperature', 'flame', 'humidity'])
        
        scaled_input = scaler.transform(input_data)
        probs = model.predict_proba(scaled_input)[0]
        prediction = class_names[np.argmax(probs)]
        confidence = float(np.max(probs))

        # 4. DATABASE INSERT
        conn = mysql.connector.connect(**DB_CONFIG)
        cur = conn.cursor()
        sql = """
            INSERT INTO sensor_readings
            (node_id, timestamp, local_timestamp, temperature, humidity, flame, smoke, 
             latitude, longitude, rssi, snr, ai_prediction, confidence)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cur.execute(sql, (
            node, ph_timestamp, ph_timestamp,
            temp, hum, flame, smoke,
            lat, lon, rssi, snr,
            prediction, confidence
        ))
        conn.commit()

        # 5. API NOTIFICATION (Enriched with AI results)
        new_reading = {
            "node_id": node,
            "timestamp": ph_timestamp,
            "temperature": temp,
            "humidity": hum,
            "flame": flame,
            "smoke": smoke,
            "latitude": lat,
            "longitude": lon,
            "rssi": rssi,
            "snr": snr,
            "ai_prediction": prediction,  # NEW: Frontend can now show "FIRE"
            "confidence": f"{confidence*100:.2f}%" # NEW: Frontend shows %
        }

        try:
            requests.post(
                "https://api-production-32ac.up.railway.app/notify-new-data",
                json=new_reading,
                timeout=2
            )
            print(f"Notified WebSocket: {prediction} ({confidence*100:.1f}%)")
        except Exception as e:
            print(f"Notify failed: {e}")
        finally:
            cur.close()
            conn.close()

    except Exception as e:
        print("Error in on_message:", e)

# --- START CLIENT ---
client = mqtt.Client()
client.username_pw_set(USER, PASS)
client.tls_set()
client.on_connect = on_connect
client.on_message = on_message

print("Starting AI Gateway with WebSocket Notification...")
client.connect(BROKER, PORT, 60)
client.loop_forever()
