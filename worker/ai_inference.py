import os
import json
from datetime import datetime
import paho.mqtt.client as mqtt
import mysql.connector
from mysql.connector import Error
import json, requests

import joblib
import numpy as np
import pandas as pd

# --- AI ASSETS LOADING ---
# Ensure these .pkl files are in the same folder as this script on the RPi
try:
    model = joblib.load('fire_model.pkl')
    scaler = joblib.load('scaler.pkl')
    class_names = joblib.load('classes.pkl')
    print("AI Model and Scaler loaded successfully.")
except Exception as e:
    print(f"AI Loading Warning: {e}. (Ensure .pkl files are present)")

# HiveMQ config (from env vars)
BROKER = os.getenv("HIVEMQ_HOST")
PORT   = 8883
USER   = os.getenv("HIVEMQ_USER")
PASS   = os.getenv("HIVEMQ_PASS")
TOPIC  = "lora/uplink"

# MySQL (Railway auto-injected)
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

        # Use the gateway's received_at directly (already in PH time with +08:00)
        ph_timestamp = wrapper.get("received_at")  # e.g. "2026-02-17T11:20:26+08:00"

        # Extract other fields
        temp  = payload.get("temp")
        hum   = payload.get("hum")
        flame = payload.get("flame", 0)
        smoke = payload.get("smoke", 0)
        lat   = payload.get("lat")
        lon   = payload.get("lon")
        rssi  = wrapper.get("rssi")
        snr   = wrapper.get("snr")

        # AI INFERENCE
        input_data = pd.DataFrame([[smoke, temp, flame, hum]], 
                                 columns=['smoke', 'temperature', 'flame', 'humidity'])
        
        scaled_input = scaler.transform(input_data)
        probabilities = model.predict_proba(scaled_input)[0]
        prediction = class_names[np.argmax(probabilities)]
        confidence = float(np.max(probabilities))

        conn = mysql.connector.connect(**DB_CONFIG)
        cur = conn.cursor()
        sql = """
            INSERT INTO sensor_readings
            (node_id, timestamp, local_timestamp, temperature, humidity, flame, smoke, latitude, longitude, rssi, snr, ai_prediction, confidence)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cur.execute(sql, (
            node,
            ph_timestamp,          # original (for reference)
            ph_timestamp,          # store same value in local_timestamp (already PH)
            temp,
            hum,
            flame,
            smoke,
            lat,
            lon,
            rssi,
            snr,
            prediction,        # eg. Fire
            confidence         # eg. 0.98
        ))
        conn.commit()
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
            "ai_prediction": prediction,
            "confidence":  f"{confidence*100:.2f}%"

         # add any other fields you want frontend to receive instantly
        }

        try:
            # Tell API to broadcast to all WebSocket clients
            requests.post(
                "https://api-production-32ac.up.railway.app/notify-new-data",
                json=new_reading,
                timeout=2
            )
            print("Notified WebSocket clients")
        except Exception as e:
            print(f"Notify failed: {e}")
        finally:
            cur.close()
            conn.close()

        print(f"Inserted node {node} with PH timestamp: {ph_timestamp}")

    except Exception as e:
        print("Error:", e)

client = mqtt.Client()
client.username_pw_set(USER, PASS)
client.tls_set()
client.on_connect = on_connect
client.on_message = on_message

print("Starting MQTT to MySQL worker...")
client.connect(BROKER, PORT, 60)
client.loop_forever()