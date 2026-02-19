import os
import json
import joblib
import pandas as pd
import numpy as np
import paho.mqtt.client as mqtt
import mysql.connector
import requests
from datetime import datetime
from mysql.connector import Error

# --- 1. LOAD AI ASSETS ---
try:
    model = joblib.load('fire_model.pkl')
    scaler = joblib.load('scaler.pkl')
    class_names = joblib.load('classes.pkl')
    print("AI Model and Scaler loaded successfully.")
except Exception as e:
    print(f"AI Loading Error: {e}")

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
            print("No node -> skip")
            return

        # Extract fields
        ph_timestamp = wrapper.get("received_at")
        temp  = payload.get("temp")
        hum   = payload.get("hum")
        flame = payload.get("flame", 0)
        smoke = payload.get("smoke", 0)
        lat   = payload.get("lat")
        lon   = payload.get("lon")
        rssi  = wrapper.get("rssi")
        snr   = wrapper.get("snr")

        # --- AI INFERENCE ---
        input_df = pd.DataFrame([[smoke, temp, flame, hum]], 
                                columns=['smoke', 'temperature', 'flame', 'humidity'])
        
        scaled_input = scaler.transform(input_df)
        probabilities = model.predict_proba(scaled_input)[0]
        prediction_index = np.argmax(probabilities)
        
        final_label = class_names[prediction_index]
        confidence = float(probabilities[prediction_index])

        # --- DATABASE INSERT ---
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
            final_label, confidence
        ))
        conn.commit()

        # ── NEW: Decide whether to broadcast incident_update ────────────────────────────────
        should_broadcast_incident = False
        final_label_lower = str(final_label).lower().strip()

        if final_label_lower == "fire" and confidence >= 0.50:
            should_broadcast_incident = True
            print(f"DEBUG: Fire detected → will broadcast incident_update (conf {confidence:.2f})")
        elif final_label_lower in ("normal", "false", "no fire", "safe", "none") and confidence >= 0.70:
            should_broadcast_incident = True
            print(f"DEBUG: High-conf normal/safe → will broadcast possible resolution (conf {confidence:.2f})")
        else:
            print(f"DEBUG: No incident broadcast needed (label='{final_label_lower}', conf={confidence:.2f})")

        if should_broadcast_incident:
            incident_update = {
                "type": "incident_update",
                "node_id": node,
                "ai_prediction": final_label,
                "confidence": confidence,
                "timestamp": ph_timestamp,
                "latitude": lat,
                "longitude": lon
            }

            try:
                requests.post(
                    "https://flamesapp.up.railway.app/notify-new-data",
                    json=incident_update,
                    timeout=2
                )
                print(f"Broadcasted incident_update for node {node} ({final_label}, conf {confidence:.2f})")
            except Exception as e:
                print(f"Incident WS notify failed: {e}")

        # --- WEBHOOK NOTIFICATION (original basic reading broadcast) ---
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
            "ai_prediction": final_label,
            "confidence": f"{confidence * 100:.2f}%"
        }

        try:
            requests.post(
                "https://flamesapp.up.railway.app/notify-new-data",
                json=new_reading,
                timeout=2
            )
            print("Notified WebSocket clients (basic reading)")
        except Exception as e:
            print(f"Notify failed: {e}")

        finally:
            cur.close()
            conn.close()

        print(f"Inserted node {node}. Result: {final_label} ({confidence*100:.1f}%)")

    except Exception as e:
        print("Error:", e)

client = mqtt.Client()
client.username_pw_set(USER, PASS)
client.tls_set()
client.on_connect = on_connect
client.on_message = on_message

print("Starting AI-Enhanced MQTT to MySQL worker...")
client.connect(BROKER, PORT, 60)
client.loop_forever()