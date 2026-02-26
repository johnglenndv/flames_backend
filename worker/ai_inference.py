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
        
def ensure_gateway_exists(conn, gateway_id: str):
    cur = conn.cursor()
    try:
        cur.execute("SELECT 1 FROM gateways WHERE gateway_id = %s LIMIT 1", (gateway_id,))
        if cur.fetchone():
            return  # already exists

        now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        cur.execute("""
            INSERT INTO gateways 
            (gateway_id, location_name, status, created_at)
            -- org_id is intentionally omitted → becomes NULL
            VALUES (%s, %s, %s, %s)
        """, (
            gateway_id,
            f"Gateway {gateway_id} — location & org pending",
            "maintenance",           # or 'offline' / 'active' — whatever you prefer as default
            now
        ))
        conn.commit()
        print(f"→ Auto-created gateway: {gateway_id} (org_id = NULL)")
    except mysql.connector.Error as err:
        print(f"Error creating gateway {gateway_id}: {err}")
        conn.rollback()
    finally:
        cur.close()

def on_message(client, userdata, msg):
    try:
        raw = msg.payload.decode()
        data = json.loads(raw)
        print("Received:", data)

        wrapper = data
        gateway_id = wrapper.get("gateway")
        payload    = wrapper.get("payload", {})
        node       = payload.get("node")

        if not gateway_id:
            print("Missing gateway id → skipping message")
            return
        if not node:
            print("Missing node id → skipping message")
            return

        # Extract sensor fields
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

        # --- DATABASE OPERATIONS ---
        conn = mysql.connector.connect(**DB_CONFIG)
        try:
            # 1. Ensure gateway exists
            ensure_gateway_exists(conn, gateway_id)

            # 2. Insert sensor reading (now includes gateway_id)
            cur = conn.cursor()
            sql = """
                INSERT INTO sensor_readings
                (gateway_id, node_id, timestamp, local_timestamp, temperature, humidity, flame, smoke, 
                 latitude, longitude, rssi, snr, ai_prediction, confidence)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cur.execute(sql, (
                gateway_id,
                node,
                ph_timestamp,
                ph_timestamp,
                temp, hum, flame, smoke,
                lat, lon, rssi, snr,
                final_label,
                confidence
            ))
            conn.commit()

            # ── Decide whether to broadcast incident_update ──
            should_broadcast_incident = False
            final_label_lower = str(final_label).lower().strip()

            if final_label_lower == "fire" and confidence >= 0.50:
                should_broadcast_incident = True
                print(f"DEBUG: Fire detected → broadcast incident_update (conf {confidence:.2f})")
            elif final_label_lower in ("normal", "false", "no fire", "safe", "none") and confidence >= 0.70:
                should_broadcast_incident = True
                print(f"DEBUG: High-conf normal/safe → broadcast resolution (conf {confidence:.2f})")
            else:
                print(f"DEBUG: No broadcast needed (label='{final_label_lower}', conf={confidence:.2f})")

            if should_broadcast_incident:
                incident_update = {
                    "type": "incident_update",
                    "node_id": node,
                    "ai_prediction": final_label,
                    "confidence": confidence,
                    "timestamp": ph_timestamp,
                    "latitude": lat,
                    "longitude": lon,
                    "gateway_id": gateway_id   # ← optional: added for better context
                }
                try:
                    requests.post(
                        "https://flamesapp.up.railway.app/notify-new-data",
                        json=incident_update,
                        timeout=2
                    )
                    print(f"Broadcasted incident_update for {node} ({final_label}, conf {confidence:.2f})")
                except Exception as e:
                    print(f"Incident notify failed: {e}")

            # --- Basic reading notification ---
            new_reading = {
                "node_id": node,
                "gateway_id": gateway_id,          # ← added
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

            print(f"Inserted → gateway={gateway_id} node={node} → {final_label} ({confidence*100:.1f}%)")

        except mysql.connector.Error as db_err:
            print(f"Database error: {db_err}")
            conn.rollback()
        finally:
            if 'cur' in locals():
                cur.close()
            conn.close()

    except Exception as e:
        print("General error in on_message:", e)

client = mqtt.Client()
client.username_pw_set(USER, PASS)
client.tls_set()
client.on_connect = on_connect
client.on_message = on_message

print("Starting AI-Enhanced MQTT to MySQL worker...")
client.connect(BROKER, PORT, 60)
client.loop_forever()