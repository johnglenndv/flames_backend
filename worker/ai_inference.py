import os, json, joblib, requests
import pandas as pd
import numpy as np
import paho.mqtt.client as mqtt
import mysql.connector
from datetime import datetime

# --- 1. LOAD AI ASSETS ---
try:
    # Using the 'final' filenames from your new training script
    model = joblib.load('fire_model_final.pkl')
    scaler = joblib.load('scaler_final.pkl')
    class_names = joblib.load('classes_final.pkl')
    print("AI Assets (Temporal V4) Loaded.")
except Exception as e:
    print(f"Loading Error: {e}")

# --- 2. MEMORY & CONFIG ---
node_history = {} 
BROKER = os.getenv("HIVEMQ_HOST")
TOPIC = "lora/uplink"

DB_CONFIG = {
    "host": os.getenv("MYSQLHOST"),
    "port": int(os.getenv("MYSQLPORT", 3306)),
    "user": os.getenv("MYSQLUSER"),
    "password": os.getenv("MYSQLPASSWORD"),
    "database": os.getenv("MYSQLDATABASE"),
}

# --- 3. DATABASE UTILITIES ---
def ensure_gateway_exists(conn, gateway_id):
    cur = conn.cursor()
    try:
        cur.execute("SELECT 1 FROM gateways WHERE gateway_id = %s LIMIT 1", (gateway_id,))
        if not cur.fetchone():
            now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            cur.execute("""
                INSERT INTO gateways (gateway_id, location_name, status, created_at) 
                VALUES (%s, %s, %s, %s)
            """, (gateway_id, f"Gateway {gateway_id}", "active", now))
            conn.commit()
            print(f"Auto-created gateway: {gateway_id}")
    finally:
        cur.close()

# --- 4. MAIN INFERENCE LOGIC ---
def on_message(client, userdata, msg):
    global node_history
    try:
        data = json.loads(msg.payload.decode())
        gateway_id = data.get("gateway")
        payload = data.get("payload", {})
        node = payload.get("node")

        if not gateway_id or not node: return

        # 1. Extract All Vital Data (Restored)
        t, h = payload.get("temp"), payload.get("hum")
        f, s = payload.get("flame", 0), payload.get("smoke", 0)
        lat, lon = payload.get("lat"), payload.get("lon")
        rssi, snr = data.get("rssi"), data.get("snr")
        
        # Timestamps
        received_at_final = data.get("received_at", datetime.utcnow().isoformat())
        received_at_early = data.get("received_at_early", received_at_final)

        # 2. Temporal Feature Engineering
        prev = node_history.get(node, {'temp': t, 'smoke': s, 'flame': f, 'hum': h})
        t_delta, s_delta = t - prev['temp'], s - prev['smoke']
        f_delta, h_delta = f - prev['flame'], h - prev['hum']
        t_roll, s_roll = (t + prev['temp']) / 2, (s + prev['smoke']) / 2
        
        # Update Memory for next message
        node_history[node] = {'temp': t, 'smoke': s, 'flame': f, 'hum': h}

        # 3. AI Calculation (10 Features)
        features = [s, t, f, h, t_delta, s_delta, f_delta, h_delta, t_roll, s_roll]
        input_df = pd.DataFrame([features], columns=[
            'smoke', 'temperature', 'flame', 'humidity', 
            'temp_delta', 'smoke_delta', 'flame_delta', 'hum_delta', 
            'temp_roll_avg', 'smoke_roll_avg'
        ])
        
        scaled_input = scaler.transform(input_df)
        probs = model.predict_proba(scaled_input)[0]
        final_label = class_names[np.argmax(probs)]
        confidence = float(np.max(probs))

        # Stability Override (Hot Day Fix)
        if final_label.lower() == "possible" and abs(t_delta) < 0.1 and s < 50:
            final_label, confidence = "Normal", 0.99

        # 4. Database Logging (All columns restored)
        conn = mysql.connector.connect(**DB_CONFIG)
        ensure_gateway_exists(conn, gateway_id)
        cur = conn.cursor()
        sql = """
            INSERT INTO sensor_readings 
            (gateway_id, node_id, timestamp, local_timestamp, temperature, humidity, 
             flame, smoke, latitude, longitude, rssi, snr, ai_prediction, confidence, 
             temp_delta, smoke_delta) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cur.execute(sql, (
            gateway_id, node, received_at_final, received_at_early, 
            t, h, f, s, lat, lon, rssi, snr, 
            final_label, confidence, t_delta, s_delta
        ))
        conn.commit()
        cur.close()
        conn.close()

        # 5. Dashboard Notification
        requests.post("https://flamesapp.up.railway.app/notify-new-data", json={
            "type": "new_reading",
            "node_id": node,
            "gateway_id": gateway_id,
            "ai_prediction": final_label,
            "confidence": f"{confidence*100:.2f}%",
            "temperature": t,
            "humidity": h,
            "smoke": s,
            "flame": f,
            "latitude": lat,
            "longitude": lon,
            "rssi": rssi,
            "snr": snr,
            "temp_delta": round(t_delta, 2)
        }, timeout=2)

        print(f"Processed: Node {node} -> {final_label}")

    except Exception as e:
        print(f"Error in on_message: {e}")

# --- 5. MQTT START ---
client = mqtt.Client()
client.username_pw_set(os.getenv("HIVEMQ_USER"), os.getenv("HIVEMQ_PASS"))
client.tls_set()
client.on_connect = lambda c, u, f, rc: c.subscribe(TOPIC) if rc==0 else print(f"Failed: {rc}")
client.on_message = on_message
client.connect(BROKER, 8883, 60)
client.loop_forever()