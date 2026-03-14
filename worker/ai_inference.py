import os, json, joblib, requests
import pandas as pd
import numpy as np
import paho.mqtt.client as mqtt
import mysql.connector
from datetime import datetime

# --- 1. CONFIG & ASSETS ---
try:
    model = joblib.load('fire_model_final.pkl')
    scaler = joblib.load('scaler_final.pkl')
    class_names = joblib.load('classes_final.pkl')
    print("AI Assets Loaded.")
except Exception as e:
    print(f"Loading Error: {e}")

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

# --- 2. DB UTILITY ---
def ensure_gateway_exists(conn, gateway_id):
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM gateways WHERE gateway_id = %s LIMIT 1", (gateway_id,))
    if not cur.fetchone():
        now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        cur.execute(
            "INSERT INTO gateways (gateway_id, location_name, status, created_at) VALUES (%s, %s, %s, %s)",
            (gateway_id, f"Gateway {gateway_id}", "active", now)
        )
        conn.commit()
    cur.close()

# --- 3. MQTT LOGIC ---
def on_message(client, userdata, msg):
    global node_history
    try:
        data = json.loads(msg.payload.decode())
        gateway_id = data.get("gateway")
        payload = data.get("payload", {})
        node = payload.get("node")
        if not node: return

        # Raw Data Extraction
        t, h = payload.get("temp"), payload.get("hum")
        f, s = payload.get("flame", 0), payload.get("smoke", 0)
        lat, lon = payload.get("lat"), payload.get("lon")
        rssi, snr = data.get("rssi"), data.get("snr")
        ts_final = data.get("received_at", datetime.utcnow().isoformat())
        ts_early = data.get("received_at_early", ts_final)

        # ── Manual fire button flag ──
        manual_fire = payload.get("manual_fire", False)
        if manual_fire:
            print(f"  🚨 MANUAL FIRE BUTTON pressed on {node} — bypassing AI")

        # REAL-TIME TEMPORAL ENGINEERING
        prev = node_history.get(node, {'t': t, 's': s, 'f': f, 'h': h, 'manual_fire_active': False})

        t_delta, s_delta = t - prev['t'], s - prev['s']
        f_delta, h_delta = f - prev['f'], h - prev['h']
        t_roll, s_roll = (t + prev['t']) / 2, (s + prev['s']) / 2

        node_history[node] = {
            't': t, 's': s, 'f': f, 'h': h,
            'manual_fire_active': node_history.get(node, {}).get('manual_fire_active', False)
        }

        # AI INFERENCE
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

        # ── MANUAL FIRE OVERRIDE — check FIRST, bypasses AI + safety filter ──
        if manual_fire:
            final_label = "fire"
            confidence  = 1.0
            node_history[node]['manual_fire_active'] = True  # Lock incident open
            print(f"  ✅ Manual fire override applied: label=fire, confidence=1.0")
        else:
            # HOT DAY OVERRIDE (Safety Filter) — only runs if NOT a manual fire
            if final_label.lower() in ["fire", "false"]:
                if abs(t_delta) < 0.2 and s < 30:
                    # Don't resolve if manual fire incident is still active
                    if not node_history.get(node, {}).get('manual_fire_active', False):
                        final_label, confidence = "Normal", 0.98

            # Only clear manual_fire_active lock if sensors confirm sustained all-clear
            if node_history.get(node, {}).get('manual_fire_active', False):
                if final_label.lower() == "normal" and f == 0 and s < 20 and t < 40:
                    node_history[node]['manual_fire_active'] = False
                    print(f"  ✅ Manual fire lock released for {node} — sensors confirm normal")
                else:
                    # Keep fire label active while lock is on
                    final_label = "fire"
                    confidence  = 1.0
                    print(f"  🔒 Manual fire lock still active for {node}")

        # DATABASE INSERT
        conn = mysql.connector.connect(**DB_CONFIG)
        ensure_gateway_exists(conn, gateway_id)
        cur = conn.cursor()
        sql = """
            INSERT INTO sensor_readings
            (gateway_id, node_id, timestamp, local_timestamp, temperature, humidity,
             flame, smoke, latitude, longitude, rssi, snr, ai_prediction, confidence,
             temp_delta, smoke_delta, flame_delta, hum_delta, temp_roll_avg, smoke_roll_avg)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cur.execute(sql, (
            gateway_id, node, ts_final, ts_early, t, h, f, s, lat, lon, rssi, snr,
            final_label, confidence, t_delta, s_delta, f_delta, h_delta, t_roll, s_roll
        ))
        conn.commit()
        cur.close()
        conn.close()

        # DASHBOARD NOTIFY
        requests.post("https://flamesapp.up.railway.app/notify-new-data", json={
            "type":          "new_reading",
            "node_id":       node,
            "ai_prediction": final_label,
            "confidence":    f"{confidence*100:.2f}%",
            "temperature":   t,
            "smoke":         s,
            "flame":         f,
            "rssi":          rssi,
            "temp_delta":    round(t_delta, 2),
            "manual_fire":   manual_fire
        }, timeout=2)

        print(f"  Result: {final_label} ({confidence*100:.1f}%)"
              + (" [MANUAL BUTTON]" if manual_fire else ""))

    except Exception as e:
        print(f"Error: {e}")

# --- 4. START ---
client = mqtt.Client()
client.username_pw_set(os.getenv("HIVEMQ_USER"), os.getenv("HIVEMQ_PASS"))
client.tls_set()
client.on_connect = lambda c, u, f, rc: c.subscribe(TOPIC)
client.on_message = on_message
client.connect(BROKER, 8883, 60)
client.loop_forever()