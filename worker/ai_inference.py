
from zoneinfo import ZoneInfo
import os, json, joblib, requests
import pandas as pd
import numpy as np
import paho.mqtt.client as mqtt
import mysql.connector
from datetime import datetime

PH_ZONE = ZoneInfo("Asia/Manila")

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
        payload    = data.get("payload", {})
        node       = payload.get("node")
        if not node: return

        # ── Raw sensor data — always real values from device ──
        t   = payload.get("temp")
        h   = payload.get("hum")
        f   = payload.get("flame", 0)
        s   = payload.get("smoke", 0)
        lat = payload.get("lat")
        lon = payload.get("lon")
        rssi        = data.get("rssi")
        snr         = data.get("snr")
        
        # ts_final = UTC timestamp (for the timestamp column)
        ts_final = data.get("received_at", datetime.utcnow().isoformat())

        # ts_early = PH local time (UTC+8) for the local_timestamp column
        #   Convert UTC received_at to PH time
        try:
            _utc_dt = datetime.fromisoformat(ts_final.replace('Z', '+00:00'))
            _ph_dt  = _utc_dt.astimezone(PH_ZONE)
            ts_early = _ph_dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            # Fallback: use current PH time
            ts_early = datetime.now(PH_ZONE).strftime("%Y-%m-%d %H:%M:%S")

                # ── Manual fire flag — set by physical button on device ──
                #      This flag alone is enough to trigger fire alert.
                # Real sensor values are stored in DB regardless.
        manual_fire = payload.get("manual_fire", False)

        # ── Temporal feature engineering ──
        prev = node_history.get(node, {
            't': t, 's': s, 'f': f, 'h': h,
            'manual_fire_active': False
        })

        t_delta = t - prev['t']
        s_delta = s - prev['s']
        f_delta = f - prev['f']
        h_delta = h - prev['h']
        t_roll  = (t + prev['t']) / 2
        s_roll  = (s + prev['s']) / 2

        # Update history with real values
        node_history[node] = {
            't': t, 's': s, 'f': f, 'h': h,
            'manual_fire_active': prev.get('manual_fire_active', False)
        }

        # ── AI inference on REAL sensor values ──
        features = [s, t, f, h, t_delta, s_delta, f_delta, h_delta, t_roll, s_roll]
        input_df = pd.DataFrame([features], columns=[
            'smoke', 'temperature', 'flame', 'humidity',
            'temp_delta', 'smoke_delta', 'flame_delta', 'hum_delta',
            'temp_roll_avg', 'smoke_roll_avg'
        ])
        scaled_input = scaler.transform(input_df)
        probs        = model.predict_proba(scaled_input)[0]
        ai_label     = class_names[np.argmax(probs)]
        ai_confidence = float(np.max(probs))

        # ── Decision logic ──
        #
        # Priority order:
        # 1. manual_fire=true  → always fire, confidence 1.0, lock node
        # 2. manual_fire=false + lock active → keep fire until sensors clear
        # 3. normal AI result  → use AI with safety filter
        #
        final_label = ai_label
        confidence  = ai_confidence

        if manual_fire:
            # ── CASE 1: Button pressed on device ──
            # Force fire regardless of AI or sensor readings.
            # Real sensor values are still stored in DB for audit.
            final_label = "fire"
            confidence  = 1.0
            node_history[node]['manual_fire_active'] = True
            print(f"  🚨 [{node}] MANUAL FIRE BUTTON — label forced to fire")

        elif prev.get('manual_fire_active', False):
            # ── CASE 2: Button was pressed in a previous reading ──
            # Keep fire label active until sensors confirm all-clear.
            # This prevents the incident from auto-resolving on the
            # next 60s reading just because the button is no longer held.
            sensors_all_clear = (
                ai_label.lower() == "normal" and
                f == 0 and
                s < 20 and
                t < 40
            )
            if sensors_all_clear:
                # Sensors genuinely clear — release the lock
                node_history[node]['manual_fire_active'] = False
                final_label = "Normal"
                confidence  = ai_confidence
                print(f"  ✅ [{node}] Manual fire lock released — sensors confirm normal")
            else:
                # Lock still holds — keep fire label
                final_label = "fire"
                confidence  = 1.0
                print(f"  🔒 [{node}] Manual fire lock active — keeping fire label")

        else:
    # ── CASE 3: Normal AI path ──
    # Apply safety filter only when no manual override involved
            if final_label.lower() in ["fire", "false"]:
                # A real fire needs MULTIPLE sensor agreement — not just flame alone.
                # Flame sensor alone (high f, low smoke, normal temp) is almost always
                # ambient IR / sunlight interference.
                smoke_is_low   = s < 50
                temp_is_normal = t < 40
                temp_stable    = abs(t_delta) < 1.0
                no_smoke_rise  = s_delta < 20
                # Flame-only false positive: high flame reading but no smoke, normal temp
                flame_only_trigger = f > 0 and smoke_is_low and temp_is_normal

                if flame_only_trigger and temp_stable and no_smoke_rise:
                    final_label = "Normal"
                    confidence  = 0.97
                    print(f"  🌡  [{node}] Safety filter: flame-only trigger, no smoke/temp rise → Normal")
                elif abs(t_delta) < 0.2 and s < 30:
                    final_label = "Normal"
                    confidence  = 0.98
                    print(f"  🌡  [{node}] Safety filter: stable temp + low smoke → Normal")

        print(f"  [{node}] AI={ai_label}({ai_confidence*100:.1f}%) "
              f"→ Final={final_label}({confidence*100:.1f}%) "
              f"| T={t} H={h} Flame={f} Smoke={s}"
              f"{' [MANUAL BUTTON]' if manual_fire else ''}")

        # ── Database insert — always stores REAL sensor values ──
        conn = mysql.connector.connect(**DB_CONFIG)
        ensure_gateway_exists(conn, gateway_id)
        cur = conn.cursor()
        sql = """
            INSERT INTO sensor_readings
            (gateway_id, node_id, timestamp, local_timestamp,
             temperature, humidity, flame, smoke,
             latitude, longitude, rssi, snr,
             ai_prediction, confidence,
             temp_delta, smoke_delta, flame_delta, hum_delta,
             temp_roll_avg, smoke_roll_avg)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        cur.execute(sql, (
            gateway_id, node, ts_final, ts_early,
            t, h, f, s,              # real sensor values
            lat, lon, rssi, snr,
            final_label, confidence, # overridden label if manual
            t_delta, s_delta, f_delta, h_delta,
            t_roll, s_roll
        ))
        conn.commit()
        cur.close()
        conn.close()

        # ── Dashboard notify ──
        requests.post(
            "https://flamesapp.up.railway.app/notify-new-data",
            json={
                "type":          "new_reading",
                "node_id":       node,
                "ai_prediction": final_label,
                "confidence":    f"{confidence*100:.2f}%",
                "temperature":   t,
                "humidity":      h,
                "smoke":         s,
                "flame":         f,
                "latitude":      lat,
                "longitude":     lon,
                "rssi":          rssi,
                "temp_delta":    round(t_delta, 2),
                "manual_fire":   manual_fire,
            },
            timeout=2
        )

    except Exception as e:
        print(f"Error in on_message: {e}")

# --- 4. START ---
client = mqtt.Client()
client.username_pw_set(os.getenv("HIVEMQ_USER"), os.getenv("HIVEMQ_PASS"))
client.tls_set()
client.on_connect = lambda c, u, f, rc: c.subscribe(TOPIC)
client.on_message = on_message
client.connect(BROKER, 8883, 60)
client.loop_forever()