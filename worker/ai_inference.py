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

        t   = payload.get("temp")
        h   = payload.get("hum")
        f   = payload.get("flame", 0)
        s   = payload.get("smoke", 0)
        lat = payload.get("lat")
        lon = payload.get("lon")
        rssi        = data.get("rssi")
        snr         = data.get("snr")
        manual_fire = payload.get("manual_fire", False)

        ts_final = data.get("received_at", datetime.utcnow().isoformat())

        try:
            _utc_dt = datetime.fromisoformat(ts_final.replace('Z', '+00:00'))
            _ph_dt  = _utc_dt.astimezone(PH_ZONE)
            ts_early = _ph_dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            ts_early = datetime.now(PH_ZONE).strftime("%Y-%m-%d %H:%M:%S")

        # Temporal feature engineering
        prev = node_history.get(node, {
            't': t, 's': s, 'f': f, 'h': h,
            'manual_fire_active': False,
            'dashboard_blocked': False,
        })

        t_delta = t - prev['t']
        s_delta = s - prev['s']
        f_delta = f - prev['f']
        h_delta = h - prev['h']
        t_roll  = (t + prev['t']) / 2
        s_roll  = (s + prev['s']) / 2

        # AI inference
        features = [s, t, f, h, t_delta, s_delta, f_delta, h_delta, t_roll, s_roll]
        input_df = pd.DataFrame([features], columns=[
            'smoke', 'temperature', 'flame', 'humidity',
            'temp_delta', 'smoke_delta', 'flame_delta', 'hum_delta',
            'temp_roll_avg', 'smoke_roll_avg'
        ])
        scaled_input  = scaler.transform(input_df)
        probs         = model.predict_proba(scaled_input)[0]
        ai_label      = class_names[np.argmax(probs)]
        ai_confidence = float(np.max(probs))

        # DECISION LOGIC & SOURCE TRACKING
        final_label    = ai_label
        confidence     = ai_confidence
        trigger_source = "ai"

        if manual_fire:
            # Kung ang previous na state ay hindi active (button was off before this press),
            # ito ay isang BAGONG press — valid agad, kahit may dashboard_resolved pa sa DB.
            was_previously_active = prev.get('manual_fire_active', False)
            was_blocked           = prev.get('dashboard_blocked', False)

            if not was_previously_active:
                # FRESH press — bagong emergency, i-clear ang dashboard_resolved at mag-fire agad
                _conn_clr = mysql.connector.connect(**DB_CONFIG)
                _cur_clr  = _conn_clr.cursor()
                _cur_clr.execute("""
                    UPDATE fire_incidents
                    SET dashboard_resolved = 0
                    WHERE node_id = %s AND status = 'resolved' AND dashboard_resolved = 1
                """, (node,))
                _conn_clr.commit()
                _cur_clr.close()
                _conn_clr.close()

                final_label    = "fire"
                confidence     = 1.0
                trigger_source = "manual"
                node_history[node] = {
                    't': t, 's': s, 'f': f, 'h': h,
                    'manual_fire_active': True,
                    'dashboard_blocked': False,
                }
                print(f"  [{node}] FRESH MANUAL FIRE PRESS — new incident")

            elif was_blocked:
                # Button hawak pa pero naka-block pa rin (dashboard resolved, hindi pa nire-release)
                trigger_source = "ai"
                node_history[node] = {
                    't': t, 's': s, 'f': f, 'h': h,
                    'manual_fire_active': False,
                    'dashboard_blocked': True,
                }
                print(f"  [{node}] manual_fire BLOCKED — dashboard already resolved this incident")

            else:
                # Button hawak pa, active pa, walang block — normal continuation
                final_label    = "fire"
                confidence     = 1.0
                trigger_source = "manual"
                node_history[node] = {
                    't': t, 's': s, 'f': f, 'h': h,
                    'manual_fire_active': True,
                    'dashboard_blocked': False,
                }
                print(f"  [{node}] MANUAL FIRE BUTTON ACTIVE (continuing)")

        else:
            # manual_fire = False — button cancelled or not pressed
            # Clear dashboard_resolved in DB para re-armed na
            _conn_clr = mysql.connector.connect(**DB_CONFIG)
            _cur_clr  = _conn_clr.cursor()
            _cur_clr.execute("""
                UPDATE fire_incidents
                SET dashboard_resolved = 0
                WHERE node_id = %s AND status = 'resolved' AND dashboard_resolved = 1
            """, (node,))
            _conn_clr.commit()
            _cur_clr.close()
            _conn_clr.close()

            if prev.get('manual_fire_active', False):
                # Coming from active manual fire
                sensors_all_clear = (ai_label.lower() == "normal" and s < 20 and t < 40)
                if sensors_all_clear:
                    node_history[node] = {
                        't': t, 's': s, 'f': f, 'h': h,
                        'manual_fire_active': False,
                        'dashboard_blocked': False,
                    }
                    trigger_source = "ai"
                    print(f"  [{node}] Manual fire cancelled — button re-armed")
                else:
                    final_label    = "fire"
                    confidence     = 1.0
                    trigger_source = "manual_lock"
                    node_history[node].update({
                        't': t, 's': s, 'f': f, 'h': h,
                        'dashboard_blocked': False,
                    })
                    print(f"  [{node}] Manual lock still active (sensors not clear)")
            else:
                # Normal AI path
                node_history[node] = {
                    't': t, 's': s, 'f': f, 'h': h,
                    'manual_fire_active': False,
                    'dashboard_blocked': False,
                }
                if final_label.lower() in ["fire", "false"]:
                    if (f > 0 and s < 50 and t < 40 and abs(t_delta) < 1.0) or (abs(t_delta) < 0.2 and s < 30):
                        final_label = "Normal"
                        confidence  = 0.98

        # Database insert
        conn = mysql.connector.connect(**DB_CONFIG)
        ensure_gateway_exists(conn, gateway_id)
        cur = conn.cursor()

        sql = """
            INSERT INTO sensor_readings
            (gateway_id, node_id, timestamp, local_timestamp,
             temperature, humidity, flame, smoke,
             latitude, longitude, rssi, snr,
             ai_prediction, confidence, trigger_source,
             temp_delta, smoke_delta, flame_delta, hum_delta,
             temp_roll_avg, smoke_roll_avg)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        cur.execute(sql, (
            gateway_id, node, ts_final, ts_early,
            t, h, f, s,
            lat, lon, rssi, snr,
            final_label, confidence, trigger_source,
            t_delta, s_delta, f_delta, h_delta,
            t_roll, s_roll
        ))
        conn.commit()
        cur.close()
        conn.close()

        # Dashboard notify
        requests.post(
            "https://flamesapp.up.railway.app/notify-new-data",
            json={
                "type":           "new_reading",
                "node_id":        node,
                "ai_prediction":  final_label,
                "confidence":     f"{confidence*100:.2f}%",
                "trigger_source": trigger_source,
                "temperature":    t,
                "humidity":       h,
                "smoke":          s,
                "flame":          f,
                "latitude":       lat,
                "longitude":      lon,
                "rssi":           rssi,
                "temp_delta":     round(t_delta, 2),
                "manual_fire":    manual_fire,
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