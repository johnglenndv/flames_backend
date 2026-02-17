# worker/main.py
import os
import json
import paho.mqtt.client as mqtt
import mysql.connector
from mysql.connector import Error
from datetime import datetime

# ── Config ──────────────────────────────────────────────
BROKER = os.getenv("HIVEMQ_HOST", "528e719c19214a19b07c0e7322298267.s1.eu.hivemq.cloud")
PORT   = 8883
USER   = os.getenv("HIVEMQ_USER", "Uplink01")
PASS   = os.getenv("HIVEMQ_PASS", "Uplink01")
TOPIC  = "lora/uplink"

DB_CONFIG = {
    "host":     os.getenv("MYSQLHOST"),
    "port":     int(os.getenv("MYSQLPORT", 3306)),
    "user":     os.getenv("MYSQLUSER"),
    "password": os.getenv("MYSQLPASSWORD"),
    "database": os.getenv("MYSQLDATABASE"),
}

def on_connect(client, userdata, flags, rc):
    print(f"Connected to MQTT (rc={rc})")
    client.subscribe(TOPIC)
    print(f"Subscribed to {TOPIC}")

def on_message(client, userdata, msg):
    try:
        raw = msg.payload.decode()
        data = json.loads(raw)
        print("Received:", data)

        wrapper = data
        payload = wrapper.get("payload", {})
        node    = payload.get("node")

        if not node:
            print("No node field → skipping")
            return

        # Map your payload fields (adjust keys if needed)
        row = (
            node,
            wrapper.get("received_at"),
            payload.get("temp"),
            payload.get("hum"),
            payload.get("flame", 0),
            payload.get("smoke", 0),
            payload.get("lat"),
            payload.get("lon"),
            wrapper.get("rssi"),
            wrapper.get("snr")
        )

        conn = mysql.connector.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO sensor_readings
            (node_id, timestamp, temperature, humidity, flame, smoke, latitude, longitude, rssi, snr)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, row)
        conn.commit()
        cur.close()
        conn.close()

        print(f"Inserted data for {node}")

    except Exception as e:
        print("Error processing message:", e)

client = mqtt.Client()
client.username_pw_set(USER, PASS)
client.tls_set()
client.on_connect = on_connect
client.on_message = on_message

print("Starting MQTT to MySQL worker...")
client.connect(BROKER, PORT, 60)
client.loop_forever()