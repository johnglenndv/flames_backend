from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import mysql.connector
import os
from datetime import datetime
from zoneinfo import ZoneInfo

app = FastAPI(title="FLAMES API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_CONFIG = {
    "host": os.getenv("MYSQLHOST"),
    "port": int(os.getenv("MYSQLPORT", 3306)),
    "user": os.getenv("MYSQLUSER"),
    "password": os.getenv("MYSQLPASSWORD"),
    "database": os.getenv("MYSQLDATABASE"),
}

PH_ZONE = ZoneInfo("Asia/Manila")

def convert_to_ph_time(db_timestamp):
    """Convert MySQL timestamp (datetime object) to PH local string"""
    if not db_timestamp:
        return "N/A"
    
    try:
        # db_timestamp is already a datetime object (from mysql-connector)
        # Assume it's UTC → convert to PH
        ph_dt = db_timestamp.astimezone(PH_ZONE)
        return ph_dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        print(f"Timezone conversion error: {e}")
        # Fallback: return original as string
        return str(db_timestamp)

@app.get("/latest/{node_id}")
async def get_latest(node_id: str):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)
    
    cur.execute("""
        SELECT id, node_id, timestamp, temperature, humidity, flame, smoke,
               latitude, longitude, rssi, snr
        FROM sensor_readings 
        WHERE node_id = %s 
        ORDER BY id DESC 
        LIMIT 1
    """, (node_id,))
    
    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        return {"status": "no_data"}

    # Convert timestamp to PH time
    row["display_timestamp"] = convert_to_ph_time(row.get("timestamp"))

    return row

@app.get("/history/{node_id}")
async def get_history(node_id: str, limit: int = 50):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)
    
    cur.execute("""
        SELECT id, node_id, timestamp, temperature, humidity, flame, smoke,
               latitude, longitude, rssi, snr
        FROM sensor_readings 
        WHERE node_id = %s 
        ORDER BY id DESC 
        LIMIT %s
    """, (node_id, limit))
    
    rows = cur.fetchall()
    cur.close()
    conn.close()

    # Convert each row
    for row in rows:
        row["display_timestamp"] = convert_to_ph_time(row.get("timestamp"))

    return rows

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
    
@app.get("/nodes")
async def get_all_nodes():
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)
    
    # Get distinct nodes + their most recent reading
    # Use id DESC instead of timestamp (your working sort)
    # No reference to non-existent display_timestamp
    cur.execute("""
        SELECT DISTINCT s.node_id,
               (SELECT temperature FROM sensor_readings WHERE node_id = s.node_id ORDER BY id DESC LIMIT 1) AS temperature,
               (SELECT humidity   FROM sensor_readings WHERE node_id = s.node_id ORDER BY id DESC LIMIT 1) AS humidity,
               (SELECT flame      FROM sensor_readings WHERE node_id = s.node_id ORDER BY id DESC LIMIT 1) AS flame,
               (SELECT smoke      FROM sensor_readings WHERE node_id = s.node_id ORDER BY id DESC LIMIT 1) AS smoke,
               (SELECT latitude   FROM sensor_readings WHERE node_id = s.node_id ORDER BY id DESC LIMIT 1) AS latitude,
               (SELECT longitude  FROM sensor_readings WHERE node_id = s.node_id ORDER BY id DESC LIMIT 1) AS longitude,
               (SELECT rssi       FROM sensor_readings WHERE node_id = s.node_id ORDER BY id DESC LIMIT 1) AS rssi,
               (SELECT snr        FROM sensor_readings WHERE node_id = s.node_id ORDER BY id DESC LIMIT 1) AS snr,
               (SELECT timestamp  FROM sensor_readings WHERE node_id = s.node_id ORDER BY id DESC LIMIT 1) AS timestamp
        FROM sensor_readings s
        ORDER BY s.node_id
    """)
    
    nodes = cur.fetchall()
    cur.close()
    conn.close()

    # Convert timestamps to PH time in Python (same as /latest)
    ph_nodes = []
    for row in nodes:
        row_copy = row.copy()
        if row_copy.get("timestamp"):
            row_copy["display_timestamp"] = convert_to_ph_time(row_copy["timestamp"])
        else:
            row_copy["display_timestamp"] = "N/A"
        ph_nodes.append(row_copy)

    return ph_nodes if ph_nodes else []