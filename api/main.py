from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import mysql.connector
import os
from datetime import datetime
from zoneinfo import ZoneInfo

app = FastAPI(title="FLAMES API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Change to your actual frontend domain later for security
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Railway auto-injected DB vars
DB_CONFIG = {
    "host": os.getenv("MYSQLHOST"),
    "port": int(os.getenv("MYSQLPORT", 3306)),
    "user": os.getenv("MYSQLUSER"),
    "password": os.getenv("MYSQLPASSWORD"),
    "database": os.getenv("MYSQLDATABASE"),
}

# PH timezone
PH_ZONE = ZoneInfo("Asia/Manila")

def convert_to_ph_time(utc_str):
    """Convert UTC timestamp string from DB to PH local time string"""
    try:
        # MySQL returns UTC string without offset → force UTC parse
        utc_dt = datetime.fromisoformat(utc_str + '+00:00')
        ph_dt = utc_dt.astimezone(PH_ZONE)
        return ph_dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        print(f"Timezone conversion error: {e}")
        return utc_str  # fallback to original if parsing fails

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

    # Convert timestamp to PH local time
    if row.get("timestamp"):
        row["display_timestamp"] = convert_to_ph_time(row["timestamp"])
    else:
        row["display_timestamp"] = "N/A"

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

    # Convert timestamps for all rows
    for row in rows:
        if row.get("timestamp"):
            row["display_timestamp"] = convert_to_ph_time(row["timestamp"])
        else:
            row["display_timestamp"] = "N/A"

    return rows

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))