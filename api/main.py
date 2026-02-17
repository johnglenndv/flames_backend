from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import mysql.connector
import os
from datetime import datetime, timezone, timedelta

app = FastAPI(title="FLAMES API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # ← tighten later (e.g. your frontend domain)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Railway auto-injected MySQL variables
DB_CONFIG = {
    "host": os.getenv("MYSQLHOST"),
    "port": int(os.getenv("MYSQLPORT", 3306)),
    "user": os.getenv("MYSQLUSER"),
    "password": os.getenv("MYSQLPASSWORD"),
    "database": os.getenv("MYSQLDATABASE"),
}

# Philippine timezone (UTC+8)
PH_TZ = timezone(timedelta(hours=8))

@app.get("/latest/{node_id}")
async def get_latest(node_id: str):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)
    
    # Sort by id DESC (most recent insert) — reliable in your current setup
    cur.execute("""
        SELECT id, node_id, timestamp, local_timestamp,
               temperature, humidity, flame, smoke,
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

    # If local_timestamp exists → use it
    # If not → convert timestamp on-the-fly (fallback)
    display_time = row.get("local_timestamp")
    if not display_time and row.get("timestamp"):
        try:
            # Assume gateway timestamp is UTC → convert to PH
            utc_time = datetime.fromisoformat(row["timestamp"].replace('Z', '+00:00'))
            display_time = utc_time.astimezone(PH_TZ).strftime('%Y-%m-%d %H:%M:%S')
        except:
            display_time = row["timestamp"]  # fallback to original if conversion fails

    row["display_timestamp"] = display_time  # nice name for frontend

    return row

@app.get("/history/{node_id}")
async def get_history(node_id: str, limit: int = 50):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)
    
    cur.execute("""
        SELECT id, node_id, timestamp, local_timestamp,
               temperature, humidity, flame, smoke,
               latitude, longitude, rssi, snr
        FROM sensor_readings 
        WHERE node_id = %s 
        ORDER BY id DESC 
        LIMIT %s
    """, (node_id, limit))
    
    rows = cur.fetchall()
    cur.close()
    conn.close()

    # Convert timestamps to PH time for each row
    ph_rows = []
    for row in rows:
        display_time = row.get("local_timestamp")
        if not display_time and row.get("timestamp"):
            try:
                utc_time = datetime.fromisoformat(row["timestamp"].replace('Z', '+00:00'))
                display_time = utc_time.astimezone(PH_TZ).strftime('%Y-%m-%d %H:%M:%S')
            except:
                display_time = row["timestamp"]
        
        row_copy = row.copy()
        row_copy["display_timestamp"] = display_time
        ph_rows.append(row_copy)

    return ph_rows

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))