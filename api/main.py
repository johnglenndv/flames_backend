from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import mysql.connector
import os

app = FastAPI(title="FLAMES API")

# Allow your frontend to connect from anywhere (tighten later)
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

@app.get("/latest/{node_id}")
async def get_latest(node_id: str):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT * FROM sensor_readings 
        WHERE node_id = %s 
        ORDER BY timestamp DESC LIMIT 1
    """, (node_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row or {"status": "no_data"}

@app.get("/history/{node_id}")
async def get_history(node_id: str, limit: int = 50):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT * FROM sensor_readings 
        WHERE node_id = %s 
        ORDER BY timestamp DESC LIMIT %s
    """, (node_id, limit))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))