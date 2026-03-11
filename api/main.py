<<<<<<< Updated upstream
from fastapi import FastAPI, Depends, HTTPException, status, WebSocket, WebSocketDisconnect
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from pydantic import BaseModel, EmailStr
import mysql.connector
import os
import asyncio
import httpx
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from jose import JWTError, jwt
from passlib.context import CryptContext
import secrets, string
from typing import List, Optional

#-------WEBSOCKET MANAGER START HERE----------------
import asyncio

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        dead = []
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception:
                dead.append(connection)
        for connection in dead:
            self.disconnect(connection)

manager = ConnectionManager()
#-------WEBSOCKET MANAGER END HERE----------------

#-------SHARED TRAFFIC STATE START HERE----------------
# In-memory traffic state shared across all devices on this server instance.
# Survives page refresh and new tabs - everyone gets same traffic age.
# Key: 'cards' for card ticker, or str(incident_id) for detail route.
import json as _json
_shared_traffic: dict = {}

def _get_traffic(key: str):
    return _shared_traffic.get(str(key))

def _set_traffic(key: str, data: dict):
    _shared_traffic[str(key)] = data
#-------SHARED TRAFFIC STATE END HERE----------------


#-----DATA MODELS START HERE----------------
class UserCreate(BaseModel):
    username: str
    email: EmailStr | None = None
    password: str
    invite_code: str

class Token(BaseModel):
    access_token: str
    token_type: str

class Organization(BaseModel):
    id: int
    name: str
    invite_code: str | None = None

class GatewayCreate(BaseModel):
    gateway_id: str
    location_name: str | None = None
    latitude: float | None = None
    longitude: float | None = None

class OrganizationCreate(BaseModel):
    name: str
    invite_code: str | None = None

class InviteCodeCreate(BaseModel):
    code: str | None = None
    expires_days: int = 30
    max_uses: int = 1

class AssignOrgBody(BaseModel):
    org_id: int

class PinCreate(BaseModel):
    name: str | None = None
    latitude: float
    longitude: float

class InviteCodeAdminCreate(BaseModel):
    org_id: int
    code: str | None = None
    expires_days: int = 30
    max_uses: int = 1

class ResolveIncidentBody(BaseModel):
    notes: str | None = None

class RespondIncidentBody(BaseModel):
    organization_id: int | None = None
    organization_name: str | None = None

#-----DATA MODELS END HERE----------------

# TomTom config
# FIX 1: was "# TomTom cache = os.getenv(...)" — the # made it a comment so TOMTOM_KEY was never defined,
#         causing a silent NameError in every _fetch_tomtom_point call → all 10 segments failed
TOMTOM_KEY = os.getenv("TOMTOM_KEY", "iy3ljq06nVjJYIdgJdqJZAHiDaYPattE")
TOMTOM_FLOW_BASE = "https://api.tomtom.com/traffic/services/4/flowSegmentData/relative0/10/json"
TOMTOM_INTERVAL = 300  # FIX 2: was 300_000 — asyncio.sleep() takes seconds, not ms (300_000s = ~83 hours!)
_traffic_cache: dict = {"data": None, "timestamp": None, "failed_at": None}
_traffic_gateway: dict = {"lat": None, "lng": None}

# ── DB helpers for persistent traffic cache ───────────────────────────────────
def _db_save_traffic(avg_speed: float, avg_jam: float, timestamp_ms: int):
    """Persist latest TomTom result to traffic_cache table."""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO traffic_cache (id, avg_speed, avg_jam, timestamp_ms, fetched_at)
            VALUES (1, %s, %s, %s, NOW())
            ON DUPLICATE KEY UPDATE
                avg_speed    = VALUES(avg_speed),
                avg_jam      = VALUES(avg_jam),
                timestamp_ms = VALUES(timestamp_ms),
                fetched_at   = VALUES(fetched_at)
        """, (avg_speed, avg_jam, timestamp_ms))
        conn.commit()
        cur.close(); conn.close()
    except Exception as e:
        print(f"[FLAMES] Failed to save traffic to DB: {e}")

def _db_load_traffic():
    """Load last TomTom result from DB into memory on startup."""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT avg_speed, avg_jam, timestamp_ms FROM traffic_cache WHERE id = 1")
        row = cur.fetchone()
        cur.close(); conn.close()
        if row and row["timestamp_ms"]:
            _traffic_cache["data"] = {"avgJam": row["avg_jam"], "avgSpeed": row["avg_speed"]}
            _traffic_cache["timestamp"] = row["timestamp_ms"]
            print(f"[FLAMES] Traffic cache restored from DB — speed={row['avg_speed']} km/h ts={row['timestamp_ms']}")
        else:
            print("[FLAMES] No traffic cache in DB yet")
    except Exception as e:
        print(f"[FLAMES] Failed to load traffic from DB: {e}")

def _db_load_gateway_coords():
    """Seed _traffic_gateway from DB on startup so TomTom works after a server restart."""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cur = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT latitude, longitude FROM gateways
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
            LIMIT 1
        """)
        row = cur.fetchone()
        cur.close(); conn.close()
        if row:
            _traffic_gateway["lat"] = float(row["latitude"])
            _traffic_gateway["lng"] = float(row["longitude"])
            print(f"[FLAMES] Gateway coords seeded from DB: {_traffic_gateway['lat']}, {_traffic_gateway['lng']}")
        else:
            print("[FLAMES] No gateway coords in DB yet — TomTom waits until a client sets them")
    except Exception as e:
        print(f"[FLAMES] Failed to seed gateway coords: {e}")

async def _fetch_tomtom_point(client: httpx.AsyncClient, lat: float, lng: float):
    try:
        url = f"{TOMTOM_FLOW_BASE}?point={lat},{lng}&unit=KMPH&openLr=false&key={TOMTOM_KEY}"
        r = await client.get(url, timeout=8.0)
        if not r.is_success:
            return None
        j = r.json()
        fd = j["flowSegmentData"]
        cs = fd["currentSpeed"]
        ff = fd["freeFlowSpeed"]
        jam = max(0.0, min(10.0, (1 - cs / ff) * 10)) if ff > 0 else 0.0
        return {"currentSpeed": cs, "freeFlowSpeed": ff, "jamFactor": jam, "confidence": fd.get("confidence", 1)}
    except Exception:
        return None

async def _tomtom_background_task():
    await asyncio.sleep(5)
    while True:
        gw = _traffic_gateway
        if gw["lat"] is not None and gw["lng"] is not None:
            try:
                async with httpx.AsyncClient() as client:
                    pts = [(gw["lat"] + (i - 4.5) * 0.002, gw["lng"]) for i in range(10)]
                    results = await asyncio.gather(*[_fetch_tomtom_point(client, lat, lng) for lat, lng in pts], return_exceptions=True)
                    valid = [r for r in results if isinstance(r, dict)]
                    now_ms = int(time.time() * 1000)
                    if valid:
                        tc = sum(v.get("confidence", 1) for v in valid)
                        avg_jam = sum(v["jamFactor"] * v.get("confidence", 1) for v in valid) / tc
                        avg_speed = round(sum(v["currentSpeed"] * v.get("confidence", 1) for v in valid) / tc)
                        _traffic_cache["data"] = {"avgJam": avg_jam, "avgSpeed": avg_speed, "segments": valid}
                        _traffic_cache["timestamp"] = now_ms
                        _traffic_cache["failed_at"] = None
                        _db_save_traffic(avg_speed, avg_jam, now_ms)
                        print(f"[FLAMES] TomTom fetch OK — speed={avg_speed} km/h jam={avg_jam:.2f}")
                        await manager.broadcast({"type": "traffic_update", "data": {"avgJam": avg_jam, "avgSpeed": avg_speed}, "timestamp": now_ms})
                    else:
                        print(f"[FLAMES] TomTom returned no valid segments (all {len(results)} failed)")
                        _traffic_cache["failed_at"] = now_ms
                        await manager.broadcast({"type": "traffic_update", "data": None, "timestamp": _traffic_cache["timestamp"], "failed_at": now_ms})
            except Exception as e:
                print(f"[FLAMES] TomTom error: {e}")
        else:
            print("[FLAMES] TomTom skipped — no gateway coords set")
        await asyncio.sleep(TOMTOM_INTERVAL)

@asynccontextmanager
async def lifespan(app: FastAPI):
    _db_load_gateway_coords()
    _db_load_traffic()
    task = asyncio.create_task(_tomtom_background_task())
    yield
    task.cancel()

app = FastAPI(title="FLAMES API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

SECRET_KEY = os.getenv("JWT_SECRET_KEY", "your-super-secret-key-change-this-immediately")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24  # 1 day

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")

DB_CONFIG = {
    "host": os.getenv("MYSQLHOST"),
    "port": int(os.getenv("MYSQLPORT", 3306)),
    "user": os.getenv("MYSQLUSER"),
    "password": os.getenv("MYSQLPASSWORD"),
    "database": os.getenv("MYSQLDATABASE"),
}

PH_ZONE = ZoneInfo("Asia/Manila")

#-----UTILITY FUNCTIONS START HERE----------------
def convert_to_ph_time(db_timestamp):
    if not db_timestamp:
        return "N/A"
    try:
        ph_dt = db_timestamp.astimezone(PH_ZONE)
        return ph_dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        print(f"Timezone conversion error: {e}")
        return str(db_timestamp)

def format_local_timestamp(ts):
    if not ts:
        return "N/A"
    try:
        if isinstance(ts, str):
            return ts
        return ts.strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        print(f"format_local_timestamp error: {e}")
        return str(ts)

def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password):
    return pwd_context.hash(password)

def create_access_token(data: dict, expires_delta: timedelta | None = None):
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception

    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM users WHERE username = %s", (username,))
    user = cur.fetchone()
    cur.close()
    conn.close()
    if user is None:
        raise credentials_exception
    return user

async def admin_required(current_user: dict = Depends(get_current_user)):
    if current_user.get('is_admin') != 1:
        raise HTTPException(status_code=403, detail="Admin access required")
    return current_user
#-----UTILITY FUNCTIONS END HERE----------------

def upsert_fire_incident(cur, reading: dict):
    pred       = (reading.get("ai_prediction") or "").lower()
    node_id    = reading.get("node_id")
    gateway_id = reading.get("gateway_id")
    now_str    = reading.get("local_timestamp") or \
                 datetime.now(PH_ZONE).strftime("%Y-%m-%d %H:%M:%S")

    if pred not in ("fire", "false"):
        cur.execute("""
            UPDATE fire_incidents
            SET status = 'resolved', resolved_at = %s
            WHERE node_id = %s AND status = 'active'
        """, (now_str, node_id))
        return

    cur.execute("""
        SELECT id FROM fire_incidents
        WHERE node_id = %s AND status = 'active'
        LIMIT 1
    """, (node_id,))
    existing = cur.fetchone()

    if existing:
        cur.execute("""
            UPDATE fire_incidents SET
                ai_prediction   = %s,
                confidence      = %s,
                temperature     = %s,
                humidity        = %s,
                flame           = %s,
                smoke           = %s,
                latitude        = %s,
                longitude       = %s,
                last_updated_at = %s
            WHERE id = %s
        """, (
            pred,
            reading.get("confidence", 0),
            reading.get("temperature"),
            reading.get("humidity"),
            reading.get("flame"),
            reading.get("smoke"),
            reading.get("latitude"),
            reading.get("longitude"),
            now_str,
            existing["id"],
        ))
    else:
        cur.execute("""
            INSERT INTO fire_incidents
                (node_id, gateway_id, ai_prediction, confidence,
                 temperature, humidity, flame, smoke,
                 latitude, longitude, status, started_at, last_updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'active',%s,%s)
        """, (
            node_id,
            gateway_id,
            pred,
            reading.get("confidence", 0),
            reading.get("temperature"),
            reading.get("humidity"),
            reading.get("flame"),
            reading.get("smoke"),
            reading.get("latitude"),
            reading.get("longitude"),
            now_str,
            now_str,
        ))

#----WEBSOCKET ENDPOINTS START HERE----------------
@app.get("/traffic/latest")
async def get_latest_traffic(current_user: dict = Depends(get_current_user)):
    return {"data": _traffic_cache["data"], "timestamp": _traffic_cache["timestamp"], "failed_at": _traffic_cache["failed_at"]}

class GatewayCoords(BaseModel):
    lat: float
    lng: float

@app.post("/traffic/gateway")
async def set_traffic_gateway(coords: GatewayCoords, current_user: dict = Depends(get_current_user)):
    _traffic_gateway["lat"] = coords.lat
    _traffic_gateway["lng"] = coords.lng
    print(f"[FLAMES] TomTom gateway set to: {coords.lat}, {coords.lng}")
    return {"ok": True}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    if _traffic_cache["timestamp"] is not None:
        await websocket.send_json({"type": "traffic_update", "data": _traffic_cache["data"], "timestamp": _traffic_cache["timestamp"], "failed_at": _traffic_cache["failed_at"]})
    try:
        while True:
            try:
                # Wait for client message with 30s timeout, then send ping to keep alive
                data = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
            except asyncio.TimeoutError:
                # Send ping to keep Railway proxy from killing idle connection
                try:
                    await websocket.send_json({"type": "ping"})
                except Exception:
                    break
    except WebSocketDisconnect:
        pass
    finally:
        manager.disconnect(websocket)
#----WEBSOCKET ENDPOINTS END HERE----------------

#-----API ENDPOINTS START HERE----------------
@app.get("/latest/{node_id}")
async def get_latest(node_id: str, current_user: dict = Depends(get_current_user)):
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
    row["display_timestamp"] = convert_to_ph_time(row.get("timestamp"))
    return row

@app.get("/history/{node_id}")
async def get_history(node_id: str, limit: int = 50, current_user: dict = Depends(get_current_user)):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT id, node_id, timestamp, local_timestamp, temperature, humidity, flame, smoke,
               latitude, longitude, rssi, snr, ai_prediction, confidence
        FROM sensor_readings
        WHERE node_id = %s
        ORDER BY id DESC
        LIMIT %s
    """, (node_id, limit))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    for row in rows:
        row["display_timestamp"] = convert_to_ph_time(row.get("timestamp"))
    return rows

@app.get("/nodes")
async def get_all_nodes(current_user: dict = Depends(get_current_user)):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)

    is_admin = current_user.get('is_admin') == 1
    user_org = current_user.get('org_id')

    LATEST = """
        (SELECT temperature    FROM sensor_readings WHERE node_id = s.node_id ORDER BY id DESC LIMIT 1) AS temperature,
        (SELECT humidity       FROM sensor_readings WHERE node_id = s.node_id ORDER BY id DESC LIMIT 1) AS humidity,
        (SELECT flame          FROM sensor_readings WHERE node_id = s.node_id ORDER BY id DESC LIMIT 1) AS flame,
        (SELECT smoke          FROM sensor_readings WHERE node_id = s.node_id ORDER BY id DESC LIMIT 1) AS smoke,
        (SELECT latitude       FROM sensor_readings WHERE node_id = s.node_id ORDER BY id DESC LIMIT 1) AS latitude,
        (SELECT longitude      FROM sensor_readings WHERE node_id = s.node_id ORDER BY id DESC LIMIT 1) AS longitude,
        (SELECT rssi           FROM sensor_readings WHERE node_id = s.node_id ORDER BY id DESC LIMIT 1) AS rssi,
        (SELECT snr            FROM sensor_readings WHERE node_id = s.node_id ORDER BY id DESC LIMIT 1) AS snr,
        (SELECT timestamp      FROM sensor_readings WHERE node_id = s.node_id ORDER BY id DESC LIMIT 1) AS timestamp,
        (SELECT local_timestamp FROM sensor_readings WHERE node_id = s.node_id ORDER BY id DESC LIMIT 1) AS local_timestamp,
        (SELECT ai_prediction  FROM sensor_readings WHERE node_id = s.node_id ORDER BY id DESC LIMIT 1) AS ai_prediction,
        (SELECT confidence     FROM sensor_readings WHERE node_id = s.node_id ORDER BY id DESC LIMIT 1) AS confidence
    """

    if is_admin:
        cur.execute(f"""
            SELECT DISTINCT s.node_id, {LATEST}
            FROM sensor_readings s
            ORDER BY s.node_id
        """)
    else:
        if not user_org:
            cur.close(); conn.close()
            return []
        cur.execute(f"""
            SELECT DISTINCT s.node_id, {LATEST}
            FROM sensor_readings s
            INNER JOIN gateways g ON g.gateway_id = s.gateway_id
            WHERE g.org_id = %s
            ORDER BY s.node_id
        """, (user_org,))

    nodes = cur.fetchall()
    cur.close()
    conn.close()

    ph_nodes = []
    for row in nodes:
        row_copy = row.copy()
        row_copy["display_timestamp"] = convert_to_ph_time(row_copy["timestamp"]) if row_copy.get("timestamp") else "N/A"
        ph_nodes.append(row_copy)
    return ph_nodes


@app.get("/organizations", response_model=list[Organization])
async def get_organizations(current_user: dict = Depends(get_current_user)):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)
    if current_user.get('is_admin') == 1:
        cur.execute("SELECT id, name, invite_code FROM organizations ORDER BY name")
    else:
        cur.execute(
            "SELECT id, name, invite_code FROM organizations WHERE id = %s",
            (current_user.get('org_id'),)
        )
    orgs = cur.fetchall()
    cur.close()
    conn.close()
    return orgs


@app.post("/organizations")
async def create_organization(
    org: OrganizationCreate,
    current_user: dict = Depends(get_current_user),
    _admin: dict = Depends(admin_required)
):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)

    cur.execute("SELECT id FROM organizations WHERE name = %s", (org.name,))
    if cur.fetchone():
        cur.close(); conn.close()
        raise HTTPException(status_code=400, detail="Organization name already exists")

    if org.invite_code:
        cur.execute("SELECT id FROM organizations WHERE invite_code = %s", (org.invite_code,))
        if cur.fetchone():
            cur.close(); conn.close()
            raise HTTPException(status_code=400, detail="Invite code already in use by another organization")

    cur.execute("""
        INSERT INTO organizations (name, invite_code, invite_code_expires, created_by)
        VALUES (%s, %s, NULL, %s)
    """, (org.name, org.invite_code or None, current_user['id']))

    conn.commit()
    new_id = cur.lastrowid
    cur.close()
    conn.close()

    return {
        "message":     "Organization created",
        "id":          new_id,
        "name":        org.name,
        "invite_code": org.invite_code,
    }


@app.get("/gateways")
async def get_gateways(
    org_id: Optional[int] = None,
    current_user: dict = Depends(get_current_user)
):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)

    is_admin = current_user.get('is_admin') == 1
    user_org = current_user.get('org_id')

    if is_admin:
        if org_id is not None:
            cur.execute("""
                SELECT gateway_id, org_id, location_name, latitude, longitude, created_at
                FROM gateways WHERE org_id = %s ORDER BY gateway_id
            """, (org_id,))
        else:
            cur.execute("""
                SELECT g.gateway_id, g.org_id, g.location_name,
                       g.latitude, g.longitude, g.created_at,
                       o.name AS org_name
                FROM gateways g
                LEFT JOIN organizations o ON g.org_id = o.id
                ORDER BY g.gateway_id
            """)
    else:
        if not user_org:
            cur.close(); conn.close()
            return []
        cur.execute("""
            SELECT gateway_id, org_id, location_name, latitude, longitude, created_at
            FROM gateways WHERE org_id = %s ORDER BY gateway_id
        """, (user_org,))

    gateways = cur.fetchall()
    cur.close()
    conn.close()
    return gateways


@app.patch("/gateways/{gateway_id}/assign-org")
async def assign_gateway_to_org(
    gateway_id: str,
    body: AssignOrgBody,
    current_user: dict = Depends(get_current_user),
    _admin: dict = Depends(admin_required)
):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)

    cur.execute("SELECT id FROM organizations WHERE id = %s", (body.org_id,))
    if not cur.fetchone():
        cur.close(); conn.close()
        raise HTTPException(404, "Organization not found")

    cur.execute("UPDATE gateways SET org_id = %s WHERE gateway_id = %s", (body.org_id, gateway_id))
    if cur.rowcount == 0:
        cur.close(); conn.close()
        raise HTTPException(404, f"Gateway '{gateway_id}' not found")

    conn.commit()
    cur.close()
    conn.close()
    return {"message": f"Gateway '{gateway_id}' assigned to org {body.org_id}"}


@app.patch("/gateways/{gateway_id}/disassociate-org")
async def disassociate_gateway_from_org(
    gateway_id: str,
    current_user: dict = Depends(get_current_user),
    _admin: dict = Depends(admin_required)
):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)

    cur.execute("SELECT gateway_id, org_id FROM gateways WHERE gateway_id = %s", (gateway_id,))
    gw = cur.fetchone()
    if not gw:
        cur.close(); conn.close()
        raise HTTPException(404, f"Gateway '{gateway_id}' not found")

    if gw['org_id'] is None:
        cur.close(); conn.close()
        raise HTTPException(400, f"Gateway '{gateway_id}' is not assigned to any organization")

    cur.execute("UPDATE gateways SET org_id = NULL WHERE gateway_id = %s", (gateway_id,))
    conn.commit()
    cur.close()
    conn.close()
    return {"message": f"Gateway '{gateway_id}' disassociated from its organization"}


@app.get("/me")
async def get_current_user_info(current_user: dict = Depends(get_current_user)):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT u.id, u.username, u.email, u.created_at, u.is_admin,
               o.id AS org_id, o.name AS organization_name
        FROM users u
        LEFT JOIN organizations o ON u.org_id = o.id
        WHERE u.id = %s
    """, (current_user["id"],))
    full_user = cur.fetchone()
    cur.close()
    conn.close()
    if not full_user:
        raise HTTPException(404, "User not found")
    return {
        "user_id":           full_user["id"],
        "username":          full_user["username"],
        "email":             full_user.get("email"),
        "created_at":        full_user["created_at"].isoformat() if full_user["created_at"] else None,
        "is_admin":          bool(full_user["is_admin"]),
        "organization_id":   full_user["org_id"],
        "organization_name": full_user["organization_name"] or "—",
    }

#-----API ENDPOINTS END HERE----------------

#---DELETION----
@app.delete("/users/{user_id}")
async def delete_user(user_id: int, current_user: dict = Depends(admin_required)):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("DELETE FROM users WHERE id = %s", (user_id,))
    if cur.rowcount == 0:
        cur.close(); conn.close()
        raise HTTPException(status_code=404, detail="User not found")
    conn.commit()
    cur.close(); conn.close()
    return {"message": "User deleted successfully"}

@app.delete("/gateways/{gateway_id}")
async def delete_gateway(gateway_id: str, current_user: dict = Depends(admin_required)):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("DELETE FROM gateways WHERE gateway_id = %s", (gateway_id,))
    if cur.rowcount == 0:
        cur.close(); conn.close()
        raise HTTPException(status_code=404, detail="Gateway not found")
    conn.commit()
    cur.close(); conn.close()
    return {"message": "Gateway deleted successfully"}

@app.delete("/invite-codes/{code}")
async def delete_invite_code(code: str, current_user: dict = Depends(admin_required)):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("DELETE FROM invite_codes WHERE code = %s", (code,))
    if cur.rowcount == 0:
        cur.close(); conn.close()
        raise HTTPException(status_code=404, detail="Invite code not found")
    conn.commit()
    cur.close(); conn.close()
    return {"message": "Invite code deleted"}

@app.delete("/organizations/{org_id}")
async def delete_organization(org_id: int, current_user: dict = Depends(admin_required)):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM users WHERE org_id = %s", (org_id,))
    if cur.fetchone()[0] > 0:
        cur.close(); conn.close()
        raise HTTPException(status_code=400, detail="Cannot delete organization with active users")
    cur.execute("DELETE FROM organizations WHERE id = %s", (org_id,))
    if cur.rowcount == 0:
        cur.close(); conn.close()
        raise HTTPException(status_code=404, detail="Organization not found")
    conn.commit()
    cur.close(); conn.close()
    return {"message": "Organization deleted"}
#--------------------DELETION END HERE-------------------


# ══════════════════════════════════════════════════════════════
#  INCIDENTS
# ══════════════════════════════════════════════════════════════

# Traffic state endpoints
@app.get("/traffic-state")
async def get_traffic_state(current_user: dict = Depends(get_current_user)):
    """Return the server-side shared traffic state so all clients/devices stay in sync."""
    return _shared_traffic

@app.post("/traffic-state")
async def save_traffic_state(payload: dict, current_user: dict = Depends(get_current_user)):
    """Frontend posts latest TomTom traffic data here so all devices share same state."""
    import time
    for key, data in payload.items():
        if data and isinstance(data, dict):
            data["fetchedAt"] = int(time.time() * 1000)  # server-authoritative timestamp ms
            _set_traffic(key, data)
    # Broadcast to all connected clients so they update immediately
    await manager.broadcast({"type": "traffic_update", "state": _shared_traffic})
    return {"ok": True}

@app.get("/incidents/active")
async def get_active_incidents(current_user: dict = Depends(get_current_user)):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)

    is_admin = current_user.get('is_admin') == 1
    user_org = current_user.get('org_id')

    if is_admin:
        cur.execute("""
            SELECT
                fi.id            AS incident_id,
                fi.node_id,
                fi.gateway_id,
                fi.ai_prediction,
                fi.confidence,
                fi.temperature,
                fi.humidity,
                fi.flame,
                fi.smoke,
                fi.latitude,
                fi.longitude,
                fi.started_at,
                fi.last_updated_at,
                fi.assigned_team,
                CONCAT('Node ', fi.node_id) AS location_name
            FROM fire_incidents fi
            WHERE fi.status = 'active'
            ORDER BY fi.last_updated_at DESC
            LIMIT 50
        """)
    else:
        if not user_org:
            cur.close(); conn.close()
            return []
        cur.execute("""
            SELECT
                fi.id            AS incident_id,
                fi.node_id,
                fi.gateway_id,
                fi.ai_prediction,
                fi.confidence,
                fi.temperature,
                fi.humidity,
                fi.flame,
                fi.smoke,
                fi.latitude,
                fi.longitude,
                fi.started_at,
                fi.last_updated_at,
                fi.assigned_team,
                CONCAT('Node ', fi.node_id) AS location_name
            FROM fire_incidents fi
            INNER JOIN gateways g ON g.gateway_id = fi.gateway_id
            WHERE fi.status = 'active'
              AND g.org_id = %s
            ORDER BY fi.last_updated_at DESC
            LIMIT 50
        """, (user_org,))

    rows = cur.fetchall()
    cur.close()
    conn.close()

    incidents = []
    for row in rows:
        pred          = (row["ai_prediction"] or "").lower()
        confidence_val = float(row["confidence"]) if row["confidence"] is not None else 0.0
        confidence_pct = confidence_val if confidence_val <= 1.0 else confidence_val / 100.0

        if pred == "fire":
            inc_status   = "Active"
            status_class = "txt-active"
        else:
            inc_status   = "Possible Fire"
            status_class = "txt-contained"

        incidents.append({
            "incident_id":    row["incident_id"],
            "node_id":        row["node_id"],
            "location_name":  row["location_name"],
            "latitude":       row["latitude"],
            "longitude":      row["longitude"],
            "ai_prediction":  row["ai_prediction"],
            "confidence":     confidence_pct,
            "confidence_pct": round(confidence_pct * 100, 1),
            "status":         inc_status,
            "status_class":   status_class,
            "temperature":    row["temperature"],
            "humidity":       row["humidity"],
            "smoke":          row["smoke"],
            "flame":          row["flame"],
            "timestamp":      format_local_timestamp(row["last_updated_at"]),
            "started_at":     format_local_timestamp(row["started_at"]),
            "assigned_team":  row["assigned_team"],
        })
    return incidents


@app.get("/incidents/history")
async def get_incident_history(
    limit: int = 100,
    current_user: dict = Depends(get_current_user)
):
    """Returns both active and resolved incidents for history/audit."""
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)

    is_admin = current_user.get('is_admin') == 1
    user_org = current_user.get('org_id')

    if is_admin:
        cur.execute("""
            SELECT fi.*,
                   CONCAT('Node ', fi.node_id) AS location_name
            FROM fire_incidents fi
            ORDER BY fi.started_at DESC
            LIMIT %s
        """, (limit,))
    else:
        if not user_org:
            cur.close(); conn.close()
            return []
        cur.execute("""
            SELECT fi.*,
                   CONCAT('Node ', fi.node_id) AS location_name
            FROM fire_incidents fi
            INNER JOIN gateways g ON g.gateway_id = fi.gateway_id
            WHERE g.org_id = %s
            ORDER BY fi.started_at DESC
            LIMIT %s
        """, (user_org, limit))

    rows = cur.fetchall()
    cur.close()
    conn.close()

    for row in rows:
        row["started_at"]      = format_local_timestamp(row["started_at"])
        row["last_updated_at"] = format_local_timestamp(row["last_updated_at"])
        row["resolved_at"]     = format_local_timestamp(row["resolved_at"]) if row.get("resolved_at") else None
    return rows


@app.get("/incidents/resolved")
async def get_resolved_incidents(
    limit: int = 100,
    current_user: dict = Depends(get_current_user)
):
    """Returns only resolved incidents, most recently resolved first."""
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)

    is_admin = current_user.get('is_admin') == 1
    user_org = current_user.get('org_id')

    if is_admin:
        cur.execute("""
            SELECT fi.*,
                   CONCAT('Node ', fi.node_id) AS location_name
            FROM fire_incidents fi
            WHERE fi.status = 'resolved'
            ORDER BY fi.resolved_at DESC
            LIMIT %s
        """, (limit,))
    else:
        if not user_org:
            cur.close(); conn.close()
            return []
        cur.execute("""
            SELECT fi.*,
                   CONCAT('Node ', fi.node_id) AS location_name
            FROM fire_incidents fi
            INNER JOIN gateways g ON g.gateway_id = fi.gateway_id
            WHERE fi.status = 'resolved'
              AND g.org_id = %s
            ORDER BY fi.resolved_at DESC
            LIMIT %s
        """, (user_org, limit))

    rows = cur.fetchall()
    cur.close()
    conn.close()

    result = []
    for row in rows:
        confidence_val = float(row["confidence"]) if row.get("confidence") is not None else 0.0
        confidence_pct = confidence_val if confidence_val <= 1.0 else confidence_val / 100.0

        result.append({
            "incident_id":    row["id"],
            "node_id":        row["node_id"],
            "location_name":  row["location_name"],
            "latitude":       row.get("latitude"),
            "longitude":      row.get("longitude"),
            "ai_prediction":  row.get("ai_prediction"),
            "confidence":     confidence_pct,
            "confidence_pct": round(confidence_pct * 100, 1),
            "status":         "Resolved",
            "status_class":   "txt-resolved",
            "temperature":    row.get("temperature"),
            "smoke":          row.get("smoke"),
            "flame":          row.get("flame"),
            "notes":          row.get("notes"),
            "timestamp":      format_local_timestamp(row.get("last_updated_at")),
            "started_at":     format_local_timestamp(row.get("started_at")),
            "resolved_at":    format_local_timestamp(row.get("resolved_at")) if row.get("resolved_at") else None,
            "assigned_team":  row.get("assigned_team"),
        })
    return result


# ── GET single incident by ID ──────────────────────────────────
@app.get("/incidents/{incident_id}")
async def get_incident_by_id(
    incident_id: int,
    current_user: dict = Depends(get_current_user)
):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT fi.*,
               CONCAT('Node ', fi.node_id) AS location_name
        FROM fire_incidents fi
        WHERE fi.id = %s
        LIMIT 1
    """, (incident_id,))
    row = cur.fetchone()
    cur.close(); conn.close()

    if not row:
        raise HTTPException(404, "Incident not found")

    confidence_val = float(row["confidence"]) if row.get("confidence") is not None else 0.0
    confidence_pct = confidence_val if confidence_val <= 1.0 else confidence_val / 100.0

    return {
        "incident_id":    row["id"],
        "node_id":        row["node_id"],
        "gateway_id":     row.get("gateway_id"),
        "location_name":  row["location_name"],
        "latitude":       row.get("latitude"),
        "longitude":      row.get("longitude"),
        "ai_prediction":  row.get("ai_prediction"),
        "confidence":     confidence_pct,
        "confidence_pct": round(confidence_pct * 100, 1),
        "status":         row.get("status"),
        "temperature":    row.get("temperature"),
        "humidity":       row.get("humidity"),
        "smoke":          row.get("smoke"),
        "flame":          row.get("flame"),
        "notes":          row.get("notes"),
        "assigned_team":  row.get("assigned_team"),
        "timestamp":      format_local_timestamp(row.get("last_updated_at")),
        "started_at":     format_local_timestamp(row.get("started_at")),
        "resolved_at":    format_local_timestamp(row.get("resolved_at")) if row.get("resolved_at") else None,
    }


@app.patch("/incidents/{incident_id}/respond")
async def respond_to_incident(
    incident_id: int,
    body: RespondIncidentBody,
    current_user: dict = Depends(get_current_user)
):
    """Mark an organization as responding to an incident."""
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)

    cur.execute("SELECT id, node_id, status FROM fire_incidents WHERE id = %s", (incident_id,))
    inc = cur.fetchone()
    if not inc:
        cur.close(); conn.close()
        raise HTTPException(404, "Incident not found")

    assigned_team = body.organization_name or str(body.organization_id) or "Unknown"
    cur.execute("""
        UPDATE fire_incidents SET assigned_team = %s WHERE id = %s
    """, (assigned_team, incident_id))
    conn.commit()

    node_id = inc["node_id"]
    cur.close(); conn.close()

    await manager.broadcast({
        "type":        "incident_update",
        "incident_id": incident_id,
        "action":      "responded",
        "node_id":     node_id,
        "org_name":    assigned_team,
    })
    await manager.broadcast({
        "type":    "node_update",
        "node_id": node_id,
    })

    return {"message": f"Incident {incident_id} assigned to {assigned_team}"}


@app.patch("/incidents/{incident_id}/resolve")
async def resolve_incident(
    incident_id: int,
    body: ResolveIncidentBody,
    current_user: dict = Depends(get_current_user)
):
    """Manually resolve an active fire incident."""
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)
    now_str = datetime.now(PH_ZONE).strftime("%Y-%m-%d %H:%M:%S")

    cur.execute("SELECT id, node_id, status FROM fire_incidents WHERE id = %s", (incident_id,))
    inc = cur.fetchone()
    if not inc:
        cur.close(); conn.close()
        raise HTTPException(404, "Incident not found")
    if inc["status"] == "resolved":
        cur.close(); conn.close()
        raise HTTPException(400, "Incident is already resolved")

    node_id = inc["node_id"]

    cur.execute("""
        UPDATE fire_incidents
        SET status = 'resolved', resolved_at = %s, notes = %s
        WHERE id = %s
    """, (now_str, body.notes, incident_id))
    conn.commit()
    cur.close(); conn.close()

    await manager.broadcast({
        "type":        "incident_update",
        "incident_id": incident_id,
        "action":      "resolved",
        "node_id":     node_id,
    })
    await manager.broadcast({
        "type":    "node_update",
        "node_id": node_id,
    })

    return {"message": f"Incident {incident_id} resolved", "resolved_at": now_str}


# ── Pinned Locations ──────────────────────────────────
@app.get("/me/pins")
async def get_my_pins(current_user: dict = Depends(get_current_user)):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT id, name, latitude, longitude, created_at
        FROM user_pinned_locations
        WHERE user_id = %s
        ORDER BY created_at DESC
    """, (current_user['id'],))
    pins = cur.fetchall()
    cur.close(); conn.close()
    return pins

@app.post("/me/pins")
async def add_pin(pin: PinCreate, current_user: dict = Depends(get_current_user)):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO user_pinned_locations (user_id, name, latitude, longitude)
        VALUES (%s, %s, %s, %s)
    """, (current_user['id'], pin.name, pin.latitude, pin.longitude))
    conn.commit()
    new_id = cur.lastrowid
    cur.close(); conn.close()
    return {"message": "Pin saved", "id": new_id}

@app.delete("/me/pins/{pin_id}")
async def delete_pin(pin_id: int, current_user: dict = Depends(get_current_user)):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute(
        "DELETE FROM user_pinned_locations WHERE id = %s AND user_id = %s",
        (pin_id, current_user['id'])
    )
    if cur.rowcount == 0:
        cur.close(); conn.close()
        raise HTTPException(404, "Pin not found or not owned by you")
    conn.commit()
    cur.close(); conn.close()
    return {"message": "Pin deleted"}

# ── Login / Signup ────────────────────────────────────
@app.post("/signup")
async def signup(user: UserCreate):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)

    cur.execute("""
        SELECT id, name FROM organizations
        WHERE invite_code = %s
        AND (invite_code_expires IS NULL OR invite_code_expires > NOW())
    """, (user.invite_code,))
    org = cur.fetchone()

    org_id = None
    if org:
        org_id = org['id']
    else:
        cur.execute("""
            SELECT org_id, code, expires_at, max_uses, uses
            FROM invite_codes
            WHERE code = %s
            AND (expires_at IS NULL OR expires_at > NOW())
        """, (user.invite_code,))
        invite = cur.fetchone()
        if invite:
            if invite['max_uses'] > 0 and invite['uses'] >= invite['max_uses']:
                cur.close(); conn.close()
                raise HTTPException(status_code=400, detail="Invite code has reached maximum uses")
            org_id = invite['org_id']
            cur.execute("UPDATE invite_codes SET uses = uses + 1 WHERE code = %s", (user.invite_code,))
            conn.commit()
        else:
            cur.close(); conn.close()
            raise HTTPException(status_code=400, detail="Invalid or expired invite code")

    if not org_id:
        raise HTTPException(status_code=400, detail="Invalid or expired invite code")

    cur.execute(
        "SELECT id FROM users WHERE username = %s OR (email = %s AND email IS NOT NULL)",
        (user.username, user.email)
    )
    if cur.fetchone():
        cur.close(); conn.close()
        raise HTTPException(status_code=400, detail="Username or email already taken")

    hashed = get_password_hash(user.password)
    cur.execute(
        "INSERT INTO users (username, email, password_hash, org_id) VALUES (%s, %s, %s, %s)",
        (user.username, user.email, hashed, org_id)
    )
    conn.commit()
    cur.close(); conn.close()
    return {"message": "Account created", "organization_id": org_id}

@app.post("/login", response_model=Token)
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM users WHERE username = %s", (form_data.username,))
    user = cur.fetchone()
    cur.close(); conn.close()
    if not user or not verify_password(form_data.password, user["password_hash"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token = create_access_token(data={"sub": user["username"]})
    return {"access_token": access_token, "token_type": "bearer"}

@app.post("/gateways")
async def register_gateway(
    gateway: GatewayCreate,
    current_user: dict = Depends(get_current_user),
    _admin: dict = Depends(admin_required)
):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT id FROM gateways WHERE gateway_id = %s", (gateway.gateway_id,))
    if cur.fetchone():
        cur.close(); conn.close()
        raise HTTPException(status_code=400, detail="Gateway ID already registered")
    cur.execute("""
        INSERT INTO gateways (gateway_id, org_id, location_name, latitude, longitude, created_by)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (gateway.gateway_id, current_user['org_id'], gateway.location_name,
          gateway.latitude, gateway.longitude, current_user['id']))
    conn.commit()
    cur.close(); conn.close()
    return {
        "message":         "Gateway registered successfully",
        "gateway_id":      gateway.gateway_id,
        "organization_id": current_user['org_id'],
    }

@app.post("/admin/invite-codes")
async def admin_create_invite_code(
    invite: InviteCodeAdminCreate,
    current_user: dict = Depends(get_current_user),
    _admin: dict = Depends(admin_required)
):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT id, name FROM organizations WHERE id = %s", (invite.org_id,))
    org = cur.fetchone()
    if not org:
        cur.close(); conn.close()
        raise HTTPException(404, "Organization not found")
    code = invite.code or ''.join(secrets.choice(string.ascii_uppercase + string.digits) for _ in range(8))
    expires = datetime.now() + timedelta(days=invite.expires_days) if invite.expires_days else None
    cur.execute("""
        INSERT INTO invite_codes (org_id, code, expires_at, max_uses, created_by)
        VALUES (%s, %s, %s, %s, %s)
    """, (invite.org_id, code, expires, invite.max_uses, current_user['id']))
    conn.commit()
    cur.close(); conn.close()
    return {
        "message":           "Admin created invite code",
        "code":              code,
        "organization_id":   invite.org_id,
        "organization_name": org['name'],
        "expires_at":        expires.isoformat() if expires else None,
        "max_uses":          invite.max_uses,
    }

@app.post("/organizations/{org_id}/invite-codes")
async def create_invite_code_for_org(
    org_id: int,
    invite: InviteCodeCreate,
    current_user: dict = Depends(get_current_user),
    _admin: dict = Depends(admin_required)
):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT id, name FROM organizations WHERE id = %s", (org_id,))
    org = cur.fetchone()
    if not org:
        cur.close(); conn.close()
        raise HTTPException(404, "Organization not found")
    code = ''.join(secrets.choice(string.ascii_uppercase + string.digits) for _ in range(8))
    expires = datetime.now() + timedelta(days=invite.expires_days) if invite.expires_days else None
    cur.execute("""
        INSERT INTO invite_codes (org_id, code, expires_at, max_uses, created_by)
        VALUES (%s, %s, %s, %s, %s)
    """, (org_id, code, expires, invite.max_uses, current_user['id']))
    conn.commit()
    cur.close(); conn.close()
    return {
        "code":            code,
        "organization_id": org_id,
        "expires_at":      expires.isoformat() if expires else None,
        "max_uses":        invite.max_uses,
    }

@app.post("/notify-new-data")
async def notify_new_data(data: dict):
    await manager.broadcast(data)
    return {"status": "broadcasted"}


# ══════════════════════════════════════════════════════════════
#  SIMULATION ENDPOINTS
# ══════════════════════════════════════════════════════════════

@app.post("/dev/simulate-fire")
async def simulate_fire(
    node_id: str = "Node1",
    gateway_id: str = "GW1",
    current_user: dict = Depends(get_current_user)
):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)

    is_admin = current_user.get('is_admin') == 1
    user_org = current_user.get('org_id')

    if not is_admin:
        cur.execute("SELECT org_id FROM gateways WHERE gateway_id = %s", (gateway_id,))
        gw = cur.fetchone()
        if not gw:
            cur.close(); conn.close()
            raise HTTPException(404, f"Gateway '{gateway_id}' not found")
        if gw['org_id'] != user_org:
            cur.close(); conn.close()
            raise HTTPException(403, "You can only simulate fire on your own organization's gateways")

    now = datetime.now(PH_ZONE).strftime("%Y-%m-%d %H:%M:%S")

    cur.execute("""
        INSERT INTO sensor_readings
        (gateway_id, node_id, timestamp, local_timestamp,
         temperature, humidity, flame, smoke,
         latitude, longitude, rssi, snr, ai_prediction, confidence)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (gateway_id, node_id, now, now,
          52.3, 25, 1, 920,
          16.0435, 120.3351,
          -68, 7.2, 'fire', 0.95))

    upsert_fire_incident(cur, {
        "node_id":        node_id,
        "gateway_id":     gateway_id,
        "ai_prediction":  "fire",
        "confidence":     0.95,
        "temperature":    52.3,
        "humidity":       25,
        "flame":          1,
        "smoke":          920,
        "latitude":       16.0435,
        "longitude":      120.3351,
        "local_timestamp": now,
    })

    conn.commit()
    cur.close(); conn.close()

    await manager.broadcast({
        "type":          "incident_update",
        "node_id":       node_id,
        "gateway_id":    gateway_id,
        "ai_prediction": "fire",
        "confidence":    0.95,
        "timestamp":     now,
        "latitude":      16.0435,
        "longitude":     120.3351,
    })
    await manager.broadcast({
        "type":    "node_update",
        "node_id": node_id,
    })

    return {"message": f"Fire simulated on {node_id} via {gateway_id}", "timestamp": now}


@app.post("/dev/simulate-false")
async def simulate_false(
    node_id: str = "Node1",
    gateway_id: str = "GW1",
    current_user: dict = Depends(get_current_user)
):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)

    is_admin = current_user.get('is_admin') == 1
    user_org = current_user.get('org_id')

    if not is_admin:
        cur.execute("SELECT org_id FROM gateways WHERE gateway_id = %s", (gateway_id,))
        gw = cur.fetchone()
        if not gw:
            cur.close(); conn.close()
            raise HTTPException(404, f"Gateway '{gateway_id}' not found")
        if gw['org_id'] != user_org:
            cur.close(); conn.close()
            raise HTTPException(403, "You can only simulate on your own organization's gateways")

    now = datetime.now(PH_ZONE).strftime("%Y-%m-%d %H:%M:%S")

    cur.execute("""
        INSERT INTO sensor_readings
        (gateway_id, node_id, timestamp, local_timestamp,
         temperature, humidity, flame, smoke,
         latitude, longitude, rssi, snr, ai_prediction, confidence)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (gateway_id, node_id, now, now,
          38.5, 40, 1, 450,
          16.0435, 120.3351,
          -68, 7.2, 'false', 0.55))

    upsert_fire_incident(cur, {
        "node_id":        node_id,
        "gateway_id":     gateway_id,
        "ai_prediction":  "false",
        "confidence":     0.55,
        "temperature":    38.5,
        "humidity":       40,
        "flame":          1,
        "smoke":          450,
        "latitude":       16.0435,
        "longitude":      120.3351,
        "local_timestamp": now,
    })

    conn.commit()
    cur.close(); conn.close()

    await manager.broadcast({
        "type":          "incident_update",
        "node_id":       node_id,
        "gateway_id":    gateway_id,
        "ai_prediction": "false",
        "confidence":    0.55,
        "timestamp":     now,
        "latitude":      16.0435,
        "longitude":     120.3351,
    })
    await manager.broadcast({
        "type":    "node_update",
        "node_id": node_id,
    })

    return {"message": f"'False' prediction simulated on {node_id} via {gateway_id}", "timestamp": now}


@app.post("/dev/simulate-normal")
async def simulate_normal(
    node_id: str = "Node1",
    gateway_id: str = "GW1",
    current_user: dict = Depends(get_current_user)
):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)

    is_admin = current_user.get('is_admin') == 1
    user_org = current_user.get('org_id')

    if not is_admin:
        cur.execute("SELECT org_id FROM gateways WHERE gateway_id = %s", (gateway_id,))
        gw = cur.fetchone()
        if not gw:
            cur.close(); conn.close()
            raise HTTPException(404, f"Gateway '{gateway_id}' not found")
        if gw['org_id'] != user_org:
            cur.close(); conn.close()
            raise HTTPException(403, "You can only simulate on your own organization's gateways")

    now = datetime.now(PH_ZONE).strftime("%Y-%m-%d %H:%M:%S")

    cur.execute("""
        INSERT INTO sensor_readings
        (gateway_id, node_id, timestamp, local_timestamp,
         temperature, humidity, flame, smoke,
         latitude, longitude, rssi, snr, ai_prediction, confidence)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (gateway_id, node_id, now, now,
          24.5, 61, 0, 0,
          16.0435, 120.3351,
          -68, 7.2, 'normal', 0.99))

    upsert_fire_incident(cur, {
        "node_id":        node_id,
        "gateway_id":     gateway_id,
        "ai_prediction":  "normal",
        "local_timestamp": now,
    })

    conn.commit()
    cur.close(); conn.close()

    await manager.broadcast({
        "type":          "incident_update",
        "node_id":       node_id,
        "gateway_id":    gateway_id,
        "ai_prediction": "normal",
        "confidence":    0.99,
        "timestamp":     now,
        "latitude":      16.0435,
        "longitude":     120.3351,
    })
    await manager.broadcast({
        "type":    "node_update",
        "node_id": node_id,
    })

    return {"message": f"Normal reading simulated on {node_id} via {gateway_id}", "timestamp": now}


# ══════════════════════════════════════════════════════════════
#  ORG DROPDOWN
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        log_level="info",
        proxy_headers=True,
        forwarded_allow_ips="*"
    )
=======
>>>>>>> Stashed changes
