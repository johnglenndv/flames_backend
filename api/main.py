from fastapi import FastAPI, Depends, HTTPException, status, WebSocket, WebSocketDisconnect
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr
import mysql.connector
import os
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from jose import JWTError, jwt
from passlib.context import CryptContext
import secrets, string
from typing import List

#-------WEBSOCKET MANAGER START HERE----------------
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            await connection.send_json(message)

manager = ConnectionManager()
#-------WEBSOCKET MANAGER END HERE----------------

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
    invite_code: str | None = None          # optional: admin can set it
    invite_code_expires_days: int | None = 30  # how many days valid
    
class InviteCodeCreate(BaseModel):
    code: str | None = None               # optional: auto-generate if empty
    expires_days: int = 30
    max_uses: int = 1                     # 1 = single-use, 0 = unlimited
    
class AssignOrgBody(BaseModel):
    org_id: int

class OrganizationCreateV2(BaseModel):
    name: str
    invite_code: str | None = None   # permanent code, no expiry
    
#-----DATA MODELS END HERE----------------




app = FastAPI(title="FLAMES API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# JWT settings - CHANGE THIS SECRET KEY IN RAILWAY VARIABLES!
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
    
    
def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password):
    return pwd_context.hash(password)

def create_access_token(data: dict, expires_delta: timedelta | None = None):
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

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
        raise HTTPException(
            status_code=403,
            detail="Admin access required"
        )
    return current_user
#-----UTILITY FUNCTIONS END HERE----------------

#----WEBSOCKET ENDPOINTS START HERE----------------
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()  # keep alive (can be ignored)
    except WebSocketDisconnect:
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

    # Convert timestamp to PH time
    row["display_timestamp"] = convert_to_ph_time(row.get("timestamp"))

    return row

@app.get("/history/{node_id}")
async def get_history(node_id: str, limit: int = 50, current_user: dict = Depends(get_current_user)):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)
    
    cur.execute("""
        SELECT id, node_id, timestamp, temperature, humidity, flame, smoke,
               latitude, longitude, rssi, snr, ai_prediction, confidence
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

@app.get("/nodes")
async def get_all_nodes(current_user: dict = Depends(get_current_user)):
    """
    Returns nodes from sensor_readings, scoped by org:
      - Admins: all nodes (from all orgs)
      - Members: only nodes whose gateway belongs to their org
    """
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)

    is_admin = current_user.get('is_admin') == 1
    user_org_id = current_user.get('org_id')

    if is_admin:
        # All nodes, unfiltered
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
                   (SELECT ai_prediction FROM sensor_readings WHERE node_id = s.node_id ORDER BY id DESC LIMIT 1) AS ai_prediction,
                   (SELECT confidence    FROM sensor_readings WHERE node_id = s.node_id ORDER BY id DESC LIMIT 1) AS confidence,
                   (SELECT timestamp  FROM sensor_readings WHERE node_id = s.node_id ORDER BY id DESC LIMIT 1) AS timestamp
            FROM sensor_readings s
            ORDER BY s.node_id
        """)
    else:
        # Member: only nodes whose most-recent reading has a gateway_id that belongs to their org.
        # If sensor_readings has a gateway_id column, filter on it:
        if not user_org_id:
            cur.close(); conn.close()
            return []
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
                   (SELECT ai_prediction FROM sensor_readings WHERE node_id = s.node_id ORDER BY id DESC LIMIT 1) AS ai_prediction,
                   (SELECT confidence    FROM sensor_readings WHERE node_id = s.node_id ORDER BY id DESC LIMIT 1) AS confidence,
                   (SELECT timestamp  FROM sensor_readings WHERE node_id = s.node_id ORDER BY id DESC LIMIT 1) AS timestamp
            FROM sensor_readings s
            INNER JOIN gateways g ON g.gateway_id = s.gateway_id
            WHERE g.org_id = %s
            ORDER BY s.node_id
        """, (user_org_id,))

    nodes = cur.fetchall()
    cur.close()
    conn.close()

    ph_nodes = []
    for row in nodes:
        row_copy = row.copy()
        row_copy["display_timestamp"] = convert_to_ph_time(row_copy.get("timestamp")) if row_copy.get("timestamp") else "N/A"
        ph_nodes.append(row_copy)

    return ph_nodes

@app.post("/organizations")
async def create_organization(
    org: OrganizationCreateV2,
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
    cur.close(); conn.close()

    return {"message": "Organization created", "id": new_id, "name": org.name, "invite_code": org.invite_code}
    
class InviteCodeAdminCreate(BaseModel):
    org_id: int
    code: str | None = None
    expires_days: int = 30
    max_uses: int = 1

@app.post("/admin/invite-codes")
async def admin_create_invite_code(
    invite: InviteCodeAdminCreate,
    current_user: dict = Depends(get_current_user), 
    _admin: dict = Depends(admin_required)
):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)

    # Verify org exists
    cur.execute("SELECT id, name FROM organizations WHERE id = %s", (invite.org_id,))
    org = cur.fetchone()
    if not org:
        cur.close()
        conn.close()
        raise HTTPException(404, "Organization not found")

    code = invite.code
    if not code:
        alphabet = string.ascii_uppercase + string.digits
        code = ''.join(secrets.choice(alphabet) for _ in range(8))

    expires = datetime.now() + timedelta(days=invite.expires_days) if invite.expires_days else None

    cur.execute("""
        INSERT INTO invite_codes 
        (org_id, code, expires_at, max_uses, created_by)
        VALUES (%s, %s, %s, %s, %s)
    """, (invite.org_id, code, expires, invite.max_uses, current_user['id']))

    conn.commit()
    new_id = cur.lastrowid
    cur.close()
    conn.close()

    return {
        "message": "Admin created invite code",
        "code": code,
        "organization_id": invite.org_id,
        "organization_name": org['name'],
        "expires_at": expires.isoformat() if expires else None,
        "max_uses": invite.max_uses
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

    # Verify org exists
    cur.execute("SELECT id, name FROM organizations WHERE id = %s", (org_id,))
    org = cur.fetchone()
    if not org:
        cur.close()
        conn.close()
        raise HTTPException(404, "Organization not found")

    code = ''.join(secrets.choice(string.ascii_uppercase + string.digits) for _ in range(8))
    expires = datetime.now() + timedelta(days=invite.expires_days) if invite.expires_days else None

    cur.execute("""
        INSERT INTO invite_codes (org_id, code, expires_at, max_uses, created_by)
        VALUES (%s, %s, %s, %s, %s)
    """, (org_id, code, expires, invite.max_uses, current_user['id']))

    conn.commit()
    cur.close()
    conn.close()

    return {
        "code": code,
        "organization_id": org_id,
        "expires_at": expires.isoformat() if expires else None,
        "max_uses": invite.max_uses
    }

@app.get("/me")
async def get_current_user_info(current_user: dict = Depends(get_current_user)):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)
    
    cur.execute("""
        SELECT 
            u.id, u.username, u.email, u.created_at,
            u.is_admin,
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
        "user_id": full_user["id"],
        "username": full_user["username"],
        "email": full_user.get("email"),
        "created_at": full_user["created_at"].isoformat() if full_user["created_at"] else None,
        "is_admin": bool(full_user["is_admin"]),
        "organization_id": full_user["org_id"],
        "organization_name": full_user["organization_name"] or "—"
    }
    
@app.get("/gateways")
async def get_gateways(
    org_id: int | None = None,
    current_user: dict = Depends(get_current_user)
):
    """
    Returns gateways visible to the current user:
      - Admins: all gateways (optionally filtered by ?org_id=X for admin panel)
      - Members: only gateways belonging to their own organization
    """
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)

    is_admin = current_user.get('is_admin') == 1
    user_org_id = current_user.get('org_id')

    if is_admin:
        # Admin can see all, or filter by a specific org for the admin panel table
        if org_id is not None:
            cur.execute("""
                SELECT gateway_id, org_id, location_name, latitude, longitude, created_at
                FROM gateways WHERE org_id = %s ORDER BY gateway_id
            """, (org_id,))
        else:
            cur.execute("""
                SELECT g.gateway_id, g.org_id, g.location_name, g.latitude, g.longitude,
                       g.created_at, o.name AS org_name
                FROM gateways g
                LEFT JOIN organizations o ON g.org_id = o.id
                ORDER BY g.gateway_id
            """)
    else:
        # Member: only their org's gateways
        if not user_org_id:
            cur.close(); conn.close()
            return []
        cur.execute("""
            SELECT gateway_id, org_id, location_name, latitude, longitude, created_at
            FROM gateways WHERE org_id = %s ORDER BY gateway_id
        """, (user_org_id,))

    gateways = cur.fetchall()
    cur.close(); conn.close()
    return gateways

#-----API ENDPOINTS ENdPOINTS END HERE----------------

#---PATCH START HERE----
@app.patch("/gateways/{gateway_id}/assign-org")
async def assign_gateway_to_org(
    gateway_id: str,
    body: AssignOrgBody,
    current_user: dict = Depends(get_current_user),
    _admin: dict = Depends(admin_required)
):
    """Reassign a gateway to a specific organization."""
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)

    # Verify org exists
    cur.execute("SELECT id FROM organizations WHERE id = %s", (body.org_id,))
    if not cur.fetchone():
        cur.close(); conn.close()
        raise HTTPException(404, "Organization not found")

    cur.execute("UPDATE gateways SET org_id = %s WHERE gateway_id = %s", (body.org_id, gateway_id))
    if cur.rowcount == 0:
        cur.close(); conn.close()
        raise HTTPException(404, f"Gateway '{gateway_id}' not found")

    conn.commit()
    cur.close(); conn.close()
    return {"message": f"Gateway '{gateway_id}' assigned to org {body.org_id}"}
#---PATCH END HERE----

#---DELETION----
# DELETE user
@app.delete("/users/{user_id}")
async def delete_user(user_id: int, current_user: dict = Depends(admin_required)):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("DELETE FROM users WHERE id = %s", (user_id,))
    if cur.rowcount == 0:
        cur.close()
        conn.close()
        raise HTTPException(status_code=404, detail="User not found")
    conn.commit()
    cur.close()
    conn.close()
    return {"message": "User deleted successfully"}

# DELETE gateway
@app.delete("/gateways/{gateway_id}")
async def delete_gateway(gateway_id: str, current_user: dict = Depends(admin_required)):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("DELETE FROM gateways WHERE gateway_id = %s", (gateway_id,))
    if cur.rowcount == 0:
        cur.close()
        conn.close()
        raise HTTPException(status_code=404, detail="Gateway not found")
    conn.commit()
    cur.close()
    conn.close()
    return {"message": "Gateway deleted successfully"}

# DELETE invite code (by code or ID)
@app.delete("/invite-codes/{code}")
async def delete_invite_code(code: str, current_user: dict = Depends(admin_required)):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("DELETE FROM invite_codes WHERE code = %s", (code,))
    if cur.rowcount == 0:
        cur.close()
        conn.close()
        raise HTTPException(status_code=404, detail="Invite code not found")
    conn.commit()
    cur.close()
    conn.close()
    return {"message": "Invite code deleted"}

# DELETE organization (dangerous — cascades or blocks if users exist)
@app.delete("/organizations/{org_id}")
async def delete_organization(org_id: int, current_user: dict = Depends(admin_required)):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    # Optional safety: check if org has users
    cur.execute("SELECT COUNT(*) FROM users WHERE org_id = %s", (org_id,))
    if cur.fetchone()[0] > 0:
        cur.close()
        conn.close()
        raise HTTPException(status_code=400, detail="Cannot delete organization with active users")

    cur.execute("DELETE FROM organizations WHERE id = %s", (org_id,))
    if cur.rowcount == 0:
        cur.close()
        conn.close()
        raise HTTPException(status_code=404, detail="Organization not found")
    
    conn.commit()
    cur.close()
    conn.close()
    return {"message": "Organization deleted"}
#--------------------DELETION END HERE-------------------

#-------Machine Learning Inference Endpoint (called by worker)----------------
@app.get("/incidents/active")
async def get_active_incidents(current_user: dict = Depends(get_current_user)):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)
    
    # Get the MOST RECENT reading for each node
    # Then filter / classify based on ai_prediction + confidence
    cur.execute("""
        SELECT 
            sr.node_id,
            sr.timestamp,
            sr.temperature,
            sr.humidity,
            sr.flame,
            sr.smoke,
            sr.latitude,
            sr.longitude,
            sr.ai_prediction,
            sr.confidence,
            -- Optional: derive friendly location name later
            CONCAT('Node ', sr.node_id) AS location_name
        FROM sensor_readings sr
        INNER JOIN (
            SELECT node_id, MAX(id) AS max_id
            FROM sensor_readings
            GROUP BY node_id
        ) latest ON sr.node_id = latest.node_id AND sr.id = latest.max_id
        WHERE sr.ai_prediction = 'fire' 
           OR (sr.ai_prediction = 'FALSE' AND sr.confidence < 0.3)  -- example filter
        ORDER BY sr.timestamp DESC
        LIMIT 20   -- prevent sending too many at once
    """)
    
    rows = cur.fetchall()
    cur.close()
    conn.close()

    incidents = []
    for row in rows:
        confidence_pct = float(row['confidence']) if row['confidence'] is not None else 0.0
        pred = row['ai_prediction']

        # You can tune these rules – this is just an example
        if pred == 'fire':
            if confidence_pct >= 0.70:
                status = "Active"
                status_class = "txt-active"
            elif confidence_pct >= 0.40:
                status = "Possible"
                status_class = "txt-contained"
            else:
                status = "Low Confidence"
                status_class = "txt-resolved"
        else:
            status = "Normal"
            status_class = "txt-resolved"

        incidents.append({
            "node_id": row["node_id"],
            "location_name": row["location_name"],
            "latitude": row["latitude"],
            "longitude": row["longitude"],
            "ai_prediction": pred,
            "confidence": confidence_pct,
            "confidence_pct": round(confidence_pct * 100, 1),
            "status": status,
            "status_class": status_class,
            "temperature": row["temperature"],
            "smoke": row["smoke"],
            "flame": row["flame"],
            "timestamp": convert_to_ph_time(row["timestamp"]),
            "assigned_team": None   # ← add logic later if you have teams table
        })

    return incidents

#---------This endpoint is called by the worker after AI inference to store results and notify frontend via WebSocket.---------

# ── Pinned Locations ───────────────────────────────────────────────

class PinCreate(BaseModel):
    name: str | None = None
    latitude: float
    longitude: float

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
    cur.close()
    conn.close()
    
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
    cur.close()
    conn.close()
    
    return {"message": "Pin saved", "id": new_id}

# Optional: delete pin
@app.delete("/me/pins/{pin_id}")
async def delete_pin(pin_id: int, current_user: dict = Depends(get_current_user)):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    cur.execute("""
        DELETE FROM user_pinned_locations 
        WHERE id = %s AND user_id = %s
    """, (pin_id, current_user['id']))
    
    if cur.rowcount == 0:
        cur.close()
        conn.close()
        raise HTTPException(404, "Pin not found or not owned by you")
    
    conn.commit()
    cur.close()
    conn.close()
    
    return {"message": "Pin deleted"}
#---------END OF PIN ENDPOINTS----------------
    
#----------LOGIN SIGNUP STARTS HERE---------------
@app.post("/signup")
async def signup(user: UserCreate):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)

    # 1. Try to find organization by invite code in organizations table (old method)
    cur.execute("""
        SELECT id, name FROM organizations 
        WHERE invite_code = %s 
        AND (invite_code_expires IS NULL OR invite_code_expires > NOW())
    """, (user.invite_code,))
    org = cur.fetchone()

    org_id = None

    if org:
        # Found in organizations table — classic method
        org_id = org['id']
        print(f"Signup using organization invite code: {user.invite_code}")
    else:
        # 2. Not found → check invite_codes table (new method)
        cur.execute("""
            SELECT org_id, code, expires_at, max_uses, uses 
            FROM invite_codes 
            WHERE code = %s 
            AND (expires_at IS NULL OR expires_at > NOW())
        """, (user.invite_code,))
        invite = cur.fetchone()

        if invite:
            # Code found — check usage limit
            if invite['max_uses'] > 0 and invite['uses'] >= invite['max_uses']:
                cur.close()
                conn.close()
                raise HTTPException(status_code=400, detail="Invite code has reached maximum uses")

            org_id = invite['org_id']
            print(f"Signup using invite_codes code: {user.invite_code} (org_id {org_id})")

            # Increment uses count
            cur.execute("""
                UPDATE invite_codes 
                SET uses = uses + 1 
                WHERE code = %s
            """, (user.invite_code,))
            conn.commit()
        else:
            cur.close()
            conn.close()
            raise HTTPException(status_code=400, detail="Invalid or expired invite code")

    # Proceed only if we found a valid org_id
    if not org_id:
        raise HTTPException(status_code=400, detail="Invalid or expired invite code")

    # Check uniqueness of username/email
    cur.execute("SELECT id FROM users WHERE username = %s OR (email = %s AND email IS NOT NULL)", 
                (user.username, user.email))
    if cur.fetchone():
        cur.close()
        conn.close()
        raise HTTPException(status_code=400, detail="Username or email already taken")

    hashed = get_password_hash(user.password)

    cur.execute("""
        INSERT INTO users (username, email, password_hash, org_id)
        VALUES (%s, %s, %s, %s)
    """, (user.username, user.email, hashed, org_id))

    conn.commit()
    cur.close()
    conn.close()

    return {"message": "Account created", "organization_id": org_id}

@app.post("/login", response_model=Token)
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM users WHERE username = %s", (form_data.username,))
    user = cur.fetchone()
    cur.close()
    conn.close()

    if not user or not verify_password(form_data.password, user["password_hash"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    access_token = create_access_token(data={"sub": user["username"]})

    return {"access_token": access_token, "token_type": "bearer"}

@app.post("/gateways")
async def register_gateway(gateway: GatewayCreate, current_user: dict = Depends(get_current_user), _admin: dict = Depends(admin_required)):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)

    # Check if gateway_id already exists
    cur.execute("SELECT id FROM gateways WHERE gateway_id = %s", (gateway.gateway_id,))
    if cur.fetchone():
        cur.close()
        conn.close()
        raise HTTPException(status_code=400, detail="Gateway ID already registered")

    # Optional: check if current user is in an organization that can register gateways
    # For now: any logged-in user can register (demo-friendly)

    cur.execute("""
        INSERT INTO gateways (gateway_id, org_id, location_name, latitude, longitude, created_by)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        gateway.gateway_id,
        current_user['org_id'],           # ← assigns to the user's own organization
        gateway.location_name,
        gateway.latitude,
        gateway.longitude,
        current_user['id']
    ))

    conn.commit()
    new_id = cur.lastrowid
    cur.close()
    conn.close()

    return {
        "message": "Gateway registered successfully",
        "gateway_id": gateway.gateway_id,
        "organization_id": current_user['org_id']
    }
    
@app.post("/organizations")
async def create_organization(org: OrganizationCreate, current_user: dict = Depends(get_current_user), _admin: dict = Depends(admin_required)):
    # Optional: restrict to admins only (add is_admin column later)
    # For now: any logged-in user can create orgs (demo-friendly)

    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)

    # Check if org name already exists
    cur.execute("SELECT id FROM organizations WHERE name = %s", (org.name,))
    if cur.fetchone():
        cur.close()
        conn.close()
        raise HTTPException(status_code=400, detail="Organization name already exists")

    expires = None
    if org.invite_code_expires_days:
        expires = datetime.now() + timedelta(days=org.invite_code_expires_days)

    cur.execute("""
        INSERT INTO organizations (name, invite_code, invite_code_expires, created_by)
        VALUES (%s, %s, %s, %s)
    """, (org.name, org.invite_code, expires, current_user['id']))

    conn.commit()
    new_id = cur.lastrowid
    cur.close()
    conn.close()

    return {
        "message": "Organization created",
        "id": new_id,
        "name": org.name,
        "invite_code": org.invite_code
    }

@app.get("/organizations", response_model=list[Organization])
async def get_organizations():
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT id, name, invite_code FROM organizations ORDER BY name")
    orgs = cur.fetchall()
    cur.close()
    conn.close()
    return orgs
#--------------------LOGIN SIGNUP ENDS HERE-------------------

@app.post("/notify-new-data")
async def notify_new_data(data: dict):
    await manager.broadcast(data)
    return {"status": "broadcasted"}


# at the very bottom
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(
        "main:app",               # or whatever your file is called
        host="0.0.0.0",
        port=port,
        log_level="info",
        proxy_headers=True,       # ← important for Railway / proxies
        forwarded_allow_ips="*"   # ← very important on Railway
    )