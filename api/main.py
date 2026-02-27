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
from typing import List, Optional

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

#-----DATA MODELS END HERE----------------


app = FastAPI(title="FLAMES API")

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

#----WEBSOCKET ENDPOINTS START HERE----------------
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
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
        (SELECT temperature   FROM sensor_readings WHERE node_id = s.node_id ORDER BY id DESC LIMIT 1) AS temperature,
        (SELECT humidity      FROM sensor_readings WHERE node_id = s.node_id ORDER BY id DESC LIMIT 1) AS humidity,
        (SELECT flame         FROM sensor_readings WHERE node_id = s.node_id ORDER BY id DESC LIMIT 1) AS flame,
        (SELECT smoke         FROM sensor_readings WHERE node_id = s.node_id ORDER BY id DESC LIMIT 1) AS smoke,
        (SELECT latitude      FROM sensor_readings WHERE node_id = s.node_id ORDER BY id DESC LIMIT 1) AS latitude,
        (SELECT longitude     FROM sensor_readings WHERE node_id = s.node_id ORDER BY id DESC LIMIT 1) AS longitude,
        (SELECT rssi          FROM sensor_readings WHERE node_id = s.node_id ORDER BY id DESC LIMIT 1) AS rssi,
        (SELECT snr           FROM sensor_readings WHERE node_id = s.node_id ORDER BY id DESC LIMIT 1) AS snr,
        (SELECT timestamp     FROM sensor_readings WHERE node_id = s.node_id ORDER BY id DESC LIMIT 1) AS timestamp
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
                FROM gateways
                WHERE org_id = %s
                ORDER BY gateway_id
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
            FROM gateways
            WHERE org_id = %s
            ORDER BY gateway_id
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


@app.get("/incidents/active")
async def get_active_incidents(current_user: dict = Depends(get_current_user)):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)

    is_admin = current_user.get('is_admin') == 1
    user_org = current_user.get('org_id')

    # Pure AI judgment: only show nodes whose LATEST reading was predicted as 'fire'.
    # No manual confidence thresholds here — the AI model already decided via the worker.
    # The confidence value is used only for display (coloring the badge), not for filtering.
    if is_admin:
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
                CONCAT('Node ', sr.node_id) AS location_name
            FROM sensor_readings sr
            INNER JOIN (
                SELECT node_id, MAX(id) AS max_id
                FROM sensor_readings
                GROUP BY node_id
            ) latest ON sr.node_id = latest.node_id AND sr.id = latest.max_id
            WHERE LOWER(sr.ai_prediction) = 'fire'
            ORDER BY sr.timestamp DESC
            LIMIT 20
        """)
    else:
        if not user_org:
            cur.close(); conn.close()
            return []
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
                CONCAT('Node ', sr.node_id) AS location_name
            FROM sensor_readings sr
            INNER JOIN (
                SELECT sr2.node_id, MAX(sr2.id) AS max_id
                FROM sensor_readings sr2
                INNER JOIN gateways g ON g.gateway_id = sr2.gateway_id
                WHERE g.org_id = %s
                GROUP BY sr2.node_id
            ) latest ON sr.node_id = latest.node_id AND sr.id = latest.max_id
            WHERE LOWER(sr.ai_prediction) = 'fire'
            ORDER BY sr.timestamp DESC
            LIMIT 20
        """, (user_org,))

    rows = cur.fetchall()
    cur.close()
    conn.close()

    incidents = []
    for row in rows:
        confidence_val = float(row['confidence']) if row['confidence'] is not None else 0.0
        # Normalize: model stores confidence as 0.0–1.0
        confidence_pct = confidence_val if confidence_val <= 1.0 else confidence_val / 100.0

        # Status label is purely for display — reflects how confident the AI was,
        # but the incident is shown regardless because AI said 'fire'
        if confidence_pct >= 0.70:
            inc_status, status_class = "Active",    "txt-active"
        elif confidence_pct >= 0.40:
            inc_status, status_class = "Possible",  "txt-contained"
        else:
            inc_status, status_class = "Detected",  "txt-contained"

        incidents.append({
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
            "smoke":          row["smoke"],
            "flame":          row["flame"],
            "timestamp":      convert_to_ph_time(row["timestamp"]),
            "assigned_team":  None,
        })
    return incidents


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
async def register_gateway(gateway: GatewayCreate, current_user: dict = Depends(get_current_user), _admin: dict = Depends(admin_required)):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT id FROM gateways WHERE gateway_id = %s", (gateway.gateway_id,))
    if cur.fetchone():
        cur.close(); conn.close()
        raise HTTPException(status_code=400, detail="Gateway ID already registered")
    cur.execute("""
        INSERT INTO gateways (gateway_id, org_id, location_name, latitude, longitude, created_by)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (gateway.gateway_id, current_user['org_id'], gateway.location_name, gateway.latitude, gateway.longitude, current_user['id']))
    conn.commit()
    new_id = cur.lastrowid
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
    
    
#---simulation for fire----
@app.post("/dev/simulate-fire")
async def simulate_fire(
    node_id: str = "Node1",
    gateway_id: str = "GW1",
    current_user: dict = Depends(get_current_user)  # any logged-in user
):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)

    # Optional but good: verify the gateway actually belongs to the user's org
    # so a member can't simulate fire on another org's gateway
    is_admin = current_user.get('is_admin') == 1
    user_org = current_user.get('org_id')

    if not is_admin:
        cur.execute(
            "SELECT org_id FROM gateways WHERE gateway_id = %s", (gateway_id,)
        )
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
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        gateway_id, node_id, now, now,
        52.3, 25, 1, 920,
        16.0435, 120.3351,
        -68, 7.2,
        'fire', 0.95
    ))
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

    return {"message": f"Fire simulated on {node_id} via {gateway_id}", "timestamp": now}

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
        cur.execute(
            "SELECT org_id FROM gateways WHERE gateway_id = %s", (gateway_id,)
        )
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
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        gateway_id, node_id, now, now,
        24.5, 61, 0, 0,        # normal room temp, no flame, no smoke
        16.0435, 120.3351,
        -68, 7.2,
        'normal', 0.99
    ))
    conn.commit()
    cur.close(); conn.close()

    await manager.broadcast({
        "type":          "new_reading",
        "node_id":       node_id,
        "gateway_id":    gateway_id,
        "ai_prediction": "normal",
        "confidence":    0.99,
        "timestamp":     now,
        "latitude":      16.0435,
        "longitude":     120.3351,
    })

    return {"message": f"Normal reading simulated on {node_id} via {gateway_id}", "timestamp": now}

#----end of simulation endpoints----