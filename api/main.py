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

@app.get("/nodes")
async def get_all_nodes(current_user: dict = Depends(get_current_user)):
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

@app.post("/organizations/{org_id}/invite-codes")
async def create_invite_code(
    org_id: int,
    invite: InviteCodeCreate,
    current_user: dict = Depends(get_current_user), 
    _admin: dict = Depends(admin_required)
):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)

    # Check user belongs to this org
    if current_user['org_id'] != org_id:
        raise HTTPException(403, "You can only create codes for your own organization")

    # Check org exists
    cur.execute("SELECT id FROM organizations WHERE id = %s", (org_id,))
    if not cur.fetchone():
        raise HTTPException(404, "Organization not found")

    # Auto-generate code if none provided (simple random string)
    alphabet = string.ascii_uppercase + string.digits  # A-Z + 0-9
    code = ''.join(secrets.choice(alphabet) for _ in range(8))  # e.g. "K7N4P8X2"

    expires = datetime.now() + timedelta(days=invite.expires_days)

    cur.execute("""
        INSERT INTO invite_codes (org_id, code, expires_at, max_uses, created_by)
        VALUES (%s, %s, %s, %s, %s)
    """, (org_id, code, expires, invite.max_uses, current_user['id']))

    conn.commit()
    new_id = cur.lastrowid
    cur.close()
    conn.close()

    return {
        "message": "Invite code created",
        "code": code,
        "expires_at": expires.isoformat(),
        "max_uses": invite.max_uses
    }

@app.get("/me")
async def get_current_user_info(current_user: dict = Depends(get_current_user)):
    return {
        "username": current_user["username"],
        "email": current_user.get("email"),
        "user_id": current_user["id"]
    }

#-----API ENDPOINTS ENdPOINTS END HERE----------------

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
    
#----------LOGIN SIGNUP STARTS HERE---------------
@app.post("/signup")
async def signup(user: UserCreate):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)

    # Validate invite code
    cur.execute("""
        SELECT id, name FROM organizations 
        WHERE invite_code = %s 
        AND (invite_code_expires IS NULL OR invite_code_expires > NOW())
    """, (user.invite_code,))
    org = cur.fetchone()

    if not org:
        cur.close()
        conn.close()
        raise HTTPException(status_code=400, detail="Invalid or expired invite code")

    # Check uniqueness
    cur.execute("SELECT id FROM users WHERE username = %s OR (email = %s AND email IS NOT NULL)", (user.username, user.email))
    if cur.fetchone():
        cur.close()
        conn.close()
        raise HTTPException(status_code=400, detail="Username or email already taken")

    hashed = get_password_hash(user.password)

    cur.execute("""
        INSERT INTO users (username, email, password_hash, org_id)
        VALUES (%s, %s, %s, %s)
    """, (user.username, user.email, hashed, org['id']))

    conn.commit()
    cur.close()
    conn.close()

    return {"message": "Account created", "organization": org['name']}

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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))