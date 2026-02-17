from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr
import mysql.connector
import os
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from jose import JWTError, jwt
from passlib.context import CryptContext


class UserCreate(BaseModel):
    username: str
    email: EmailStr | None = None
    password: str

class Token(BaseModel):
    access_token: str
    token_type: str

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

@app.post("/signup")
async def signup(user: UserCreate):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)

    # Check if username or email exists
    cur.execute("SELECT id FROM users WHERE username = %s OR (email = %s AND email IS NOT NULL)", (user.username, user.email))
    if cur.fetchone():
        cur.close()
        conn.close()
        raise HTTPException(status_code=400, detail="Username or email already registered")

    hashed_password = get_password_hash(user.password)

    cur.execute("""
        INSERT INTO users (username, email, password_hash)
        VALUES (%s, %s, %s)
    """, (user.username, user.email, hashed_password))

    conn.commit()
    cur.close()
    conn.close()

    return {"message": "User created successfully"}

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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))