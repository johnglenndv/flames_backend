import time
import spidev
import lgpio
import json
import ssl
import paho.mqtt.client as mqtt
from datetime import datetime, timezone, timedelta

# ==============================
# CONFIG
# ==============================
PH_TZ = timezone(timedelta(hours=8))

GATEWAY_ID = "GW1"

# HiveMQ Cloud
MQTT_BROKER = "528e719c19214a19b07c0e7322298267.s1.eu.hivemq.cloud"
MQTT_PORT   = 8883
MQTT_TOPIC  = "lora/uplink"

MQTT_USERNAME = "Uplink01"
MQTT_PASSWORD = "Uplink01"

# GPIO / SPI
RESET_PIN = 25
SPI_BUS = 0
SPI_CS  = 0

# ==============================
# SX1278 Registers
# ==============================
REG_FIFO                 = 0x00
REG_OP_MODE              = 0x01
REG_FRF_MSB              = 0x06
REG_FRF_MID              = 0x07
REG_FRF_LSB              = 0x08
REG_PA_CONFIG            = 0x09
REG_LNA                  = 0x0C
REG_FIFO_ADDR_PTR        = 0x0D
REG_FIFO_TX_BASE_ADDR    = 0x0E
REG_FIFO_RX_BASE_ADDR    = 0x0F
REG_FIFO_RX_CURRENT_ADDR = 0x10
REG_IRQ_FLAGS            = 0x12
REG_RX_NB_BYTES          = 0x13
REG_PKT_SNR_VALUE        = 0x19
REG_PKT_RSSI_VALUE       = 0x1A
REG_MODEM_CONFIG_1       = 0x1D
REG_MODEM_CONFIG_2       = 0x1E
REG_PREAMBLE_MSB         = 0x20
REG_PREAMBLE_LSB         = 0x21
REG_PAYLOAD_LENGTH       = 0x22
REG_MODEM_CONFIG_3       = 0x26
REG_SYNC_WORD            = 0x39
REG_VERSION              = 0x42

IRQ_RX_DONE           = 0x40
IRQ_TX_DONE           = 0x08
IRQ_PAYLOAD_CRC_ERROR = 0x20

# ==============================
# SPI + GPIO setup
# ==============================
spi = spidev.SpiDev()
spi.open(SPI_BUS, SPI_CS)
spi.max_speed_hz = 5_000_000
spi.mode = 0

gpio = lgpio.gpiochip_open(0)
lgpio.gpio_claim_output(gpio, RESET_PIN)

# ==============================
# Helpers
# ==============================
def write_reg(a, v):
    spi.xfer2([a | 0x80, v & 0xFF])

def read_reg(a):
    return spi.xfer2([a & 0x7F, 0])[1]

def reset_lora():
    lgpio.gpio_write(gpio, RESET_PIN, 0)
    time.sleep(0.1)
    lgpio.gpio_write(gpio, RESET_PIN, 1)
    time.sleep(0.1)

def set_frequency(freq):
    frf = int(freq / 61.03515625)
    write_reg(REG_FRF_MSB, frf >> 16)
    write_reg(REG_FRF_MID, frf >> 8)
    write_reg(REG_FRF_LSB, frf)

def rssi_dbm():
    return read_reg(REG_PKT_RSSI_VALUE) - 164

def snr_db():
    v = read_reg(REG_PKT_SNR_VALUE)
    return (v - 256 if v & 0x80 else v) / 4

# ==============================
# MQTT (HiveMQ Cloud)
# ==============================
def on_connect(client, userdata, flags, rc):
    # rc=0 means success
    print("✅ MQTT connected (rc =", rc, ")")

mqttc = mqtt.Client(client_id=f"gateway-{GATEWAY_ID}", clean_session=True)
mqttc.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
mqttc.tls_set(cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLSv1_2)
mqttc.tls_insecure_set(False)
mqttc.on_connect = on_connect
mqttc.connect(MQTT_BROKER, MQTT_PORT, 60)
mqttc.loop_start()

# ==============================
# SEND ACK (FAST PATH)
# ==============================
def send_ack(node: str):
    # Keep this fast and simple (your proven working one)
    msg = f"ACK:{node}".encode("utf-8")

    write_reg(REG_OP_MODE, 0x81)  # standby
    time.sleep(0.005)

    write_reg(REG_FIFO_TX_BASE_ADDR, 0x00)
    write_reg(REG_FIFO_ADDR_PTR, 0x00)

    for b in msg:
        write_reg(REG_FIFO, b)

    write_reg(REG_PAYLOAD_LENGTH, len(msg))
    write_reg(REG_IRQ_FLAGS, 0xFF)
    write_reg(REG_OP_MODE, 0x83)  # TX

    t0 = time.time()
    while time.time() - t0 < 0.8:
        if read_reg(REG_IRQ_FLAGS) & IRQ_TX_DONE:
            break

    write_reg(REG_IRQ_FLAGS, 0xFF)
    write_reg(REG_OP_MODE, 0x85)  # back to RX continuous

# ==============================
# INIT RADIO
# ==============================
reset_lora()

write_reg(REG_OP_MODE, 0x80)  # sleep
time.sleep(0.05)
write_reg(REG_OP_MODE, 0x81)  # standby
time.sleep(0.05)

ver = read_reg(REG_VERSION)
print(f"✅ SX1278 REG_VERSION = 0x{ver:02X}")

set_frequency(433_000_000)

write_reg(REG_PA_CONFIG, 0x8F)
write_reg(REG_LNA, 0x23)

write_reg(REG_FIFO_RX_BASE_ADDR, 0x00)
write_reg(REG_FIFO_ADDR_PTR, 0x00)

write_reg(REG_MODEM_CONFIG_1, 0x72)  # BW125 CR4/5
write_reg(REG_MODEM_CONFIG_2, 0xC4)  # SF12 CRC ON
write_reg(REG_MODEM_CONFIG_3, 0x0C)  # LDRO + AGC
write_reg(REG_SYNC_WORD, 0x12)

write_reg(REG_IRQ_FLAGS, 0xFF)
write_reg(REG_OP_MODE, 0x85)  # RX continuous

print("\n📡 Gateway RX READY (ACK + MQTT, ACK-first)\n")

# ==============================
# MAIN LOOP
# ==============================
try:
    while True:
        irq = read_reg(REG_IRQ_FLAGS)

        if not irq:
            time.sleep(0.01)
            continue

        write_reg(REG_IRQ_FLAGS, irq)

        if irq & IRQ_PAYLOAD_CRC_ERROR:
            print("⚠️ CRC error — dropped")
            continue

        if irq & IRQ_RX_DONE:
            length = read_reg(REG_RX_NB_BYTES)
            fifo_addr = read_reg(REG_FIFO_RX_CURRENT_ADDR)
            write_reg(REG_FIFO_ADDR_PTR, fifo_addr)

            payload = bytes(read_reg(REG_FIFO) for _ in range(length))
            raw = payload.decode(errors="replace")

            if not raw.startswith("{") or not raw.endswith("}"):
                print("⚠️ Non-JSON — dropped")
                continue

            # Minimal parse ONLY to extract node for ACK
            try:
                node_json = json.loads(raw)
                node = node_json.get("node")
                if not node:
                    continue
            except:
                continue

            # Capture RF metrics immediately (before any slow ops)
            r = rssi_dbm()
            s = snr_db()
            ts = datetime.now(PH_TZ).isoformat()

            # ===== FAST PATH: ACK FIRST =====
            send_ack(node)

            # ===== Logging =====
            print(f"📥 RX | Node {node} | RSSI {r} dBm | SNR {s:.2f}")
            print(f"RAW JSON: {raw}")
            print(f"📤 ACK:{node}")

            # ===== SLOW PATH: MQTT AFTER ACK =====
            wrapper = {
                "gateway": GATEWAY_ID,
                "rssi": r,
                "snr": s,
                "received_at": ts,
                "payload": node_json
            }
            mqtt_payload = json.dumps(wrapper)

            mqttc.publish(MQTT_TOPIC, mqtt_payload)

            print(f"📡 MQTT pub → topic={MQTT_TOPIC}")
            print(f"MQTT JSON: {mqtt_payload}\n")

except KeyboardInterrupt:
    print("\n🛑 Exiting")

finally:
    try:
        mqttc.loop_stop()
    except:
        pass
    try:
        lgpio.gpiochip_close(gpio)
    except:
        pass
    try:
        spi.close()
    except:
        pass
