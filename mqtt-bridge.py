#!/usr/bin/env python3
"""
HiveOS MQTT Bridge for Home Assistant discovery.

Features:
- Publishes HA discovery payload for buttons + sensors.
- Publishes sensor updates periodically from /var/run/hive/last_stat.json and gpu-stats.json.
- Subscribes to command topics to run local commands (miner start/stop/restart, reboot, shutdown).
- LWT + availability handling.
- Robust paho-mqtt callback handling for multiple callback API versions.
- CPU load as 3-value string (no %), Memory sensors renamed, Bridge uptime sensor added.
"""

import os
import sys
import time
import json
import yaml
import logging
import subprocess
from pathlib import Path
from threading import Event, Thread
from typing import Any, Dict, List, Optional

import paho.mqtt.client as mqtt

# ---------- Constants & paths ----------
SCRIPT_DIR = Path(__file__).parent
CONFIG_PATH = SCRIPT_DIR / "config.yaml"
HIVE_DIR = Path("/var/run/hive")
LAST_STAT = HIVE_DIR / "last_stat.json"
GPU_STATS = HIVE_DIR / "gpu-stats.json"
GPU_DETECT = HIVE_DIR / "gpu-detect.json"
CUR_MINER = HIVE_DIR / "cur_miner"
MINER_STATUS = HIVE_DIR / "miner_status.1"
START_TIME = time.time()

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format="[%(asctime)s] [%(levelname)s] %(message)s")
logger = logging.getLogger("HiveOS-MQTT-Bridge")

# ---------- Load config ----------
if not CONFIG_PATH.exists():
    logger.error("Config not found: %s", CONFIG_PATH)
    sys.exit(1)

with open(CONFIG_PATH) as f:
    cfg = yaml.safe_load(f) or {}

MQTT_CONF = cfg.get("mqtt", {})
DEVICE_CONF = cfg.get("device", {})
BRIDGE_CONF = cfg.get("bridge", {})
CMDS = cfg.get("commands", {})

# MQTT values with defaults
MQTT_HOST = MQTT_CONF.get("host", "127.0.0.1")
MQTT_PORT = int(MQTT_CONF.get("port", 1883))
MQTT_USER = MQTT_CONF.get("username")
MQTT_PASS = MQTT_CONF.get("password")
BASE_TOPIC = MQTT_CONF.get("base_topic", "hiveos/rig")
DISC_PREFIX = MQTT_CONF.get("discovery_prefix", "homeassistant")
AVAIL_TOPIC = MQTT_CONF.get("availability_topic", f"{BASE_TOPIC}/availability")
CLIENT_ID = MQTT_CONF.get("client_id", f"hiveos-mqtt-bridge-{DEVICE_CONF.get('identifier','rig')}")
KEEPALIVE = int(MQTT_CONF.get("keepalive", 60))

STATS_INTERVAL = int(BRIDGE_CONF.get("stats_interval", 15))
DISCOVERY_RETAINED = bool(BRIDGE_CONF.get("discovery_retained", True))
TELEMETRY_RETAINED = bool(BRIDGE_CONF.get("telemetry_retained", False))

DEVICE_NAME = DEVICE_CONF.get("name", "HiveRig")
DEVICE_MODEL = DEVICE_CONF.get("model", "HiveOS Rig")
DEVICE_ID = DEVICE_CONF.get("identifier", "hiverig")

# ---------- Helpers ----------
def read_json(path: Path) -> Optional[Any]:
    try:
        if path.exists():
            return json.loads(path.read_text())
    except Exception as e:
        logger.debug("read_json error %s: %s", path, e)
    return None

def read_text(path: Path) -> str:
    try:
        if path.exists():
            return path.read_text().strip()
    except Exception as e:
        logger.debug("read_text error %s: %s", path, e)
    return ""

def safe_int(v):
    try:
        return int(v)
    except Exception:
        try:
            return int(float(v))
        except Exception:
            return None

def safe_float(v):
    try:
        return float(v)
    except Exception:
        return None

def _safe_payload(val):
    """Return an MQTT-safe payload: str/int/float/None."""
    if val is None:
        return None
    # numeric allowed directly
    if isinstance(val, (int, float)):
        return val
    if isinstance(val, str):
        return val
    if isinstance(val, list):
        return ",".join(str(x) for x in val)
    return str(val)

# ---------- Hive stats flattening ----------
def get_hive_stats(start_time: float = START_TIME) -> Dict[str, Any]:
    """
    Return a flat dict with:
      - miner_name, miner_status, algo, total_hashrate (MH/s), miner_uptime (s)
      - cpuavg (triple string), cputemp
      - mem_total, mem_used
      - free_disk_gb
      - gpus: list of dicts with index,name,hashrate(MH/s),fan,temp,mtemp,jtemp,power
      - bridge_uptime (s)
    """
    stats: Dict[str, Any] = {}
    last = read_json(LAST_STAT) or {}
    gpu_stats = read_json(GPU_STATS) or {}
    gpu_detect = read_json(GPU_DETECT) or {}
    miner_status = read_json(MINER_STATUS) or {}

    params = last.get("params", {}) or {}
    miner_stats = params.get("miner_stats", {}) or {}

    # Miner info
    miner_name = read_text(CUR_MINER) or params.get("miner") or ""
    stats["miner_name"] = miner_name
    stats["miner_status"] = miner_status.get("status") or miner_stats.get("status") or "unknown"
    stats["algo"] = miner_stats.get("algo") or ""
    total_khs = params.get("total_khs")
    stats["total_hashrate"] = (safe_float(total_khs) / 1000.0) if total_khs is not None else None
    stats["miner_uptime"] = safe_int(miner_stats.get("uptime") or 0)

    # CPU average triple
    cpuavg_list = params.get("cpuavg") or []
    if isinstance(cpuavg_list, list) and cpuavg_list:
        # present as "x,y,z" (no percent)
        stats["cpuavg"] = ",".join(str(x) for x in cpuavg_list)
    else:
        stats["cpuavg"] = ""

    # CPU temp
    cputemp_list = params.get("cputemp") or []
    stats["cputemp"] = safe_int(cputemp_list[0]) if cputemp_list else None

    # Memory: mem array usually [total_mb, available_mb]
    mem_list = params.get("mem") or []
    if isinstance(mem_list, list):
        if len(mem_list) >= 2:
            stats["mem_total"] = safe_int(mem_list[0])
            stats["mem_used"] = safe_int(mem_list[1])
        elif len(mem_list) == 1:
            stats["mem_total"] = safe_int(mem_list[0])
            stats["mem_used"] = None
    else:
        stats["mem_total"] = None
        stats["mem_used"] = None

    # Disk free parsing (df like "206G")
    df_raw = params.get("df") or ""
    free_disk_gb = None
    if isinstance(df_raw, str) and df_raw:
        try:
            if df_raw.endswith("G"):
                free_disk_gb = float(df_raw[:-1])
            elif df_raw.endswith("T"):
                free_disk_gb = float(df_raw[:-1]) * 1024.0
            else:
                free_disk_gb = float(df_raw.rstrip("GgTt"))
        except Exception:
            free_disk_gb = None
    stats["free_disk_gb"] = free_disk_gb

    # GPUs
    gpus: List[Dict[str, Any]] = []
    hs_list = miner_stats.get("hs") or []
    # gpu_detect is usually list of gpu dicts that include busid, name, brand etc
    for i, g in enumerate(gpu_detect):
        # gpu_stats fields are strings; convert safely
        def get_from_gpu_stats(key):
            try:
                arr = gpu_stats.get(key) or []
                if i < len(arr):
                    return arr[i]
            except Exception:
                pass
            return None

        fan = safe_int(get_from_gpu_stats("fan"))
        temp = safe_int(get_from_gpu_stats("temp"))
        power = safe_int(get_from_gpu_stats("power"))
        mtemp = safe_int(get_from_gpu_stats("mtemp"))
        jtemp = safe_int(get_from_gpu_stats("jtemp"))
        # hashrate from miner_stats.hs (kH/s -> MH/s)
        hashrate_mh = None
        try:
            if isinstance(hs_list, list) and i < len(hs_list):
                hashrate_mh = safe_float(hs_list[i]) / 1000.0
        except Exception:
            hashrate_mh = None

        g_entry = {
            "index": i,
            "name": g.get("name") or f"GPU{i}",
            "busid": g.get("busid"),
            "brand": g.get("brand"),
            "fan": fan if fan is not None else 0,
            "temp": temp if temp is not None else 0,
            "mtemp": mtemp if mtemp is not None else 0,
            "jtemp": jtemp if jtemp is not None else 0,
            "power": power if power is not None else 0,
            "hashrate": float(hashrate_mh) if hashrate_mh is not None else 0.0,
        }
        gpus.append(g_entry)

    stats["gpus"] = gpus

    # Bridge uptime in seconds (script uptime)
    stats["bridge_uptime"] = int(time.time() - START_TIME)

    return stats

# ---------- MQTT setup ----------
def create_mqtt_client() -> mqtt.Client:
    # Use callback API v2 if available (paho >= 1.6 provides CallbackAPIVersion)
    try:
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=CLIENT_ID, protocol=mqtt.MQTTv311)
    except Exception:
        # fallback
        client = mqtt.Client(client_id=CLIENT_ID, protocol=mqtt.MQTTv311)
    if MQTT_USER:
        client.username_pw_set(MQTT_USER, MQTT_PASS)
    return client

# helper to extract rc from various callback signatures
def _extract_rc(args: tuple) -> int:
    for a in args:
        if isinstance(a, int):
            return a
    return 0

# ---------- MQTT callbacks ----------
def on_connect(client, userdata, *args):
    rc = _extract_rc(args)
    if rc == 0:
        logger.info("MQTT connected")
        try:
            avail = userdata.get("mqtt", {}).get("availability_topic")
            if avail:
                client.publish(avail, "online", qos=1, retain=DISCOVERY_RETAINED)
        except Exception:
            pass
        # subscribe cmd topics and publish discovery
        try:
            subscribe_commands(client, userdata)
            publish_discovery(client, userdata)
        except Exception as e:
            logger.exception("Error in on_connect followups: %s", e)
    else:
        logger.error("MQTT connect failed rc=%s", rc)

def on_disconnect(client, userdata, *args):
    rc = _extract_rc(args)
    logger.warning("MQTT disconnected (rc=%s)", rc)

def on_message(client, userdata, msg):
    try:
        topic = msg.topic
        payload = msg.payload.decode(errors="ignore") if msg.payload else ""
        logger.info("MQTT msg: %s -> %s", topic, payload)
        cfg_local = userdata if isinstance(userdata, dict) else {}
        base = cfg_local.get("mqtt", {}).get("base_topic", BASE_TOPIC)
        commands = cfg_local.get("commands", {}) or CMDS
        if topic.startswith(f"{base}/cmd/"):
            cmd_name = topic.split("/")[-1]
            cmd = commands.get(cmd_name)
            if cmd:
                logger.info("Executing command %s -> %s", cmd_name, cmd)
                try:
                    subprocess.Popen(cmd, shell=True)
                except Exception as e:
                    logger.exception("Command execution failed: %s", e)
            else:
                logger.warning("Unknown command topic: %s", cmd_name)
        else:
            logger.debug("Unhandled message topic: %s", topic)
    except Exception:
        logger.exception("Error handling message")

# ---------- Subscriptions ----------
def subscribe_commands(client: mqtt.Client, userdata: Dict[str, Any]):
    cfg_local = userdata if isinstance(userdata, dict) else {}
    base = cfg_local.get("mqtt", {}).get("base_topic", BASE_TOPIC)
    for cmd in ["start_miner", "stop_miner", "restart_miner", "reboot", "shutdown"]:
        t = f"{base}/cmd/{cmd}"
        client.subscribe(t, qos=1)
        logger.info("Subscribed to command topic: %s", t)

# ---------- Home Assistant helpers ----------
def get_unit(metric: str) -> Optional[str]:
    return {
        "fan": "%",
        "temp": "°C",
        "mtemp": "°C",
        "jtemp": "°C",
        "power": "W",
        "hashrate": "MH/s",
        "miner_uptime": "s",
        "bridge_uptime": "s",
        "cputemp": "°C",
        "free_disk_gb": "GB",
        "mem_total": "MB",
        "mem_used": "MB"
    }.get(metric)

def get_device_class(metric: str) -> Optional[str]:
    return {
        "temp": "temperature",
        "mtemp": "temperature",
        "jtemp": "temperature",
        "cputemp": "temperature",
        "power": "power",
    }.get(metric)

def get_state_class(metric: str) -> Optional[str]:
    return {
        "fan": "measurement",
        "temp": "measurement",
        "mtemp": "measurement",
        "jtemp": "measurement",
        "power": "measurement",
        "hashrate": "measurement",
        "miner_uptime": "total_increasing",
        "bridge_uptime": "total_increasing",
        "free_disk_gb": "measurement",
        "mem_total": "measurement",
        "mem_used": "measurement",
        "cputemp": "measurement"
    }.get(metric)

# ---------- Discovery publishing ----------
def publish_discovery(client: mqtt.Client, userdata: Dict[str, Any]):
    if not isinstance(userdata, dict):
        logger.warning("publish_discovery: userdata not dict")
        return
    cfg_local = userdata
    mqtt_conf = cfg_local.get("mqtt", {})
    device_conf = cfg_local.get("device", {})
    bridge_conf_local = cfg_local.get("bridge", {})

    discovery_prefix = mqtt_conf.get("discovery_prefix", DISC_PREFIX)
    base = mqtt_conf.get("base_topic", BASE_TOPIC)
    retain = bridge_conf_local.get("discovery_retained", DISCOVERY_RETAINED)

    device = {
        "identifiers": [device_conf.get("identifier", DEVICE_ID)],
        "name": device_conf.get("name", DEVICE_NAME),
        "model": device_conf.get("model", DEVICE_MODEL),
        "manufacturer": "HiveOS"
    }

    stats = get_hive_stats(START_TIME)
    gpus = stats.get("gpus", [])

    # Buttons (commands)
    for cmd in ["start_miner", "stop_miner", "restart_miner", "reboot", "shutdown"]:
        obj_id = f"{device['identifiers'][0]}_{cmd}"
        topic = f"{discovery_prefix}/button/{device['identifiers'][0]}/{obj_id}/config"
        payload = {
            "name": f"{device['name']} {cmd.replace('_',' ').title()}",
            "unique_id": obj_id,
            "command_topic": f"{base}/cmd/{cmd}",
            "availability_topic": mqtt_conf.get("availability_topic", AVAIL_TOPIC),
            "payload_press": "ON",
            "device": device
        }
        client.publish(topic, json.dumps(payload), retain=retain)
        logger.debug("Published discovery button %s", obj_id)

    # GPU sensors
    gpu_metrics = ["hashrate", "fan", "temp", "mtemp", "jtemp", "power"]
    for gpu in gpus:
        idx = gpu.get("index")
        gpu_name = gpu.get("name") or f"GPU{idx}"
        for metric in gpu_metrics:
            obj_id = f"{device['identifiers'][0]}_gpu{idx}_{metric}"
            topic = f"{discovery_prefix}/sensor/{device['identifiers'][0]}/{obj_id}/config"
            payload = {
                "name": f"{gpu_name} {metric.capitalize()}",
                "unique_id": obj_id,
                "state_topic": f"{base}/tele/gpu/{idx}/{metric}",
                "availability_topic": mqtt_conf.get("availability_topic", AVAIL_TOPIC),
                "device": device
            }
            unit = get_unit(metric)
            if unit:
                payload["unit_of_measurement"] = unit
            dev_class = get_device_class(metric)
            if dev_class:
                payload["device_class"] = dev_class
            state_class = get_state_class(metric)
            if state_class:
                payload["state_class"] = state_class
            # Add suggested_display_precision for integer-only sensors (temperatures, etc.)
            if metric in ("temp", "mtemp", "jtemp"):
                payload["suggested_display_precision"] = 0
            client.publish(topic, json.dumps(payload), retain=retain)
            logger.debug("Published discovery sensor %s", obj_id)

    # Miner sensors (individual)
    miner_sensors = {
        "miner_name": {"name": "Miner Name", "topic": f"{base}/tele/miner_name", "unit": None},
        "miner_status": {"name": "Miner Status", "topic": f"{base}/tele/miner_status", "unit": None},
        "algo": {"name": "Algorithm", "topic": f"{base}/tele/algo", "unit": None},
        "total_hashrate": {"name": "Total Hashrate", "topic": f"{base}/tele/total_hashrate", "unit": "MH/s", "state_class": "measurement"},
        "miner_uptime": {"name": "Miner Uptime", "topic": f"{base}/tele/miner_uptime", "unit": "s", "device_class": "duration", "state_class": "total_increasing", "suggested_display_precision": 0},
    }
    for key, meta in miner_sensors.items():
        obj_id = f"{device['identifiers'][0]}_{key}"
        topic = f"{discovery_prefix}/sensor/{device['identifiers'][0]}/{obj_id}/config"
        payload = {
            "name": f"{device['name']} {meta['name']}",
            "unique_id": obj_id,
            "state_topic": meta["topic"],
            "availability_topic": mqtt_conf.get("availability_topic", AVAIL_TOPIC),
            "device": device
        }
        if meta.get("unit"):
            payload["unit_of_measurement"] = meta["unit"]
        if meta.get("device_class"):
            payload["device_class"] = meta["device_class"]
        if meta.get("state_class"):
            payload["state_class"] = meta["state_class"]
        if meta.get("suggested_display_precision") is not None:
            payload["suggested_display_precision"] = meta["suggested_display_precision"]
        client.publish(topic, json.dumps(payload), retain=retain)
        logger.debug("Published miner sensor %s", obj_id)

    # System sensors: cpuavg (string), cputemp, free_disk_gb, mem_total, mem_used, bridge_uptime
    # CPU Load (string triple)
    obj_id = f"{device['identifiers'][0]}_cpuavg"
    client.publish(f"{discovery_prefix}/sensor/{device['identifiers'][0]}/{obj_id}/config",
                   json.dumps({
                       "name": f"{device['name']} CPU Load Average",
                       "unique_id": obj_id,
                       "state_topic": f"{base}/tele/cpuavg",
                       "availability_topic": mqtt_conf.get("availability_topic", AVAIL_TOPIC),
                       "device": device
                   }), retain=retain)

    # CPU temp (no decimals)
    obj_id = f"{device['identifiers'][0]}_cputemp"
    client.publish(f"{discovery_prefix}/sensor/{device['identifiers'][0]}/{obj_id}/config",
                   json.dumps({
                       "name": f"{device['name']} CPU Temperature",
                       "unique_id": obj_id,
                       "state_topic": f"{base}/tele/cputemp",
                       "unit_of_measurement": "°C",
                       "device_class": "temperature",
                       "state_class": "measurement",
                       "suggested_display_precision": 0,
                       "availability_topic": mqtt_conf.get("availability_topic", AVAIL_TOPIC),
                       "device": device
                   }), retain=retain)

    # Free disk
    obj_id = f"{device['identifiers'][0]}_free_disk_gb"
    client.publish(f"{discovery_prefix}/sensor/{device['identifiers'][0]}/{obj_id}/config",
                   json.dumps({
                       "name": f"{device['name']} Free Disk (GB)",
                       "unique_id": obj_id,
                       "state_topic": f"{base}/tele/free_disk_gb",
                       "unit_of_measurement": "GB",
                       "state_class": "measurement",
                       "suggested_display_precision": 0,
                       "availability_topic": mqtt_conf.get("availability_topic", AVAIL_TOPIC),
                       "device": device
                   }), retain=retain)

    # Memory sensors (renamed)
    obj_id_total = f"{device['identifiers'][0]}_mem_total"
    client.publish(f"{discovery_prefix}/sensor/{device['identifiers'][0]}/{obj_id_total}/config",
                   json.dumps({
                       "name": f"{device['name']} Memory Total",
                       "unique_id": obj_id_total,
                       "state_topic": f"{base}/tele/mem_total",
                       "unit_of_measurement": "MB",
                       "state_class": "measurement",
                       "availability_topic": mqtt_conf.get("availability_topic", AVAIL_TOPIC),
                       "device": device
                   }), retain=retain)

    obj_id_used = f"{device['identifiers'][0]}_mem_used"
    client.publish(f"{discovery_prefix}/sensor/{device['identifiers'][0]}/{obj_id_used}/config",
                   json.dumps({
                       "name": f"{device['name']} Memory Used",
                       "unique_id": obj_id_used,
                       "state_topic": f"{base}/tele/mem_used",
                       "unit_of_measurement": "MB",
                       "state_class": "measurement",
                       "availability_topic": mqtt_conf.get("availability_topic", AVAIL_TOPIC),
                       "device": device
                   }), retain=retain)

    # Bridge uptime sensor
    obj_id = f"{device['identifiers'][0]}_bridge_uptime"
    client.publish(f"{discovery_prefix}/sensor/{device['identifiers'][0]}/{obj_id}/config",
                   json.dumps({
                       "name": f"{device['name']} Bridge Uptime",
                       "unique_id": obj_id,
                       "state_topic": f"{base}/tele/bridge_uptime",
                       "unit_of_measurement": "s",
                       "device_class": "duration",
                       "state_class": "total_increasing",
                       "suggested_display_precision": 0,
                       "availability_topic": mqtt_conf.get("availability_topic", AVAIL_TOPIC),
                       "device": device
                   }), retain=retain)

    logger.info("Discovery published")

# ---------- Telemetry publishing ----------
def publish_telemetry(client: mqtt.Client, userdata: Dict[str, Any]):
    stats = get_hive_stats(START_TIME)
    base = userdata.get("mqtt", {}).get("base_topic", BASE_TOPIC) if isinstance(userdata, dict) else BASE_TOPIC
    retain = userdata.get("bridge", {}).get("telemetry_retained", TELEMETRY_RETAINED) if isinstance(userdata, dict) else TELEMETRY_RETAINED

    # per-GPU metrics
    for gpu in stats.get("gpus", []):
        idx = gpu.get("index")
        for metric in ["hashrate", "fan", "temp", "mtemp", "jtemp", "power"]:
            topic = f"{base}/tele/gpu/{idx}/{metric}"
            payload = _safe_payload(gpu.get(metric))
            if payload is not None:
                client.publish(topic, payload, retain=retain)

    # miner metrics
    miner_map = {
        "miner_name": stats.get("miner_name"),
        "miner_status": stats.get("miner_status"),
        "algo": stats.get("algo"),
        "total_hashrate": stats.get("total_hashrate"),
        "miner_uptime": stats.get("miner_uptime"),
    }
    for key, val in miner_map.items():
        if val is not None:
            client.publish(f"{base}/tele/{key}", _safe_payload(val), retain=retain)

    # system metrics
    client.publish(f"{base}/tele/cpuavg", _safe_payload(stats.get("cpuavg")), retain=retain)
    if stats.get("cputemp") is not None:
        client.publish(f"{base}/tele/cputemp", _safe_payload(stats.get("cputemp")), retain=retain)
    if stats.get("free_disk_gb") is not None:
        client.publish(f"{base}/tele/free_disk_gb", _safe_payload(stats.get("free_disk_gb")), retain=retain)

    # memory renamed sensors
    if stats.get("mem_total") is not None:
        client.publish(f"{base}/tele/mem_total", _safe_payload(stats.get("mem_total")), retain=retain)
    if stats.get("mem_used") is not None:
        client.publish(f"{base}/tele/mem_used", _safe_payload(stats.get("mem_used")), retain=retain)

    # bridge uptime
    client.publish(f"{base}/tele/bridge_uptime", _safe_payload(stats.get("bridge_uptime")), retain=retain)

# ---------- Main ----------
def graceful_shutdown(signum=None, frame=None):
    logger.info("Shutting down...")
    try:
        client.publish(AVAIL_TOPIC, "offline", qos=1, retain=DISCOVERY_RETAINED)
    except Exception:
        pass
    try:
        client.disconnect()
    except Exception:
        pass
    try:
        client.loop_stop()
    except Exception:
        pass
    sys.exit(0)

import signal
signal.signal(signal.SIGINT, graceful_shutdown)
signal.signal(signal.SIGTERM, graceful_shutdown)

if __name__ == "__main__":
    logger.info("Starting HiveOS MQTT Bridge...")

    client = create_mqtt_client()
    client.user_data_set(cfg)  # callbacks can access config
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    # LWT
    try:
        client.will_set(AVAIL_TOPIC, payload="offline", qos=1, retain=DISCOVERY_RETAINED)
    except Exception:
        logger.debug("will_set not supported in this environment")

    logger.info("Connecting to MQTT broker %s:%s", MQTT_HOST, MQTT_PORT)
    client.connect(MQTT_HOST, MQTT_PORT, KEEPALIVE)
    client.loop_start()

    # Allow on_connect to run and publish discovery
    try:
        time.sleep(0.5)
        while True:
            publish_telemetry(client, cfg)
            time.sleep(STATS_INTERVAL)
    except KeyboardInterrupt:
        graceful_shutdown()
