import asyncio
import json
import os
import re
import secrets
import select
import shutil
import threading
import time
import uuid
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Dict, List, Optional

import paramiko
from fastapi import Depends, FastAPI, HTTPException, Header, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

APP_USER = os.getenv("APP_USER", "admin")
APP_PASS = os.getenv("APP_PASS", "changeme")
DATA_DIR = Path(os.getenv("DATA_DIR", "/app/data"))
MINIO_DATA_DIR = Path(os.getenv("MINIO_DATA_DIR", "/minio-data"))
POLL_INTERVAL_SEC = max(5, min(int(os.getenv("POLL_INTERVAL_SEC", "8")), 30))
SERVERS_FILE = DATA_DIR / "servers.json"
LOGS_FILE = DATA_DIR / "logs.json"
ALERT_CONFIG_FILE = DATA_DIR / "alerts.json"
DEFAULT_WAL_LAG_THRESHOLD_SEC = max(10, int(os.getenv("WAL_LAG_ALERT_SEC", "120")))

# In-memory sessions, background job logs, and real-time telemetry state
SESSIONS: Dict[str, float] = {}
JOBS: Dict[str, Dict] = {}
JOB_LOCK = threading.Lock()
LOG_FILE_LOCK = threading.Lock()
STATE_LOCK = threading.Lock()
POLL_WAKE_EVENT = threading.Event()
POLL_STOP_EVENT = threading.Event()
TELEMETRY_HISTORY: Dict[str, deque] = {}
ALERT_ACTIVE: Dict[str, bool] = {}
STATE_CACHE: Dict = {"storage": None, "stanzas": [], "updated_at": None, "ts": 0.0, "alerts": []}
TZ_BKK = timezone(timedelta(hours=7))
MAIN_LOOP: Optional[asyncio.AbstractEventLoop] = None


def load_alert_config() -> Dict:
    default_cfg = {"wal_lag_threshold_sec": DEFAULT_WAL_LAG_THRESHOLD_SEC, "enabled": True}
    if not ALERT_CONFIG_FILE.exists():
        return default_cfg
    try:
        data = json.loads(ALERT_CONFIG_FILE.read_text(encoding="utf-8"))
        return {
            "wal_lag_threshold_sec": max(10, min(int(data.get("wal_lag_threshold_sec", DEFAULT_WAL_LAG_THRESHOLD_SEC)), 86400)),
            "enabled": bool(data.get("enabled", True)),
        }
    except Exception:
        return default_cfg


def save_alert_config(cfg: Dict) -> Dict:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    clean = {
        "wal_lag_threshold_sec": max(10, min(int(cfg.get("wal_lag_threshold_sec", DEFAULT_WAL_LAG_THRESHOLD_SEC)), 86400)),
        "enabled": bool(cfg.get("enabled", True)),
    }
    ALERT_CONFIG_FILE.write_text(json.dumps(clean, indent=2), encoding="utf-8")
    return clean


class ConnectionManager:
    """Manages authenticated WebSocket clients for real-time push updates."""

    def __init__(self) -> None:
        self.active_connections: set[WebSocket] = set()
        self._lock = threading.Lock()

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        with self._lock:
            self.active_connections.add(websocket)

    def disconnect(self, websocket: WebSocket) -> None:
        with self._lock:
            self.active_connections.discard(websocket)

    def count(self) -> int:
        with self._lock:
            return len(self.active_connections)

    async def send_json(self, websocket: WebSocket, payload: Dict) -> None:
        await websocket.send_json(payload)

    async def broadcast(self, payload: Dict) -> None:
        with self._lock:
            targets = list(self.active_connections)
        if not targets:
            return
        dead: List[WebSocket] = []
        for ws in targets:
            try:
                await ws.send_json(payload)
            except Exception:
                dead.append(ws)
        if dead:
            with self._lock:
                for ws in dead:
                    self.active_connections.discard(ws)


ws_manager = ConnectionManager()


def broadcast_sync(payload: Dict) -> None:
    """Thread-safe bridge to push WebSocket messages from worker/poller threads."""
    if MAIN_LOOP is None or MAIN_LOOP.is_closed() or ws_manager.count() == 0:
        return
    try:
        asyncio.run_coroutine_threadsafe(ws_manager.broadcast(payload), MAIN_LOOP)
    except Exception:
        pass


@asynccontextmanager
async def lifespan(_: FastAPI):
    global MAIN_LOOP
    MAIN_LOOP = asyncio.get_running_loop()
    POLL_STOP_EVENT.clear()
    threading.Thread(target=status_poller_loop, daemon=True).start()
    try:
        yield
    finally:
        POLL_STOP_EVENT.set()
        POLL_WAKE_EVENT.set()


app = FastAPI(title="pgBackRest Manager", lifespan=lifespan)


def load_logs() -> List[Dict]:
    if not LOGS_FILE.exists():
        return []
    try:
        return json.loads(LOGS_FILE.read_text(encoding="utf-8"))
    except Exception:
        return []


def save_logs(entries: List[Dict]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_FILE.write_text(json.dumps(entries[:500], indent=2, ensure_ascii=False), encoding="utf-8")


def add_log_entry(
    category: str,
    action: str,
    detail: str,
    status: str = "info",
    server_id: Optional[str] = None,
    job_id: Optional[str] = None,
    client_ip: Optional[str] = None,
    output: Optional[List[str]] = None,
) -> str:
    entry_id = job_id or uuid.uuid4().hex[:12]
    entry = {
        "id": entry_id,
        "timestamp": datetime.now(TZ_BKK).strftime("%Y-%m-%d %H:%M:%S+07"),
        "category": category,
        "action": action,
        "server_id": server_id,
        "detail": detail,
        "status": status,
        "client_ip": client_ip,
        "duration_sec": None,
        "output": output or [],
        "_started_ts": time.time(),
    }
    with LOG_FILE_LOCK:
        entries = load_logs()
        entries.insert(0, entry)
        save_logs(entries)
    return entry_id


def update_log_entry(entry_id: str, status: str, output: Optional[List[str]] = None) -> None:
    with LOG_FILE_LOCK:
        entries = load_logs()
        for e in entries:
            if e.get("id") == entry_id:
                e["status"] = status
                if output is not None:
                    e["output"] = output
                started = e.pop("_started_ts", None)
                if started is not None:
                    e["duration_sec"] = round(time.time() - started, 1)
                break
        save_logs(entries)


def load_servers() -> List[Dict]:
    if not SERVERS_FILE.exists():
        return []
    return json.loads(SERVERS_FILE.read_text(encoding="utf-8"))


def save_servers(servers: List[Dict]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    SERVERS_FILE.write_text(json.dumps(servers, indent=2, ensure_ascii=False), encoding="utf-8")


def validate_token(token: Optional[str]) -> bool:
    if not token or token not in SESSIONS:
        return False
    if time.time() - SESSIONS[token] > 86400:
        SESSIONS.pop(token, None)
        return False
    return True


def require_auth(x_auth_token: Optional[str] = Header(default=None)):
    if not validate_token(x_auth_token):
        raise HTTPException(status_code=401, detail="Unauthorized or session expired")
    return True


def _ssh_connect(server: Dict) -> paramiko.SSHClient:
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    connect_kwargs: Dict = {
        "hostname": server["host"],
        "port": int(server.get("port", 22)),
        "username": server["ssh_user"],
        "timeout": 10,
        "look_for_keys": False,
        "allow_agent": False,
    }
    key_path = server.get("ssh_key_path")
    if key_path and Path(key_path).exists():
        connect_kwargs["key_filename"] = key_path
    elif server.get("ssh_pass"):
        connect_kwargs["password"] = server["ssh_pass"]
    client.connect(**connect_kwargs)
    return client


def _wrap_sudo(server: Dict, command: str) -> str:
    sudo_pass = server.get("sudo_pass") or server.get("ssh_pass") or ""
    if sudo_pass:
        return f"echo {json.dumps(sudo_pass)} | sudo -S -p '' bash -lc {json.dumps(command)}"
    return f"sudo -n bash -lc {json.dumps(command)}"


def ssh_run(server: Dict, command: str, timeout: int = 30) -> tuple[int, str, str]:
    client = _ssh_connect(server)
    try:
        wrapped = _wrap_sudo(server, command)
        _, stdout, stderr = client.exec_command(wrapped, timeout=timeout)
        out = stdout.read().decode("utf-8", errors="replace")
        err = stderr.read().decode("utf-8", errors="replace")
        rc = stdout.channel.recv_exit_status()
        return rc, out, err
    finally:
        client.close()


def ssh_run_stream(server: Dict, command: str, on_line: Callable[[str], None], timeout: int = 600) -> int:
    """Execute command over SSH and stream combined stdout/stderr lines in real time."""
    client = _ssh_connect(server)
    try:
        wrapped = _wrap_sudo(server, command)
        _, stdout, _ = client.exec_command(wrapped, timeout=timeout)
        channel = stdout.channel
        channel.set_combine_stderr(True)
        buf = ""
        start_ts = time.time()
        while not channel.exit_status_ready() or channel.recv_ready():
            if time.time() - start_ts > timeout:
                raise TimeoutError(f"SSH command timed out after {timeout}s")
            if channel.recv_ready():
                chunk = channel.recv(4096).decode("utf-8", errors="replace")
                if chunk:
                    buf += chunk
                    while "\n" in buf:
                        line, buf = buf.split("\n", 1)
                        cleaned = line.rstrip("\r")
                        if cleaned.strip():
                            on_line(cleaned)
            else:
                select.select([channel], [], [], 0.15)
        if buf.strip():
            on_line(buf.rstrip("\r"))
        return channel.recv_exit_status()
    finally:
        client.close()


def append_job_log(job_id: str, msg: str):
    ts = datetime.now(TZ_BKK).strftime("%H:%M:%S")
    formatted = f"[{ts}] {msg}"
    status = "running"
    with JOB_LOCK:
        if job_id in JOBS:
            JOBS[job_id]["logs"].append(formatted)
            status = JOBS[job_id].get("status", "running")
    broadcast_sync({
        "type": "job_log",
        "job_id": job_id,
        "line": formatted,
        "status": status,
    })


class LoginReq(BaseModel):
    username: str
    password: str


class ServerReq(BaseModel):
    id: str
    name: str
    host: str
    port: int = 22
    ssh_user: str = "ubuntu"
    ssh_pass: str = ""
    ssh_key_path: Optional[str] = None
    db_type: str = "postgres"  # postgres or mariadb
    stanza: str
    env: str = "UAT"
    pg_service: str = "postgresql@16-main"
    allow_restore: bool = False


class JobReq(BaseModel):
    server_id: str
    action: str  # check, diff, full


class RestoreReq(BaseModel):
    source_server_id: str
    target_server_id: str
    mode: str = "time"  # time or default
    target_time: Optional[str] = None
    confirm_stanza: str


@app.get("/api/health")
def health():
    return {"status": "ok"}


@app.post("/api/login")
def login(req: LoginReq, request: Request):
    ip = request.client.host if request.client else None
    if req.username != APP_USER or req.password != APP_PASS:
        add_log_entry("AUTH", "login", f"Failed login attempt for user '{req.username}'", status="failed", client_ip=ip)
        raise HTTPException(status_code=401, detail="Invalid username or password")
    token = secrets.token_hex(24)
    SESSIONS[token] = time.time()
    add_log_entry("AUTH", "login", f"User '{req.username}' logged in", status="success", client_ip=ip)
    return {"token": token, "username": req.username}


@app.post("/api/logout")
def logout(request: Request, x_auth_token: Optional[str] = Header(default=None)):
    ip = request.client.host if request.client else None
    if x_auth_token:
        SESSIONS.pop(x_auth_token, None)
    add_log_entry("AUTH", "logout", "User logged out", status="info", client_ip=ip)
    return {"ok": True}


def collect_storage_info() -> Dict:
    target = MINIO_DATA_DIR if MINIO_DATA_DIR.exists() else Path("/")
    usage = shutil.disk_usage(target)
    return {
        "total_gb": round(usage.total / (1024**3), 1),
        "used_gb": round(usage.used / (1024**3), 1),
        "free_gb": round(usage.free / (1024**3), 1),
        "used_pct": round(usage.used * 100 / usage.total, 1) if usage.total else 0,
    }


def collect_mariadb_stanza(srv: Dict, entry: Dict) -> Dict:
    stanza = srv["stanza"]
    meta_file = f"/var/lib/mariadb-backup-meta/{stanza}/manifest.json"
    cmd = (
        "systemctl is-active mariadb && echo '___MDB___' && "
        "mariadb -Nse \"SELECT @@log_bin, COALESCE(SUM(data_length + index_length), 0) FROM information_schema.tables;\" && "
        "echo '___MANIFEST___' && "
        f"(cat {meta_file} 2>/dev/null || echo '[]')"
    )
    rc, out, err = ssh_run(srv, cmd, timeout=15)
    if rc != 0:
        entry["status"] = "not configured"
        entry["error"] = (err or out).strip()[:200]
        return entry

    parts = out.split("___MDB___")
    svc_status = parts[0].strip()
    entry["status"] = "ok" if svc_status == "active" else svc_status

    rest = parts[1] if len(parts) > 1 else ""
    subparts = rest.split("___MANIFEST___")
    mdb_line = subparts[0].strip().split()
    log_bin_on = (mdb_line[0] == "1") if mdb_line else False
    entry["log_bin"] = "ON" if log_bin_on else "OFF"

    manifest_raw = subparts[1].strip() if len(subparts) > 1 else "[]"
    try:
        backups = json.loads(manifest_raw) if manifest_raw else []
    except Exception:
        backups = []

    entry["backups"] = backups
    if backups:
        earliest = backups[0].get("stop") or backups[0].get("start")
        latest = backups[-1].get("stop") or backups[-1].get("start")
        if earliest:
            entry["recoverable_from"] = datetime.fromtimestamp(earliest, tz=TZ_BKK).strftime("%Y-%m-%d %H:%M:%S+07")
        if latest:
            latest_str = datetime.fromtimestamp(latest, tz=TZ_BKK).strftime("%Y-%m-%d %H:%M:%S+07")
            entry["recoverable_to"] = latest_str
            entry["last_archived_time"] = latest_str
            entry["archive_lag_sec"] = max(0, int(time.time()) - int(latest))
    return entry


def collect_single_stanza(srv: Dict) -> Dict:
    stanza = srv["stanza"]
    db_type = srv.get("db_type", "postgres")
    entry = {
        "server_id": srv["id"],
        "name": srv["name"],
        "host": srv["host"],
        "env": srv.get("env", "UAT"),
        "db_type": db_type,
        "stanza": stanza,
        "allow_restore": bool(srv.get("allow_restore", False)),
        "status": "unknown",
        "backups": [],
        "wal_min": None,
        "wal_max": None,
        "last_archived_wal": None,
        "last_archived_time": None,
        "archive_lag_sec": None,
        "archive_failed_count": 0,
        "recoverable_from": None,
        "recoverable_to": None,
        "error": None,
    }
    try:
        if db_type == "mariadb":
            return collect_mariadb_stanza(srv, entry)
        cmd = (
            f"sudo -u postgres pgbackrest --stanza={stanza} --output=json info && "
            "echo '___ARCHIVER___' && "
            "sudo -u postgres psql -tAc \"SELECT COALESCE(last_archived_wal,''), COALESCE(to_char(last_archived_time AT TIME ZONE 'Asia/Bangkok', 'YYYY-MM-DD HH24:MI:SS'), ''), COALESCE(EXTRACT(EPOCH FROM (now() - last_archived_time))::int, -1), failed_count FROM pg_stat_archiver;\""
        )
        rc, out, err = ssh_run(srv, cmd, timeout=15)
        if rc != 0:
            entry["status"] = "not configured"
            entry["error"] = (err or out).strip()[:200]
        else:
            parts = out.split("___ARCHIVER___")
            parsed = json.loads(parts[0].strip())
            if parsed:
                st = parsed[0]
                entry["status"] = st.get("status", {}).get("message", "unknown")
                archives = st.get("archive") or []
                if archives:
                    entry["wal_min"] = archives[0].get("min")
                    entry["wal_max"] = archives[0].get("max")
                for b in st.get("backup", []):
                    entry["backups"].append({
                        "label": b.get("label"),
                        "type": b.get("type"),
                        "start": b.get("timestamp", {}).get("start"),
                        "stop": b.get("timestamp", {}).get("stop"),
                        "db_size": b.get("info", {}).get("size"),
                        "repo_size": b.get("info", {}).get("repository", {}).get("size"),
                    })
                if entry["backups"]:
                    earliest_stop = entry["backups"][0].get("stop")
                    if earliest_stop:
                        entry["recoverable_from"] = datetime.fromtimestamp(earliest_stop, tz=TZ_BKK).strftime("%Y-%m-%d %H:%M:%S+07")
            if len(parts) > 1 and parts[1].strip():
                cols = parts[1].strip().split("|")
                if len(cols) >= 4:
                    entry["last_archived_wal"] = cols[0] or None
                    entry["last_archived_time"] = (cols[1] + "+07") if cols[1] else None
                    lag = int(cols[2])
                    entry["archive_lag_sec"] = lag if lag >= 0 else None
                    entry["archive_failed_count"] = int(cols[3] or 0)
                    if entry["last_archived_time"]:
                        entry["recoverable_to"] = entry["last_archived_time"]
    except Exception as e:
        entry["status"] = "unreachable"
        entry["error"] = str(e)[:200]
    return entry


def collect_stanzas_info() -> List[Dict]:
    servers = load_servers()
    if not servers:
        return []
    workers = min(len(servers), 8)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        return list(pool.map(collect_single_stanza, servers))


def record_state_and_telemetry(storage_info: Dict, stanzas: List[Dict]) -> Dict:
    now_ts = int(time.time())
    updated_at = datetime.now(TZ_BKK).strftime("%Y-%m-%d %H:%M:%S+07")
    alert_cfg = load_alert_config()
    threshold = alert_cfg["wal_lag_threshold_sec"]
    alerts_enabled = alert_cfg["enabled"]
    active_alerts: List[Dict] = []
    new_alerts: List[Dict] = []

    with STATE_LOCK:
        for st in stanzas:
            sid = st["server_id"]
            lag = st.get("archive_lag_sec")
            failed = st.get("archive_failed_count", 0)
            if sid not in TELEMETRY_HISTORY:
                TELEMETRY_HISTORY[sid] = deque(maxlen=360)
            TELEMETRY_HISTORY[sid].append({
                "ts": now_ts,
                "lag_sec": lag,
                "failed_count": failed,
                "status": st.get("status"),
                "backup_count": len(st.get("backups", [])),
                "total_repo_bytes": sum((b.get("repo_size") or 0) for b in st.get("backups", [])),
            })

            is_lagging = alerts_enabled and (lag is not None) and (lag > threshold)
            if is_lagging:
                alert_obj = {
                    "server_id": sid,
                    "stanza": st.get("stanza"),
                    "host": st.get("host"),
                    "env": st.get("env"),
                    "lag_sec": lag,
                    "threshold_sec": threshold,
                    "failed_count": failed,
                }
                active_alerts.append(alert_obj)
                if not ALERT_ACTIVE.get(sid):
                    ALERT_ACTIVE[sid] = True
                    new_alerts.append(alert_obj)
                    add_log_entry(
                        "ALERT",
                        "wal_lag_high",
                        f"WAL lag on {st.get('stanza')} ({st.get('host')}) is {lag}s > {threshold}s",
                        status="failed",
                        server_id=sid,
                    )
            elif (lag is not None) and (lag <= threshold) and ALERT_ACTIVE.get(sid):
                ALERT_ACTIVE[sid] = False
                add_log_entry(
                    "ALERT",
                    "wal_lag_recovered",
                    f"WAL lag on {st.get('stanza')} ({st.get('host')}) recovered to {lag}s",
                    status="success",
                    server_id=sid,
                )

        STATE_CACHE["storage"] = storage_info
        STATE_CACHE["stanzas"] = stanzas
        STATE_CACHE["updated_at"] = updated_at
        STATE_CACHE["ts"] = time.time()
        STATE_CACHE["alerts"] = active_alerts
        telemetry_copy = {k: list(v) for k, v in TELEMETRY_HISTORY.items()}

    return {
        "type": "state_update",
        "updated_at": updated_at,
        "poll_interval_sec": POLL_INTERVAL_SEC,
        "alert_config": alert_cfg,
        "alerts": active_alerts,
        "new_alerts": new_alerts,
        "storage": storage_info,
        "stanzas": stanzas,
        "telemetry": telemetry_copy,
    }


def refresh_state_snapshot(force_broadcast: bool = True) -> Dict:
    storage_info = collect_storage_info()
    stanzas = collect_stanzas_info()
    payload = record_state_and_telemetry(storage_info, stanzas)
    if force_broadcast:
        broadcast_sync(payload)
    return payload


def status_poller_loop() -> None:
    """Background poller: collects Stanza/WAL/Storage every POLL_INTERVAL_SEC (5-10s) when clients are connected."""
    while not POLL_STOP_EVENT.is_set():
        POLL_WAKE_EVENT.wait(timeout=POLL_INTERVAL_SEC)
        POLL_WAKE_EVENT.clear()
        if POLL_STOP_EVENT.is_set():
            break
        if ws_manager.count() > 0:
            try:
                refresh_state_snapshot(force_broadcast=True)
            except Exception:
                pass


@app.get("/api/storage")
def storage(_: bool = Depends(require_auth)):
    return collect_storage_info()


@app.get("/api/servers")
def list_servers(_: bool = Depends(require_auth)):
    servers = load_servers()
    safe = []
    for s in servers:
        item = {k: v for k, v in s.items() if k not in ("ssh_pass", "sudo_pass")}
        safe.append(item)
    return safe


@app.post("/api/servers")
def add_server(req: ServerReq, request: Request, _: bool = Depends(require_auth)):
    if not re.match(r"^[a-zA-Z0-9_-]+$", req.id) or not re.match(r"^[a-zA-Z0-9_-]+$", req.stanza):
        raise HTTPException(status_code=400, detail="Invalid id or stanza format")
    servers = load_servers()
    existing = next((s for s in servers if s["id"] == req.id), None)
    data = req.model_dump()
    if not data["ssh_pass"] and not data.get("ssh_key_path"):
        if existing and existing.get("ssh_pass"):
            data["ssh_pass"] = existing["ssh_pass"]
            if existing.get("sudo_pass"):
                data["sudo_pass"] = existing["sudo_pass"]
        elif existing and existing.get("ssh_key_path"):
            data["ssh_key_path"] = existing["ssh_key_path"]
        else:
            raise HTTPException(status_code=400, detail="SSH password or SSH key path is required for new server")
    servers = [s for s in servers if s["id"] != req.id]
    servers.append(data)
    save_servers(servers)
    ip = request.client.host if request.client else None
    add_log_entry("SERVER", "save_server", f"Saved server {req.name} ({req.host}, env={req.env}, allow_restore={req.allow_restore})", status="success", server_id=req.id, client_ip=ip)
    return {"ok": True}


@app.delete("/api/servers/{server_id}")
def delete_server(server_id: str, request: Request, _: bool = Depends(require_auth)):
    servers = [s for s in load_servers() if s["id"] != server_id]
    save_servers(servers)
    ip = request.client.host if request.client else None
    add_log_entry("SERVER", "delete_server", f"Deleted server '{server_id}'", status="info", server_id=server_id, client_ip=ip)
    return {"ok": True}


@app.post("/api/servers/{server_id}/test")
def test_server(server_id: str, request: Request, _: bool = Depends(require_auth)):
    servers = load_servers()
    srv = next((s for s in servers if s["id"] == server_id), None)
    if not srv:
        raise HTTPException(status_code=404, detail="Server not found")
    ip = request.client.host if request.client else None
    try:
        if srv.get("db_type", "postgres") == "mariadb":
            test_cmd = "mariadb-backup --version && systemctl is-active mariadb && aws --endpoint-url http://10.1.1.211:9000 s3 ls s3://pg-backups/ >/dev/null && echo 'S3 OK'"
        else:
            test_cmd = "pgbackrest version && systemctl is-active " + srv.get("pg_service", "postgresql@16-main")
        rc, out, err = ssh_run(srv, test_cmd, timeout=12)
        output_text = (out + "\n" + err).strip()
        add_log_entry("SERVER", "test_ssh", f"SSH/{srv.get('db_type','postgres')} test on {srv['host']}", status="success" if rc == 0 else "failed", server_id=server_id, client_ip=ip, output=output_text.splitlines())
        return {"ok": rc == 0, "output": output_text}
    except Exception as e:
        add_log_entry("SERVER", "test_ssh", f"SSH test failed on {srv['host']}: {e}", status="failed", server_id=server_id, client_ip=ip, output=[str(e)])
        return {"ok": False, "output": str(e)}


@app.get("/api/stanzas")
def get_stanzas(_: bool = Depends(require_auth)):
    stanzas = collect_stanzas_info()
    storage_info = collect_storage_info()
    record_state_and_telemetry(storage_info, stanzas)
    return stanzas


class AlertConfigReq(BaseModel):
    wal_lag_threshold_sec: int = 120
    enabled: bool = True


@app.get("/api/alerts/config")
def get_alert_config(_: bool = Depends(require_auth)):
    return load_alert_config()


@app.post("/api/alerts/config")
def update_alert_config(req: AlertConfigReq, request: Request, _: bool = Depends(require_auth)):
    saved = save_alert_config(req.model_dump())
    ip = request.client.host if request.client else None
    add_log_entry(
        "ALERT",
        "alert_config",
        f"Updated WAL lag alert threshold to {saved['wal_lag_threshold_sec']}s (enabled={saved['enabled']})",
        status="info",
        client_ip=ip,
    )
    POLL_WAKE_EVENT.set()
    return saved


@app.get("/api/telemetry")
def get_telemetry(_: bool = Depends(require_auth)):
    alert_cfg = load_alert_config()
    with STATE_LOCK:
        return {
            "updated_at": STATE_CACHE.get("updated_at"),
            "poll_interval_sec": POLL_INTERVAL_SEC,
            "alert_config": alert_cfg,
            "alerts": list(STATE_CACHE.get("alerts") or []),
            "telemetry": {k: list(v) for k, v in TELEMETRY_HISTORY.items()},
        }


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    token = websocket.query_params.get("token")
    if not validate_token(token):
        await websocket.close(code=4401)
        return

    await ws_manager.connect(websocket)
    try:
        alert_cfg = load_alert_config()
        with STATE_LOCK:
            cached_stanzas = STATE_CACHE.get("stanzas") or []
            cached_storage = STATE_CACHE.get("storage")
            cached_updated = STATE_CACHE.get("updated_at")
            cached_ts = STATE_CACHE.get("ts", 0.0)
            cached_alerts = list(STATE_CACHE.get("alerts") or [])
            telemetry_copy = {k: list(v) for k, v in TELEMETRY_HISTORY.items()}

        if cached_storage is not None and cached_stanzas:
            await ws_manager.send_json(websocket, {
                "type": "state_update",
                "updated_at": cached_updated,
                "poll_interval_sec": POLL_INTERVAL_SEC,
                "alert_config": alert_cfg,
                "alerts": cached_alerts,
                "new_alerts": [],
                "storage": cached_storage,
                "stanzas": cached_stanzas,
                "telemetry": telemetry_copy,
            })

        with JOB_LOCK:
            running_jobs = [
                {"id": j["id"], "status": j["status"], "logs": list(j["logs"])}
                for j in JOBS.values()
                if j.get("status") == "running"
            ]
        for rj in running_jobs:
            await ws_manager.send_json(websocket, {
                "type": "job_snapshot",
                "job_id": rj["id"],
                "status": rj["status"],
                "logs": rj["logs"],
            })

        if time.time() - cached_ts > max(4, POLL_INTERVAL_SEC - 1):
            POLL_WAKE_EVENT.set()

        while True:
            raw = await websocket.receive_text()
            try:
                msg = json.loads(raw)
            except Exception:
                continue
            mtype = msg.get("type")
            if mtype == "ping":
                await ws_manager.send_json(websocket, {"type": "pong", "ts": int(time.time())})
            elif mtype == "refresh":
                POLL_WAKE_EVENT.set()
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        ws_manager.disconnect(websocket)


@app.get("/api/schedules/{server_id}")
def get_schedules(server_id: str, _: bool = Depends(require_auth)):
    srv = next((s for s in load_servers() if s["id"] == server_id), None)
    if not srv:
        raise HTTPException(status_code=404, detail="Server not found")
    try:
        if srv.get("db_type", "postgres") == "mariadb":
            cron_cmd = "sudo crontab -l 2>/dev/null || echo '(no cron configured)'"
        else:
            cron_cmd = "sudo crontab -u postgres -l 2>/dev/null || echo '(no cron configured)'"
        rc, out, err = ssh_run(srv, cron_cmd, timeout=10)
        return {"ok": True, "cron": out.strip()}
    except Exception as e:
        return {"ok": False, "cron": str(e)}


def _finalize_job(job_id: str, status: str) -> None:
    with JOB_LOCK:
        if job_id in JOBS:
            JOBS[job_id]["status"] = status
            logs_copy = list(JOBS[job_id]["logs"])
        else:
            logs_copy = []
    update_log_entry(job_id, status=status, output=logs_copy)
    broadcast_sync({"type": "job_status", "job_id": job_id, "status": status, "logs": logs_copy})
    POLL_WAKE_EVENT.set()


def _build_mariadb_job_cmd(stanza: str, action: str) -> tuple[str, int]:
    meta_dir = f"/var/lib/mariadb-backup-meta/{stanza}"
    s3_prefix = f"s3://pg-backups/mariadb/{stanza}"
    ep = "http://10.1.1.211:9000"
    if action == "check":
        cmd = (
            "mariadb-backup --version && "
            "echo 'Service:' $(systemctl is-active mariadb) && "
            "mariadb -Nse \"SELECT CONCAT('log_bin=', @@log_bin, ' binlog_format=', @@binlog_format);\" && "
            f"aws --endpoint-url {ep} s3 ls {s3_prefix}/ || true"
        )
        return cmd, 60

    btype = "full" if action == "full" else "incr"
    script = f"""set -euo pipefail
mkdir -p {meta_dir} {meta_dir}/lsn_tmp
LABEL=$(date +'%Y%m%d-%H%M%S')_{btype.upper()}
OBJ_KEY="mariadb/{stanza}/${{LABEL}}.mbstream.zst"
START_TS=$(date +%s)
INCR_ARG=""
if [ "{btype}" = "incr" ]; then
  if [ ! -f {meta_dir}/latest_lsn ]; then
    echo "ERROR: No base Full backup LSN found in {meta_dir}/latest_lsn. Run Full backup first."
    exit 1
  fi
  LAST_LSN=$(cat {meta_dir}/latest_lsn)
  INCR_ARG="--incremental-lsn=${{LAST_LSN}}"
  echo "Starting MariaDB Incremental backup from LSN=${{LAST_LSN}} -> s3://pg-backups/${{OBJ_KEY}}"
else
  echo "Starting MariaDB Full backup -> s3://pg-backups/${{OBJ_KEY}}"
fi
rm -rf {meta_dir}/lsn_tmp/*
mariadb-backup --backup --stream=mbstream --extra-lsndir={meta_dir}/lsn_tmp ${{INCR_ARG}} 2>>/var/log/mariadb-backup-{stanza}.log \\
  | zstd -T2 -3 \\
  | aws --endpoint-url {ep} s3 cp - s3://pg-backups/${{OBJ_KEY}}
STOP_TS=$(date +%s)
TO_LSN=$(awk -F= '/^to_lsn/ {{gsub(/ /,"",$2); print $2}}' {meta_dir}/lsn_tmp/xtrabackup_checkpoints)
echo "${{TO_LSN}}" > {meta_dir}/latest_lsn
REPO_SIZE=$(aws --endpoint-url {ep} s3api head-object --bucket pg-backups --key "${{OBJ_KEY}}" --query ContentLength --output text 2>/dev/null || echo 0)
DB_SIZE=$(mariadb -Nse "SELECT COALESCE(SUM(data_length + index_length), 0) FROM information_schema.tables;" 2>/dev/null || echo 0)
python3 - <<PY
import json, pathlib
mf = pathlib.Path("{meta_dir}/manifest.json")
items = json.loads(mf.read_text()) if mf.exists() else []
items.append({{
    "label": "${{LABEL}}",
    "type": "{btype}",
    "start": int("${{START_TS}}"),
    "stop": int("${{STOP_TS}}"),
    "db_size": int("${{DB_SIZE}}"),
    "repo_size": int("${{REPO_SIZE}}"),
    "to_lsn": "${{TO_LSN}}",
    "object": "${{OBJ_KEY}}",
}})
mf.write_text(json.dumps(items[-60:], indent=2))
PY
echo "Completed ${{LABEL}} (${{btype}}) LSN=${{TO_LSN}} repo_size=${{REPO_SIZE}} bytes in $((STOP_TS - START_TS))s"
"""
    return script, 7200


@app.post("/api/job")
def run_job(req: JobReq, request: Request, _: bool = Depends(require_auth)):
    if req.action not in ("check", "diff", "full"):
        raise HTTPException(status_code=400, detail="Unsupported action")
    srv = next((s for s in load_servers() if s["id"] == req.server_id), None)
    if not srv:
        raise HTTPException(status_code=404, detail="Server not found")

    job_id = uuid.uuid4().hex[:12]
    stanza = srv["stanza"]
    db_type = srv.get("db_type", "postgres")
    ip = request.client.host if request.client else None
    with JOB_LOCK:
        JOBS[job_id] = {"id": job_id, "status": "running", "logs": []}
    add_log_entry("JOB", req.action, f"{db_type} {req.action} on {srv['name']} ({srv['host']})", status="running", server_id=srv["id"], job_id=job_id, client_ip=ip)

    def worker():
        try:
            if db_type == "mariadb":
                cmd, job_timeout = _build_mariadb_job_cmd(stanza, req.action)
                append_job_log(job_id, f"Running MariaDB {req.action} on {srv['host']} ({stanza})...")
            else:
                cmd = (
                    f"sudo -u postgres pgbackrest --stanza={stanza} check"
                    if req.action == "check"
                    else f"sudo -u postgres pgbackrest --stanza={stanza} --type={req.action} backup"
                )
                job_timeout = 600
                append_job_log(job_id, f"Running on {srv['host']} ({stanza}): {cmd}")
            rc = ssh_run_stream(srv, cmd, lambda line: append_job_log(job_id, line), timeout=job_timeout)
            status = "success" if rc == 0 else "failed"
            append_job_log(job_id, f"Finished with exit code {rc}")
            _finalize_job(job_id, status)
        except Exception as e:
            append_job_log(job_id, f"ERROR: {e}")
            _finalize_job(job_id, "failed")

    threading.Thread(target=worker, daemon=True).start()
    return {"job_id": job_id}


@app.post("/api/restore")
def run_restore(req: RestoreReq, request: Request, _: bool = Depends(require_auth)):
    servers = load_servers()
    src = next((s for s in servers if s["id"] == req.source_server_id), None)
    tgt = next((s for s in servers if s["id"] == req.target_server_id), None)
    if not src or not tgt:
        raise HTTPException(status_code=404, detail="Source or target server not found")
    if not tgt.get("allow_restore", False):
        raise HTTPException(status_code=403, detail="Target server is locked against direct restore (PROD protection)")
    if req.confirm_stanza.strip() != src["stanza"]:
        raise HTTPException(status_code=400, detail="Confirmation stanza does not match")

    target_arg = ""
    if req.mode == "time":
        if not req.target_time or not re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}([+-]\d{2}(:\d{2})?)?$", req.target_time.strip()):
            raise HTTPException(status_code=400, detail="Invalid target_time format (expected YYYY-MM-DD HH:MM:SS+07)")
        target_arg = f'--type=time --target="{req.target_time.strip()}"'
    else:
        target_arg = "--type=default"

    job_id = uuid.uuid4().hex[:12]
    stanza = src["stanza"]
    pg_svc = tgt.get("pg_service", "postgresql@16-main")
    ip = request.client.host if request.client else None

    with JOB_LOCK:
        JOBS[job_id] = {"id": job_id, "status": "running", "logs": []}
    add_log_entry(
        "RESTORE",
        "pitr_restore",
        f"Restore {src['stanza']} -> {tgt['name']} ({tgt['host']}) [{req.mode}{' ' + req.target_time if req.target_time else ''}]",
        status="running",
        server_id=tgt["id"],
        job_id=job_id,
        client_ip=ip,
    )

    def worker():
        try:
            append_job_log(job_id, f"1. Stopping {pg_svc} on {tgt['host']}...")
            rc, out, err = ssh_run(tgt, f"systemctl stop {pg_svc}", timeout=60)
            if rc != 0:
                raise RuntimeError(f"Failed to stop PostgreSQL: {err or out}")
            append_job_log(job_id, "Stopped PostgreSQL cleanly.")

            if src["id"] != tgt["id"]:
                append_job_log(job_id, f"Checking stanza [{stanza}] in /etc/pgbackrest.conf on {tgt['host']}...")
                ensure_cmd = (
                    f"grep -q '^\\[{stanza}\\]$' /etc/pgbackrest.conf || "
                    f"printf '\\n[{stanza}]\\npg1-path=/var/lib/postgresql/16/main\\n' >> /etc/pgbackrest.conf"
                )
                rc_cfg, _, err_cfg = ssh_run(tgt, ensure_cmd, timeout=15)
                if rc_cfg != 0:
                    raise RuntimeError(f"Failed to prepare /etc/pgbackrest.conf for {stanza}: {err_cfg}")

            restore_cmd = f"sudo -u postgres pgbackrest --stanza={stanza} --delta {target_arg} --target-timeline=latest --target-action=promote restore"
            append_job_log(job_id, f"2. Running: {restore_cmd}")
            rc = ssh_run_stream(tgt, restore_cmd, lambda line: append_job_log(job_id, line), timeout=900)
            if rc != 0:
                raise RuntimeError(f"pgBackRest restore failed (rc={rc})")

            append_job_log(job_id, f"3. Starting {pg_svc} and waiting for promote...")
            rc, out, err = ssh_run(tgt, f"systemctl start {pg_svc}", timeout=60)
            if rc != 0:
                raise RuntimeError(f"Failed to start PostgreSQL: {err or out}")

            ready = False
            for _ in range(30):
                rc2, out2, _ = ssh_run(tgt, "sudo -u postgres psql -tAc 'SELECT pg_is_in_recovery();'", timeout=10)
                if rc2 == 0 and out2.strip() == "f":
                    ready = True
                    break
                time.sleep(1)

            if not ready:
                raise RuntimeError("PostgreSQL started but did not finish recovery within 30s")

            append_job_log(job_id, "PITR Restore completed successfully (pg_is_in_recovery=f)!")
            _finalize_job(job_id, "success")
        except Exception as e:
            append_job_log(job_id, f"ERROR: {e}")
            _finalize_job(job_id, "failed")

    threading.Thread(target=worker, daemon=True).start()
    return {"job_id": job_id}


@app.get("/api/job/{job_id}")
def get_job(job_id: str, _: bool = Depends(require_auth)):
    with JOB_LOCK:
        job = JOBS.get(job_id)
        if job:
            return job
    with LOG_FILE_LOCK:
        for e in load_logs():
            if e.get("id") == job_id:
                return {"id": job_id, "status": e.get("status", "unknown"), "logs": e.get("output", [])}
    raise HTTPException(status_code=404, detail="Job not found")


@app.get("/api/logs")
def get_logs(limit: int = 100, category: str = "ALL", server_id: str = "ALL", _: bool = Depends(require_auth)):
    with LOG_FILE_LOCK:
        entries = load_logs()
    if category != "ALL":
        entries = [e for e in entries if e.get("category") == category]
    if server_id != "ALL":
        entries = [e for e in entries if e.get("server_id") == server_id]
    return entries[: max(1, min(limit, 500))]


@app.get("/api/logs/remote/{server_id}")
def get_remote_pgbackrest_logs(server_id: str, lines: int = 150, _: bool = Depends(require_auth)):
    srv = next((s for s in load_servers() if s["id"] == server_id), None)
    if not srv:
        raise HTTPException(status_code=404, detail="Server not found")
    n = max(20, min(lines, 500))
    if srv.get("db_type", "postgres") == "mariadb":
        cmd = (
            f"tail -n {n} /var/log/mariadb-backup-{srv['stanza']}.log /var/log/mysql/error.log 2>/dev/null "
            "|| echo '(No MariaDB backup/error logs found on this server)'"
        )
    else:
        cmd = (
            f"ls -1t /var/log/pgbackrest/*.log 2>/dev/null | head -n 3 | xargs -r tail -n {n} "
            f"|| echo '(No /var/log/pgbackrest/*.log files found on this server)'"
        )
    try:
        rc, out, err = ssh_run(srv, cmd, timeout=15)
        return {"ok": rc == 0, "server": srv["name"], "host": srv["host"], "content": (out or err).strip()}
    except Exception as e:
        return {"ok": False, "server": srv["name"], "host": srv["host"], "content": str(e)}


app.mount("/static", StaticFiles(directory="/app/static"), name="static")


@app.get("/", response_class=HTMLResponse)
def index():
    return FileResponse("/app/static/index.html")
