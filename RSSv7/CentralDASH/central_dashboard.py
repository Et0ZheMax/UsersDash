#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
central_dashboard.py — стабильная сборка v8.0  (02 Aug 2025)

Главные изменения vs v7.2
• Асинхронно-параллельный сбор метрик с reuse-Session
• Полный health-check до старта сервера
• Конфиги в dataclass, строгая типизация
• Ограничение /api/central/refresh rate-limit 15 сек
• Flask debug выключен в production
"""

from __future__ import annotations
import os, sys, json, time, base64, ctypes, logging, subprocess, threading
from dataclasses import dataclass, asdict, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, TypedDict
from urllib.parse import quote

import requests
from flask import (
    Flask, jsonify, render_template, send_from_directory,
    abort, redirect, request, Response
)
from concurrent.futures import ThreadPoolExecutor, as_completed

# Общая загрузка /.env из корня репозитория (без перезаписи системных env).
def _load_root_env() -> None:
    current_file = Path(__file__).resolve()
    for parent in (current_file.parent, *current_file.parents):
        if (parent / ".git").exists():
            if str(parent) not in sys.path:
                sys.path.insert(0, str(parent))
            break

    from shared.env_loader import load_root_env_file

    load_root_env_file(current_file)

# ========== Консольный заголовок ==========
ctypes.windll.kernel32.SetConsoleTitleW("CentralDash v8.0") if sys.platform == "win32" else None  # type: ignore

# ========== Константы / окружение ==========
ROOT            = Path(__file__).resolve().parent
CFG_PATH        = ROOT / "central_config.json"
DEBUG           = bool(int(os.getenv("CDASH_DEBUG", "0")))
REQUEST_TIMEOUT = int(os.getenv("CDASH_HTTP_TIMEOUT", 4))
CHECK_INTERVAL  = int(os.getenv("CDASH_CHECK_INTERVAL", 900))
MAX_WORKERS     = 16

# ---------- Логирование ----------
logging.basicConfig(
    format="%(asctime)s — %(levelname)s — %(message)s",
    level=logging.DEBUG if DEBUG else logging.INFO
)
LOG = logging.getLogger("central_dash")
requests.packages.urllib3.disable_warnings()

# ========== Dataclasses & типы ==========
class PayStat(TypedDict):
    overdue: int
    soon: int
    missing: int

@dataclass
class ServerCfg:
    name: str
    ip:   str
    url:  str                      # ← было
    mon_url: str = ""              # ← НОВОЕ (может быть пустым)
    log_path: str = ""
    include_acc: bool = True
    main_script: str = ""
    monitor_scripts: dict[str, str] = field(default_factory=lambda: {
        "RssCounterWebV7": "RssV7",
        "clo.exe":         "CLO",
    })


@dataclass
class Settings:
    screens_dir     : Path               = Path(r"C:\Screens")
    check_interval  : int                = 900
    offline_thr     : int                = 7200
    log_thr         : int                = 3600
    widget_width    : int                = 20
    tasks_to_check  : List[str]          = field(default_factory=lambda: ["LD_Check", "AutoRefreshLogs"])
    telegram_token  : str               = ""
    telegram_chat   : str               = ""
    servers         : List[ServerCfg]    = field(default_factory=list)
    font_size       : int                = 14
    pay_cols        : List[int]          = field(default_factory=lambda: [45, 25, 30])

    @staticmethod
    def default() -> "Settings":
        return Settings(
            servers=[ServerCfg(
                name="208",
                ip="185.186.143.208",
                url="https://hotly-large-coral.cloudpub.ru",
                log_path=r"C:\gnbots\logs"
            )]
        )

# ========== Health-check ==========
def health_check(cfg: Settings) -> None:
    ok = True

    def err(msg: str) -> None:
        nonlocal ok
        LOG.error("[HEALTH] %s", msg)
        ok = False

    # 1) права администратора (Win) / root (POSIX)
    try:
        is_admin = bool(ctypes.windll.shell32.IsUserAnAdmin())  # type: ignore
    except Exception:
        is_admin = (os.geteuid() == 0) if hasattr(os, "geteuid") else False  # type: ignore

    if not is_admin:
        err("скрипт не запущен от имени администратора — запуск остановлен")

    # 2) директория для скриншотов
    try:
        cfg.screens_dir.mkdir(exist_ok=True, parents=True)
    except PermissionError:
        err(f"нет прав на запись {cfg.screens_dir}")

    # 3) конфигурация серверов
    if not cfg.servers:
        err("в конфиге нет ни одного сервера")

    # 4) проверка занятости порта 5010
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        if s.connect_ex(("0.0.0.0", 5010)) == 0:
            err("порт 5010 уже занят, завершаем запуск")

    if not ok:
        LOG.error("Health-check FAILED ➜ процесс остановлен\n")
        sys.exit(1)

# ========== Загрузка конфигурации ==========
# ========== Загрузка конфигурации ==========
# ========== Загрузка конфигурации ==========
def load_cfg() -> Settings:
    """
    Читаем central_config.json.  Если файл отсутствует — создаём дефолтный.
    Поддерживаем обратную совместимость:
    • переименовываем legacy-поля (log_threshold → log_thr и т.д.);
    • приводим screens_dir к pathlib.Path.
    """
    if not CFG_PATH.exists():                                       # ➊ свежая установка
        CFG_PATH.write_text(
            json.dumps(asdict(Settings.default()), ensure_ascii=False, indent=2),
            "utf-8"
        )
        return Settings.default()

    # ---------- читаем файл ----------
    try:
        raw: Dict[str, Any] = json.loads(CFG_PATH.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):              # ➋ битый файл
        bak = CFG_PATH.with_suffix(".bak")
        bak.write_bytes(CFG_PATH.read_bytes())
        CFG_PATH.write_text(
            json.dumps(asdict(Settings.default()), ensure_ascii=False, indent=2),
            "utf-8"
        )
        LOG.warning("конфиг повреждён, создан новый, backup ➜ %s", bak)
        return Settings.default()

    # ---------- 👉 legacy aliases ------------------------------------------
    aliases = {
        "log_threshold":      "log_thr",
        "offline_threshold":  "offline_thr",
        "widgetWidth":        "widget_width",
        "pay_columns":        "pay_cols",
    }
    for old, new in aliases.items():
        if old in raw and new not in raw:
            raw[new] = raw.pop(old)

    # ---------- автозаполняем mon_url, если отсутствует -------------------
    for srv in raw.get("servers", []):
        if "mon_url" not in srv or not srv["mon_url"]:
            ip = srv.get("ip", "")
            if ip:
                srv["mon_url"] = f"http://{ip}:5016"


    # ---------- 👣 пути → Path ---------------------------------------------
    if isinstance(raw.get("screens_dir"), str):
        raw["screens_dir"] = Path(raw["screens_dir"])

    # ---------- сервера в dataclass ----------------------------------------
    raw["servers"] = [
        ServerCfg(**srv) if isinstance(srv, dict) else srv
        for srv in raw.get("servers", [])
    ]

    return Settings(**raw)

CFG = load_cfg()

health_check(CFG)

# ========== Глобалы & кеши ==========
SCREENS_DIR   = CFG.screens_dir
OFFLINE_THR   = CFG.offline_thr
LOG_THR       = CFG.log_thr
TASKS         = CFG.tasks_to_check
WIDGET_WIDTH  = CFG.widget_width
_session      = requests.Session()
_cache        : Dict[str, Dict[str, Any]] = {}
_alerted      : Dict[str, str] = {}
_last_manual  : float = 0.0                # защита от спама refresh
_lock         = threading.Lock()

# ========== Утилиты ==========
def ping(ip: str) -> bool:
    param = "-n" if os.name == "nt" else "-c"
    cmd   = f"ping {param} 1 -w 800 {ip}".split()
    return subprocess.call(cmd, stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL) == 0

def http_json(url: str, timeout: int = REQUEST_TIMEOUT) -> Any | None:
    try:
        resp = _session.get(url, timeout=timeout, verify=False)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        LOG.debug("HTTP %s -> %s", url, e)
        return None

def map_ldconfig(srv: ServerCfg, cfg_name: str) -> str:
    mapping = http_json(f"{srv.url}/api/ldmap") or {}
    return mapping.get(cfg_name, cfg_name)

# ========== Сбор метрик ==========
def collect_one(srv: ServerCfg) -> Dict[str, Any]:
    """Основная тяжёлая функция: собираем всё, что можем с конкретного сервера."""
    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")

    out: Dict[str, Any] = {
        "name": srv.name,
        "url":  srv.url,
        "ts":   now_iso,
        "ping": ping(srv.ip),
        "updated_iso": now_iso,
        "includeAcc": srv.include_acc
    }

    # --- быстрый выход, если сервер offline ---
    if not out["ping"]:
        return out

    # ================ API block ================
    st_resp = http_json(f"{srv.url}/api/serverStatus") or {}
    st_raw = st_resp.get("status", st_resp).get(srv.name, {})
    out["gnOk"]    = st_raw.get("gnOk") or st_raw.get("gn") or st_raw.get("gn_running") or True
    out["dnOk"]    = st_raw.get("dnOk") or st_raw.get("dn") or st_raw.get("dn_running") or True
    # --- dnCount (окна LDPlayer) -----------------------------------------
    dn_json = http_json(f"{srv.mon_url}/api/dnCount") if srv.mon_url else None
    out["dnCount"] = dn_json.get("dnCount") if dn_json else 0


    # --- аккаунты / доход ---
    res          = http_json(f"{srv.url}/api/resources") or {}
    accounts     = res.get("accounts", [])
    out["accCount"] = len(accounts)

    inc            = http_json(f"{srv.url}/api/income") or {}
    out["incomeTotal"] = float(inc.get("total", 0))
    out["incomeLeft"]  = float(inc.get("left",  0))

    # --- scriptStatus (RssV7 / CLO и др.) ---------------------------------
    script_states: dict[str, bool] = {}
    for internal, disp in srv.monitor_scripts.items():
        api = f"{srv.mon_url}/api/scriptStatus?script={quote(internal)}" if srv.mon_url else ""
        st  = http_json(api) if api else {}
        script_states[disp] = bool(st.get("running", False))
    out["scripts"] = script_states



    # --- log age ---
    try:
        last = max(datetime.fromisoformat(acc["last_updated"]) for acc in accounts)
        out["no_logs"] = (datetime.now(timezone.utc) - last).total_seconds() > LOG_THR
    except Exception:
        out["no_logs"] = True

    # --- Tasks-scheduler ---
    ordered: Dict[str, Optional[bool]] = {}
    for t in TASKS:
        raw = (http_json(f"{srv.url}/api/taskState?name={quote(t)}") or {}).get("enabled")
        ordered[t] = None if raw is None else bool(raw)
    out["tasks"] = ordered

    # --- Fix-счётчик ---
    lg = http_json(f"{srv.url}/api/logstatus") or {}
    out["fixCount"] = sum(1 for v in lg.values()
                          if v.get("hasUpdateGame") or v.get("noMarch") or v.get("zeroGain"))

    # --- Pay-alert ---
    pays  = http_json(f"{srv.url}/api/payalert") or []
    ru    = {"overdue": "Просрочена", "soon": "Скоро", "missing": "Нет данных"}
    stat  = {"overdue": 0, "soon": 0, "missing": 0}
    for p in pays:
        p["status"] = ru.get(p["status"], p["status"])
        key = next(k for k, v in ru.items() if v == p["status"])
        stat[key] += 1
    out["payCounts"] = stat
    out["payList"]   = pays[:10]

    # --- Crashed windows ---
    crashed = (
        http_json(f"{srv.url}/api/crashed") or
        http_json(f"{srv.url}/api/crashedEmus") or []
    )
    out["crashCount"] = len(crashed)
    out["crashList"]  = [map_ldconfig(srv, c) for c in crashed][:10]

    # --- Screenshot (кешируем между тиками) ---
    scr_file = SCREENS_DIR / f"{srv.name}.png"
    if not scr_file.exists() or time.time() - scr_file.stat().st_mtime > CHECK_INTERVAL:
        img = http_json(f"{srv.url}/api/screenshot", timeout=6)
        if img and "data" in img:
            try:
                scr_file.write_bytes(base64.b64decode(img["data"].split(",", 1)[1]))
            except Exception as e:
                LOG.debug("cannot save screenshot %s: %s", srv.name, e)

    if scr_file.exists():
        out["screenshot"] = f"/screens/{scr_file.name}?ts={int(scr_file.stat().st_mtime)}"

    # --- Ленивая заглушка для логов ---
    out["logLines"] = []
    return out

# ========== Алерты ==========
def send_tg(msg: str) -> None:
    if not CFG.telegram_token or not CFG.telegram_chat:
        return
    try:
        _session.post(
            f"https://api.telegram.org/bot{CFG.telegram_token}/sendMessage",
            json={"chat_id": CFG.telegram_chat, "text": msg},
            timeout=REQUEST_TIMEOUT, verify=False
        )
    except requests.RequestException:
        pass

def check_alerts(prev: Dict[str, Any] | None, cur: Dict[str, Any]) -> None:
    n = cur["name"]
    if not cur["ping"]:
        if _alerted.get(n) != "ping":
            send_tg(f"❌ {n} offline"); _alerted[n] = "ping"
        return
    issue = (not cur.get("gnOk") or not cur.get("dnOk") or cur.get("no_logs"))
    if issue and _alerted.get(n) != "issue":
        send_tg(f"⚠️ {n}: проблемы с ботом/логами"); _alerted[n] = "issue"
    if not issue:
        _alerted.pop(n, None)

# ========== Главный цикл ==========
def collect_all() -> None:
    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(CFG.servers))) as ex:
        fut = {ex.submit(collect_one, s): s.name for s in CFG.servers}
        for f in as_completed(fut):
            name = fut[f]
            try:
                cur = f.result()
                with _lock:
                    check_alerts(_cache.get(name), cur)
                    _cache[name] = cur
            except Exception as e:
                LOG.error("collect %s: %s", name, e)

def loop() -> None:
    while True:
        collect_all()
        time.sleep(CHECK_INTERVAL)

# ========== Flask ==========
app = Flask(__name__, template_folder="templates", static_folder="static")

@app.after_request
def add_headers(resp: Response) -> Response:
    resp.headers["Cache-Control"] = "no-store"
    return resp

@app.route("/")
def root(): 
    return redirect("/central", code=302)

@app.route("/central")
def central(): 
    return render_template("central.html")

@app.route("/api/central/status")
def api_status():
    if not _cache:
        stub = [{"name": s.name, "url": s.url, "loading": True} for s in CFG.servers]
        return jsonify({
            "updated": datetime.now().isoformat(),
            "widget_width": WIDGET_WIDTH,
            "servers": stub
        })
    return jsonify({
        "updated": datetime.now().isoformat(),
        "widget_width": WIDGET_WIDTH,
        "servers": list(_cache.values())
    })

@app.route("/api/central/refresh")
def api_refresh():
    global _last_manual
    now = time.time()
    if now - _last_manual < 15:          # rate-limit
        return jsonify({"status":"busy"})
    _last_manual = now
    threading.Thread(target=collect_all, daemon=True).start()
    return jsonify({"status": "ok"})

@app.route("/api/log_lazy")
def log_lazy():
    url   = request.args.get("url", "")
    lines = int(request.args.get("n", "50"))
    data  = http_json(f"{url}/api/log_slice?lines={lines}") or {"lines": []}
    return jsonify(data)

@app.route("/screens/<path:fname>")
def screens(fname):
    fp = SCREENS_DIR / fname
    if not fp.exists():
        abort(404)
    return send_from_directory(SCREENS_DIR, fname, mimetype="image/png")

@app.route("/api/central/summary")
def api_summary():
    with _lock:
        acc_sum   = sum(s.get("accCount",0)   for s in _cache.values() if s.get("includeAcc",True))
        total_sum = sum(s.get("incomeTotal",0.0) for s in _cache.values() if s.get("includeAcc",True))
        left_sum  = sum(s.get("incomeLeft",0.0)  for s in _cache.values() if s.get("includeAcc",True))
    return jsonify({
        "accounts": acc_sum,
        "total":    round(total_sum, 2),
        "left":     round(left_sum,  2)
    })

# ----- конфиг CRUD -----
@app.route("/api/config", methods=["GET", "POST"])
def api_config():
    """
    CRUD-энд-пойнт для central_config.json

    • GET  — вернуть текущий конфиг (Path → str, dataclass → dict)
    • POST — принять новый конфиг, сохранить на диск и перезагрузить сервис
    """
    global CFG

    # ---------- GET ----------
    if request.method == "GET":
        cfg_dict = asdict(CFG)                              # dataclass → dict
        # Pathlib.Path и прочие нестандартные объекты → str
        return jsonify(json.loads(json.dumps(cfg_dict, default=str, ensure_ascii=False)))

        # ---------- POST ----------
    new_raw: Dict[str, Any] = request.get_json(force=True)

    # 1) legacy aliases ────────────────────────────────────────────────────
    aliases_top = {
        "log_threshold":     "log_thr",
        "offline_threshold": "offline_thr",
        "widgetWidth":       "widget_width",
        "pay_columns":       "pay_cols",
    }
    for old, new in aliases_top.items():
        if old in new_raw:
            # переименовываем ВСЕГДА, даже если новое поле уже есть
            new_raw[new] = new_raw.get(new, new_raw.pop(old))

    # 1.1) удаляем все лишние top-level ключи, которых нет в Settings
    allowed_top = {
        "screens_dir","check_interval","offline_thr","log_thr","widget_width",
        "tasks_to_check","telegram_token","telegram_chat","servers",
        "font_size","pay_cols"
    }
    new_raw = {k:v for k,v in new_raw.items() if k in allowed_top}


    # 2) screens_dir: str → Path
    if isinstance(new_raw.get("screens_dir"), str):
        new_raw["screens_dir"] = Path(new_raw["screens_dir"])

    # 3) servers: фильтруем лишнее и автодополняем mon_url
    allowed_keys = {
        "name", "ip", "url", "mon_url", "log_path",
        "include_acc", "main_script", "monitor_scripts"
    }
    clean_servers: list[ServerCfg] = []
    for srv in new_raw.get("servers", []):
        if not isinstance(srv, dict):
            continue

        # autocomplete mon_url
        if not srv.get("mon_url") and srv.get("ip"):
            srv["mon_url"] = f"http://{srv['ip']}:5016"

        srv_clean = {k: v for k, v in srv.items() if k in allowed_keys}
        try:
            clean_servers.append(ServerCfg(**srv_clean))
        except TypeError as e:
            LOG.error("пропускаю сервер %s: %s", srv.get("name", "?"), e)

    new_raw["servers"] = clean_servers

    # 4) конструируем новый Settings
    new_cfg = Settings(**new_raw)

    # 5) сохраняем и применяем
    CFG_PATH.write_text(
        json.dumps(asdict(new_cfg), ensure_ascii=False, indent=2, default=str),
        "utf-8"
    )
    CFG = new_cfg
    with _lock:
        _cache.clear()

    return jsonify({"status": "ok"})



# ========== Entrypoint ==========
if __name__ == "__main__":
    _load_root_env()
    if os.environ.get("WERKZEUG_RUN_MAIN") == "true":    # не выполнять loop в reloader-процессе
        threading.Thread(target=loop, daemon=True).start()
    app.run(host="0.0.0.0", port=5010, debug=False)
