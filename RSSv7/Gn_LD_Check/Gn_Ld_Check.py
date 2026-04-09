#!/usr/bin/env python3
# GN_LD_CHECK (v4.2.1)
# -----------------------------------------------------------------------------
# Новое в 4.2.1:
# - Совместимость с разными версиями python-telegram-bot (v20–v21): не импортируем
#   отсутствующий в новых версиях Unauthorized. Кейс "Unauthorized" ловится через
#   общий TelegramError по имени класса (e.__class__.__name__).
# Остальное:
# - Инкрементальное чтение логов через SQLite (по маске botYYYYMMDD*.txt).
# - Детектор "живой" активности по паттернам, игнор "шумных" строк.
# - Если ≥ inactivity_minutes нет ЖИВОЙ активности — алерт + ребут.
# - Устойчивые отправки в Telegram (таймауты, ретраи, RetryAfter, локальный спул).
# - Запуск от имени администратора (UAC), health-check, авто-очистка БД.
# -----------------------------------------------------------------------------

import os
import sys
import json
import glob
import psutil
import subprocess
import time
import ctypes
import asyncio
import sqlite3
import re
from dataclasses import dataclass, field
from typing import List, Optional, Tuple
from datetime import datetime, timedelta, timezone

# telegram v20+
from telegram import Bot
from telegram.error import (
    TelegramError, RetryAfter, TimedOut, NetworkError, BadRequest, Forbidden
)
from telegram.request import HTTPXRequest  # для настраиваемых таймаутов/пула

# Общая загрузка /.env из корня репозитория (без перезаписи системных env).
def _load_root_env() -> None:
    from pathlib import Path

    current_file = Path(__file__).resolve()
    for parent in (current_file.parent, *current_file.parents):
        if (parent / ".git").exists():
            if str(parent) not in sys.path:
                sys.path.insert(0, str(parent))
            break

    from shared.env_loader import load_root_env_file

    load_root_env_file(current_file)

# ------------------------------ UAC -----------------------------------------
def _is_admin() -> bool:
    try:
        return bool(ctypes.windll.shell32.IsUserAnAdmin()) if sys.platform == "win32" else True
    except Exception:
        return False

def _relaunch_as_admin():
    if sys.platform != "win32":
        return
    params = " ".join(f'"{a}"' for a in sys.argv[1:])
    ctypes.windll.shell32.ShellExecuteW(
        None, "runas", sys.executable, f'"{os.path.abspath(sys.argv[0])}" {params}', None, 1
    )

# ------------------------------ Console title --------------------------------
if sys.platform == "win32":
    try:
        ctypes.windll.kernel32.SetConsoleTitleW("GN_LD_CHECK v4.2.1")
    except Exception:
        pass

# ------------------------------ Config --------------------------------------
BASE_DIR   = os.path.dirname(__file__)
CONFIG_DIR = os.path.join(BASE_DIR, "settings")
DATA_DIR   = os.path.join(BASE_DIR, "data")
CONFIG_PATH = os.path.join(CONFIG_DIR, "gn_ld_check.json")
SPOOL_PATH  = os.path.join(DATA_DIR, "tg_spool.jsonl")

DEFAULTS = {
    # Telegram
    "telegram_token": "__ENV__:GNLDCHECK_TELEGRAM_TOKEN",
    "chat_id": "275483461",
    "thread_id": None,

    # Порог по окнам
    "threshold_windows": 6,

    # Ярлык GnBots
    "gnbots_shortcut": r"C:\Users\administrator\Desktop\GnBots.lnk",

    # Логи
    "log_dir": r"C:\Program Files\GnBots\logs",
    "days_back_scan": 1,          # сегодня + вчера
    "inactivity_minutes": 20,     # по ЖИВЫМ событиям

    # БД
    "db_path": "data/gn_ld_check.sqlite3",
    "retention_days": 2,
    "tail_init_bytes": 65536,

    # Паттерны
    "live_patterns": [
        r"\|[0-9a-f]{16}\|",
        r"\b(getMap|Refreshing Image|pulse)\b",
        r"\b(Marches:|Reached Maximum of Marches)\b",
        r"\b(Found world)\b"
    ],
    "idle_patterns": [
        r"\bAnySessionsBootingAsync\b",
        r"\bHandleErrorsAsync: Scanning for error windows\b",
        r"\bCurrent Error Counters:\b"
    ],

    # Telegram HTTP/повторы
    "tg_read_timeout": 20,
    "tg_connect_timeout": 10,
    "tg_write_timeout": 20,
    "tg_pool_timeout": 10,
    "tg_pool_size": 8,
    "tg_send_retries": 5,
    "tg_retry_base": 2,
    "tg_retry_max": 30
}

@dataclass
class Settings:
    telegram_token: str
    chat_id: str
    thread_id: Optional[int]
    threshold_windows: int
    gnbots_shortcut: str
    log_dir: str
    days_back_scan: int
    inactivity_minutes: int
    db_path: str
    retention_days: int
    tail_init_bytes: int
    live_patterns: List[str] = field(default_factory=list)
    idle_patterns: List[str] = field(default_factory=list)
    tg_read_timeout: int = 20
    tg_connect_timeout: int = 10
    tg_write_timeout: int = 20
    tg_pool_timeout: int = 10
    tg_pool_size: int = 8
    tg_send_retries: int = 5
    tg_retry_base: int = 2
    tg_retry_max: int = 30

def _ensure_config() -> None:
    os.makedirs(CONFIG_DIR, exist_ok=True)
    os.makedirs(DATA_DIR, exist_ok=True)
    if not os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(DEFAULTS, f, ensure_ascii=False, indent=2)
        print(f"[INIT] Создан конфиг: {CONFIG_PATH}")

def _split_env_regex_list(var_name: str) -> Optional[List[str]]:
    v = os.getenv(var_name, "").strip()
    if not v:
        return None
    return [p.strip() for p in v.split(";") if p.strip()] or None

def _get_int_env(name: str, fallback: int) -> int:
    v = os.getenv(name)
    if v and v.strip().isdigit():
        return int(v.strip())
    return fallback


def require_env(name: str) -> str:
    """[SECURITY] Возвращает обязательную env-переменную или бросает понятную ошибку."""
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Не задана обязательная переменная окружения: {name}")
    return value

def _load_config() -> Settings:
    _ensure_config()
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    def _int_field(env, key):
        return _get_int_env(env, int(data.get(key, DEFAULTS[key])))

    token = require_env("GNLDCHECK_TELEGRAM_TOKEN")
    chat_id = os.getenv("GNLDCHECK_CHAT_ID", str(data.get("chat_id", DEFAULTS["chat_id"])))
    thread_id_env = os.getenv("GNLDCHECK_THREAD_ID", "")
    thread_id = int(thread_id_env) if thread_id_env.strip().isdigit() else data.get("thread_id")

    threshold_windows = _int_field("GNLDCHECK_THRESHOLD_WINDOWS", "threshold_windows")
    shortcut = os.getenv("GNLDCHECK_GNBOTS_SHORTCUT", data.get("gnbots_shortcut", DEFAULTS["gnbots_shortcut"]))

    log_dir = os.getenv("GNLDCHECK_LOG_DIR", data.get("log_dir", DEFAULTS["log_dir"]))
    days_back_scan = _int_field("GNLDCHECK_DAYS_BACK_SCAN", "days_back_scan")
    inactivity_minutes = _int_field("GNLDCHECK_INACTIVITY_MINUTES", "inactivity_minutes")

    db_path = os.getenv("GNLDCHECK_DB_PATH", data.get("db_path", DEFAULTS["db_path"]))
    if not os.path.isabs(db_path):
        db_path = os.path.join(BASE_DIR, db_path)
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    retention_days = _int_field("GNLDCHECK_RETENTION_DAYS", "retention_days")
    tail_init_bytes = _int_field("GNLDCHECK_TAIL_INIT_BYTES", "tail_init_bytes")

    env_live = _split_env_regex_list("GNLDCHECK_LIVE_PATTERNS")
    env_idle = _split_env_regex_list("GNLDCHECK_IDLE_PATTERNS")
    live_patterns = env_live if env_live is not None else data.get("live_patterns", DEFAULTS["live_patterns"])
    idle_patterns = env_idle if env_idle is not None else data.get("idle_patterns", DEFAULTS["idle_patterns"])

    tg_read_timeout  = _int_field("GNLDCHECK_TG_READ_TIMEOUT",  "tg_read_timeout")
    tg_connect_timeout = _int_field("GNLDCHECK_TG_CONNECT_TIMEOUT", "tg_connect_timeout")
    tg_write_timeout = _int_field("GNLDCHECK_TG_WRITE_TIMEOUT", "tg_write_timeout")
    tg_pool_timeout  = _int_field("GNLDCHECK_TG_POOL_TIMEOUT",  "tg_pool_timeout")
    tg_pool_size     = _int_field("GNLDCHECK_TG_POOL_SIZE",     "tg_pool_size")
    tg_send_retries  = _int_field("GNLDCHECK_TG_SEND_RETRIES",  "tg_send_retries")
    tg_retry_base    = _int_field("GNLDCHECK_TG_RETRY_BASE",    "tg_retry_base")
    tg_retry_max     = _int_field("GNLDCHECK_TG_RETRY_MAX",     "tg_retry_max")

    return Settings(
        telegram_token=token,
        chat_id=str(chat_id),
        thread_id=thread_id if thread_id is None else int(thread_id),
        threshold_windows=threshold_windows,
        gnbots_shortcut=shortcut,
        log_dir=log_dir,
        days_back_scan=days_back_scan,
        inactivity_minutes=inactivity_minutes,
        db_path=db_path,
        retention_days=retention_days,
        tail_init_bytes=tail_init_bytes,
        live_patterns=live_patterns,
        idle_patterns=idle_patterns,
        tg_read_timeout=tg_read_timeout,
        tg_connect_timeout=tg_connect_timeout,
        tg_write_timeout=tg_write_timeout,
        tg_pool_timeout=tg_pool_timeout,
        tg_pool_size=tg_pool_size,
        tg_send_retries=tg_send_retries,
        tg_retry_base=tg_retry_base,
        tg_retry_max=tg_retry_max
    )

# ------------------------------ Telegram helpers ----------------------------
def build_bot(cfg: Settings) -> Bot:
    req = HTTPXRequest(
        connection_pool_size=cfg.tg_pool_size,
        read_timeout=cfg.tg_read_timeout,
        write_timeout=cfg.tg_write_timeout,
        connect_timeout=cfg.tg_connect_timeout,
        pool_timeout=cfg.tg_pool_timeout,
    )
    return Bot(token=cfg.telegram_token, request=req)

async def flush_spool(bot: Bot, cfg: Settings) -> None:
    if not os.path.exists(SPOOL_PATH):
        return
    try:
        with open(SPOOL_PATH, "r", encoding="utf-8") as f:
            lines = f.read().splitlines()
        open(SPOOL_PATH, "w", encoding="utf-8").close()
        for line in lines:
            try:
                item = json.loads(line)
                text = item.get("text", "")
                thread_id = item.get("thread_id", None)
                await safe_send(bot, cfg, text, thread_id)
            except Exception as e:
                print(f"[WARN] Не удалось дослать из спула: {e}")
                with open(SPOOL_PATH, "a", encoding="utf-8") as fa:
                    fa.write(line + "\n")
    except Exception as e:
        print(f"[WARN] Ошибка работы со спулом: {e}")

async def safe_send(bot: Bot, cfg: Settings, text: str, thread_id: int | None = None) -> None:
    """
    Устойчивая отправка:
      - RetryAfter: ждём указанное сервером время;
      - TimedOut/NetworkError: экспоненциальные повторы;
      - BadRequest по темам: шлём без thread_id;
      - Forbidden/Unauthorized: не ретраим (ловим через имя класса, чтобы не зависеть от версии PTB);
      - при полном провале — пишем в локальный спул (будет дослано).
    """
    attempts = max(1, cfg.tg_send_retries)
    delay = max(1, cfg.tg_retry_base)
    max_delay = max(delay, cfg.tg_retry_max)

    for i in range(1, attempts + 1):
        try:
            await bot.send_message(
                chat_id=cfg.chat_id,
                text=text,
                message_thread_id=thread_id,
                read_timeout=cfg.tg_read_timeout
            )
            return
        except RetryAfter as e:
            wait = int(getattr(e, "retry_after", delay)) + 1
            print(f"[INFO] Telegram RetryAfter, ждём {wait}s… (attempt {i}/{attempts})")
            await asyncio.sleep(min(wait, max_delay))
        except (TimedOut, NetworkError) as e:
            print(f"[WARN] Сетевой таймаут Telegram: {e} (attempt {i}/{attempts})")
            await asyncio.sleep(min(delay, max_delay))
            delay = min(delay * 2, max_delay)
        except BadRequest as e:
            s = str(e).lower()
            if "message thread" in s or "topic" in s:
                print("[WARN] BadRequest: thread_id недоступен, шлём без темы…")
                try:
                    await bot.send_message(chat_id=cfg.chat_id, text=text, read_timeout=cfg.tg_read_timeout)
                    return
                except Exception as e2:
                    print(f"[WARN] Повтор без thread_id не удался: {e2}")
            # остальной BadRequest обычно бессмысленно ретраить
            break
        except TelegramError as e:
            # Универсальный блок для разных версий PTB:
            cls = e.__class__.__name__
            if cls in ("Forbidden", "Unauthorized"):
                print(f"[WARN] Telegram {cls}: {e} — не повторяем.")
                break
            print(f"[WARN] TelegramError: {e} (attempt {i}/{attempts})")
            await asyncio.sleep(min(delay, max_delay))
            delay = min(delay * 2, max_delay)
        except Exception as e:
            print(f"[WARN] Неожиданная ошибка Telegram: {e}")
            break

    # Если не удалось — положим сообщение в спул
    try:
        with open(SPOOL_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps({"ts": int(time.time()), "text": text, "thread_id": thread_id}) + "\n")
        print("[INFO] Сообщение положено в локальный спул и будет дослано при следующем запуске.")
    except Exception as e:
        print(f"[WARN] Не удалось записать в спул: {e}")

# ------------------------------ Proc utils ----------------------------------
def is_process_running(name: str) -> bool:
    for proc in psutil.process_iter(['name']):
        try:
            if proc.info['name'] and proc.info['name'].lower() == name.lower():
                return True
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return False

def count_processes(name: str) -> int:
    cnt = 0
    for proc in psutil.process_iter(['name']):
        try:
            if proc.info['name'] and proc.info['name'].lower() == name.lower():
                cnt += 1
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return cnt

def kill_process(name: str, soft_timeout: int = 5, hard_timeout: int = 5) -> list[int]:
    killed: list[int] = []
    for proc in psutil.process_iter(['pid', 'name']):
        try:
            if proc.info['name'] and proc.info['name'].lower() == name.lower():
                pid = proc.info['pid']
                try:
                    proc.terminate()
                    proc.wait(timeout=soft_timeout)
                except psutil.TimeoutExpired:
                    try:
                        proc.kill()
                        proc.wait(timeout=hard_timeout)
                    except psutil.TimeoutExpired:
                        subprocess.run(
                            ['taskkill', '/F', '/T', '/PID', str(pid)],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
                        )
                killed.append(pid)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return killed

# ------------------------------ Log path utils ------------------------------
def _date_mask_strings(days_back: int) -> List[str]:
    res = []
    today = datetime.now()
    for i in range(days_back + 1):
        res.append((today - timedelta(days=i)).strftime("%Y%m%d"))
    return res

def build_log_patterns(cfg: Settings) -> List[str]:
    masks: List[str] = []
    for ds in _date_mask_strings(cfg.days_back_scan):
        masks.append(os.path.join(cfg.log_dir, f"bot{ds}*.txt"))
    return masks

def _expand_masks(masks: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for m in masks:
        for f in glob.glob(os.path.expandvars(os.path.expanduser(m))):
            if os.path.isfile(f) and f not in seen:
                seen.add(f)
                out.append(f)
    return out

# ------------------------------ SQLite state --------------------------------
SQL_SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
CREATE TABLE IF NOT EXISTS log_state (
    file_path         TEXT PRIMARY KEY,
    last_seen_mtime   REAL NOT NULL,
    last_seen_size    INTEGER NOT NULL,
    last_offset       INTEGER NOT NULL,
    last_activity_ts  REAL NOT NULL,
    last_update_ts    REAL NOT NULL,
    last_live_ts      REAL NOT NULL DEFAULT 0,
    last_idle_ts      REAL NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS meta_kv (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""

def db_connect(path: str) -> sqlite3.Connection:
    con = sqlite3.connect(path, timeout=15, isolation_level=None)
    con.execute("PRAGMA foreign_keys=ON;")
    for stmt in SQL_SCHEMA.strip().split(";"):
        s = stmt.strip()
        if s:
            con.execute(s)
    cols = {r[1] for r in con.execute("PRAGMA table_info(log_state);").fetchall()}
    if "last_live_ts" not in cols:
        con.execute("ALTER TABLE log_state ADD COLUMN last_live_ts REAL NOT NULL DEFAULT 0;")
    if "last_idle_ts" not in cols:
        con.execute("ALTER TABLE log_state ADD COLUMN last_idle_ts REAL NOT NULL DEFAULT 0;")
    return con

def db_get_kv(con: sqlite3.Connection, key: str, default: Optional[str] = None) -> Optional[str]:
    row = con.execute("SELECT value FROM meta_kv WHERE key = ?;", (key,)).fetchone()
    return row[0] if row else default

def db_set_kv(con: sqlite3.Connection, key: str, value: str) -> None:
    con.execute(
        "INSERT INTO meta_kv(key,value) VALUES(?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value;",
        (key, value)
    )

def now_ts() -> float:
    return time.time()

# ------------------------------ Log parsing utils ---------------------------
TIMESTAMP_PREFIX_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\.\d+\s+\+\d{2}:\d{2}\s+\[[A-Z]+\]\s+"
)
TS_PARSE_RE = re.compile(
    r"^(?P<y>\d{4})-(?P<m>\d{2})-(?P<d>\d{2})\s+"
    r"(?P<h>\d{2}):(?P<mi>\d{2}):(?P<s>\d{2})\.(?P<ms>\d+)\s+\+"
    r"(?P<tz_h>\d{2}):(?P<tz_m>\d{2})"
)

def parse_line_timestamp_epoch(line: str) -> Optional[float]:
    m = TS_PARSE_RE.match(line)
    if not m:
        return None
    try:
        y = int(m.group("y")); mo = int(m.group("m")); d = int(m.group("d"))
        h = int(m.group("h")); mi = int(m.group("mi")); s = int(m.group("s"))
        ms = int(m.group("ms"))
        tz_h = int(m.group("tz_h")); tz_m = int(m.group("tz_m"))
        tz = timezone(timedelta(hours=tz_h, minutes=tz_m))
        dt = datetime(y, mo, d, h, mi, s, ms*1000, tzinfo=tz)
        return dt.timestamp()
    except Exception:
        return None

def normalize_line(line: str) -> str:
    return TIMESTAMP_PREFIX_RE.sub("", line).strip()

def compile_regex_list(patterns: List[str]) -> List[re.Pattern]:
    out: List[re.Pattern] = []
    for p in patterns:
        try:
            out.append(re.compile(p, flags=re.IGNORECASE))
        except re.error as e:
            print(f"[WARN] Некорректный регэксп '{p}': {e}")
    return out

# ------------------------------ Incremental scan ----------------------------
def scan_logs_incremental(cfg: Settings, con: sqlite3.Connection) -> Tuple[Optional[float], dict]:
    """
    Возвращает (max_last_live_ts, details).
    details[filepath] = {
        'last_live_min_ago', 'last_idle_min_ago', 'last_activity_min_ago',
        'read_bytes', 'read_from', 'init_read', 'size', 'mtime_min_ago'
    }
    """
    live_re = compile_regex_list(cfg.live_patterns)
    idle_re = compile_regex_list(cfg.idle_patterns)

    masks = build_log_patterns(cfg)
    files = _expand_masks(masks)
    details: dict = {}
    if not files:
        return None, details

    cur_ts = now_ts()
    max_live_ts: Optional[float] = None

    for path in files:
        try:
            st = os.stat(path)
            size = int(st.st_size)
            mtime = float(st.st_mtime)
        except Exception as e:
            details[path] = {"error": f"stat_failed: {e}"}
            continue

        row = con.execute(
            "SELECT last_seen_mtime, last_seen_size, last_offset, last_activity_ts, last_live_ts, last_idle_ts "
            "FROM log_state WHERE file_path = ?;", (path,)
        ).fetchone()

        if row is None:
            read_from = max(0, size - cfg.tail_init_bytes)
            init_read = True
            prev_activity_ts = 0.0
            prev_live_ts = 0.0
            prev_idle_ts = 0.0
            last_seen_mtime_old = 0.0
            last_offset_old = 0
        else:
            last_seen_mtime_old, last_seen_size_old, last_offset_old, prev_activity_ts, prev_live_ts, prev_idle_ts = row
            rotated_or_truncated = size < int(last_offset_old) or mtime < float(last_seen_mtime_old)
            read_from = max(0, size - cfg.tail_init_bytes) if rotated_or_truncated else int(last_offset_old)
            init_read = rotated_or_truncated

        new_bytes = 0
        live_ts_candidate: Optional[float] = None
        idle_ts_candidate: Optional[float] = None

        if size > read_from:
            try:
                with open(path, "rb") as f:
                    f.seek(read_from, os.SEEK_SET)
                    chunk = f.read(size - read_from)
                    new_bytes = len(chunk)

                text = chunk.decode("utf-8", errors="ignore")
                for raw_line in text.splitlines():
                    if not raw_line.strip():
                        continue
                    line_ts = parse_line_timestamp_epoch(raw_line) or cur_ts
                    body = normalize_line(raw_line)

                    if any(r.search(body) for r in live_re):
                        if (live_ts_candidate is None) or (line_ts > live_ts_candidate):
                            live_ts_candidate = line_ts
                        continue
                    if any(r.search(body) for r in idle_re):
                        if (idle_ts_candidate is None) or (line_ts > idle_ts_candidate):
                            idle_ts_candidate = line_ts
                        continue
            except Exception as e:
                details[path] = {"error": f"read_failed: {e}"}
                con.execute(
                    "INSERT INTO log_state(file_path,last_seen_mtime,last_seen_size,last_offset,"
                    " last_activity_ts,last_update_ts,last_live_ts,last_idle_ts) "
                    "VALUES(?,?,?,?,?,?,?,?) "
                    "ON CONFLICT(file_path) DO UPDATE SET "
                    "last_seen_mtime=excluded.last_seen_mtime, "
                    "last_seen_size=excluded.last_seen_size, "
                    "last_update_ts=excluded.last_update_ts;",
                    (path, mtime, size, read_from, prev_activity_ts, cur_ts, prev_live_ts, prev_idle_ts)
                )
                continue

        activity_ts = prev_activity_ts
        if new_bytes > 0:
            activity_ts = cur_ts

        new_live_ts = max(prev_live_ts, live_ts_candidate or 0.0)
        new_idle_ts = max(prev_idle_ts, idle_ts_candidate or 0.0)

        con.execute(
            "INSERT INTO log_state(file_path,last_seen_mtime,last_seen_size,last_offset,"
            " last_activity_ts,last_update_ts,last_live_ts,last_idle_ts) "
            "VALUES(?,?,?,?,?,?,?,?) "
            "ON CONFLICT(file_path) DO UPDATE SET "
            "last_seen_mtime=excluded.last_seen_mtime, "
            "last_seen_size=excluded.last_seen_size, "
            "last_offset=excluded.last_offset, "
            "last_activity_ts=excluded.last_activity_ts, "
            "last_update_ts=excluded.last_update_ts, "
            "last_live_ts=CASE WHEN excluded.last_live_ts > log_state.last_live_ts "
            "                  THEN excluded.last_live_ts ELSE log_state.last_live_ts END, "
            "last_idle_ts=CASE WHEN excluded.last_idle_ts > log_state.last_idle_ts "
            "                  THEN excluded.last_idle_ts ELSE log_state.last_idle_ts END;",
            (path, mtime, size, size, activity_ts, cur_ts, new_live_ts, new_idle_ts)
        )

        details[path] = {
            "size": size,
            "mtime_min_ago": (cur_ts - mtime) / 60.0,
            "read_from": read_from,
            "read_bytes": new_bytes,
            "init_read": init_read,
            "last_activity_min_ago": (cur_ts - activity_ts) / 60.0,
            "last_live_min_ago": (cur_ts - new_live_ts) / 60.0 if new_live_ts > 0 else None,
            "last_idle_min_ago": (cur_ts - new_idle_ts) / 60.0 if new_idle_ts > 0 else None
        }

        if new_live_ts > 0 and ((max_live_ts is None) or (new_live_ts > max_live_ts)):
            max_live_ts = new_live_ts

    return max_live_ts, details

# ------------------------------ Cleanup DB ----------------------------------
def cleanup_db(cfg: Settings, con: sqlite3.Connection, force: bool = False) -> None:
    cur_ts = now_ts()
    last_cleanup = float(db_get_kv(con, "last_cleanup_ts", str(0)) or 0)
    if not force and (cur_ts - last_cleanup) < 12 * 3600:
        return
    horizon = cur_ts - cfg.retention_days * 86400
    con.execute("DELETE FROM log_state WHERE last_update_ts < ?;", (horizon,))
    try:
        con.execute("PRAGMA wal_checkpoint(TRUNCATE);")
        con.execute("VACUUM;")
    except Exception as e:
        print(f"[WARN] VACUUM/WAL checkpoint failed: {e}")
    db_set_kv(con, "last_cleanup_ts", str(cur_ts))
    print("[CLEANUP] DB cleaned and vacuumed.")

# ------------------------------ Health-check --------------------------------
def health_check(cfg: Settings) -> list[str]:
    warnings: list[str] = []
    if not _is_admin():
        print("[FATAL] Скрипт не с правами администратора. Перезапускаюсь с UAC…")
        _relaunch_as_admin()
        sys.exit(0)
    if not cfg.telegram_token or len(cfg.telegram_token) < 30:
        warnings.append("⚠️ TELEGRAM_TOKEN пустой/подозрительный.")
    if not cfg.chat_id:
        warnings.append("⚠️ CHAT_ID не задан.")
    if not os.path.isdir(cfg.log_dir):
        warnings.append(f"⚠️ Папка логов не найдена: {cfg.log_dir}")
    else:
        masks = build_log_patterns(cfg)
        files = _expand_masks(masks)
        if not files:
            warnings.append("⚠️ По маскам botYYYYMMDD*.txt файлы не найдены (сегодня/вчера).")
        else:
            try:
                latest_f = max(files, key=lambda p: os.path.getmtime(p))
                minutes_ago = (time.time() - os.path.getmtime(latest_f)) / 60.0
                print(f"[HEALTH] Самый свежий лог: {latest_f}")
                print(f"[HEALTH] Последняя модификация была {minutes_ago:.1f} мин назад.")
            except Exception:
                pass
    if not os.path.exists(cfg.gnbots_shortcut):
        warnings.append(f"⚠️ Ярлык для запуска GnBots не найден: {cfg.gnbots_shortcut}")
    if cfg.threshold_windows < 1:
        warnings.append("⚠️ threshold_windows < 1 — проверь конфиг.")
    print("[HEALTH] Проверка завершена. Предупреждений:", len(warnings))
    for w in warnings:
        print("        ", w)
    return warnings

# ------------------------------ Helpers (format) ----------------------------
def _min_or_none(values: List[Optional[float]]) -> Optional[float]:
    vals = [v for v in values if v is not None]
    return min(vals) if vals else None

def _fmt_min(val: Optional[float]) -> str:
    return f"{val:.1f} мин" if (val is not None) else "—"

def _summarize_log_details(details: dict) -> dict:
    if not details:
        return {"files_count": 0, "latest_file": None, "live_min_ago": None, "idle_min_ago": None, "any_min_ago": None}
    live_min = _min_or_none([d.get("last_live_min_ago") for d in details.values()])
    idle_min = _min_or_none([d.get("last_idle_min_ago") for d in details.values()])
    any_min  = _min_or_none([d.get("last_activity_min_ago") for d in details.values()])
    try:
        latest_file = min(details.items(), key=lambda kv: kv[1].get("mtime_min_ago", 1e9))[0]
    except Exception:
        latest_file = None
    return {"files_count": len(details), "latest_file": latest_file,
            "live_min_ago": live_min, "idle_min_ago": idle_min, "any_min_ago": any_min}

def _build_alert_text(problems: List[str], metrics: dict, cfg: Settings) -> str:
    parts = []
    parts.append("F99🚨 GN_LD_CHECK: Триггеры перезагрузки")
    for p in problems:
        parts.append(f"• {p}")
    parts.append("")
    parts.append("📊 Диагностика:")
    parts.append(f"— GnBots.exe: {'запущен' if metrics.get('gn_running') else 'не запущен'}")
    parts.append(f"— Окна dnplayer.exe: {metrics.get('dn_count')}/{cfg.threshold_windows}")
    if metrics.get("logs_found") is False:
        parts.append(f"— Логи: не найдены по маске botYYYYMMDD*.txt (папка: {cfg.log_dir})")
    else:
        ld = metrics.get("logs_diag", {})
        parts.append(f"— Логи: найдено файлов: {ld.get('files_count', 0)}; последний: {ld.get('latest_file') or '—'}")
        parts.append(f"— Живая активность: {_fmt_min(ld.get('live_min_ago'))}")
        parts.append(f"— «Шумная» активность: {_fmt_min(ld.get('idle_min_ago'))}")
        parts.append(f"— Любая запись в логах: {_fmt_min(ld.get('any_min_ago'))}")
    parts.append("")
    parts.append(f"⏱ Порог простоя (живого): {cfg.inactivity_minutes} мин")
    return "\n".join(parts)

# ------------------------------ Main logic ----------------------------------
async def check_and_reboot(cfg: Settings):
    bot = build_bot(cfg)
    await flush_spool(bot, cfg)

    gn_running = is_process_running("GnBots.exe")
    dn_count = count_processes("dnplayer.exe")

    masks = build_log_patterns(cfg)
    files = _expand_masks(masks)
    logs_found = len(files) > 0

    con = db_connect(cfg.db_path)
    try:
        cleanup_db(cfg, con, force=False)
    except Exception as e:
        print(f"[WARN] cleanup_db: {e}")

    problems: list[str] = []

    if not gn_running:
        problems.append("❗ GnBots.exe не запущен")

    if dn_count < cfg.threshold_windows:
        problems.append(f"❗ Мало окон dnplayer.exe: {dn_count} < {cfg.threshold_windows}")

    max_live_ts = None
    details = {}
    scan_error = None
    if not logs_found:
        problems.append("❗ Логи по маске botYYYYMMDD*.txt не найдены (сегодня/вчера)")
    else:
        try:
            max_live_ts, details = scan_logs_incremental(cfg, con)
        except Exception as e:
            scan_error = str(e)
            problems.append(f"❗ Ошибка сканирования логов: {e}")

    if scan_error is None and logs_found:
        if max_live_ts is None or (now_ts() - float(max_live_ts)) / 60.0 >= cfg.inactivity_minutes:
            problems.append(
                f"❗ Нет ЖИВОЙ активности ≥ {cfg.inactivity_minutes} мин "
                f"(циклические AnySessionsBootingAsync/HandleErrorsAsync/Current Error Counters не считаем)"
            )

    if not problems:
        if details:
            ld = _summarize_log_details(details)
            print(f"[OK] GnBots запущен, dnplayer={dn_count}/{cfg.threshold_windows}, "
                  f"живая активность: {_fmt_min(ld.get('live_min_ago'))}.")
        else:
            print(f"[OK] GnBots запущен, dnplayer={dn_count}/{cfg.threshold_windows}, логи найдены: {logs_found}.")
        con.close()
        return

    metrics = {
        "gn_running": gn_running,
        "dn_count": dn_count,
        "logs_found": logs_found,
        "logs_diag": _summarize_log_details(details) if details else {
            "files_count": 0, "latest_file": None, "live_min_ago": None, "idle_min_ago": None, "any_min_ago": None
        }
    }
    alert_text = _build_alert_text(problems, metrics, cfg)

    await safe_send(bot, cfg, alert_text, cfg.thread_id)
    await safe_send(bot, cfg, "F99🔄 Ребут: убиваю процессы и запускаю ярлык…", cfg.thread_id)

    kd = kill_process("dnplayer.exe", soft_timeout=5, hard_timeout=3)
    kb = kill_process("GnBots.exe", soft_timeout=5, hard_timeout=3)
    kh = kill_process("Ld9BoxHeadless.exe", soft_timeout=5, hard_timeout=3)
    time.sleep(2)

    try:
        os.startfile(cfg.gnbots_shortcut)
        await safe_send(
            bot, cfg,
            "F99✅ Ребут завершён.\n"
            f"Убиты PID: dnplayer={kd}, GnBots={kb}, Headless={kh}.\n"
            f"Запущен ярлык: {os.path.basename(cfg.gnbots_shortcut)}",
            cfg.thread_id
        )
    except Exception as e:
        await safe_send(bot, cfg, f"❗ Не удалось запустить ярлык: {e}", cfg.thread_id)
    finally:
        con.close()

# -------------------------------- Entry -------------------------------------
if __name__ == "__main__":
    _load_root_env()
    os.chdir(os.path.dirname(__file__))
    cfg = _load_config()
    _ = health_check(cfg)
    try:
        asyncio.run(check_and_reboot(cfg))
    except KeyboardInterrupt:
        print("Прерывание пользователем.")
    except Exception as e:
        print(f"[FATAL] Необработанная ошибка: {e}")
