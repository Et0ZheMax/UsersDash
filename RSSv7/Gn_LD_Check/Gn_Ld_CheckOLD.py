#!/usr/bin/env python3
# GN_LD_CHECK (v4, SQLite tail + "live" log patterns)
# -----------------------------------------------------------------------------
# ЧТО НОВОГО:
# - Инкрементальное чтение логов через SQLite (без перечитывания целиком).
# - Детекция "живой" активности по содержимому строк (регэксп-паттерны).
#   * "Живая" активность: getMap/Refreshing Image/pulse/Marches/Found world/ID-сессии и др.
#   * "Пустая" активность: AnySessionsBootingAsync/HandleErrorsAsync/Current Error Counters.
# - Решение: если за inactivity_minutes не было ЖИВЫХ событий (даже если файл растёт
#   "пустыми" строками) — отправить алерт и перезапустить бота.
# - Папка логов: C:\Program Files (x86)\GnBots\logs, маски: botYYYYMMDD*.txt (+ вчера).
# - БД очищается раз в retention_days (по-умолчанию 2). WAL + VACUUM.
#
# ЗАПУСК: из планировщика каждые 3–5 минут, ОТ ИМЕНИ АДМИНИСТРАТОРА (UAC автоподъём).
# -----------------------------------------------------------------------------
# ENV overrides:
#   GNLDCHECK_TELEGRAM_TOKEN, GNLDCHECK_CHAT_ID, GNLDCHECK_THREAD_ID,
#   GNLDCHECK_THRESHOLD_WINDOWS, GNLDCHECK_GNBOTS_SHORTCUT,
#   GNLDCHECK_LOG_DIR, GNLDCHECK_DAYS_BACK_SCAN, GNLDCHECK_INACTIVITY_MINUTES,
#   GNLDCHECK_DB_PATH, GNLDCHECK_RETENTION_DAYS, GNLDCHECK_TAIL_INIT_BYTES
#   GNLDCHECK_LIVE_PATTERNS, GNLDCHECK_IDLE_PATTERNS  (через ;, это регэкспы)
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
from telegram.error import TelegramError, RetryAfter

# ------------------------------ UAC Elevation -------------------------------
def _is_admin() -> bool:
    """Проверка прав администратора (UAC). На non-Windows возвращает True."""
    try:
        return bool(ctypes.windll.shell32.IsUserAnAdmin()) if sys.platform == "win32" else True
    except Exception:
        return False

def _relaunch_as_admin():
    """Перезапуск этого скрипта с правами администратора (UAC)."""
    if sys.platform != "win32":
        return
    params = " ".join(f'"{a}"' for a in sys.argv[1:])
    ctypes.windll.shell32.ShellExecuteW(
        None, "runas", sys.executable, f'"{os.path.abspath(sys.argv[0])}" {params}', None, 1
    )

# ------------------------------ Console title --------------------------------
if sys.platform == "win32":
    try:
        ctypes.windll.kernel32.SetConsoleTitleW("GN_LD_CHECK v4")
    except Exception:
        pass

# ------------------------------ Конфигурация --------------------------------
BASE_DIR = os.path.dirname(__file__)
CONFIG_DIR = os.path.join(BASE_DIR, "settings")
DATA_DIR   = os.path.join(BASE_DIR, "data")
CONFIG_PATH = os.path.join(CONFIG_DIR, "gn_ld_check.json")

DEFAULTS = {
    # --- Telegram ---
    "telegram_token": "7460479135:AAEUcUZdO01AEOVxgA0xlV8ZoLOmZcKw-Uc",  # замени при необходимости
    "chat_id": "275483461",
    "thread_id": None,

    # --- Порог по окнам эмулятора ---
    "threshold_windows": 6,

    # --- Ярлык для старта GnBots ---
    "gnbots_shortcut": r"C:\Users\administrator\Desktop\GnBots.lnk",

    # --- Логи GnBots ---
    "log_dir": r"C:\Program Files (x86)\GnBots\logs",
    # Сколько дней назад дополнительно сканировать маски дат (0=только сегодня, 1=ещё вчера)
    "days_back_scan": 1,

    # Минуты тишины (по ЖИВЫМ строкам) для алерта/ребута
    "inactivity_minutes": 20,

    # --- БД / очистка ---
    "db_path": "data/gn_ld_check.sqlite3",
    "retention_days": 2,
    "tail_init_bytes": 65536,

    # --- Паттерны строк (регэкспы) ---
    # "Живые" события: любые строки, показывающие реальную работу сессий/кликов/карт/поиска и т.п.
    "live_patterns": [
        r"\|[0-9a-f]{16}\|",                         # наличие sessionId между |...|
        r"\b(getMap|Refreshing Image|pulse)\b",
        r"\b(Marches:|Reached Maximum of Marches)\b",
        r"\b(Found world)\b"
    ],
    # "Пустые" события (циклический шум, не считать активностью):
    "idle_patterns": [
        r"\bAnySessionsBootingAsync\b",
        r"\bHandleErrorsAsync: Scanning for error windows\b",
        r"\bCurrent Error Counters:\b"
    ]
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

def _ensure_config() -> None:
    """Создаёт конфиг и служебные папки при отсутствии."""
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
    # Разделитель — ';' (можно экранировать \; при необходимости)
    parts = [p for p in (x.strip() for x in v.split(";")) if p]
    return parts or None

def _load_config() -> Settings:
    """Загружает конфиг и применяет ENV-переопределения."""
    _ensure_config()
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    def _int_env(name: str, fallback_key: str, default_val: int) -> int:
        v = os.getenv(name)
        if v and v.strip().isdigit():
            return int(v.strip())
        return int(data.get(fallback_key, default_val))

    token = os.getenv("GNLDCHECK_TELEGRAM_TOKEN", data.get("telegram_token", DEFAULTS["telegram_token"]))
    chat_id = os.getenv("GNLDCHECK_CHAT_ID", str(data.get("chat_id", DEFAULTS["chat_id"])))
    thread_id_env = os.getenv("GNLDCHECK_THREAD_ID", "")
    thread_id = int(thread_id_env) if thread_id_env.strip().isdigit() else data.get("thread_id")

    threshold_windows = _int_env("GNLDCHECK_THRESHOLD_WINDOWS", "threshold_windows", DEFAULTS["threshold_windows"])
    shortcut = os.getenv("GNLDCHECK_GNBOTS_SHORTCUT", data.get("gnbots_shortcut", DEFAULTS["gnbots_shortcut"]))

    log_dir = os.getenv("GNLDCHECK_LOG_DIR", data.get("log_dir", DEFAULTS["log_dir"]))
    days_back_scan = _int_env("GNLDCHECK_DAYS_BACK_SCAN", "days_back_scan", DEFAULTS["days_back_scan"])
    inactivity_minutes = _int_env("GNLDCHECK_INACTIVITY_MINUTES", "inactivity_minutes", DEFAULTS["inactivity_minutes"])

    db_path = os.getenv("GNLDCHECK_DB_PATH", data.get("db_path", DEFAULTS["db_path"]))
    if not os.path.isabs(db_path):
        db_path = os.path.join(BASE_DIR, db_path)
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    retention_days = _int_env("GNLDCHECK_RETENTION_DAYS", "retention_days", DEFAULTS["retention_days"])
    tail_init_bytes = _int_env("GNLDCHECK_TAIL_INIT_BYTES", "tail_init_bytes", DEFAULTS["tail_init_bytes"])

    # Паттерны из ENV, иначе из конфига, иначе из DEFAULTS
    env_live = _split_env_regex_list("GNLDCHECK_LIVE_PATTERNS")
    env_idle = _split_env_regex_list("GNLDCHECK_IDLE_PATTERNS")
    live_patterns = env_live if env_live is not None else data.get("live_patterns", DEFAULTS["live_patterns"])
    idle_patterns = env_idle if env_idle is not None else data.get("idle_patterns", DEFAULTS["idle_patterns"])

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
        idle_patterns=idle_patterns
    )

# ------------------------------ Telegram utils ------------------------------
async def safe_send(bot: Bot, chat_id: str, text: str, thread_id: int | None = None) -> None:
    """Безопасная отправка телеграм-сообщений с учётом flood-limit."""
    while True:
        try:
            await bot.send_message(chat_id=chat_id, text=text, message_thread_id=thread_id)
            return
        except RetryAfter as e:
            wait = int(getattr(e, "retry_after", 5)) + 1
            print(f"[INFO] Telegram flood-limit, ждём {wait}s…")
            await asyncio.sleep(wait)
        except TelegramError as e:
            print(f"[WARN] TelegramError при отправке: {e}")
            return
        except Exception as e:
            print(f"[WARN] Неожиданная ошибка Telegram: {e}")
            return

# ------------------------------ Proc utils ----------------------------------
def is_process_running(name: str) -> bool:
    """Точный матч имени процесса."""
    for proc in psutil.process_iter(['name']):
        try:
            if proc.info['name'] and proc.info['name'].lower() == name.lower():
                return True
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return False

def count_processes(name: str) -> int:
    """Подсчёт процессов по точному имени."""
    cnt = 0
    for proc in psutil.process_iter(['name']):
        try:
            if proc.info['name'] and proc.info['name'].lower() == name.lower():
                cnt += 1
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return cnt

def kill_process(name: str, soft_timeout: int = 5, hard_timeout: int = 5) -> list[int]:
    """
    Мягко terminate(), затем kill(), затем taskkill /F /T — для всех процессов с именем name.
    Возвращает список PID.
    """
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
                            stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL
                        )
                killed.append(pid)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return killed

# ------------------------------ Log path utils ------------------------------
def _date_mask_strings(days_back: int) -> List[str]:
    """Список датовых меток 'YYYYMMDD' для сегодня и n предыдущих дней."""
    res = []
    today = datetime.now()
    for i in range(days_back + 1):
        d = (today - timedelta(days=i)).strftime("%Y%m%d")
        res.append(d)
    return res

def build_log_patterns(cfg: Settings) -> List[str]:
    """
    Маски логов вида: <log_dir>\botYYYYMMDD*.txt
    Покрывает botYYYYMMDD.txt и botYYYYMMDD_001.txt и т.д. (сегодня + days_back_scan).
    """
    masks: List[str] = []
    for ds in _date_mask_strings(cfg.days_back_scan):
        masks.append(os.path.join(cfg.log_dir, f"bot{ds}*.txt"))
    return masks

def _expand_masks(masks: List[str]) -> List[str]:
    """Разворачивает список масок в существующие файлы без дубликатов."""
    seen = set()
    out: List[str] = []
    for m in masks:
        m_exp = os.path.expandvars(os.path.expanduser(m))
        for f in glob.glob(m_exp):
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
    last_activity_ts  REAL NOT NULL,   -- "любой" рост файла (для отладки)
    last_update_ts    REAL NOT NULL,   -- когда последний раз видели файл
    last_live_ts      REAL NOT NULL DEFAULT 0,  -- когда последний раз была ЖИВАЯ строка
    last_idle_ts      REAL NOT NULL DEFAULT 0   -- когда последний раз была ПУСТАЯ строка
);
CREATE TABLE IF NOT EXISTS meta_kv (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""

def db_connect(path: str) -> sqlite3.Connection:
    """Открывает/инициализирует SQLite, включает WAL, выполняет миграции."""
    con = sqlite3.connect(path, timeout=15, isolation_level=None)
    con.execute("PRAGMA foreign_keys=ON;")
    # Базовая схема
    for stmt in SQL_SCHEMA.strip().split(";"):
        s = stmt.strip()
        if s:
            con.execute(s)
    # Миграция: убедимся, что колонки last_live_ts/last_idle_ts есть
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
# Удаляем таймштамп и уровень логирования в начале строки:
# Пример префикса: 2025-10-09 04:06:16.338 +03:00 [DBG] 
TIMESTAMP_PREFIX_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\.\d+\s+\+\d{2}:\d{2}\s+\[[A-Z]+\]\s+"
)

# Парсинг метки времени в начале строки -> epoch (UTC)
TS_PARSE_RE = re.compile(
    r"^(?P<y>\d{4})-(?P<m>\d{2})-(?P<d>\d{2})\s+"
    r"(?P<h>\d{2}):(?P<mi>\d{2}):(?P<s>\d{2})\.(?P<ms>\d+)\s+\+"
    r"(?P<tz_h>\d{2}):(?P<tz_m>\d{2})"
)

def parse_line_timestamp_epoch(line: str) -> Optional[float]:
    """Пробуем вытащить timestamp из начала строки и конвертировать в epoch."""
    m = TS_PARSE_RE.match(line)
    if not m:
        return None
    try:
        y = int(m.group("y")); mo = int(m.group("m")); d = int(m.group("d"))
        h = int(m.group("h")); mi = int(m.group("mi")); s = int(m.group("s"))
        ms = int(m.group("ms"))
        tz_h = int(m.group("tz_h")); tz_m = int(m.group("tz_m"))
        # Лог содержит +HH:MM -> локальное время опережает UTC
        tz = timezone(timedelta(hours=tz_h, minutes=tz_m))
        dt = datetime(y, mo, d, h, mi, s, ms*1000, tzinfo=tz)
        return dt.timestamp()  # epoch в UTC
    except Exception:
        return None

def normalize_line(line: str) -> str:
    """Убираем префикс с датой/временем/уровнем, остаётся смысловая часть."""
    return TIMESTAMP_PREFIX_RE.sub("", line).strip()

def compile_regex_list(patterns: List[str]) -> List[re.Pattern]:
    """Компилируем список регэкспов (безопасно)."""
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
    Инкрементальный проход по логам:
      - читаем только новый хвост (или tail_init_bytes при первом визите/ротации);
      - анализируем СОДЕРЖИМОЕ новых строк;
      - обновляем last_live_ts (по ЖИВЫМ паттернам) и last_idle_ts (по ПУСТЫМ).
    Возвращает: (max_last_live_ts_по_всем_файлам_или_None, подробности_по_файлам).
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
            "FROM log_state WHERE file_path = ?;",
            (path,)
        ).fetchone()

        # Определяем, откуда читать
        if row is None:
            read_from = max(0, size - cfg.tail_init_bytes)  # хвост для первого визита
            init_read = True
            prev_activity_ts = 0.0
            prev_live_ts = 0.0
            prev_idle_ts = 0.0
            last_offset_old = 0
            last_seen_size_old = 0
            last_seen_mtime_old = 0.0
        else:
            last_seen_mtime_old, last_seen_size_old, last_offset_old, prev_activity_ts, prev_live_ts, prev_idle_ts = row
            rotated_or_truncated = size < int(last_offset_old) or mtime < float(last_seen_mtime_old)
            read_from = max(0, size - cfg.tail_init_bytes) if rotated_or_truncated else int(last_offset_old)
            init_read = rotated_or_truncated

        # Читаем новые байты, если есть
        new_bytes = 0
        live_ts_candidate: Optional[float] = None
        idle_ts_candidate: Optional[float] = None

        if size > read_from:
            try:
                with open(path, "rb") as f:
                    f.seek(read_from, os.SEEK_SET)
                    chunk = f.read(size - read_from)
                    new_bytes = len(chunk)

                # --- Анализ содержимого новых строк ---
                text = chunk.decode("utf-8", errors="ignore")
                # На некоторых ротаторах могут попадать куски без окончания строки — норм
                for raw_line in text.splitlines():
                    if not raw_line.strip():
                        continue

                    # timestamp строки (если есть) — используем реальное время из лога, иначе текущий ts
                    line_ts = parse_line_timestamp_epoch(raw_line) or cur_ts
                    body = normalize_line(raw_line)

                    # Живые?
                    if any(r.search(body) for r in live_re):
                        if (live_ts_candidate is None) or (line_ts > live_ts_candidate):
                            live_ts_candidate = line_ts
                        continue

                    # Пустые?
                    if any(r.search(body) for r in idle_re):
                        if (idle_ts_candidate is None) or (line_ts > idle_ts_candidate):
                            idle_ts_candidate = line_ts
                        continue

                    # Иначе: ни живое, ни пустое. Здесь по-умолчанию НЕ увеличиваем live_ts,
                    # чтобы не ловить шум. При желании можно включить эвристику:
                    # - если строка содержит sessionId-подобный паттерн — это уже покрыто live_patterns.
                    # - можно считать рост разнообразия токенов "живым", но держим минимализм => off.

            except Exception as e:
                details[path] = {"error": f"read_failed: {e}"}
                # Даже при ошибке чтения — обновим наблюдение файла
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

        # last_activity_ts — факт роста файла (для отладки)
        activity_ts = prev_activity_ts
        if new_bytes > 0:
            activity_ts = cur_ts

        # last_live_ts / last_idle_ts — кандидаты из содержимого
        new_live_ts = max(prev_live_ts, live_ts_candidate or 0.0)
        new_idle_ts = max(prev_idle_ts, idle_ts_candidate or 0.0)

        # Обновляем состояние
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

        # Детали по файлу (для консольного лога/диагностики)
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
    """
    Удаляет устаревшие записи из БД и периодически делает VACUUM/WAL checkpoint.
    Порог устаревания: cfg.retention_days (по last_update_ts).
    Частота полной уборки: не чаще 1 раза в 12 часов (или force=True).
    """
    cur_ts = now_ts()
    last_cleanup = float(db_get_kv(con, "last_cleanup_ts", str(0)) or 0)
    if not force and (cur_ts - last_cleanup) < 12*3600:
        return  # чистка уже была недавно

    horizon = cur_ts - cfg.retention_days * 86400
    con.execute("DELETE FROM log_state WHERE last_update_ts < ?;", (horizon,))

    # Освобождаем место
    try:
        con.execute("PRAGMA wal_checkpoint(TRUNCATE);")
        con.execute("VACUUM;")
    except Exception as e:
        print(f"[WARN] VACUUM/WAL checkpoint failed: {e}")

    db_set_kv(con, "last_cleanup_ts", str(cur_ts))
    print("[CLEANUP] DB cleaned and vacuumed.")

# ------------------------------ Health-check --------------------------------
def health_check(cfg: Settings) -> list[str]:
    """
    Проверяет критичные условия перед выполнением. Возвращает список предупреждений.
    Fatal условия (нет админ-прав) — сразу перезапуск с UAC и выход.
    """
    warnings: list[str] = []

    # 0) Админ-права
    if not _is_admin():
        print("[FATAL] Скрипт не с правами администратора. Перезапускаюсь с UAC…")
        _relaunch_as_admin()
        sys.exit(0)

    # 1) Телеграм настройки
    if not cfg.telegram_token or len(cfg.telegram_token) < 30:
        warnings.append("F99⚠️ TELEGRAM_TOKEN пустой/подозрительный.")
    if not cfg.chat_id:
        warnings.append("F99⚠️ CHAT_ID не задан.")

    # 2) Папка логов
    if not os.path.isdir(cfg.log_dir):
        warnings.append(f"F99⚠️ Папка логов не найдена: {cfg.log_dir}")
    else:
        masks = build_log_patterns(cfg)
        files = _expand_masks(masks)
        if not files:
            warnings.append("F99⚠️ По маскам botYYYYMMDD*.txt файлы не найдены (сегодня/вчера).")
        else:
            latest_m = -1.0
            latest_f = None
            for f in files:
                try:
                    m = os.path.getmtime(f)
                    if m > latest_m:
                        latest_m = m
                        latest_f = f
                except Exception:
                    continue
            if latest_f:
                minutes_ago = (time.time() - latest_m) / 60.0
                print(f"[HEALTH] Самый свежий лог: {latest_f}")
                print(f"[HEALTH] Последняя модификация была {minutes_ago:.1f} мин назад.")

    # 3) Ярлык
    if not os.path.exists(cfg.gnbots_shortcut):
        warnings.append(f"F99⚠️ Ярлык для запуска GnBots не найден: {cfg.gnbots_shortcut}")

    # 4) Порог окон
    if cfg.threshold_windows < 1:
        warnings.append("F99⚠️ threshold_windows < 1 — проверь конфиг.")

    print("[HEALTH] Проверка завершена. Предупреждений:", len(warnings))
    for w in warnings:
        print("        ", w)
    return warnings

# ------------------------------ Основная логика ------------------------------
async def check_and_reboot(cfg: Settings):
    """
    Запускает проверки и при проблемах шлёт алерт + делает ребут GnBots.
    Ключевой критерий: если нет ЖИВОЙ активности в логах ≥ cfg.inactivity_minutes.
    """
    bot = Bot(token=cfg.telegram_token)

    # Подключаем БД и делаем периодическую уборку
    con = db_connect(cfg.db_path)
    try:
        cleanup_db(cfg, con, force=False)
    except Exception as e:
        print(f"[WARN] cleanup_db: {e}")

    problems: list[str] = []

    # A) Процесс GnBots.exe
    if not is_process_running("GnBots.exe"):
        problems.append("F99❗ GnBots.exe не запущен")

    # B) Кол-во окон dnplayer.exe
    dn_count = count_processes("dnplayer.exe")
    if dn_count < cfg.threshold_windows:
        problems.append(f"F99❗ Окон dnplayer.exe: {dn_count} (требуется ≥ {cfg.threshold_windows})")

    # C) Активность логов (ЖИВАЯ)
    try:
        max_live_ts, details = scan_logs_incremental(cfg, con)
        # Для диагностики можно кратко вывести сводку:
        if details:
            # Покажем по самому «позднему» файлу live-метки (если есть)
            latest_live_min = None
            for p, d in details.items():
                if d.get("last_live_min_ago") is not None:
                    if latest_live_min is None or d["last_live_min_ago"] < latest_live_min:
                        latest_live_min = d["last_live_min_ago"]
            if latest_live_min is not None:
                print(f"[LIVE] Последняя ЖИВАЯ активность была {latest_live_min:.1f} мин назад.")
    except Exception as e:
        max_live_ts, details = None, {}
        problems.append(f"F99❗ Ошибка сканирования логов: {e}")

    if max_live_ts is None or (now_ts() - float(max_live_ts)) / 60.0 >= cfg.inactivity_minutes:
        problems.append(
            f"F99❗ Нет ЖИВОЙ активности в логах ≥ {cfg.inactivity_minutes} мин "
            f"(игнорируя циклические строки типа AnySessionsBootingAsync/HandleErrorsAsync/Current Error Counters)"
        )

    # Если проблем нет — выходим
    if not problems:
        print(f"[OK] Всё ок: GnBots запущен, dnplayer={dn_count}, есть живая активность.")
        con.close()
        return

    # Иначе — алерт и ребут
    header = "F99🚨 GN_LD_CHECK: Обнаружены проблемы:"
    alert_text = header + "\n" + "\n".join(problems)
    await safe_send(bot, cfg.chat_id, alert_text, cfg.thread_id)

    await safe_send(bot, cfg.chat_id, "🔄 Ребут: убиваю процессы и запускаю ярлык…", cfg.thread_id)

    kd = kill_process("dnplayer.exe", soft_timeout=5, hard_timeout=3)
    kb = kill_process("GnBots.exe", soft_timeout=5, hard_timeout=3)
    kh = kill_process("Ld9BoxHeadless.exe", soft_timeout=5, hard_timeout=3)

    time.sleep(2)

    try:
        os.startfile(cfg.gnbots_shortcut)
        await safe_send(
            bot, cfg.chat_id,
            (
                "F99✅ Ребут завершён.\n"
                f"Убиты PID: dnplayer={kd}, GnBots={kb}, Headless={kh}.\n"
                f"Запущен ярлык: {os.path.basename(cfg.gnbots_shortcut)}"
            ),
            cfg.thread_id
        )
    except Exception as e:
        await safe_send(bot, cfg.chat_id, f"F99❗ Не удалось запустить ярлык: {e}", cfg.thread_id)
    finally:
        con.close()

# -------------------------------- Запуск ------------------------------------
if __name__ == "__main__":
    # Работаем из папки скрипта (удобно для относительных путей)
    os.chdir(os.path.dirname(__file__))

    # Загружаем конфиг
    cfg = _load_config()

    # Health-check (лог в консоль). Если нет админ-прав — перезапустит себя и выйдет.
    _ = health_check(cfg)

    # Выполнить проверку
    try:
        asyncio.run(check_and_reboot(cfg))
    except KeyboardInterrupt:
        print("Прерывание пользователем.")
    except Exception as e:
        print(f"[FATAL] Необработанная ошибка: {e}")
