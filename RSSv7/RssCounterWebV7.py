import os
import json
import re
import stat
import sqlite3
import psutil
import subprocess
import shlex
import time
import shutil
import tempfile
import ctypes
import socket
import typing as t
import paramiko
import requests
from cryptography.fernet import Fernet
from io import BytesIO
from PIL import ImageGrab
import base64
import pythoncom
import wmi
import sys
import csv
import threading
import inactive_monitor
from copy import deepcopy
from icmplib import ping as icmp_ping
from pathlib import Path
from flask import jsonify, request
from datetime import datetime, timezone, date, timedelta
from flask import Flask, jsonify, render_template, request
from flask_cors import CORS

# Установка всего: python -m pip install -U psutil paramiko requests Pillow pywin32 WMI icmplib Flask Flask-Cors

# Пример: задаём своему скрипту заголовок «MyUniqueScript»
DEFAULT_TITLE = "RssV7"
if sys.platform == "win32":
    ctypes.windll.kernel32.SetConsoleTitleW(DEFAULT_TITLE)

# -------------------------------------------------
# Функции для запуска с правами администратора
# -------------------------------------------------
def is_admin() -> bool:
    try:
        return ctypes.windll.shell32.IsUserAnAdmin()
    except Exception:
        return False

if not is_admin():
    # Перезапуск под админом
    script = os.path.abspath(sys.argv[0])
    params = " ".join([f'"{script}"'] + [f'"{arg}"' for arg in sys.argv[1:]])
    ctypes.windll.shell32.ShellExecuteW(None, "runas", sys.executable, params, None, 1)
    sys.exit(0)


DEBUG = True  # или False, если не нужен режим отладки

# === Атомарная запись JSON (tmp + os.replace) ================================
def safe_write_json(path: str, data):
    """Пишет JSON атомарно: сначала во временный файл, затем os.replace."""
    d = os.path.dirname(path) or "."
    os.makedirs(d, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".tmp_", dir=d)
    os.close(fd)
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    except Exception:
        try:
            os.remove(tmp)
        except Exception:
            pass
        raise

# -------------------------------------------------
# Функции health_check
# -------------------------------------------------

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# === Пути под профили/шаблоны/схему ==========================================
SETTINGS_DIR = os.path.join(BASE_DIR, "settings")
PROFILES_DIR = os.path.join(SETTINGS_DIR, "profiles")     # профили аккаунтов *.json
TEMPLATES_DIR = os.path.join(SETTINGS_DIR, "templates")   # шаблоны TRAIN.json, 650.json и т.д.
TEMPLATES_BACKUP_DIR = os.path.join(TEMPLATES_DIR, "_backup")
TEMPLATE_ALIASES_PATH = os.path.join(SETTINGS_DIR, "template_aliases.json")
SCHEMA_CACHE_PATH = os.path.join(SETTINGS_DIR, "schema_cache.json")  # авто-накапливаемая «схема»
TEMPLATE_GAPS_CACHE_PATH = os.path.join(SETTINGS_DIR, "template_schema_gaps.json")
SERVER_LINKS_PATH = os.path.join(SETTINGS_DIR, "server_links.enc")
SERVER_LINKS_KEY_PATH = os.path.join(SETTINGS_DIR, "server_links.key")

os.makedirs(SETTINGS_DIR, exist_ok=True)
os.makedirs(PROFILES_DIR, exist_ok=True)
os.makedirs(TEMPLATES_DIR, exist_ok=True)
if not os.path.isfile(SCHEMA_CACHE_PATH):
    safe_write_json(SCHEMA_CACHE_PATH, {})
if not os.path.isfile(TEMPLATE_GAPS_CACHE_PATH):
    safe_write_json(TEMPLATE_GAPS_CACHE_PATH, {"checked_at": None, "gaps": []})

CONFIG_PATH = os.path.join(BASE_DIR, "config.json")

def _write_default_config(path):
    default = {
        "LOGS_DIR": r"C:\\Program Files\\GnBots\\logs",
        "PROFILE_PATH": r"C:\\Program Files\\GnBots\\config\\profiles.json",
        "SRC_VMS": r"D:\\Backups\\VMs",
        "DST_VMS": r"D:\\Prod\\VMs",
        "GNBOTS_SHORTCUT": r"C:\\Program Files\\GnBots\\GnBots.lnk",
        "SERVER_NAME": socket.gethostname(),
        "TELEGRAM_TOKEN": "",
        "TELEGRAM_CHAT_ID": "",
        "USERSDASH_API_URL": "",
        "USERSDASH_API_TOKEN": "",
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(default, f, ensure_ascii=False, indent=2)

# 1) Загружаем/создаём конфиг
if not os.path.isfile(CONFIG_PATH):
    print("[CONFIG] config.json not found, creating default…")
    _write_default_config(CONFIG_PATH)

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    CONFIG = json.load(f)

CONFIG.setdefault("USERSDASH_API_URL", "")
CONFIG.setdefault("USERSDASH_API_TOKEN", "")

# 2) Единый источник путей из конфига (убираем дубли: LOG_DIR != LOGS_DIR)
LOGS_DIR        = CONFIG.get("LOGS_DIR", r"C:\Program Files\GnBots\logs")
PROFILE_PATH    = CONFIG.get("PROFILE_PATH", "")
SRC_VMS         = CONFIG.get("SRC_VMS", "")
DST_VMS         = CONFIG.get("DST_VMS", "")
GNBOTS_SHORTCUT = CONFIG.get("GNBOTS_SHORTCUT", "")
GNBOTS_PROFILES_PATH = PROFILE_PATH
SCHEMA_TTL_SECONDS   = 600  # 10 минут
SERVER_NAME     = os.getenv("SERVER_NAME", CONFIG.get("SERVER_NAME", socket.gethostname()))
USERSDASH_API_URL = (os.getenv("USERSDASH_API_URL") or CONFIG.get("USERSDASH_API_URL", "")).strip()
USERSDASH_API_TOKEN = (os.getenv("USERSDASH_API_TOKEN") or CONFIG.get("USERSDASH_API_TOKEN", "")).strip()
LD_PROBLEMS_SUMMARY_PATH = os.getenv(
    "LD_PROBLEMS_SUMMARY_PATH",
    CONFIG.get("LD_PROBLEMS_SUMMARY_PATH", r"C:\\LDPlayer\\ldChecker\\problems_summary.json"),
)
GATHER_TILES_STATE_PATH = os.path.join(BASE_DIR, "gather_tiles_state.json")
GATHER_TILES_LOG_LIMIT = 1500
GATHER_TILES_STREAK = 3
GATHER_TILES_PATTERNS = (
    "gather: cannot find tile",
    "gather: cannot find level menu",
)

# 3) БД
RESOURCES_DB   = os.path.join(BASE_DIR, "resources_web.db")
LOGS_DB        = os.path.join(BASE_DIR, "logs_cache.db")
USERDASH_DB    = os.path.abspath(os.path.join(BASE_DIR, "..", "UsersDash", "data", "app.db"))

# 4) Телега — из ENV имеет приоритет, затем config.json
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN", CONFIG.get("TELEGRAM_TOKEN", ""))
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", CONFIG.get("TELEGRAM_CHAT_ID", ""))

# 4.1) Имя сервера (для заголовков/уведомлений)
APP_TITLE = f"RssV7_{SERVER_NAME}" if SERVER_NAME else DEFAULT_TITLE
if sys.platform == "win32":
    ctypes.windll.kernel32.SetConsoleTitleW(APP_TITLE)


# 5) Health-check (создаст папки БД, проверит конфиг)
def health_check():
    errs = []
    if not os.path.isfile(CONFIG_PATH):
        errs.append(f"CONFIG not found: {CONFIG_PATH}")
    for db in (RESOURCES_DB, LOGS_DB):
        d = os.path.dirname(db)
        try:
            os.makedirs(d, exist_ok=True)
        except Exception as e:
            errs.append(f"Cannot create dir {d}: {e}")
    if not LOGS_DIR or not os.path.isdir(os.path.dirname(LOGS_DIR)):
        # Мягкая проверка, путь к логам может быть на другом диске
        print(f"[HEALTH-CHECK] LOGS_DIR='{LOGS_DIR}' (exists: {os.path.isdir(LOGS_DIR)})")
    if errs:
        for e in errs: print("[HEALTH-CHECK ERROR]", e)
        sys.exit(1)
    print("[HEALTH-CHECK] OK.")

health_check()


# ──────────────────────────────────────────────────────────────────────
# --- CONFIG (schema support) ---
# ──────────────────────────────────────────────────────────────────────

# --- Вспомогательные: безопасное чтение файла ---
def _safe_json_load(path: str):
    import json, io, os
    if not path or not os.path.exists(path):
        return None
    # Пробуем несколько кодировок — UTF-8, UTF-8-SIG, UTF-16LE/BE, CP1251
    encodings = ["utf-8", "utf-8-sig", "utf-16", "utf-16le", "utf-16be", "cp1251"]
    for enc in encodings:
        try:
            with io.open(path, "r", encoding=enc, errors="strict") as f:
                return json.load(f)
        except Exception:
            continue
    # Последняя попытка: читаем байты и пытаемся json.loads как UTF-8-SIG
    try:
        with open(path, "rb") as f:
            raw = f.read()
        return json.loads(raw.decode("utf-8-sig", errors="ignore"))
    except Exception:
        return None

# === Авто-схема по живым аккаунтам + безопасный мёрдж шаблонов ===============

def _json_read_or(path, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def schema_load():
    return _json_read_or(SCHEMA_CACHE_PATH, {})

def schema_save(data: dict):
    safe_write_json(SCHEMA_CACHE_PATH, data)

def schema_learn_from_steps(steps: list):
    """Накапливаем superset полей по ScriptId, с типами/дефолтами."""
    schema = schema_load()
    changed = False

    for step in steps or []:
        sid = step.get("ScriptId")
        cfg = step.get("Config") or {}
        if not sid or not isinstance(cfg, dict):
            continue

        entry = schema.setdefault(sid, {"fields": {}})
        fields = entry["fields"]

        for k, v in cfg.items():
            f = fields.setdefault(k, {})
            # типы и дефолты
            if isinstance(v, dict) and "options" in v:
                # селект-поле: {"value": "...", "options": [...]}
                if f.get("type") != "select":
                    f["type"] = "select"; changed = True
                opts = list(v.get("options") or [])
                if f.get("options") != opts:
                    f["options"] = opts; changed = True
                dval = v.get("value")
                # дефолт: Off если есть, иначе первый из options, иначе "".
                def_val = "Off" if "Off" in opts else (opts[0] if opts else "")
                new_def = {"value": dval if dval in opts else def_val, "options": opts}
                if f.get("default") != new_def:
                    f["default"] = new_def; changed = True
            elif isinstance(v, bool):
                if f.get("type") != "bool":
                    f["type"] = "bool"; changed = True
                if "default" not in f:
                    f["default"] = False; changed = True
            elif isinstance(v, (int, float)):
                if f.get("type") != "number":
                    f["type"] = "number"; changed = True
                if "default" not in f:
                    f["default"] = 0; changed = True
            else:
                if f.get("type") != "string":
                    f["type"] = "string"; changed = True
                if "default" not in f:
                    f["default"] = ""; changed = True

    if changed:
        schema_save(schema)

def _default_value_by_spec(spec: dict):
    t = spec.get("type")
    if t == "select":
        return {"value": spec.get("default", {}).get("value", ""), "options": spec.get("options", [])}
    if t == "bool":
        return False
    if t == "number":
        return 0
    return ""

def template_inflate_with_schema(template_steps: list, schema: dict):
    """Дополняем шаблон недостающими полями по схеме (для каждого ScriptId)."""
    steps = deepcopy(template_steps or [])
    for step in steps:
        sid = step.get("ScriptId")
        cfg = step.setdefault("Config", {})
        if not sid or sid not in schema:
            continue
        # добиваем поля из схемы
        for key, spec in schema[sid]["fields"].items():
            if key not in cfg:
                cfg[key] = _default_value_by_spec(spec)
            else:
                if isinstance(cfg[key], dict) and spec.get("type") == "select":
                    if "options" not in cfg[key]:
                        cfg[key]["options"] = list(spec.get("options") or [])
    return steps


def find_template_schema_gaps(template_steps: list, schema: dict):
    """
    Возвращает [{"script_id":..., "keys":[...]}] для шагов, где по схеме не хватает ключей.
    """
    if not schema:
        return []

    gaps = []
    for step in template_steps or []:
        sid = step.get("ScriptId")
        cfg = step.get("Config") or {}
        if not sid or sid not in schema:
            continue

        missing = [k for k in (schema[sid].get("fields") or {}) if k not in cfg]
        if missing:
            gaps.append({"script_id": sid, "keys": missing})

    return gaps


def load_template_gap_cache() -> dict:
    data = _json_read_or(TEMPLATE_GAPS_CACHE_PATH, {}) or {}
    if not isinstance(data, dict):
        return {"checked_at": None, "gaps": []}
    return {"checked_at": data.get("checked_at"), "gaps": data.get("gaps", [])}


def save_template_gap_cache(payload: dict) -> None:
    safe_write_json(TEMPLATE_GAPS_CACHE_PATH, payload)


def collect_templates_schema_gaps(schema: dict | None = None) -> list[dict]:
    """Проверяет все шаблоны на наличие обязательных ключей по схеме."""

    schema = schema or schema_load()
    if not schema:
        return []

    aliases = _load_template_aliases()
    alias_targets: dict[str, list[str]] = {}
    for alias, target in aliases.items():
        alias_targets.setdefault(target, []).append(alias)

    results: list[dict] = []
    for name in sorted(os.listdir(TEMPLATES_DIR)):
        if not name.lower().endswith(".json"):
            continue

        full = os.path.join(TEMPLATES_DIR, name)
        steps = _json_read_or(full, [])
        if not isinstance(steps, list):
            continue

        gaps = find_template_schema_gaps(steps, schema)
        if not gaps:
            continue

        results.append(
            {
                "template": name,
                "label": os.path.splitext(name)[0],
                "aliases": alias_targets.get(name, []),
                "gaps": gaps,
            }
        )

    return results


def run_templates_schema_audit(schema: dict | None = None) -> dict:
    now_iso = datetime.now(timezone.utc).isoformat()
    gaps = collect_templates_schema_gaps(schema)
    payload = {"checked_at": now_iso, "gaps": gaps, "ok": not gaps}
    save_template_gap_cache(payload)
    return payload


def _backup_template_file(src_path: str) -> str | None:
    """Создаёт резервную копию шаблона в каталоге _backup."""

    try:
        os.makedirs(TEMPLATES_BACKUP_DIR, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = f"{os.path.basename(src_path)}.{ts}.bak"
        dst_path = os.path.join(TEMPLATES_BACKUP_DIR, backup_name)
        shutil.copy2(src_path, dst_path)
        return dst_path
    except Exception:
        app.logger.exception("Не удалось создать бэкап шаблона %s", src_path)
        return None


def _diff_template_with_schema(before: list, after: list) -> list[dict]:
    """Строит diff между исходным и дополненным шаблоном."""

    diffs: list[dict] = []
    if not isinstance(before, list) or not isinstance(after, list):
        return diffs

    for idx, (before_step, after_step) in enumerate(zip(before, after)):
        if not isinstance(after_step, dict):
            continue

        before_cfg = before_step.get("Config") if isinstance(before_step, dict) else {}
        after_cfg = after_step.get("Config") or {}
        if not isinstance(before_cfg, dict):
            before_cfg = {}

        changes = []
        for key, new_val in after_cfg.items():
            existed_before = isinstance(before_cfg, dict) and key in before_cfg
            old_val = before_cfg.get(key) if existed_before else None
            if old_val != new_val:
                changes.append({"key": key, "before": old_val, "after": new_val})

        if changes:
            diffs.append({
                "step_index": idx,
                "script_id": after_step.get("ScriptId"),
                "changes": changes,
            })

    return diffs

def merge_template_into_account(account_steps: list, template_steps: list):
    """Полностью заменяет конфиг аккаунта переданным шаблоном."""

    # Ранее мы пытались подмердживать настройки поверх существующих, из-за чего
    # старые ключи оставались в профиле. Это приводило к тому, что после
    # применения шаблона в manage конфиг становился смесью старых и новых
    # значений. Теперь возвращаем чистую копию шаблона, чтобы настройки
    # действительно перезаписывались.
    return deepcopy(template_steps or [])


def _build_schema():
    """Собираем «живую схему» из профилей."""
    src = _safe_json_load(GNBOTS_PROFILES_PATH) or {}
    scripts = {}

    candidates = []
    if isinstance(src, dict):
        for key in ("Accounts", "Profiles", "accounts", "profiles"):
            val = src.get(key)
            if isinstance(val, list):
                candidates.extend(val)

    if not candidates:
        candidates = [src]

    all_steps = []
    for acc in candidates:
        steps = _extract_steps_from_obj(acc)
        if steps:
            all_steps.extend(steps)

    for st in all_steps:
        sid = st.get("ScriptId")
        cfg = st.get("Config") or {}
        if not sid or not isinstance(cfg, dict):
            continue
        bucket = scripts.setdefault(sid, set())
        for k in cfg.keys():
            bucket.add(k)

    json_scripts = {sid: sorted(list(fields)) for sid, fields in scripts.items()}

    try:
        os.makedirs(os.path.dirname(SCHEMA_CACHE_PATH), exist_ok=True)
        with open(SCHEMA_CACHE_PATH, "w", encoding="utf-8") as f:
            json.dump({
                "schema": {"Scripts": json_scripts},
                "built_at": int(time.time()),
                "source_mtime": os.path.getmtime(GNBOTS_PROFILES_PATH) if os.path.exists(GNBOTS_PROFILES_PATH) else 0
            }, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Failed to write schema cache: {e}")

    return {"Scripts": json_scripts}


# --- Глобальный кеш схемы ---
_schema_cache = {
    "schema": None,
    "built_at": 0,
    "source_mtime": 0,
}

def _save_schema_cache_to_disk(schema: dict):
    try:
        with open(SCHEMA_CACHE_PATH, "w", encoding="utf-8") as f:
            json.dump({
                "schema": schema,
                "built_at": int(time.time()),
                "source_mtime": _schema_cache.get("source_mtime", 0),
            }, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

def _load_schema_cache_from_disk():
    if not os.path.exists(SCHEMA_CACHE_PATH):
        return None
    try:
        with open(SCHEMA_CACHE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data
    except Exception:
        return None

def _extract_steps_from_obj(obj, acc_id_hint=None):
    """Возвращает список шагов из произвольной структуры."""
    steps = []

    def is_step(d: dict) -> bool:
        return isinstance(d, dict) and "ScriptId" in d and "Config" in d and isinstance(d["Config"], dict)

    def try_parse_json_string(s: str):
        s_strip = s.strip()
        if not s_strip:
            return None
        looks_jsonish = s_strip[0] in "[{" and ("\"ScriptId\"" in s_strip or "'ScriptId'" in s_strip)
        if not looks_jsonish:
            return None
        try:
            return json.loads(s_strip)
        except Exception:
            try:
                return json.loads(bytes(s_strip, "utf-8").decode("unicode_escape"))
            except Exception:
                return None

    def walk(node, parent_key=None):
        nonlocal steps
        if isinstance(node, dict):
            if is_step(node):
                steps.append(node)
            for k, v in node.items():
                if isinstance(v, str):
                    parsed = try_parse_json_string(v)
                    if parsed is not None:
                        walk(parsed, parent_key=k)
                else:
                    walk(v, parent_key=k)
        elif isinstance(node, list):
            for it in node:
                walk(it, parent_key=parent_key)
        elif isinstance(node, str):
            parsed = try_parse_json_string(node)
            if parsed is not None:
                walk(parsed, parent_key=parent_key)

    walk(obj, None)
    return steps


def _build_live_schema() -> dict:
    """Возвращает effective-схему по профилям."""
    src = _safe_json_load(GNBOTS_PROFILES_PATH) or {}
    steps = _extract_steps_from_obj(src)

    scripts = {}
    for step in steps:
        sid = step.get("ScriptId")
        cfg = step.get("Config", {})
        if not sid or not isinstance(cfg, dict):
            continue
        dst = scripts.setdefault(sid, {"Defaults": {}, "Shape": {}})

        for k, v in cfg.items():
            shape = dst["Shape"].get(k)
            if isinstance(v, dict) and "options" in v:
                options = v.get("options") or []
                val = v.get("value")
                prev_opts = set(shape["options"]) if shape and shape.get("type") == "select" else set()
                new_opts = sorted(list(prev_opts | set(options)))
                dst["Shape"][k] = {"type": "select", "options": new_opts}
                if k not in dst["Defaults"]:
                    default_val = val if val in new_opts else (new_opts[0] if new_opts else val)
                    dst["Defaults"][k] = default_val
            elif isinstance(v, bool):
                dst["Shape"][k] = {"type": "bool"}
                dst["Defaults"].setdefault(k, False)
            elif isinstance(v, (int, float)):
                dst["Shape"][k] = {"type": "number"}
                dst["Defaults"].setdefault(k, 0)
            else:
                dst["Shape"][k] = {"type": "string"}
                dst["Defaults"].setdefault(k, "")

    return {"Scripts": scripts}

def _need_refresh(now:int) -> bool:
    ttl = (now - _schema_cache["built_at"]) > SCHEMA_TTL_SECONDS
    src_mtime = 0
    try:
        src_mtime = int(os.path.getmtime(GNBOTS_PROFILES_PATH))
    except Exception:
        pass
    changed = (src_mtime != _schema_cache["source_mtime"])
    return ttl or changed

def _ensure_schema(now:int=None, force:bool=False) -> dict:
    global _schema_cache
    now = now or int(time.time())

    if _schema_cache["schema"] is None:
        disk = _load_schema_cache_from_disk()
        if disk and "schema" in disk:
            _schema_cache = {
                "schema": disk["schema"],
                "built_at": disk.get("built_at", 0),
                "source_mtime": disk.get("source_mtime", 0),
            }

    if force or (_schema_cache["schema"] is None) or _need_refresh(now):
        schema = _build_live_schema()
        _schema_cache["schema"] = schema
        _schema_cache["built_at"] = now
        try:
            _schema_cache["source_mtime"] = int(os.path.getmtime(GNBOTS_PROFILES_PATH))
        except Exception:
            _schema_cache["source_mtime"] = 0
        _save_schema_cache_to_disk(schema)
    return _schema_cache["schema"]

def _normalize_template_with_schema(template_steps:list, schema:dict) -> list:
    """Возвращаем список шагов, дополненный ключами из schema."""
    scripts_schema = schema.get("Scripts", {})
    by_sid = {}
    for step in template_steps:
        sid = step.get("ScriptId")
        if sid and sid not in by_sid:
            by_sid[sid] = step

    out = deepcopy(template_steps)

    for sid, sdef in scripts_schema.items():
        if sid not in by_sid:
            defaults = sdef.get("Defaults", {})
            shape    = sdef.get("Shape", {})
            cfg = {}
            for k, form in shape.items():
                if form.get("type") == "select":
                    cfg[k] = {"value": defaults.get(k, ""), "options": form.get("options", [])}
                else:
                    cfg[k] = defaults.get(k, False if form.get("type")=="bool" else (0 if form.get("type")=="number" else ""))
            out.append({
                "ScriptId": sid,
                "Uid": f"{sid}_auto",
                "OrderId": len(out),
                "Config": cfg,
                "Id": len(out),
                "IsActive": False,
                "IsCopy": True,
                "ScheduleData": {"Active": False, "Last": "0001-01-01T00:00:00", "Daily": False, "Hourly": False, "Weekly": False},
                "ScheduleRules": []
            })

    for step in out:
        sid = step.get("ScriptId")
        sdef = scripts_schema.get(sid)
        if not sdef:
            continue
        shape = sdef.get("Shape", {})
        defaults = sdef.get("Defaults", {})
        cfg = step.setdefault("Config", {})
        for k, form in shape.items():
            if k not in cfg:
                if form.get("type") == "select":
                    cfg[k] = {"value": defaults.get(k, ""), "options": form.get("options", [])}
                else:
                    cfg[k] = defaults.get(k, False if form.get("type")=="bool" else (0 if form.get("type")=="number" else ""))
            else:
                if form.get("type") == "select" and isinstance(cfg.get(k), dict):
                    cfg[k].setdefault("options", form.get("options", []))

    return out


def _validate_template_steps(steps: t.Any) -> t.Tuple[bool, str]:
    """Простая валидация структуры шаблона."""
    if not isinstance(steps, list):
        return False, "steps must be a list"
    for idx, step in enumerate(steps):
        if not isinstance(step, dict):
            return False, f"step[{idx}] must be an object"
        sid = step.get("ScriptId")
        if not isinstance(sid, str) or not sid.strip():
            return False, f"step[{idx}] must contain ScriptId"
        cfg = step.get("Config")
        if cfg is not None and not isinstance(cfg, dict):
            return False, f"step[{idx}].Config must be an object"
    return True, ""

# ──────────────────────────────────────────────────────────────────────
# ──────────── Ш А Б Л О Н Ы ────────────
TEMPLATES = {
    "650": r"""[{"ScriptId":"vikingbot.base.gathervip","Uid":"vikingbot.base.gathervip_3","OrderId":6,"Config":{"LevelStartAt":{"value":"3","options":["1","2","3","4","5","6"]},"Monster":false,"Niflung":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Divine":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Farm":{"value":"3","options":["off","1","2","3","4","5","6","7","8","9"]},"Sawmill":{"value":"3","options":["off","1","2","3","4","5","6","7","8","9"]},"Quarry":{"value":"3","options":["off","1","2","3","4","5","6","7","8","9"]},"Gold":{"value":"3","options":["off","1","2","3","4","5","6","7","8","9"]},"RallyTime":{"value":"5min","options":["5min","10min","30min","8hours"]},"reduceLevel":false,"marches":"5","farmLowestResource":false},"Id":6,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.gathervip","Uid":"vikingbot.base.gathervip_4","OrderId":6,"Config":{"LevelStartAt":{"value":"3","options":["1","2","3","4","5","6"]},"Monster":false,"Niflung":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Divine":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Farm":{"value":"3","options":["off","1","2","3","4","5","6","7","8","9"]},"Sawmill":{"value":"3","options":["off","1","2","3","4","5","6","7","8","9"]},"Quarry":{"value":"3","options":["off","1","2","3","4","5","6","7","8","9"]},"Gold":{"value":"3","options":["off","1","2","3","4","5","6","7","8","9"]},"RallyTime":{"value":"5min","options":["5min","10min","30min","8hours"]},"reduceLevel":false,"marches":"5","farmLowestResource":false},"Id":6,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.buffs","Uid":"vikingbot.base.buffs","OrderId":2,"Config":{"Attack":{"value":"Off","options":["Off","Any"]},"Defense":{"value":"Off","options":["Off","Any"]},"Gather":{"value":"Any","options":["Off","Any"]},"Workers":{"value":"Off","options":["Off","Any"]},"Deception":{"value":"Off","options":["Off","Any"]},"Trade":{"value":"Off","options":["Off","Any"]},"Patrol":{"value":"Off","options":["Off","Any"]},"useGems":false},"Id":2,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|12:00 AM|1:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.buffs","Uid":"vikingbot.base.buffs_1","OrderId":6,"Config":{"Attack":{"value":"Off","options":["Off","Any"]},"Defense":{"value":"Off","options":["Off","Any"]},"Gather":{"value":"Any","options":["Off","Any"]},"Workers":{"value":"Off","options":["Off","Any"]},"Deception":{"value":"Off","options":["Off","Any"]},"Trade":{"value":"Off","options":["Off","Any"]},"Patrol":{"value":"Off","options":["Off","Any"]},"useGems":false},"Id":6,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|8:00 AM|9:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.buffs","Uid":"vikingbot.base.buffs_2","OrderId":6,"Config":{"Attack":{"value":"Off","options":["Off","Any"]},"Defense":{"value":"Off","options":["Off","Any"]},"Gather":{"value":"Any","options":["Off","Any"]},"Workers":{"value":"Off","options":["Off","Any"]},"Deception":{"value":"Off","options":["Off","Any"]},"Trade":{"value":"Off","options":["Off","Any"]},"Patrol":{"value":"Off","options":["Off","Any"]},"useGems":false},"Id":6,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|4:00 PM|5:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.gathervip","Uid":"vikingbot.base.gathervip_1","OrderId":3,"Config":{"LevelStartAt":{"value":"3","options":["1","2","3","4","5","6"]},"Monster":false,"Niflung":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Divine":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Farm":{"value":"3","options":["off","1","2","3","4","5","6","7","8","9"]},"Sawmill":{"value":"3","options":["off","1","2","3","4","5","6","7","8","9"]},"Quarry":{"value":"3","options":["off","1","2","3","4","5","6","7","8","9"]},"Gold":{"value":"3","options":["off","1","2","3","4","5","6","7","8","9"]},"RallyTime":{"value":"5min","options":["5min","10min","30min","8hours"]},"reduceLevel":false,"marches":"5","farmLowestResource":false},"Id":3,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]}]""",

    "PREM": r"""[{"ScriptId":"vikingbot.base.gathervip","Uid":"vikingbot.base.gathervip_2","OrderId":0,"Config":{"LevelStartAt":{"value":"6","options":["1","2","3","4","5","6"]},"Monster":false,"Niflung":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Divine":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Farm":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Sawmill":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Quarry":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Gold":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"RallyTime":{"value":"10min","options":["5min","10min","30min","8hours"]},"reduceLevel":false,"marches":"5","farmLowestResource":false},"Id":0,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.gathervip","Uid":"vikingbot.base.gathervip_2","OrderId":1,"Config":{"LevelStartAt":{"value":"6","options":["1","2","3","4","5","6"]},"Monster":false,"Niflung":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Divine":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Farm":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Sawmill":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Quarry":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Gold":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"RallyTime":{"value":"10min","options":["5min","10min","30min","8hours"]},"reduceLevel":false,"marches":"5","farmLowestResource":false},"Id":1,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.dailies","Uid":"vikingbot.base.dailies_2","OrderId":2,"Config":{"quest":true,"recruit":true,"vip":true,"worker":{"value":"Off","options":["Off","Common","Rare","Legend","All"]},"gems":true,"errands":{"value":"10","options":["Off","5","10","15","20"]},"specialFarmer":true,"skipVoyageLushLand":true,"events":false,"collectCrystals":true},"Id":2,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|3:00 AM|5:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.dailies","Uid":"vikingbot.base.dailies_2","OrderId":3,"Config":{"quest":true,"recruit":true,"vip":true,"worker":{"value":"Off","options":["Off","Common","Rare","Legend","All"]},"gems":true,"errands":{"value":"10","options":["Off","5","10","15","20"]},"specialFarmer":true,"skipVoyageLushLand":true,"events":false,"collectCrystals":true},"Id":3,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|12:00 PM|2:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.dailies","Uid":"vikingbot.base.dailies_2","OrderId":4,"Config":{"quest":true,"recruit":true,"vip":true,"worker":{"value":"Off","options":["Off","Common","Rare","Legend","All"]},"gems":true,"errands":{"value":"10","options":["Off","5","10","15","20"]},"specialFarmer":true,"skipVoyageLushLand":true,"events":false,"collectCrystals":true},"Id":4,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|6:00 PM|8:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.alliancedonation","Uid":"vikingbot.base.alliancedonation","OrderId":5,"Config":{"allianceGift":true,"allianceDonation":{"value":"Recommended","options":["Recommended","Development","Territory","War","Skills","Off"]}},"Id":5,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|12:00 AM|3:00 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.alliancedonation","Uid":"vikingbot.base.alliancedonation_2","OrderId":6,"Config":{"allianceGift":true,"allianceDonation":{"value":"Recommended","options":["Recommended","Development","Territory","War","Skills","Off"]}},"Id":6,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|10:00 AM|1:00 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.alliancedonation","Uid":"vikingbot.base.alliancedonation_1","OrderId":7,"Config":{"allianceGift":true,"allianceDonation":{"value":"Recommended","options":["Recommended","Development","Territory","War","Skills","Off"]}},"Id":7,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|7:00 PM|9:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.buffs","Uid":"vikingbot.base.buffs","OrderId":8,"Config":{"Attack":{"value":"Off","options":["Off","Any"]},"Defense":{"value":"Off","options":["Off","Any"]},"Gather":{"value":"Any","options":["Off","Any"]},"Workers":{"value":"Off","options":["Off","Any"]},"Deception":{"value":"Off","options":["Off","Any"]},"Trade":{"value":"Off","options":["Off","Any"]},"Patrol":{"value":"Off","options":["Off","Any"]},"useGems":false},"Id":8,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|12:00 AM|2:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.buffs","Uid":"vikingbot.base.buffs_1","OrderId":9,"Config":{"Attack":{"value":"Off","options":["Off","Any"]},"Defense":{"value":"Off","options":["Off","Any"]},"Gather":{"value":"Any","options":["Off","Any"]},"Workers":{"value":"Off","options":["Off","Any"]},"Deception":{"value":"Off","options":["Off","Any"]},"Trade":{"value":"Off","options":["Off","Any"]},"Patrol":{"value":"Off","options":["Off","Any"]},"useGems":false},"Id":9,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|8:00 AM|10:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.buffs","Uid":"vikingbot.base.buffs_2","OrderId":10,"Config":{"Attack":{"value":"Off","options":["Off","Any"]},"Defense":{"value":"Off","options":["Off","Any"]},"Gather":{"value":"Any","options":["Off","Any"]},"Workers":{"value":"Off","options":["Off","Any"]},"Deception":{"value":"Off","options":["Off","Any"]},"Trade":{"value":"Off","options":["Off","Any"]},"Patrol":{"value":"Off","options":["Off","Any"]},"useGems":false},"Id":10,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|4:00 PM|6:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.recruitment","Uid":"vikingbot.base.recruitment","OrderId":11,"Config":{"Infantry":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"Archer":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"Pikemen":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"Porter":{"value":"1","options":["Off","Auto","1","2","3","4","5","6","7"]},"Amount":{"value":"100%","options":["100%","75%","50%","25%"]},"UpgradeInfantry":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradeArcher":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradePikemen":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradePorter":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"useResources":true,"useSpeedUps":false},"Id":11,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|12:00 AM|2:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.recruitment","Uid":"vikingbot.base.recruitment_1","OrderId":12,"Config":{"Infantry":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"Archer":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"Pikemen":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"Porter":{"value":"1","options":["Off","Auto","1","2","3","4","5","6","7"]},"Amount":{"value":"100%","options":["100%","75%","50%","25%"]},"UpgradeInfantry":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradeArcher":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradePikemen":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradePorter":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"useResources":true,"useSpeedUps":false},"Id":12,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|8:00 AM|10:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.recruitment","Uid":"vikingbot.base.recruitment_2","OrderId":13,"Config":{"Infantry":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"Archer":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"Pikemen":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"Porter":{"value":"1","options":["Off","Auto","1","2","3","4","5","6","7"]},"Amount":{"value":"100%","options":["100%","75%","50%","25%"]},"UpgradeInfantry":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradeArcher":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradePikemen":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradePorter":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"useResources":true,"useSpeedUps":false},"Id":13,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|4:00 PM|6:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.upgrade","Uid":"vikingbot.base.upgrade","OrderId":14,"Config":{"Upgrade":{"value":"MainHall","options":["MainHall","Specfic","Villages"]},"EagleNest":false,"Warehouse":false,"HallofValor":false,"TribeHall":true,"DivinationShack":false,"Academy":false,"Watchtower":false,"Infirmary":false,"Infantry":false,"Archer":false,"Porter":false,"Pikemen":false,"SquadBase":false,"VillageHall":false,"Workshop":false,"Prison":false,"DefenderCamp":false,"SuppyHub":false,"Market":false,"useSpeedUps":false,"useResources":true},"Id":14,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|12:00 AM|2:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.upgrade","Uid":"vikingbot.base.upgrade_2","OrderId":15,"Config":{"Upgrade":{"value":"MainHall","options":["MainHall","Specfic","Villages"]},"EagleNest":false,"Warehouse":false,"HallofValor":false,"TribeHall":true,"DivinationShack":false,"Academy":false,"Watchtower":false,"Infirmary":false,"Infantry":false,"Archer":false,"Porter":false,"Pikemen":false,"SquadBase":false,"VillageHall":false,"Workshop":false,"Prison":false,"DefenderCamp":false,"SuppyHub":false,"Market":false,"useSpeedUps":false,"useResources":true},"Id":15,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|8:00 AM|10:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.upgrade","Uid":"vikingbot.base.upgrade_2","OrderId":16,"Config":{"Upgrade":{"value":"MainHall","options":["MainHall","Specfic","Villages"]},"EagleNest":false,"Warehouse":false,"HallofValor":false,"TribeHall":true,"DivinationShack":false,"Academy":false,"Watchtower":false,"Infirmary":false,"Infantry":false,"Archer":false,"Porter":false,"Pikemen":false,"SquadBase":false,"VillageHall":false,"Workshop":false,"Prison":false,"DefenderCamp":false,"SuppyHub":false,"Market":false,"useSpeedUps":false,"useResources":true},"Id":16,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|4:00 PM|6:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.research","Uid":"vikingbot.base.research","OrderId":17,"Config":{"research":{"value":"Economy","options":["Economy","Military"]},"upgrade":true,"useResources":true,"useSpeedUps":false},"Id":17,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|12:00 AM|2:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.research","Uid":"vikingbot.base.research_1","OrderId":18,"Config":{"research":{"value":"Economy","options":["Economy","Military"]},"upgrade":true,"useResources":true,"useSpeedUps":false},"Id":18,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|8:00 AM|10:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.research","Uid":"vikingbot.base.research_2","OrderId":19,"Config":{"research":{"value":"Economy","options":["Economy","Military"]},"upgrade":true,"useResources":true,"useSpeedUps":false},"Id":19,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|4:00 PM|6:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.divinationshack","Uid":"vikingbot.base.divinationshack","OrderId":20,"Config":{"SpeedUp":false,"Food":false,"Stones":false,"Gold":true,"Gems":false,"Lumber":false,"ConstructionSpeed":false,"TrainExpansion":false,"ForgingSpeed":false,"ResearchSpeed":false,"TrainingSpeed":false,"ForgingConsumption":false,"HealingSpeed":false,"TrainingConsumption":false,"HealingConsumption":false},"Id":20,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|12:00 AM|2:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.divinationshack","Uid":"vikingbot.base.divinationshack_1","OrderId":21,"Config":{"SpeedUp":false,"Food":false,"Stones":false,"Gold":true,"Gems":false,"Lumber":false,"ConstructionSpeed":false,"TrainExpansion":false,"ForgingSpeed":false,"ResearchSpeed":false,"TrainingSpeed":false,"ForgingConsumption":false,"HealingSpeed":false,"TrainingConsumption":false,"HealingConsumption":false},"Id":21,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|8:00 AM|10:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.divinationshack","Uid":"vikingbot.base.divinationshack_2","OrderId":22,"Config":{"SpeedUp":false,"Food":false,"Stones":false,"Gold":true,"Gems":false,"Lumber":false,"ConstructionSpeed":false,"TrainExpansion":false,"ForgingSpeed":false,"ResearchSpeed":false,"TrainingSpeed":false,"ForgingConsumption":false,"HealingSpeed":false,"TrainingConsumption":false,"HealingConsumption":false},"Id":22,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|4:00 PM|6:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.mail","Uid":"vikingbot.base.mail","OrderId":23,"Config":{"skip":0},"Id":23,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|3:00 AM|5:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.mail","Uid":"vikingbot.base.mail_1","OrderId":24,"Config":{"skip":0},"Id":24,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|6:00 PM|8:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.dragoncave","Uid":"vikingbot.base.dragoncave","OrderId":25,"Config":{"Resources":true,"Speedups":false,"Buffs":false,"Equipment":false,"Mounts":false,"Others":false,"ResourcesUseGold":true,"Gray":false,"Green":false,"Blue":true,"Purple":true},"Id":25,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|3:00 AM|5:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.exploration","Uid":"vikingbot.base.exploration","OrderId":26,"Config":{"AtheronSnowfields":true,"NovaForest":true,"DanaPlains":true,"MtKhajag":true,"AsltaRange":true,"Dornfjord":true,"GertlandIsland":true,"highestMission":true,"lowestMission":false,"fastestMission":false},"Id":26,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.commission","Uid":"vikingbot.base.commission","OrderId":27,"Config":{"skip":0},"Id":27,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.gathervip","Uid":"vikingbot.base.gathervip_2","OrderId":28,"Config":{"LevelStartAt":{"value":"6","options":["1","2","3","4","5","6"]},"Monster":false,"Niflung":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Divine":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Farm":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Sawmill":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Quarry":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Gold":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"RallyTime":{"value":"10min","options":["5min","10min","30min","8hours"]},"reduceLevel":false,"marches":"5","farmLowestResource":false},"Id":28,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]}]""",

    "1100": r"""[{"ScriptId":"vikingbot.base.gathervip","Uid":"vikingbot.base.gathervip_2","OrderId":0,"Config":{"LevelStartAt":{"value":"6","options":["1","2","3","4","5","6"]},"Monster":false,"Niflung":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Divine":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Farm":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Sawmill":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Quarry":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Gold":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"RallyTime":{"value":"10min","options":["5min","10min","30min","8hours"]},"reduceLevel":false,"marches":"5","farmLowestResource":false},"Id":0,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.gathervip","Uid":"vikingbot.base.gathervip_2","OrderId":1,"Config":{"LevelStartAt":{"value":"6","options":["1","2","3","4","5","6"]},"Monster":false,"Niflung":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Divine":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Farm":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Sawmill":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Quarry":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Gold":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"RallyTime":{"value":"10min","options":["5min","10min","30min","8hours"]},"reduceLevel":false,"marches":"5","farmLowestResource":false},"Id":1,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.dailies","Uid":"vikingbot.base.dailies_2","OrderId":2,"Config":{"quest":true,"recruit":true,"vip":true,"worker":{"value":"Off","options":["Off","Common","Rare","Legend","All"]},"gems":true,"errands":{"value":"10","options":["Off","5","10","15","20"]},"specialFarmer":true,"skipVoyageLushLand":true,"events":false,"collectCrystals":true},"Id":2,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|3:00 AM|5:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.dailies","Uid":"vikingbot.base.dailies_2","OrderId":3,"Config":{"quest":true,"recruit":true,"vip":true,"worker":{"value":"Off","options":["Off","Common","Rare","Legend","All"]},"gems":true,"errands":{"value":"10","options":["Off","5","10","15","20"]},"specialFarmer":true,"skipVoyageLushLand":true,"events":false,"collectCrystals":true},"Id":3,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|12:00 PM|2:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.dailies","Uid":"vikingbot.base.dailies_2","OrderId":4,"Config":{"quest":true,"recruit":true,"vip":true,"worker":{"value":"Off","options":["Off","Common","Rare","Legend","All"]},"gems":true,"errands":{"value":"10","options":["Off","5","10","15","20"]},"specialFarmer":true,"skipVoyageLushLand":true,"events":false,"collectCrystals":true},"Id":4,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|6:00 PM|8:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.alliancedonation","Uid":"vikingbot.base.alliancedonation","OrderId":5,"Config":{"allianceGift":true,"allianceDonation":{"value":"Recommended","options":["Recommended","Development","Territory","War","Skills","Off"]}},"Id":5,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|12:00 AM|3:00 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.alliancedonation","Uid":"vikingbot.base.alliancedonation_2","OrderId":6,"Config":{"allianceGift":true,"allianceDonation":{"value":"Recommended","options":["Recommended","Development","Territory","War","Skills","Off"]}},"Id":6,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|10:00 AM|1:00 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.alliancedonation","Uid":"vikingbot.base.alliancedonation_1","OrderId":7,"Config":{"allianceGift":true,"allianceDonation":{"value":"Recommended","options":["Recommended","Development","Territory","War","Skills","Off"]}},"Id":7,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|7:00 PM|9:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.buffs","Uid":"vikingbot.base.buffs","OrderId":8,"Config":{"Attack":{"value":"Off","options":["Off","Any"]},"Defense":{"value":"Off","options":["Off","Any"]},"Gather":{"value":"Any","options":["Off","Any"]},"Workers":{"value":"Off","options":["Off","Any"]},"Deception":{"value":"Off","options":["Off","Any"]},"Trade":{"value":"Off","options":["Off","Any"]},"Patrol":{"value":"Off","options":["Off","Any"]},"useGems":false},"Id":8,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|12:00 AM|2:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.buffs","Uid":"vikingbot.base.buffs_1","OrderId":9,"Config":{"Attack":{"value":"Off","options":["Off","Any"]},"Defense":{"value":"Off","options":["Off","Any"]},"Gather":{"value":"Any","options":["Off","Any"]},"Workers":{"value":"Off","options":["Off","Any"]},"Deception":{"value":"Off","options":["Off","Any"]},"Trade":{"value":"Off","options":["Off","Any"]},"Patrol":{"value":"Off","options":["Off","Any"]},"useGems":false},"Id":9,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|8:00 AM|10:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.buffs","Uid":"vikingbot.base.buffs_2","OrderId":10,"Config":{"Attack":{"value":"Off","options":["Off","Any"]},"Defense":{"value":"Off","options":["Off","Any"]},"Gather":{"value":"Any","options":["Off","Any"]},"Workers":{"value":"Off","options":["Off","Any"]},"Deception":{"value":"Off","options":["Off","Any"]},"Trade":{"value":"Off","options":["Off","Any"]},"Patrol":{"value":"Off","options":["Off","Any"]},"useGems":false},"Id":10,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|4:00 PM|6:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.mail","Uid":"vikingbot.base.mail","OrderId":23,"Config":{"skip":0},"Id":23,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|3:00 AM|5:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.mail","Uid":"vikingbot.base.mail_1","OrderId":24,"Config":{"skip":0},"Id":24,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|6:00 PM|8:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.gathervip","Uid":"vikingbot.base.gathervip_2","OrderId":28,"Config":{"LevelStartAt":{"value":"6","options":["1","2","3","4","5","6"]},"Monster":false,"Niflung":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Divine":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Farm":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Sawmill":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Quarry":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Gold":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"RallyTime":{"value":"10min","options":["5min","10min","30min","8hours"]},"reduceLevel":false,"marches":"5","farmLowestResource":false},"Id":28,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]}]""",

    "TRAIN": r"""[{"ScriptId":"vikingbot.base.stagingpost","Uid":"vikingbot.base.stagingpost","OrderId":0,"Config":{"redMission":false,"marches":"10","ignoreSuicide":false},"Id":0,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.dailies","Uid":"vikingbot.base.dailies_2","OrderId":1,"Config":{"quest":true,"recruit":true,"vip":true,"worker":{"value":"Off","options":["Off","Common","Rare","Legend","All"]},"gems":true,"errands":{"value":"10","options":["Off","5","10","15","20"]},"specialFarmer":true,"skipVoyageLushLand":true,"events":false,"collectCrystals":true},"Id":1,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|3:00 AM|5:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.dailies","Uid":"vikingbot.base.dailies_2","OrderId":2,"Config":{"quest":true,"recruit":true,"vip":true,"worker":{"value":"Off","options":["Off","Common","Rare","Legend","All"]},"gems":true,"errands":{"value":"10","options":["Off","5","10","15","20"]},"specialFarmer":true,"skipVoyageLushLand":true,"events":false,"collectCrystals":true},"Id":2,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|12:00 PM|2:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.dailies","Uid":"vikingbot.base.dailies_3","OrderId":3,"Config":{"quest":true,"recruit":true,"vip":true,"worker":{"value":"Off","options":["Off","Common","Rare","Legend","All"]},"gems":true,"errands":{"value":"10","options":["Off","5","10","15","20"]},"specialFarmer":true,"skipVoyageLushLand":true,"events":false,"collectCrystals":true},"Id":3,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|3:00 AM|5:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.dailies","Uid":"vikingbot.base.dailies_2","OrderId":4,"Config":{"quest":false,"recruit":true,"vip":false,"worker":{"value":"Off","options":["Off","Common","Rare","Legend","All"]},"gems":false,"errands":{"value":"Off","options":["Off","5","10","15","20"]},"specialFarmer":false,"skipVoyageLushLand":false,"events":false,"collectCrystals":false},"Id":4,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.alliancedonation","Uid":"vikingbot.base.alliancedonation","OrderId":5,"Config":{"allianceGift":true,"allianceDonation":{"value":"Off","options":["Recommended","Development","Territory","War","Skills","Off"]}},"Id":5,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|12:00 AM|3:00 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.alliancedonation","Uid":"vikingbot.base.alliancedonation_2","OrderId":6,"Config":{"allianceGift":true,"allianceDonation":{"value":"Off","options":["Recommended","Development","Territory","War","Skills","Off"]}},"Id":6,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|10:00 AM|1:00 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.alliancedonation","Uid":"vikingbot.base.alliancedonation_1","OrderId":7,"Config":{"allianceGift":true,"allianceDonation":{"value":"Off","options":["Recommended","Development","Territory","War","Skills","Off"]}},"Id":7,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|7:00 PM|9:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.build","Uid":"vikingbot.base.build","OrderId":8,"Config":{"skip":0},"Id":8,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.upgrade","Uid":"vikingbot.base.upgrade","OrderId":9,"Config":{"Upgrade":{"value":"MainHall","options":["MainHall","Specfic","Villages"]},"EagleNest":false,"Warehouse":false,"HallofValor":false,"TribeHall":true,"DivinationShack":false,"Academy":false,"Watchtower":false,"Infirmary":false,"Infantry":false,"Archer":false,"Porter":true,"Pikemen":false,"SquadBase":false,"VillageHall":false,"Workshop":false,"Prison":false,"DefenderCamp":false,"SuppyHub":false,"Market":false,"useSpeedUps":true,"useResources":true},"Id":9,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.upgrade","Uid":"vikingbot.base.upgrade_1","OrderId":24,"Config":{"Upgrade":{"value":"MainHall","options":["MainHall","Specfic","Villages"]},"EagleNest":false,"Warehouse":false,"HallofValor":false,"TribeHall":true,"DivinationShack":false,"Academy":false,"Watchtower":false,"Infirmary":false,"Infantry":false,"Archer":false,"Porter":true,"Pikemen":false,"SquadBase":false,"VillageHall":false,"Workshop":false,"Prison":false,"DefenderCamp":false,"SuppyHub":false,"Market":false,"useSpeedUps":false,"useResources":true},"Id":24,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.recruitment","Uid":"vikingbot.base.recruitment","OrderId":10,"Config":{"Infantry":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"Archer":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"Pikemen":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"Porter":{"value":"1","options":["Off","Auto","1","2","3","4","5","6","7"]},"Amount":{"value":"100%","options":["100%","75%","50%","25%"]},"UpgradeInfantry":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradeArcher":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradePikemen":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradePorter":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"useResources":true,"useSpeedUps":true},"Id":10,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.recruitment","Uid":"vikingbot.base.recruitment_1","OrderId":24,"Config":{"Infantry":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"Archer":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"Pikemen":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"Porter":{"value":"1","options":["Off","Auto","1","2","3","4","5","6","7"]},"Amount":{"value":"100%","options":["100%","75%","50%","25%"]},"UpgradeInfantry":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradeArcher":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradePikemen":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradePorter":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"useResources":true,"useSpeedUps":false},"Id":24,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.research","Uid":"vikingbot.base.research","OrderId":11,"Config":{"research":{"value":"Economy","options":["Economy","Military"]},"upgrade":true,"useResources":true,"useSpeedUps":true},"Id":11,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.research","Uid":"vikingbot.base.research_1","OrderId":24,"Config":{"research":{"value":"Economy","options":["Economy","Military"]},"upgrade":true,"useResources":true,"useSpeedUps":false},"Id":24,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.divinationshack","Uid":"vikingbot.base.divinationshack","OrderId":12,"Config":{"SpeedUp":true,"Food":false,"Stones":false,"Gold":false,"Gems":false,"Lumber":false,"ConstructionSpeed":false,"TrainExpansion":false,"ForgingSpeed":false,"ResearchSpeed":false,"TrainingSpeed":false,"ForgingConsumption":false,"HealingSpeed":false,"TrainingConsumption":false,"HealingConsumption":false},"Id":12,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|12:00 AM|2:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.divinationshack","Uid":"vikingbot.base.divinationshack_1","OrderId":13,"Config":{"SpeedUp":true,"Food":false,"Stones":false,"Gold":false,"Gems":false,"Lumber":false,"ConstructionSpeed":false,"TrainExpansion":false,"ForgingSpeed":false,"ResearchSpeed":false,"TrainingSpeed":false,"ForgingConsumption":false,"HealingSpeed":false,"TrainingConsumption":false,"HealingConsumption":false},"Id":13,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|8:00 AM|10:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.divinationshack","Uid":"vikingbot.base.divinationshack_2","OrderId":14,"Config":{"SpeedUp":true,"Food":false,"Stones":false,"Gold":false,"Gems":false,"Lumber":false,"ConstructionSpeed":false,"TrainExpansion":false,"ForgingSpeed":false,"ResearchSpeed":false,"TrainingSpeed":false,"ForgingConsumption":false,"HealingSpeed":false,"TrainingConsumption":false,"HealingConsumption":false},"Id":14,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|4:00 PM|6:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.mail","Uid":"vikingbot.base.mail","OrderId":15,"Config":{"skip":0},"Id":15,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|3:00 AM|5:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.mail","Uid":"vikingbot.base.mail_1","OrderId":16,"Config":{"skip":0},"Id":16,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|6:00 PM|8:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.eaglenest","Uid":"vikingbot.base.eaglenest","OrderId":17,"Config":{"skip":0},"Id":17,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.villages","Uid":"vikingbot.base.villages","OrderId":18,"Config":{"skip":0,"marches":"15"},"Id":18,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.exploration","Uid":"vikingbot.base.exploration","OrderId":19,"Config":{"AtheronSnowfields":true,"NovaForest":true,"DanaPlains":true,"MtKhajag":true,"AsltaRange":true,"Dornfjord":true,"GertlandIsland":true,"highestMission":true,"lowestMission":false,"fastestMission":false},"Id":19,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.commission","Uid":"vikingbot.base.commission","OrderId":20,"Config":{"skip":0},"Id":20,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.heal","Uid":"vikingbot.base.heal","OrderId":21,"Config":{"skip":0,"useResources":true},"Id":21,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]}]""",

    "TRAIN2": r"""[{"ScriptId":"vikingbot.base.stagingpost","Uid":"vikingbot.base.stagingpost","OrderId":0,"Config":{"redMission":false,"marches":"10","ignoreSuicide":false},"Id":0,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.dailies","Uid":"vikingbot.base.dailies_2","OrderId":1,"Config":{"quest":true,"recruit":true,"vip":true,"worker":{"value":"Off","options":["Off","Common","Rare","Legend","All"]},"gems":true,"errands":{"value":"10","options":["Off","5","10","15","20"]},"specialFarmer":true,"skipVoyageLushLand":true,"events":false,"collectCrystals":true},"Id":1,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|3:00 AM|5:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.dailies","Uid":"vikingbot.base.dailies_2","OrderId":2,"Config":{"quest":true,"recruit":true,"vip":true,"worker":{"value":"Off","options":["Off","Common","Rare","Legend","All"]},"gems":true,"errands":{"value":"10","options":["Off","5","10","15","20"]},"specialFarmer":true,"skipVoyageLushLand":true,"events":false,"collectCrystals":true},"Id":2,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|12:00 PM|2:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.dailies","Uid":"vikingbot.base.dailies_2","OrderId":3,"Config":{"quest":true,"recruit":true,"vip":true,"worker":{"value":"Off","options":["Off","Common","Rare","Legend","All"]},"gems":true,"errands":{"value":"10","options":["Off","5","10","15","20"]},"specialFarmer":true,"skipVoyageLushLand":true,"events":false,"collectCrystals":true},"Id":3,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|6:00 PM|8:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.dailies","Uid":"vikingbot.base.dailies_3","OrderId":4,"Config":{"quest":false,"recruit":true,"vip":false,"worker":{"value":"Off","options":["Off","Common","Rare","Legend","All"]},"gems":false,"errands":{"value":"Off","options":["Off","5","10","15","20"]},"specialFarmer":false,"skipVoyageLushLand":false,"events":false,"collectCrystals":false},"Id":4,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.alliancedonation","Uid":"vikingbot.base.alliancedonation","OrderId":5,"Config":{"allianceGift":true,"allianceDonation":{"value":"Recommended","options":["Recommended","Development","Territory","War","Skills","Off"]}},"Id":5,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|12:00 AM|3:00 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.alliancedonation","Uid":"vikingbot.base.alliancedonation_2","OrderId":6,"Config":{"allianceGift":true,"allianceDonation":{"value":"Recommended","options":["Recommended","Development","Territory","War","Skills","Off"]}},"Id":6,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|10:00 AM|1:00 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.alliancedonation","Uid":"vikingbot.base.alliancedonation_1","OrderId":7,"Config":{"allianceGift":true,"allianceDonation":{"value":"Recommended","options":["Recommended","Development","Territory","War","Skills","Off"]}},"Id":7,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|7:00 PM|9:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.build","Uid":"vikingbot.base.build","OrderId":8,"Config":{"skip":0},"Id":8,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.buffs","Uid":"vikingbot.base.buffs","OrderId":9,"Config":{"Attack":{"value":"Off","options":["Off","Any"]},"Defense":{"value":"Off","options":["Off","Any"]},"Gather":{"value":"Any","options":["Off","Any"]},"Workers":{"value":"Off","options":["Off","Any"]},"Deception":{"value":"Off","options":["Off","Any"]},"Trade":{"value":"Off","options":["Off","Any"]},"Patrol":{"value":"Off","options":["Off","Any"]},"useGems":false},"Id":9,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|12:00 AM|2:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.buffs","Uid":"vikingbot.base.buffs_1","OrderId":10,"Config":{"Attack":{"value":"Off","options":["Off","Any"]},"Defense":{"value":"Off","options":["Off","Any"]},"Gather":{"value":"Any","options":["Off","Any"]},"Workers":{"value":"Off","options":["Off","Any"]},"Deception":{"value":"Off","options":["Off","Any"]},"Trade":{"value":"Off","options":["Off","Any"]},"Patrol":{"value":"Off","options":["Off","Any"]},"useGems":false},"Id":10,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|8:00 AM|10:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.buffs","Uid":"vikingbot.base.buffs_2","OrderId":11,"Config":{"Attack":{"value":"Off","options":["Off","Any"]},"Defense":{"value":"Off","options":["Off","Any"]},"Gather":{"value":"Any","options":["Off","Any"]},"Workers":{"value":"Off","options":["Off","Any"]},"Deception":{"value":"Off","options":["Off","Any"]},"Trade":{"value":"Off","options":["Off","Any"]},"Patrol":{"value":"Off","options":["Off","Any"]},"useGems":false},"Id":11,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|4:00 PM|6:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.upgrade","Uid":"vikingbot.base.upgrade_1","OrderId":26,"Config":{"Upgrade":{"value":"MainHall","options":["MainHall","Specfic","Villages"]},"EagleNest":false,"Warehouse":false,"HallofValor":false,"TribeHall":true,"DivinationShack":false,"Academy":false,"Watchtower":false,"Infirmary":false,"Infantry":false,"Archer":false,"Porter":false,"Pikemen":false,"SquadBase":false,"VillageHall":false,"Workshop":false,"Prison":false,"DefenderCamp":false,"SuppyHub":false,"Market":false,"useSpeedUps":true,"useResources":true},"Id":26,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.upgrade","Uid":"vikingbot.base.upgrade","OrderId":13,"Config":{"Upgrade":{"value":"MainHall","options":["MainHall","Specfic","Villages"]},"EagleNest":false,"Warehouse":false,"HallofValor":false,"TribeHall":true,"DivinationShack":false,"Academy":false,"Watchtower":false,"Infirmary":false,"Infantry":false,"Archer":false,"Porter":false,"Pikemen":false,"SquadBase":false,"VillageHall":false,"Workshop":false,"Prison":false,"DefenderCamp":false,"SuppyHub":false,"Market":false,"useSpeedUps":false,"useResources":true},"Id":13,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.recruitment","Uid":"vikingbot.base.recruitment_1","OrderId":26,"Config":{"Infantry":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"Archer":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"Pikemen":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"Porter":{"value":"1","options":["Off","Auto","1","2","3","4","5","6","7"]},"Amount":{"value":"100%","options":["100%","75%","50%","25%"]},"UpgradeInfantry":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradeArcher":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradePikemen":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradePorter":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"useResources":true,"useSpeedUps":true},"Id":26,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.recruitment","Uid":"vikingbot.base.recruitment","OrderId":12,"Config":{"Infantry":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"Archer":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"Pikemen":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"Porter":{"value":"1","options":["Off","Auto","1","2","3","4","5","6","7"]},"Amount":{"value":"100%","options":["100%","75%","50%","25%"]},"UpgradeInfantry":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradeArcher":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradePikemen":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradePorter":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"useResources":true,"useSpeedUps":false},"Id":12,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.research","Uid":"vikingbot.base.research","OrderId":14,"Config":{"research":{"value":"Economy","options":["Economy","Military"]},"upgrade":true,"useResources":true,"useSpeedUps":true},"Id":14,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.research","Uid":"vikingbot.base.research_1","OrderId":26,"Config":{"research":{"value":"Economy","options":["Economy","Military"]},"upgrade":true,"useResources":true,"useSpeedUps":false},"Id":26,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.divinationshack","Uid":"vikingbot.base.divinationshack","OrderId":15,"Config":{"SpeedUp":true,"Food":false,"Stones":false,"Gold":false,"Gems":false,"Lumber":false,"ConstructionSpeed":false,"TrainExpansion":false,"ForgingSpeed":false,"ResearchSpeed":false,"TrainingSpeed":false,"ForgingConsumption":false,"HealingSpeed":false,"TrainingConsumption":false,"HealingConsumption":false},"Id":15,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|12:00 AM|2:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.divinationshack","Uid":"vikingbot.base.divinationshack_1","OrderId":16,"Config":{"SpeedUp":true,"Food":false,"Stones":false,"Gold":false,"Gems":false,"Lumber":false,"ConstructionSpeed":false,"TrainExpansion":false,"ForgingSpeed":false,"ResearchSpeed":false,"TrainingSpeed":false,"ForgingConsumption":false,"HealingSpeed":false,"TrainingConsumption":false,"HealingConsumption":false},"Id":16,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|8:00 AM|10:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.divinationshack","Uid":"vikingbot.base.divinationshack_2","OrderId":17,"Config":{"SpeedUp":true,"Food":false,"Stones":false,"Gold":false,"Gems":false,"Lumber":false,"ConstructionSpeed":false,"TrainExpansion":false,"ForgingSpeed":false,"ResearchSpeed":false,"TrainingSpeed":false,"ForgingConsumption":false,"HealingSpeed":false,"TrainingConsumption":false,"HealingConsumption":false},"Id":17,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|4:00 PM|6:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.mail","Uid":"vikingbot.base.mail","OrderId":18,"Config":{"skip":0},"Id":18,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|3:00 AM|5:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.mail","Uid":"vikingbot.base.mail_1","OrderId":19,"Config":{"skip":0},"Id":19,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|6:00 PM|8:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.exploration","Uid":"vikingbot.base.exploration","OrderId":20,"Config":{"AtheronSnowfields":true,"NovaForest":true,"DanaPlains":true,"MtKhajag":true,"AsltaRange":true,"Dornfjord":true,"GertlandIsland":true,"highestMission":true,"lowestMission":false,"fastestMission":false},"Id":20,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.commission","Uid":"vikingbot.base.commission","OrderId":21,"Config":{"skip":0},"Id":21,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.dragoncave","Uid":"vikingbot.base.dragoncave","OrderId":22,"Config":{"Resources":true,"Speedups":true,"Buffs":false,"Equipment":false,"Mounts":false,"Others":false,"ResourcesUseGold":true,"Gray":false,"Green":false,"Blue":true,"Purple":true},"Id":22,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|3:00 AM|5:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.gathervip","Uid":"vikingbot.base.gathervip_2","OrderId":23,"Config":{"LevelStartAt":{"value":"6","options":["1","2","3","4","5","6"]},"Monster":false,"Niflung":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Divine":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Farm":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Sawmill":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Quarry":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Gold":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"RallyTime":{"value":"10min","options":["5min","10min","30min","8hours"]},"reduceLevel":false,"marches":"7","farmLowestResource":true},"Id":23,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]}]""",
   
}
# Алиасы тарифных шаблонов для совместимости с клиентами
TEMPLATES["500"] = TEMPLATES["650"]
TEMPLATES["OnlyFarm"] = TEMPLATES["650"]
TEMPLATES["1000"] = TEMPLATES["1100"]
TEMPLATES["Extended"] = TEMPLATES["1100"]
TEMPLATES["1400"] = TEMPLATES["PREM"]
TEMPLATES["Premium"] = TEMPLATES["PREM"]
# ──────────────────────────────────────────────────────────────────────


BUILTIN_TEMPLATE_NAMES = ["650", "PREM", "1100", "TRAIN", "TRAIN2"]

DEFAULT_TEMPLATE_ALIASES = {
    "500": "650.json",
    "OnlyFarm": "650.json",
    "1000": "1100.json",
    "Extended": "1100.json",
    "1400": "PREM.json",
    "Premium": "PREM.json",
}


def _canonical_template_name(raw_name: str) -> str:
    name = os.path.basename((raw_name or "").strip())
    if not name:
        raise ValueError("empty template name")
    if not name.lower().endswith(".json"):
        name = f"{name}.json"
    return name


def _load_template_aliases() -> dict[str, str]:
    data = _json_read_or(TEMPLATE_ALIASES_PATH, {})
    if not isinstance(data, dict):
        return {}

    aliases: dict[str, str] = {}
    for k, v in data.items():
        if not k or not v:
            continue
        try:
            aliases[str(k).strip()] = _canonical_template_name(str(v))
        except Exception:
            continue
    return aliases


def _save_template_aliases(mapping: dict[str, str]):
    safe_write_json(TEMPLATE_ALIASES_PATH, mapping)


def _resolve_template_name(raw_name: str, aliases: dict[str, str] | None = None) -> str:
    aliases = aliases or _load_template_aliases()
    key = (raw_name or "").strip()
    candidates = [key]
    if key.lower().endswith(".json"):
        candidates.append(key[:-5])
    else:
        candidates.append(f"{key}.json")

    for cand in candidates:
        if cand in aliases:
            try:
                return _canonical_template_name(aliases[cand])
            except Exception:
                continue

    return _canonical_template_name(key)


def _ensure_builtin_templates():
    """Записываем встроенные шаблоны и алиасы в файлы, если их ещё нет."""

    for name in BUILTIN_TEMPLATE_NAMES:
        raw = TEMPLATES.get(name)
        if not raw:
            continue
        try:
            full_path, safe_name = _normalized_template_path(name)
        except Exception:
            continue

        if os.path.exists(full_path):
            continue

        try:
            steps = json.loads(raw)
        except Exception:
            continue

        try:
            safe_write_json(full_path, steps)
            print(f"[templates] seeded {safe_name}")
        except Exception:
            print(f"[templates] failed to seed {safe_name}")

    aliases = _load_template_aliases()
    changed = False
    for k, v in DEFAULT_TEMPLATE_ALIASES.items():
        try:
            canon = _canonical_template_name(v)
        except Exception:
            continue
        if aliases.get(k) != canon:
            aliases[k] = canon
            changed = True
    if changed:
        _save_template_aliases(aliases)


app = Flask(__name__, template_folder="templates")
CORS(app)

LAST_UPDATE_TIME = None


@app.context_processor
def inject_server_name():
    return {"server_name": SERVER_NAME}

LOG_PATTERN = re.compile(
    r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} \+\d{2}:\d{2}) "
    r"\[DBG\] DEBUG\|(.*?)\|CityResourcesAmount:\{Food:(\d+), Wood:(\d+), Stone:(\d+), Gold:(\d+), Gems:(\d+)"
)

############################################
# Список серверов (с URL) для виджета
############################################

DEFAULT_SERVER_LINKS = [
    {"name": "208", "url": "https://hotly-large-coral.cloudpub.ru/"},
    {"name": "F99", "url": "https://tastelessly-quickened-chub.cloudpub.ru/"},
    {"name": "R9", "url": "https://creakily-big-spaniel.cloudpub.ru/"},
    {"name": "RSS", "url": "https://fiendishly-awake-stickleback.cloudpub.ru/"},
]

SERVERS: list[dict[str, str]] = []


def _normalize_server_item(item: dict) -> dict[str, str] | None:
    """Оставляем только имя/URL и отсекаем пустые строки."""

    if not isinstance(item, dict):
        return None
    name = str(item.get("name", "")).strip()
    url = str(item.get("url", "")).strip()
    if not name or not url:
        return None
    return {"name": name, "url": url.rstrip("/") + "/"}


def _get_server_links_fernet() -> Fernet:
    key_env = (os.getenv("SERVER_LINKS_KEY") or "").strip()
    if key_env:
        key = key_env.encode()
    elif os.path.exists(SERVER_LINKS_KEY_PATH):
        with open(SERVER_LINKS_KEY_PATH, "rb") as fh:
            key = fh.read().strip()
    else:
        key = Fernet.generate_key()
        with open(SERVER_LINKS_KEY_PATH, "wb") as fh:
            fh.write(key)
    return Fernet(key)


def load_server_links() -> list[dict[str, str]]:
    """Достаём список ссылок из зашифрованного файла."""

    fernet = _get_server_links_fernet()
    if not os.path.exists(SERVER_LINKS_PATH):
        return deepcopy(DEFAULT_SERVER_LINKS)

    try:
        with open(SERVER_LINKS_PATH, "rb") as fh:
            payload = fh.read()
        decrypted = fernet.decrypt(payload)
        data = json.loads(decrypted.decode("utf-8"))
        cleaned = []
        for item in data if isinstance(data, list) else []:
            normalized = _normalize_server_item(item)
            if normalized:
                cleaned.append(normalized)
        return cleaned or deepcopy(DEFAULT_SERVER_LINKS)
    except Exception as exc:
        print(f"[server_links] Ошибка чтения: {exc}")
        return deepcopy(DEFAULT_SERVER_LINKS)


def save_server_links(servers: list[dict]) -> list[dict[str, str]]:
    """Сохраняем список ссылок в зашифрованном виде."""

    cleaned: list[dict[str, str]] = []
    for item in servers if isinstance(servers, list) else []:
        normalized = _normalize_server_item(item)
        if normalized:
            cleaned.append(normalized)

    cleaned = cleaned or deepcopy(DEFAULT_SERVER_LINKS)

    fernet = _get_server_links_fernet()
    try:
        token = fernet.encrypt(json.dumps(cleaned, ensure_ascii=False).encode("utf-8"))
        with open(SERVER_LINKS_PATH, "wb") as fh:
            fh.write(token)
    except Exception as exc:
        print(f"[server_links] Ошибка записи: {exc}")

    return cleaned


def get_configured_servers() -> list[dict[str, str]]:
    """Возвращает актуальный список серверов, обновляя кеш при необходимости."""

    global SERVERS
    if not SERVERS:
        SERVERS = load_server_links()
    return SERVERS



##############################
# Helper для BD
##############################
# === DB helper: единая точка открытия соединений SQLite ===
def open_db(path, *args, **kwargs):
    """
    Открывает SQLite с безопасными/быстрыми PRAGMA.
    Принимает любые args/kwargs как у sqlite3.connect (например, check_same_thread=False),
    чтобы не падать, если где-то остались старые вызовы.
    """
    import sqlite3

    # Значения по умолчанию (можно переопределить через kwargs в вызовах)
    kwargs.setdefault("check_same_thread", False)   # для многопоточного Flask
    kwargs.setdefault("timeout", 30.0)              # дольше ждём блокировки
    # row_factory удобно сразу настроить для dict-подобного доступа
    row_factory = kwargs.pop("row_factory", sqlite3.Row)

    # ВАЖНО: здесь должен быть именно sqlite3.connect, НE open_db!
    con = sqlite3.connect(path, *args, **kwargs)
    con.row_factory = row_factory

    # Настройка PRAGMA в одном месте
    try:
        with con:  # автокоммит PRAGMA
            con.execute("PRAGMA journal_mode=WAL;")
            con.execute("PRAGMA synchronous=NORMAL;")
            con.execute("PRAGMA temp_store=MEMORY;")
            con.execute("PRAGMA foreign_keys=ON;")
    except Exception as e:
        # WAL может не примениться на read-only путях — не критично
        print(f"[DB] PRAGMA setup warning for {path}: {e}")

    return con



##############################
# Утилиты для процессов / STOP
##############################

def kill_process(name: str, soft_timeout: int = 5, hard_timeout: int = 5) -> list[int]:
    """
    Пытается завершить все процессы с указанным именем:
      1) мягкий terminate() + ожидание soft_timeout секунд
      2) жесткий kill() + ожидание hard_timeout секунд
      3) taskkill /F /T на отмёт оставшиеся
    Возвращает список PID-ов, к‑т были задействованы.
    """
    killed_pids = []
    for proc in psutil.process_iter(['pid', 'name']):
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
                    # последнее средство
                    subprocess.run(
                        ['taskkill', '/F', '/T', '/PID', str(pid)],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL
                    )
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                # процесс уже ушёл или нет прав
                pass
            killed_pids.append(pid)
    return killed_pids

def is_process_running(process_name):
    for proc in psutil.process_iter(['name']):
        try:
            if process_name.lower() in proc.info['name'].lower():
                return True
        except:
            pass
    return False

def start_process(exe_path):
    subprocess.Popen(exe_path, shell=True)

def do_stop_logic() -> list[str]:
    """
    Останавливает несколько процессов и собирает логи:
      — GnBots.exe
      — dnplayer.exe
      — Ld9BoxHeadless.exe
    Возвращает список сообщений о результатах.
    """
    logs: list[str] = []
    for name in ("GnBots.exe", "dnplayer.exe", "Ld9BoxHeadless.exe"):
        logs.append(f"⏹ Останавливаем {name}...")
        # kill_process возвращает список PID‑ов, которые были убиты
        try:
            killed = kill_process(name, soft_timeout=10, hard_timeout=5)
            if killed:
                logs.append(f"✅ Процессы {name} завершены: {', '.join(map(str, killed))}")
            else:
                logs.append(f"ℹ Процесс {name} не найден или уже завершён.")
        except Exception as e:
            logs.append(f"❗ Ошибка при попытке остановить {name}: {e}")
        
        # короткая пауза перед проверкой
        time.sleep(2)
        if is_process_running(name):
            logs.append(f"❗ {name} всё ещё работает (возможно нет прав)?")

    logs.append("✔ Stop завершён.")
    return logs

def do_reboot_logic():
    logs= do_stop_logic()
    logs.append("Запускаем GnBots.exe -start")
    try:
        start_process(r"C:\Users\administrator\Desktop\GnBots.lnk")
    except Exception as e:
        logs.append("Ошибка запуска GnBots.exe: "+str(e))
    logs.append("Reboot завершён.")
    return logs

############################
# Удалённый STOP/REBOOT (SSH / WMI)
############################

def stop_remote_ssh(server):
    logs=[]
    try:
        logs.append(f"STOP {server['name']} via SSH...")
        ssh= paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(server["ip"], username=server["user"], password=server["password"], timeout=5)
        for proc in ["GnBots.exe","dnplayer.exe"]:
            cmd= f"taskkill /F /IM {proc}"
            stdin, stdout, stderr= ssh.exec_command(cmd)
            out= stdout.read().decode("cp866","ignore")
            err= stderr.read().decode("cp866","ignore")
            logs.append(f"{cmd} => {out.strip()} {err.strip()}")
        ssh.close()
    except Exception as e:
        logs.append(f"Ошибка stop_remote_ssh: {e}")
    return logs

def start_remote_ssh(server):
    logs=[]
    try:
        logs.append(f"START {server['name']} via SSH => {server['start_path']}")
        ssh= paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(server["ip"], username=server["user"], password=server["password"], timeout=5)
        cmd= f'start "" "{server["start_path"]}"'
        ssh.exec_command(cmd)
        logs.append("GnBots запущен.")
        ssh.close()
    except Exception as e:
        logs.append(f"Ошибка start_remote_ssh: {e}")
    return logs

def reboot_remote_ssh(server):
    logs= stop_remote_ssh(server)
    time.sleep(3)
    logs += start_remote_ssh(server)
    return logs

# Для 208 — WMI STOP/REBOOT
import pythoncom
import wmi

def stop_remote_wmi(server):
    logs=[]
    logs.append(f"STOP {server['name']} via WMI...")

    pythoncom.CoInitialize()
    try:
        conn = wmi.WMI(server["ip"], user=server["user"], password=server["password"])
        # Останавливаем GnBots / dnplayer
        for p in conn.Win32_Process(Name='GnBots.exe'):
            logs.append(f"Killing GnBots.exe pid={p.ProcessId}")
            p.Terminate()
        for p in conn.Win32_Process(Name='dnplayer.exe'):
            logs.append(f"Killing dnplayer.exe pid={p.ProcessId}")
            p.Terminate()
    except Exception as e:
        logs.append("Ошибка stop_remote_wmi: "+ str(e))
    finally:
        pythoncom.CoUninitialize()
    return logs

def start_remote_wmi(server):
    logs=[]
    logs.append(f"START {server['name']} via WMI => {server['start_path']}")

    pythoncom.CoInitialize()
    try:
        conn= wmi.WMI(server["ip"], user=server["user"], password=server["password"])
        cmd= server["start_path"]
        res= conn.Win32_Process.Create(CommandLine=cmd)
        logs.append(f"Create => {res}")
    except Exception as e:
        logs.append("Ошибка start_remote_wmi: "+ str(e))
    finally:
        pythoncom.CoUninitialize()
    return logs

def reboot_remote_wmi(server):
    logs= stop_remote_wmi(server)
    time.sleep(3)
    logs += start_remote_wmi(server)
    return logs

def stop_remote(server):
    """Определяем, ssh или wmi."""
    if server["name"]=="208":
        return stop_remote_wmi(server)
    else:
        return stop_remote_ssh(server)

def start_remote(server):
    if server["name"]=="208":
        return start_remote_wmi(server)
    else:
        return start_remote_ssh(server)

def reboot_remote(server):
    if server["name"]=="208":
        return reboot_remote_wmi(server)
    else:
        return reboot_remote_ssh(server)

##############################
# ИНИЦИАЛИЗАЦИЯ БАЗ
##############################

def init_resources_db():
    conn = open_db(RESOURCES_DB)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS resources (
            id TEXT PRIMARY KEY,
            nickname TEXT,
            food INTEGER,
            wood INTEGER,
            stone INTEGER,
            gold INTEGER,
            gems INTEGER,
            last_updated TEXT
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS daily_baseline (
            id TEXT,
            nickname TEXT,
            food INTEGER,
            wood INTEGER,
            stone INTEGER,
            gold INTEGER,
            gems INTEGER,
            baseline_date TEXT,
            PRIMARY KEY(id, baseline_date)
        )
    """)
    conn.commit()
    conn.close()

def init_expenses():
    conn = open_db(RESOURCES_DB)
    conn.execute("""CREATE TABLE IF NOT EXISTS expenses(
         id INTEGER PRIMARY KEY AUTOINCREMENT,
         amount INTEGER NOT NULL,
         dt TEXT NOT NULL
    )""")
    conn.commit(); conn.close()


# === ПОСЛЕ СТРОКИ: def init_logs_db(): ===
def init_logs_db():
    """
    Инициализация БД логов:
      - files_offset: смещения прочитанного для больших логов
      - cached_logs: кэшированные строки (с индексами)
      - resource_snapshots: снапшоты ресурсов с индексами
      - дедупликация по (acc_id, dt) и создание UNIQUE-индекса
    """
    conn = open_db(LOGS_DB)
    try:
        c = conn.cursor()

        # 1) Таблица смещений
        c.execute("""
            CREATE TABLE IF NOT EXISTS files_offset (
                filename TEXT PRIMARY KEY,
                last_pos INTEGER NOT NULL
            )
        """)

        # 2) Кэш логов по аккаунтам
        c.execute("""
            CREATE TABLE IF NOT EXISTS cached_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                acc_id TEXT,
                nickname TEXT,
                dt TEXT,         -- 'YYYY-MM-DD HH:MM:SS.mmm +HH:MM'
                raw_line TEXT
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_cached_logs_acc ON cached_logs(acc_id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_cached_logs_id ON cached_logs(id DESC)")

        # 3) Снапшоты ресурсов
        c.execute("""
            CREATE TABLE IF NOT EXISTS resource_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                acc_id TEXT NOT NULL,
                dt TEXT NOT NULL,   -- 'YYYY-MM-DD HH:MM:SS.mmm +HH:MM'
                food INTEGER,
                wood INTEGER,
                stone INTEGER,
                gold INTEGER
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_rs_acc_dt ON resource_snapshots(acc_id, dt)")

        # 4) Дедупликация по (acc_id, dt) перед созданием уникального индекса
        #    Оставляем запись с максимальным id
        try:
            c.execute("""
                DELETE FROM resource_snapshots
                WHERE id NOT IN (
                    SELECT MAX(id) FROM resource_snapshots
                    GROUP BY acc_id, dt
                )
            """)
        except Exception as e:
            print("[init_logs_db] dedup resource_snapshots warn:", e)

        # 5) Уникальный индекс по (acc_id, dt), чтобы не плодить дублей при перечтении логов
        try:
            c.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_rs_acc_dt ON resource_snapshots(acc_id, dt)")
        except Exception as e:
            # Если здесь ошибка, значит в таблице всё ещё остались дубликаты.
            # Можно повторно попробовать более агрессивную чистку, но, как правило, блока выше хватает.
            print("[init_logs_db] create unique index warn:", e)

        conn.commit()
        print("[init_logs_db] OK: schema ensured, indexes present")
    except Exception as e:
        print("[init_logs_db] ERROR:", e)
        # не забываем, чтобы не оставить транзакцию открытой
        try: conn.rollback()
        except: pass
        raise
    finally:
        try: conn.close()
        except: pass


# ───── после init_logs_db() добавьте ─────
def init_accounts_db():
    conn = open_db(RESOURCES_DB)
    c    = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS account_meta(
          id          TEXT PRIMARY KEY,   -- ID = тот же, что в profile.json
          email       TEXT,
          passwd      TEXT,
          igg         TEXT,
          pay_until   TEXT,   -- YYYY-MM-DD
          tariff_rub  INTEGER DEFAULT 0,
          server      TEXT,
          tg_tag      TEXT
        )
    """)

    # Таблица могла быть создана в старой версии без новых колонок.
    existing_cols = {
        row[1] for row in c.execute("PRAGMA table_info(account_meta)").fetchall()
    }
    if "server" not in existing_cols:
        c.execute("ALTER TABLE account_meta ADD COLUMN server TEXT")
    if "tg_tag" not in existing_cols:
        c.execute("ALTER TABLE account_meta ADD COLUMN tg_tag TEXT")
    conn.commit(); conn.close()


##############################
# Работа аккаунтами -данные\пароли
##############################
# ───────────────────  SYNC account_meta  ───────────────────
def sync_account_meta():
    """Держит account_meta = текущее множество активных Id."""
    profiles, ok = load_profiles(return_status=True)
    if not ok and not profiles:
        print("PROFILE read failed — skip sync_account_meta")
        return

    active_ids = {str(p["Id"]) for p in profiles if p.get("Id") is not None}

    conn = open_db(RESOURCES_DB)
    c    = conn.cursor()

    # ── удалить лишние ──
    if active_ids:
        marks = ",".join("?"*len(active_ids))
        c.execute(f"DELETE FROM account_meta WHERE id NOT IN ({marks})",
                  tuple(active_ids))
    else:                         # если активных вообще нет
        c.execute("DELETE FROM account_meta")

    # ── вставить недостающие ──
    if active_ids:
        placeholders = ",".join("(?, '', '', '', '', NULL, '', '')"
                                for _ in active_ids)
        c.execute(f"""
            INSERT OR IGNORE INTO account_meta
            (id,email,passwd,igg,pay_until,tariff_rub,server,tg_tag)
            VALUES {placeholders}
        """, tuple(active_ids))

    conn.commit(); conn.close()



##############################
# Работа с профилями
##############################

PROFILE_CACHE: list[dict[str, t.Any]] | None = None


def load_profiles(*, return_status: bool = False):
    """
    Возвращает список активных аккаунтов из PROFILE_PATH.

    :param return_status: True → вернуть (profiles, ok),
                          False → вернуть только profiles.
    """

    def _result(profiles: list[dict[str, t.Any]], ok: bool):
        return (profiles, ok) if return_status else profiles

    global PROFILE_CACHE

    if not os.path.exists(PROFILE_PATH):
        print(f"PROFILE not found: {PROFILE_PATH}")
        return _result(PROFILE_CACHE or [], False)
    try:
        with open(PROFILE_PATH, "r", encoding="utf-8") as f:
            raw = f.read().strip()
            if not raw:
                print(f"PROFILE is empty: {PROFILE_PATH}")
                return _result(PROFILE_CACHE or [], False)
            data = json.loads(raw)
    except json.JSONDecodeError as exc:
        print(f"PROFILE has invalid JSON: {exc}")
        return _result(PROFILE_CACHE or [], False)

    if not isinstance(data, list):
        print(f"PROFILE data is not a list: type={type(data)}")
        return _result(PROFILE_CACHE or [], False)

    active_profiles = [acc for acc in data if acc.get("Active")]
    PROFILE_CACHE = active_profiles
    return _result(active_profiles, True)

# вверху, рядом с load_profiles()
def load_active_names():
    """возвращает [(Id, Name)] активных аккаунтов"""
    profiles, _ = load_profiles(return_status=True)
    return [(a["Id"], a.get("Name","")) for a in profiles]



def ensure_active_in_db(active_accounts):
    conn= open_db(RESOURCES_DB)
    c=conn.cursor()
    for acc in active_accounts:
        c.execute("""
            INSERT OR IGNORE INTO resources
            (id, nickname, food, wood, stone, gold, gems, last_updated)
            VALUES(?, ?, 0,0,0,0,0, '1970-01-01T00:00:00')
        """,(acc["Id"], acc["Name"]))
    conn.commit()
    conn.close()

##############################
# Парсинг логов
##############################

def parse_logs():
    global LAST_UPDATE_TIME
    acts= load_profiles()
    if not acts:
        print("Нет активных аккаунтов => parse_logs skip")
        return
    ensure_active_in_db(acts)
    acc_map= {a["Id"]: a["Name"] for a in acts}

    do_resources_update(acc_map)
    LAST_UPDATE_TIME= datetime.now(timezone.utc)
    print("parse_logs done. LAST_UPDATE_TIME =", LAST_UPDATE_TIME.isoformat())

def do_resources_update(acc_map):
    conn_res = open_db(RESOURCES_DB)
    c_res= conn_res.cursor()

    conn_log= open_db(LOGS_DB)
    c_log= conn_log.cursor()

    offsets={}
    off_rows= c_log.execute("SELECT filename,last_pos FROM files_offset").fetchall()
    for (fn,ps) in off_rows:
        offsets[fn]= ps

    dt_now_str= datetime.now().strftime("%Y%m%d")

    if not os.path.exists(LOGS_DIR):
        print("LOGS_DIR not found:", LOGS_DIR)
        conn_res.close()
        conn_log.close()
        return

    for fname in os.listdir(LOGS_DIR):
        # ищем botYYYYmmdd*.txt
        if fname.startswith("bot"+ dt_now_str) and fname.endswith(".txt"):
            fullp= os.path.join(LOGS_DIR, fname)
            prev_pos= offsets.get(fname, 0)
            try:
                with open(fullp,"rb") as f:
                    f.seek(prev_pos,0)
                    while True:
                        line_bytes= f.readline()
                        if not line_bytes:
                            break
                        new_pos= f.tell()
                        line_str= line_bytes.decode("utf-8", "replace").rstrip("\r\n")

                        mm= LOG_PATTERN.search(line_str)
                        if mm:
                            ts_str, log_id, fd,wd,st,gd,gm= mm.groups()
                            if log_id in acc_map:
                                dt= datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S.%f %z")
                                iso_ts= dt.isoformat()
                                c_res.execute("""
                                  INSERT INTO resources(id,nickname,food,wood,stone,gold,gems,last_updated)
                                  VALUES(?,?,?,?,?,?,?,?)
                                  ON CONFLICT(id) DO UPDATE SET
                                    nickname=excluded.nickname,
                                    food=excluded.food,
                                    wood=excluded.wood,
                                    stone=excluded.stone,
                                    gold=excluded.gold,
                                    gems=excluded.gems,
                                    last_updated=excluded.last_updated
                                  WHERE excluded.last_updated>resources.last_updated
                                """,(log_id, acc_map[log_id],
                                     int(fd), int(wd), int(st), int(gd), int(gm), iso_ts))

                                local_date= dt.astimezone().strftime("%Y-%m-%d")
                                if local_date == datetime.now().strftime("%Y-%m-%d"):
                                    row_ex= c_res.execute("""
                                      SELECT 1 FROM daily_baseline
                                      WHERE id=? AND baseline_date=?
                                    """,(log_id, local_date)).fetchone()
                                    if not row_ex:
                                        c_res.execute("""
                                          INSERT INTO daily_baseline
                                          (id,nickname,food,wood,stone,gold,gems,baseline_date)
                                          VALUES(?,?,?,?,?,?,?,?)
                                        """,(log_id, acc_map[log_id],
                                             int(fd), int(wd), int(st), int(gd), int(gm),
                                             local_date))

                        # кэшируем                    
                        # кэшируем + снапшоты ресурсов
                        # кэшируем + снапшоты ресурсов (строгое сопоставление |ACC_ID|)
                        for acid, nick in acc_map.items():
                            # важно: матчим по "|<ID>|", чтобы исключить пересечения префиксов (Alex8914_1 vs Alex8914_14)
                            if f"|{acid}|" not in line_str:
                                continue

                            m = _DT_RE.match(line_str)  # ^YYYY-MM-DD ... +ZZ:ZZ
                            if not m:
                                continue
                            dt_part = m.group(1)

                            c_log.execute("""
                            INSERT INTO cached_logs(acc_id, nickname, dt, raw_line)
                            VALUES(?,?,?,?)
                            """, (acid, nick, dt_part, line_str))

                            if "CityResourcesAmount:" in line_str:
                                try:
                                    import re
                                    def _extract(name: str, s: str) -> int | None:
                                        m2 = re.search(rf"{name}\s*:\s*(\d+)", s)
                                        return int(m2.group(1)) if m2 else None

                                    food  = _extract("Food",  line_str)
                                    wood  = _extract("Wood",  line_str)
                                    stone = _extract("Stone", line_str)
                                    gold  = _extract("Gold",  line_str)

                                    # вставляем снапшот с защитой от дублей по (acc_id, dt)
                                    c_log.execute("""
                                    INSERT OR REPLACE INTO resource_snapshots(acc_id, dt, food, wood, stone, gold)
                                    VALUES(?,?,?,?,?,?)
                                    """, (acid, dt_part, food, wood, stone, gold))
                                except Exception as e:
                                    print("[parse CityResourcesAmount] skip:", e)




                        offsets[fname] = new_pos
            except Exception as e:
                print("Error reading file:", fullp, e)

    for ff in offsets:
        c_log.execute("""
          INSERT OR REPLACE INTO files_offset(filename,last_pos)
          VALUES(?,?)
        """, (ff, offsets[ff]))

    conn_res.commit()
    conn_log.commit()
    conn_res.close()
    conn_log.close()

##############################
# Helpers
##############################

# ─── robust scheduled-task checker ───────────────────────────────────────────
import subprocess

def _task_enabled(task_name: str) -> bool | None:
    """
    Возвращает:
        True   – задача существует и включена
        False  – задача существует и отключена
        None   – задача не найдена / schtasks вернул ошибку
    """
    try:
        raw = subprocess.check_output(
            ["schtasks", "/Query", "/TN", task_name, "/FO", "LIST", "/V"],
            stderr=subprocess.STDOUT, timeout=5
        )
    except subprocess.CalledProcessError:
        # задача не существует
        return None

    # пытаемся декодировать вывод
    for enc in ("cp866", "cp437", "utf-8"):
        try:
            text = raw.decode(enc, errors="ignore").lower()
            break
        except UnicodeDecodeError:
            continue
    else:
        return None

    state_val = None
    for line in text.splitlines():
        if ":" not in line:
            continue
        left, right = [p.strip() for p in line.split(":", 1)]
        left = left.lower(); right = right.lower()

        # английские локали
        if left.startswith("scheduled task state") or left.startswith("enabled"):
            state_val = right
            break
        # русские локали
        if ("состояние задачи" in left) or ("запланированной задачи" in left):
            state_val = right
            break

    # общий fallback — ищем ключевые слова в сыром тексте
    if state_val is None:
        if any(w in text for w in ("enabled", "включено", "да", "yes")):
            state_val = "enabled"
        elif any(w in text for w in ("disabled", "отключено", "нет", "no")):
            state_val = "disabled"

    # нормализуем к булю / None
    if state_val in ("enabled", "yes", "true", "да", "1"):
        return True
    if state_val in ("disabled", "no", "false", "нет", "0"):
        return False
    return None
# ─────────────────────────────────────────────────────────────────────────────


def shorten_number(num):
    if num == 0:
        return "0"
    sign = ""
    if num < 0:
        sign = "-"
        num = abs(num)
    if num < 1000:
        return f"{sign}{num}"
    elif num < 1_000_000:
        return f"{sign}{num // 1000}k"
    elif num < 1_000_000_000:
        return f"{sign}{num // 1_000_000}m"
    else:
        b = num / 1_000_000_000
        return f"{sign}{b:.1f}b"

def transformLogLine(dt_part, line_str):
    try:
        dt= datetime.strptime(dt_part,"%Y-%m-%d %H:%M:%S.%f")
        short= dt.strftime("%d-%m %H:%M")
    except:
        short= dt_part
    rest= line_str[28:].strip()
    return f"{short} {rest}"


##############################
# BACKUP
##############################

# ────────────────────────── настройки ──────────────────────────
SERVER = SERVER_NAME                                # имя сервера
BACKUP_CONFIG_SRC      = r"C:\LDPlayer\LDPlayer9\vms\config"
BACKUP_CONFIG_DST_ROOT = r"C:\LD_backup\configs"
BACKUP_ACCS_DST_ROOT   = r"C:\LD_backup\accs_data"
BACKUP_PROFILES_DST_ROOT = r"C:\LD_backup\bot_acc_configs"
FIX_BACKUP_ROOT = r"C:\LD_backup\fix_backup"   # ← NEW


# ────────────────────────── вспомогалки ───────────────────────
def _ensure_dir(path: str):
    """Создаёт каталог *path* вместе со всеми промежуточными."""
    os.makedirs(path, exist_ok=True)


def _try_copy_file(src: str, dst: str) -> bool:
    """Пытается скопировать файл и возвращает успех/неуспех."""
    try:
        shutil.copy2(src, dst)
        return True
    except Exception as e:
        print(f"[BACKUP] error copying {src} → {dst}: {e}", flush=True)
        return False


# ────────────────────── новый хелпер ────────────────────────
def _retry_failed_configs(failed_files: list[str], dst_folder: str) -> None:
    """
    Повторно пытается скопировать неудачные файлы в папку dst_folder:
      - до 3 попыток с интервалом 10 минут
      - только для текущей даты
    """
    def _worker():
        today = datetime.now().strftime("%d__%m__%Y")
        attempts = 0
        max_attempts = 3
        interval = 10 * 60  # 10 минут
        while failed_files and attempts < max_attempts and datetime.now().strftime("%d__%m__%Y") == today:
            time.sleep(interval)
            for fname in failed_files.copy():
                src = os.path.join(BACKUP_CONFIG_SRC, fname)
                if os.path.isfile(src):
                    try:
                        shutil.copy2(src, dst_folder)
                        failed_files.remove(fname)
                        print(f"[BACKUP RETRY] Success: {fname}")
                    except Exception as e:
                        print(f"[BACKUP RETRY] Error copying {fname}: {e}", flush=True)
            attempts += 1
        if failed_files:
            print(f"[BACKUP RETRY] After {max_attempts} attempts failed: {failed_files}")
    threading.Thread(target=_worker, daemon=True).start()

# ────────────────────────── BACKUP CONFIGS ────────────────────
def backup_configs() -> None:
    r"""
    Копирует все файлы из …\\vms\\config в
      C:\\LD_backup\\configs\\<ДД__ММ__ГГГГ>\\
    При ошибках копирования — планирует повторные попытки.
    """
    dst = os.path.join(BACKUP_CONFIG_DST_ROOT,
                       datetime.now().strftime("%d__%m__%Y"))
    _ensure_dir(dst)
    failed = []
    for fname in os.listdir(BACKUP_CONFIG_SRC):
        src = os.path.join(BACKUP_CONFIG_SRC, fname)
        if os.path.isfile(src):
            try:
                shutil.copy2(src, dst)
            except Exception as e:
                failed.append(fname)
                print(f"[BACKUP] error copying {fname}: {e}", flush=True)
    if failed:
        print(f"[BACKUP] Scheduling retries for: {failed}", flush=True)
        _retry_failed_configs(failed, dst)
    else:
        print(f"[BACKUP] configs  →  {dst}", flush=True)
# ───────────────────────────────────────────────────────────────

# ───── вставьте рядом с backup_configs() ─────
def emergency_replace_configs(backup_dir_override: str | None = None) -> list[str]:
    """
    Полностью заменяет все *.config в DST_VMS\\config
    на файлы из выбранного источника.
    Перед заменой делает резервную копию рабочей папки
    в  C:\\LD_backup\\fix_backup\\ДД__ММ__ГГГГ[_HH-MM-SS]\\
    """
    logs: list[str] = []
    today_stamp = datetime.now().strftime("%d__%m__%Y_%H-%M-%S")

   # ── 0) пути ────────────────────────────────────────────────
    if backup_dir_override:             # выбранная папка из «Пути»
        src_dir = os.path.join(BACKUP_CONFIG_DST_ROOT, backup_dir_override)
    else:                               # дефолт → как в обычном FIX
        src_dir = os.path.join(SRC_VMS, "config")

    dst_dir    = os.path.join(DST_VMS, "config")
    backup_dst = os.path.join(FIX_BACKUP_ROOT, today_stamp)

    # защита от «копируем сами в себя»
    if os.path.normcase(src_dir) == os.path.normcase(dst_dir):
        logs.append("ℹ Источник и приёмник совпадают — замена не требуется.")
        return logs

    # ── 1) валидация ───────────────────────────────────────────
    if not os.path.isdir(src_dir):
        logs.append(f"❌ Источник не найден: {src_dir}")
        return logs
    if not os.listdir(src_dir):
        logs.append(f"❌ Источник пустой: {src_dir}")
        return logs

    _ensure_dir(FIX_BACKUP_ROOT)

    # ── 2) бэкап текущих config’ов ─────────────────────────────
    try:
        if os.path.isdir(dst_dir):
            shutil.copytree(dst_dir, backup_dst, dirs_exist_ok=False)
            logs.append(f"🗄 Backup ⇒ {backup_dst}")
        else:
            logs.append("ℹ Рабочая папка config отсутствует — бэкап пропущен.")
    except Exception as e:
        logs.append(f"❗ Ошибка бэкапа: {e}")
        return logs          # стопаем, чтобы не потерять оригиналы

    # ── 3) копируем новые config’ы ─────────────────────────────
    copied = 0
    try:
        _ensure_dir(dst_dir)
        for fname in os.listdir(src_dir):
            if not fname.lower().endswith(".config"):
                continue
            src_f = os.path.join(src_dir, fname)
            dst_f = os.path.join(dst_dir, fname)
            try:
                if os.path.exists(dst_f):
                    os.chmod(dst_f, stat.S_IWRITE)
                shutil.copy2(src_f, dst_f)
                copied += 1
            except Exception as e:
                logs.append(f"⚠ {fname}: {e}")
        logs.append(f"✅ Скопировано {copied} файлов из {src_dir}")
    except Exception as e:
        logs.append(f"❗ Ошибка копирования: {e}")

    return logs


# ───────────────────── сбор данных аккаунтов ──────────────────
def _collect_accounts_rows():
    """
    Возвращает [(Name, Email, Pass, IGG, Pay-until, Tariff)] для активных.
    """
    active = {p["Id"]: p.get("Name", "") for p in load_profiles()}
    conn   = open_db(RESOURCES_DB)
    meta   = {r[0]: r[1:] for r in
              conn.execute("SELECT id,email,passwd,igg,pay_until,tariff_rub "
                           "FROM account_meta")}
    conn.close()

    rows = []
    for acc_id, name in active.items():
        email, passwd, igg, pu, tariff = meta.get(acc_id, ("", "", "", "", 0))
        if pu:
            try:
                pu = datetime.strptime(pu, "%Y-%m-%d").strftime("%d.%m.%y")
            except ValueError:
                pass
        rows.append((name, email, passwd, igg, pu, tariff))
    return rows

# ───────────────────── BACKUP ACCOUNTS.CSV ────────────────────
def backup_accounts_csv() -> None:
    r"""
    Формирует CSV «Имя;E-mail;Пароль;IGG;Оплата;Тариф» и сохраняет в
      C:\\LD_backup\\accs_data\\<SERVER>_<ДД__ММ__ГГГГ>\\accounts.csv
    """
    subdir   = f"{SERVER}_{datetime.now().strftime('%d__%m__%Y')}"
    dst_dir  = os.path.join(BACKUP_ACCS_DST_ROOT, subdir)
    _ensure_dir(dst_dir)
    csv_path = os.path.join(dst_dir, "accounts.csv")

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=';')
        w.writerow(["Имя", "E-mail", "Пароль", "IGG", "Оплата", "Тариф"])
        for row in _collect_accounts_rows():
            w.writerow(row)
    print(f"[BACKUP] accounts.csv  →  {csv_path}")


def backup_profiles_json() -> None:
    r"""
    Делает копию файла профилей из PROFILE_PATH в
      C:\\LD_backup\\bot_acc_configs\\<ДД__ММ__ГГГГ>\\profiles.json
    Создаёт папку, если её нет, и тихо пропускает, если исходный файл не найден.
    """
    if not PROFILE_PATH:
        print("[BACKUP] PROFILE_PATH не задан — пропускаем backup_profiles_json()", flush=True)
        return
    if not os.path.isfile(PROFILE_PATH):
        print(f"[BACKUP] profiles.json не найден: {PROFILE_PATH}", flush=True)
        return

    today_stamp = datetime.now().strftime("%d__%m__%Y")
    dst_dir = os.path.join(BACKUP_PROFILES_DST_ROOT, today_stamp)
    _ensure_dir(dst_dir)

    dst = os.path.join(dst_dir, os.path.basename(PROFILE_PATH) or "profiles.json")
    if _try_copy_file(PROFILE_PATH, dst):
        print(f"[BACKUP] profiles.json  →  {dst}", flush=True)

# ─────────────────── проверка «уже есть ли за сегодня» ───────────────────
def ensure_today_backups() -> None:
    """При старте приложения проверяет, есть ли бэкап за сегодня; если нет — делает."""
    today_stamp = datetime.now().strftime("%d__%m__%Y")
    cfg_dir  = os.path.join(BACKUP_CONFIG_DST_ROOT,  today_stamp)
    acc_dir  = os.path.join(BACKUP_ACCS_DST_ROOT, f"{SERVER}_{today_stamp}")
    profiles_path = os.path.join(BACKUP_PROFILES_DST_ROOT,
                                 today_stamp,
                                 os.path.basename(PROFILE_PATH) or "profiles.json")
    cfg_ok   = os.path.isdir(cfg_dir) and os.listdir(cfg_dir)
    acc_ok   = os.path.isfile(os.path.join(acc_dir, "accounts.csv"))
    profiles_ok = os.path.isfile(profiles_path)
    if not cfg_ok:
        backup_configs()
    if not acc_ok:
        backup_accounts_csv()
    if not profiles_ok:
        backup_profiles_json()

# ─────────────────── ежедневное расписание 00:00 ─────────────────────────
def _schedule_daily_backups() -> None:
    """Фоновый поток, который каждый день ровно в 00:00 делает два бэкапа."""
    def _worker():
        while True:
            now = datetime.now()
            next_run = datetime.combine(now.date() + timedelta(days=1),
                                        datetime.min.time())
            time.sleep(max(0, (next_run - now).total_seconds()))
            try:
                backup_configs()
                backup_accounts_csv()
                backup_profiles_json()
                run_templates_schema_audit()
            except Exception as e:
                print("[BACKUP] error:", e, flush=True)
    threading.Thread(target=_worker, daemon=True).start()

# === BACKUP END ===

def _schedule_pay_notifications() -> None:
    """
    Фоновый поток, который каждый день в 09:00 и 18:00 отправляет
    сводку по оплатам в Telegram (если есть что сообщить).
    """
    def _worker():
        while True:
            now = datetime.now()

            # вычисляем ближайший «круглый» запуск
            today9  = now.replace(hour=9,  minute=0, second=0, microsecond=0)
            today18 = now.replace(hour=18, minute=0, second=0, microsecond=0)
            candidates = [t for t in (today9, today18) if t > now]

            if candidates:                       # сегодня ещё будет рассылка
                next_run = min(candidates)
            else:                                # следующий день, 09:00
                next_run = (today9 + timedelta(days=1))

            time.sleep(max(0, (next_run - now).total_seconds()))

            try:
                msg = _compose_pay_alert_message()
                if msg:                    
                    # Сообщение для Telegram. Параметр add_fix_link игнорируется внутри _send_telegram.
                    _send_telegram(msg, add_fix_link=False)

            except Exception as e:
                print(f"[TG-scheduler] error: {e}", flush=True)

                # спим минуту, чтобы не уйти в быстрый бесконечный цикл
                time.sleep(60)

    threading.Thread(target=_worker, daemon=True).start()

# ────────────────── INACTIVE-CHECK scheduler ──────────────────
def _schedule_inactive_checker(interval_min: int = 60):
    """
    Проверка «без прироста >15 ч» каждые *interval_min* минут
    (по умолчанию – раз в час).  Работает в отдельном демоне-треде.
    """
    import threading, time
    def _worker():
        while True:
            try:
                inactive_monitor.check_inactive_accounts()
            except Exception as e:
                print("[inactive-checker]", e, flush=True)
            time.sleep(interval_min * 60)
    threading.Thread(target=_worker, daemon=True).start()


##############################
# FIX
##############################

def remove_readonly(folder_path):
    if os.path.exists(folder_path):
        for root, dirs, files in os.walk(folder_path):
            for d in dirs:
                dir_path = os.path.join(root, d)
                try:
                    file_stat = os.stat(dir_path)
                    os.chmod(dir_path, file_stat.st_mode | stat.S_IWRITE)
                except Exception as e:
                    print(f"Ошибка при изменении атрибутов {dir_path}: {e}")
            for f in files:
                file_path = os.path.join(root, f)
                try:
                    file_stat = os.stat(file_path)
                    os.chmod(file_path, file_stat.st_mode | stat.S_IWRITE)
                except Exception as e:
                    print(f"Ошибка при изменении атрибутов {file_path}: {e}")
    else:
        print(f"Папка не найдена: {folder_path}")

def do_fix_logic(acc_id: str,
                 *, only_config: bool = False,
                 cfg_src_override: str | None = None) -> list[str]:
    """
    Выполняет «FIX» для указанного аккаунта.

    :param acc_id:   ID аккаунта (GUID).
    :param only_config: True → копируется только файл leidianXX.config;
                        False → полный Fix (папка эмулятора + config).
    :return: список строк‑логов.
    """
    logs: list[str] = []
    logs.append(f"─── FIX start (only_config={only_config}) — acc_id={acc_id}")

    # ───────────────────── поиск InstanceId в профиле ─────────────────────
    profiles = load_profiles()
    inst_id, nickname = None, "???"
    for p in profiles:
        if p.get("Id") == acc_id:
            inst_id  = p.get("InstanceId")
            nickname = p.get("Name", "???")
            break

    if inst_id is None:
        logs.append("❗ Аккаунт не найден в JSON‑профиле.")
        return logs

    logs.append(f"Аккаунт {nickname}, InstanceId={inst_id}")

    # ───────────────────── копирование CONFIG ─────────────────────────────
    src_cfg_dir = cfg_src_override if cfg_src_override \
                else os.path.join(SRC_VMS, "config")
    src_cfg     = os.path.join(src_cfg_dir, f"leidian{inst_id}.config")
    dst_cfg = os.path.join(DST_VMS, "config", f"leidian{inst_id}.config")
    logs.append(f"Копируем config\n  {src_cfg}\n  → {dst_cfg}")

    # NEW — проверка совпадения ника при использовании бэкапа
    if cfg_src_override:
        try:
            with open(src_cfg, "rb") as fh:
                buf = fh.read()
            if nickname.encode("utf-8", "ignore") not in buf:
                logs.append(f"❌ Конфликт: в бэкапе нет ника «{nickname}»")
                return logs
        except Exception as e:
            logs.append(f"⚠ Не удалось проверить ник в конфиге: {e}")
            return logs


    try:
        if os.path.exists(dst_cfg):
            os.chmod(dst_cfg, stat.S_IWRITE)        # снимаем Read‑only
            os.remove(dst_cfg)
        shutil.copy2(src_cfg, dst_cfg)
        logs.append("✅ Config скопирован.")
    except Exception as e:
        logs.append(f"❗ Ошибка копирования config: {e}")

    # ───────────────────── если нужен только config – выходим ─────────────
    if only_config:
        logs.append("FIX (config‑only) завершён.")
        return logs

    # ───────────────────── СТОП процессов GnBots/dnplayer ────────────────
    for proc_name in ("GnBots.exe", "dnplayer.exe", "Ld9BoxHeadless.exe"):
        logs.append(f"⏹ Закрываю {proc_name}…")
        kill_process(proc_name)
        time.sleep(2)
        if is_process_running(proc_name):
            logs.append(f"⚠ {proc_name} всё ещё запущен.")

    # ───────────────────── копирование папки leidianXX ────────────────────
    src_dir = os.path.join(SRC_VMS, f"leidian{inst_id}")
    dst_dir = os.path.join(DST_VMS, f"leidian{inst_id}")
    logs.append(f"Копируем папку\n  {src_dir}\n  → {dst_dir}")

    try:
        real_dst = os.path.realpath(dst_dir) if os.path.islink(dst_dir) else dst_dir
        if os.path.exists(real_dst):
            shutil.rmtree(real_dst)
        shutil.copytree(src_dir, real_dst)
        logs.append("✅ Папка скопирована.")
    except Exception as e:
        logs.append(f"❗ Ошибка копирования папки: {e}")

    # ───────────────────── запуск GnBots ──────────────────────────────────
    logs.append("▶ Запускаю GnBots.exe -start")
    try:
        start_process(GNBOTS_SHORTCUT)       # путь берётся из config.json
    except Exception as e:
        logs.append(f"❗ Ошибка запуска GnBots: {e}")

    logs.append("FIX (full) завершён.")
    return logs




###########################################
# SERVER STATUS (детальная)
###########################################

LOCAL_STATUS_LOCK = threading.Lock()
LOCAL_STATUS_CACHE: dict[str, t.Any] = {}


def check_local_processes() -> tuple[bool, bool, int]:
    """Проверяем локальные процессы GnBots и dnplayer без SSH/WMI."""

    gn, dn, dn_count = False, False, 0
    for proc in psutil.process_iter(['name']):
        name = (proc.info.get('name') or '').lower()
        if 'gnbots.exe' in name:
            gn = True
        if 'dnplayer.exe' in name:
            dn = True
            dn_count += 1
    return gn, dn, dn_count


def collect_local_status() -> dict[str, t.Any]:
    """Собираем срез по текущей машине."""

    gn, dn, dn_count = check_local_processes()
    return {
        'server': SERVER_NAME,
        'pingOk': True,
        'gnOk': gn,
        'dnOk': dn,
        'dnCount': dn_count,
        'cpu': psutil.cpu_percent(interval=0.5),
        'ram': psutil.virtual_memory().percent,
        'checked_at': datetime.utcnow().isoformat() + 'Z',
    }


def get_local_status(force: bool = False) -> dict[str, t.Any]:
    """Достаём кешированный статус (обновляется раз в минуту)."""

    with LOCAL_STATUS_LOCK:
        ts = LOCAL_STATUS_CACHE.get('ts')
        needs_refresh = force or not LOCAL_STATUS_CACHE or not ts or (time.time() - ts > 55)
        if needs_refresh:
            LOCAL_STATUS_CACHE['data'] = collect_local_status()
            LOCAL_STATUS_CACHE['ts'] = time.time()
        return deepcopy(LOCAL_STATUS_CACHE.get('data', {}))


def _server_status_updater(interval: int = 60):
    while True:
        try:
            get_local_status(force=True)
        except Exception as exc:
            print(f"[server_status] Ошибка обновления: {exc}")
        time.sleep(interval)


def start_server_status_thread():
    th = threading.Thread(target=_server_status_updater, args=(60,), daemon=True)
    th.start()


def _build_self_status_url(base_url: str) -> str:
    base = (base_url or '').rstrip('/')
    return base + '/api/server/self_status'


def fetch_remote_server(server: dict) -> dict[str, t.Any]:
    """Запрашиваем self_status у соседа."""

    url = _build_self_status_url(server.get('url', ''))
    try:
        resp = requests.get(url, timeout=8)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict):
            data.setdefault('server', server.get('name'))
            return data
    except Exception as exc:
        return {
            'server': server.get('name'),
            'error': str(exc),
            'pingOk': False,
            'gnOk': False,
            'dnOk': False,
        }
    return {'server': server.get('name'), 'pingOk': False, 'gnOk': False, 'dnOk': False}


def check_all_servers():
    """Возвращаем статусы по всем серверам на основе self-status."""

    results = {}
    for srv in get_configured_servers():
        if srv.get('name') == SERVER_NAME:
            results[srv['name']] = get_local_status()
        else:
            results[srv['name']] = fetch_remote_server(srv)
    return results


@app.route("/api/server/self_status")
def api_server_self_status():
    return jsonify(get_local_status())


@app.route("/api/serverStatus")
def api_serverStatus():
    data = check_all_servers()
    return jsonify({'servers': get_configured_servers(), 'status': data})

# Запускаем фоновые замеры self_status
start_server_status_thread()

# ───── Вставьте после @app.route("/api/serverStatus") ─────

def _query_task_enabled(task_name: str) -> bool | None:
    """True/False если задача существует, None если schtasks вернул ошибку."""
    import subprocess, shlex
    try:
        out = subprocess.check_output(
            ["schtasks", "/Query", "/TN", task_name, "/FO", "LIST", "/V"],
            stderr=subprocess.STDOUT, timeout=3
        ).decode("cp866", "ignore").lower()
        return ("enabled" in out) or ("включена" in out) or ("yes" in out)
    except subprocess.CalledProcessError:
        return None                # задачи нет → None


# ───── crashed.json alias ─────
@app.route("/api/crashed")
def api_crashed_alias():
    return api_crashed_emu()       # просто проксируем


# ───────────────────  DATE PARSER  ───────────────────
def _parse_any_date(s: str) -> date | None:
    """Принимает '2025-06-21', '21.06.2025' или '21.06.25' — возвращает date."""
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d.%m.%y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    return None

# ─────────────────────  PAY-ALERTS  ─────────────────────
def _get_pay_alerts():
    """
    Возвращает список словарей для виджета «Оплата»:
      {id, name, pay, tariff, status, missing[]}

    status:
      overdue  – дата оплаты прошла или сегодня
      soon     – дата через 1-2 дня
      missing  – нет даты оплаты или других ключевых полей
                 (Email / Password / IGG / Tariff)
      ok       – скрываем из виджета
    """
    today = date.today()
    res   = []

    sync_account_meta()                      # актуализируем account_meta
    name_map = dict(load_active_names())     # показываем только активные

    conn = open_db(RESOURCES_DB)
    c    = conn.cursor()
    rows = c.execute("""
        SELECT id, email, passwd, igg, pay_until, tariff_rub
        FROM account_meta
    """).fetchall()
    conn.close()

    for acc_id, email, passwd, igg, pu, tariff in rows:
        if acc_id not in name_map:           # выключенные аккаунты пропускаем
            continue

        missing = []
        if not email:   missing.append("Email")
        if not passwd:  missing.append("Password")
        if not igg:     missing.append("IGG")
        if not tariff:  missing.append("Tariff")

        paystr = ""
        status = "ok"

        # ─── Проверяем дату оплаты ───
        if pu:                                # дата в БД присутствует
            d = _parse_any_date(pu)
            if d:
                paystr = d.strftime("%d.%m")
                delta  = (d - today).days
                if   delta <= 0: status = "overdue"
                elif delta <= 2: status = "soon"
            else:                             # не смогли распарсить
                missing.append("PayDate")
                status = "missing"
        else:                                 # даты вообще нет
            missing.append("PayDate")
            status = "missing"

        # Если другие поля пустые, но статус пока OK → делаем missing
        if status == "ok" and missing:
            status = "missing"

        if status != "ok":                    # только проблемные попадают в виджет
            res.append({
                "id"     : acc_id,
                "name"   : name_map[acc_id],
                "pay"    : paystr,
                "tariff" : tariff or 0,
                "status" : status,
                "missing": missing
            })

    # сортировка: просроченные → скоро → отсутствие данных
    order = {"overdue": 0, "soon": 1, "missing": 2}
    res.sort(key=lambda r: order.get(r["status"], 3))
    return res


# ▶▶▶ Telegram helpers ◀◀◀
# Универсальная отправка в Telegram.
# Параметр add_fix_link сохранён для обратной совместимости, но игнорируется.
def _send_telegram(text: str, add_fix_link: bool | None = None) -> None:
    # Если токены/чат не заданы — не падаем, а аккуратно сообщаем в консоль
    if not (TELEGRAM_TOKEN and TELEGRAM_CHAT_ID):
        print("[TG] skipped: TELEGRAM_TOKEN/CHAT_ID not set", flush=True)
        return
    try:
        url  = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        resp = requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text"   : text,
            "parse_mode": "HTML",
        }, timeout=15)
        if resp.status_code != 200:
            print(f"[TG] HTTP {resp.status_code}: {resp.text}", flush=True)
    except Exception as e:
        print(f"[TG] exception: {e}", flush=True)



def _compose_pay_alert_message() -> str | None:
    """
    Формирует текст для Telegram на основе _get_pay_alerts().
    Возвращает None, если актуальных уведомлений нет.
    """
    alerts = _get_pay_alerts()
    if not alerts:                      # всё в порядке — ничего не шлём
        return None

    lines = [f"<b>Сервер {SERVER}: оплата</b>"]
    for rec in alerts:
        # Иконка-префикс по статусу
        if rec["status"] == "overdue":
            icon = "❗"
        elif rec["status"] == "soon":
            icon = "⚠️"
        else:                           # missing
            icon = "❔"

        line  = f"{icon} {rec['name']}"
        if rec["pay"]:
            line += f" — до {rec['pay']}"
        if rec["tariff"]:
            line += f" ({rec['tariff']} ₽)"
        if rec["missing"]:
            line += f" — N/A {', '.join(rec['missing'])}"
        lines.append(line)

    return "\n".join(lines)
# ▲▲▲ Telegram helpers end ▲▲▲


# 
###########################################
# Flask endpoints
###########################################

@app.route("/")
def index_page():
    return render_template("index.html")

@app.route("/logs")
def logs_page():
    return render_template("logs.html")

@app.route("/fix")
def fix_page():
    return render_template("fix.html")


@app.route("/templates-editor")
def templates_editor_page():
    """Отдаём страницу редактора шаблонов."""
    return render_template("templates.html")

@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    parse_logs()
    global LAST_UPDATE_TIME
    LAST_UPDATE_TIME= datetime.now(timezone.utc)
    return {"status":"ok","last_update": LAST_UPDATE_TIME.isoformat()}

# ───── NEW: отдаём inactive15.json ─────
@app.route("/api/inactive15")
def api_inactive15():
    """
    Возвращает [{"nickname":"Alex898","hours":17.4}, …]
    или [] если файл ещё не создан.
    """
    path = Path(__file__).with_name("inactive15.json")
    if not path.is_file():
        return jsonify([])
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        # фильтруем дубликаты / пустые
        uniq = {d["nickname"]: d for d in data if d.get("nickname")}
        # сортируем по убыванию часов
        ordered = sorted(uniq.values(), key=lambda d: d["hours"], reverse=True)
        return jsonify(ordered)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
# ──────────────────────────────────────────────────────────────────



@app.route("/api/screenshot", methods=["GET"])
def api_screenshot():
    """
    Снимает скриншот экрана этого сервера и возвращает
    его в base64-представлении в JSON.
    """
    try:
        img = ImageGrab.grab()               # локальный скрин
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    buf = BytesIO()
    img.save(buf, format="PNG")
    b64 = base64.b64encode(buf.getvalue()).decode()
    return jsonify({"data": f"data:image/png;base64,{b64}"})

def apply_profile_updates(payload: list[dict]):
    rec_map = {}
    for p in payload:
        pid = p.get("id")
        if pid is None:
            continue
        rec_map[str(pid)] = p

    if not os.path.exists(PROFILE_PATH):
        raise FileNotFoundError(PROFILE_PATH)

    with open(PROFILE_PATH, "r", encoding="utf-8") as f:
        prof = json.load(f)

    for acc in prof:
        rid = str(acc.get("Id")) if acc.get("Id") is not None else None
        if rid not in rec_map:      # не меняем
            continue
        upd = rec_map[rid]

        # --- правим MenuData.Config ---
        try:
            md = json.loads(acc.get("MenuData","{}"))
        except Exception:
            md = {}
        cfg = md.setdefault("Config", {})
        cfg["Email"]    = upd.get("email","")
        cfg["Password"] = upd.get("passwd","")
        cfg["Custom"]   = upd.get("igg","")          # IGG
        acc["MenuData"] = json.dumps(md, ensure_ascii=False)

    with open(PROFILE_PATH, "w", encoding="utf-8") as f:
        json.dump(prof, f, ensure_ascii=False, indent=2)

    sync_account_meta()      # ← NEW


@app.route("/api/accounts_profile", methods=["PATCH"])
def api_accounts_profile():
    """
    Получает список {id,email,passwd,igg} и
    обновляет FRESH_NOX.json (только эти поля).
    """
    payload = request.json or []

    try:
        apply_profile_updates(payload)
    except FileNotFoundError:
        return jsonify({"err":"profile not found"}),404

    return jsonify({"status":"ok"})

@app.route("/api/log_slice")
def log_slice():
    """Отдаёт «хвост» из n строк последнего лога за сегодня.
       /api/log_slice?lines=50   (параметр необязателен)"""
    try:
        n = int(request.args.get("lines", 50))
    except (TypeError, ValueError):
        n = 50

    today = date.today().strftime("%Y-%m-%d")
    files = sorted(Path(LOGS_DIR).glob(f"*{today}*.log"))

    if not files:
        return jsonify({"lines": []})

    tail = Path(files[-1]).read_text(errors="ignore").splitlines()[-n:]
    return jsonify({"lines": tail})

# ───── применить шаблон к аккаунту ─────
# ───── применить шаблон к аккаунту ─────
@app.route("/api/manage/account/<acc_id>/apply_template", methods=["POST"])
def api_apply_template(acc_id):
    """
    POST body: {"template": "650" | "1100" | ...}
    Действие: заменяет поле Data у аккаунта выбранным шаблоном.
    Возврат: 200 {"status":"ok","acc_id":..., "template":...} или ошибка.
    """
    try:
        payload = request.get_json(silent=True) or {}
        tmpl_name = (payload.get("template") or "").strip()
        if not tmpl_name:
            return jsonify({"error": "template is required"}), 400

        template_steps = None
        template_label = tmpl_name

        aliases = _load_template_aliases()
        try:
            target_name = _resolve_template_name(tmpl_name, aliases)
            full_path, safe_template = _normalized_template_path(target_name)
        except ValueError:
            return jsonify({"error": "template not found"}), 404

        if not os.path.exists(PROFILE_PATH):
            return jsonify({"error": "profile not found"}), 404
        with open(PROFILE_PATH, "r", encoding="utf-8") as f:
            prof = json.load(f)

        acc = next((a for a in prof if a.get("Id") == acc_id), None)
        if not acc:
            return jsonify({"error": "acc not found"}), 404

        current_steps = _parse_json_field(acc.get("Data", "[]"), [])

        if os.path.isfile(full_path):
            template_steps = _json_read_or(full_path, [])
        else:
            raw_embedded = TEMPLATES.get(tmpl_name) or TEMPLATES.get(
                os.path.splitext(target_name)[0]
            )
            try:
                template_steps = json.loads(raw_embedded or "[]")
            except Exception:
                return jsonify({"error": "template invalid"}), 400

        ok, err = _validate_template_steps(template_steps)
        if not ok:
            return jsonify({"error": "template invalid", "details": err}), 400

        schema = schema_load()
        gaps = find_template_schema_gaps(template_steps, schema)
        if gaps:
            return (
                jsonify(
                    {
                        "error": "template_missing_keys",
                        "template": safe_template,
                        "missing_keys": gaps,
                    }
                ),
                409,
            )

        template_filled = template_inflate_with_schema(template_steps, schema)
        merged_steps = merge_template_into_account(current_steps, template_filled)

        acc["Data"] = json.dumps(merged_steps, ensure_ascii=False, separators=(",", ":"))

        with open(PROFILE_PATH, "w", encoding="utf-8") as f:
            json.dump(prof, f, ensure_ascii=False, indent=2)

        try:
            if "sync_account_meta" in globals():
                sync_account_meta()
        except Exception:
            app.logger.exception("sync_account_meta failed (non-critical)")

        return jsonify(
            {
                "status": "ok",
                "acc_id": acc_id,
                "template": safe_template,
                "aliases": aliases,
            }
        )

    except Exception as e:
        # Лог + понятный ответ фронту (т.е. тост станет красным)
        app.logger.exception("api_apply_template failed")
        return jsonify({"error": "internal", "details": str(e)}), 500


def _normalize_date_str(value: str) -> str:
    """Приводит строки дат к YYYY-MM-DD или возвращает ""."""
    if not value:
        return ""

    try:
        dt = datetime.fromisoformat(str(value).strip())
        return dt.date().isoformat()
    except Exception:
        try:
            dt = datetime.strptime(str(value).split()[0], "%Y-%m-%d")
            return dt.date().isoformat()
        except Exception:
            return ""


def load_accounts_meta_full(ids: set[str] | None = None) -> list[dict]:
    """Возвращает список аккаунтов c мета-данными так же, как api_accounts_meta_full."""
    # 1) читаем профиль (только активные)
    profile = []
    if os.path.exists(PROFILE_PATH):
        with open(PROFILE_PATH, "r", encoding="utf-8") as f:
            for a in json.load(f):
                if not a.get("Active"):
                    continue
                pid = a.get("Id")
                if pid is None:
                    continue
                if ids and str(pid) not in ids:
                    continue

                # --- E-mail / Pass / IGG из профиля ---
                email = passwd = igg = ""
                try:
                    md = json.loads(a.get("MenuData", "{}"))
                    cfg = md.get("Config", {})
                    email  = cfg.get("Email", "") or ""
                    passwd = cfg.get("Password", "") or ""
                    igg    = cfg.get("Custom", "") or ""
                except Exception:
                    pass

                profile.append({
                    "id": str(pid),
                    "name": a.get("Name", ""),
                    "email": email,
                    "passwd": passwd,
                    "igg": igg,
                    "server": SERVER,
                })

    # 2) account_meta: берём ВСЕ поля, чтобы был фолбэк для учёток
    conn = open_db(RESOURCES_DB)
    c = conn.cursor()
    meta = {
        str(r[0]): {
            "email":      r[1] or "",
            "passwd":     r[2] or "",
            "igg":        r[3] or "",
            "pay_until":  r[4] or "",
            "tariff_rub": r[5] or 0,
            "server":     r[6] or "",
            "tg_tag":     r[7] or "",
        }
        for r in c.execute(
            """
                SELECT id, email, passwd, igg, pay_until, tariff_rub, server, tg_tag
                FROM account_meta
            """
        )
    }
    conn.close()

    # 3) объединяем с фолбэком: пустые поля из профиля → подставляем из БД
    out = []
    for p in profile:
        m = meta.get(p["id"], {})
        merged = {
            "id": p.get("id"),
            "name": p.get("name", ""),
            "email": p.get("email") or m.get("email", ""),
            "passwd": p.get("passwd") or m.get("passwd", ""),
            "igg": p.get("igg") or m.get("igg", ""),
            "pay_until": m.get("pay_until", ""),
            "tariff_rub": m.get("tariff_rub", 0) or 0,
            "server": p.get("server") or m.get("server") or SERVER,
            "tg_tag": m.get("tg_tag", ""),
        }
        out.append(merged)

    return out


@app.route("/api/accounts_meta_full")
def api_accounts_meta_full():
    try:
        ids = set(filter(None, request.args.get("ids", "").split(","))) or None
        out = load_accounts_meta_full(ids)

        return jsonify({
            "ok": True,
            "server": SERVER,
            "count": len(out),
            "items": out,
        })
    except Exception as exc:
        app.logger.exception("api_accounts_meta_full failed")
        return jsonify({"ok": False, "error": str(exc)}), 500


def _load_usersdash_from_db(server_name: str) -> tuple[list[dict], list[str]]:
    """Читает UsersDash из локальной SQLite, если она доступна рядом."""

    errors: list[str] = []
    if not os.path.exists(USERDASH_DB):
        errors.append(f"UsersDash DB не найден: {USERDASH_DB}")
        return [], errors

    conn = open_db(USERDASH_DB)
    c = conn.cursor()
    srv_row = c.execute(
        "SELECT id, name FROM servers WHERE name=? LIMIT 1",
        (server_name,),
    ).fetchone()

    if not srv_row:
        conn.close()
        errors.append(f"Сервер '{server_name}' не найден в UsersDash")
        return [], errors

    srv_id = srv_row[0]
    rows = c.execute(
        """
            SELECT a.id, a.name, a.internal_id, a.is_active,
                   a.next_payment_at, a.next_payment_amount,
                   fd.email, fd.password, fd.igg_id, fd.server, fd.telegram_tag
            FROM accounts a
            LEFT JOIN farm_data fd
              ON fd.user_id = a.owner_id AND fd.farm_name = a.name
            WHERE a.server_id=? AND a.is_active IS NOT 0
        """,
        (srv_id,),
    ).fetchall()
    conn.close()

    items = []
    for r in rows:
        items.append({
            "usersdash_id": r[0],
            "name": r[1] or "",
            "internal_id": str(r[2]) if r[2] is not None else "",
            "is_active": bool(r[3]),
            "next_payment_at": _normalize_date_str(r[4]),
            "tariff": r[5],
            "email": r[6] or "",
            "password": r[7] or "",
            "igg_id": r[8] or "",
            "server": r[9] or "",
            "telegram": r[10] or "",
        })

    return items, errors


def _load_usersdash_from_api(server_name: str) -> tuple[list[dict], list[str]]:
    """Запрашивает UsersDash по REST API, чтобы не зависеть от локальной БД."""

    errors: list[str] = []
    api_url = (USERSDASH_API_URL or "").rstrip("/")

    if not api_url:
        return [], errors
    if not USERSDASH_API_TOKEN:
        errors.append("USERSDASH_API_TOKEN не задан")
        return [], errors

    full_url = api_url + "/api/farms/v1"
    try:
        resp = requests.get(
            full_url,
            params={"server": server_name, "token": USERSDASH_API_TOKEN},
            timeout=20,
        )
        resp.raise_for_status()
        payload = resp.json()
    except Exception as exc:
        errors.append(f"Ошибка запроса UsersDash: {exc}")
        return [], errors

    if not isinstance(payload, dict):
        errors.append("Некорректный ответ UsersDash")
        return [], errors

    if payload.get("ok") is not True:
        errors.append(str(payload.get("error") or "UsersDash вернул ошибку"))
        return [], errors

    remote_server = str(payload.get("server") or "").strip()
    if remote_server and server_name and remote_server != server_name:
        errors.append(
            f"UsersDash вернул данные для '{remote_server}', ожидали '{server_name}'"
        )

    items: list[dict] = []
    for row in payload.get("items") or []:
        if not isinstance(row, dict):
            continue
        if row.get("is_active") is False:
            continue

        items.append({
            "usersdash_id": row.get("id") or row.get("usersdash_id"),
            "name": row.get("name") or "",
            "internal_id": str(row.get("internal_id") or ""),
            "is_active": bool(row.get("is_active", True)),
            "next_payment_at": _normalize_date_str(row.get("next_payment_at")),
            "tariff": row.get("tariff"),
            "email": row.get("email") or row.get("login") or "",
            "password": row.get("password") or "",
            "igg_id": row.get("igg_id") or "",
            "server": row.get("kingdom") or row.get("server") or remote_server,
            "telegram": row.get("telegram_tag") or row.get("telegram") or "",
        })

    return items, errors


def load_usersdash_accounts(server_name: str) -> tuple[list[dict], list[str]]:
    errors: list[str] = []

    api_items, api_errors = _load_usersdash_from_api(server_name)
    errors.extend(api_errors)
    if USERSDASH_API_URL:
        if not api_errors:
            return api_items, errors
        if not os.path.exists(USERDASH_DB):
            return api_items, errors

    db_items, db_errors = _load_usersdash_from_db(server_name)
    errors.extend(db_errors)
    return db_items, errors


@app.route("/api/usersdash_sync_preview")
def api_usersdash_sync_preview():
    try:
        local_items = load_accounts_meta_full(None)
        remote_items, errors = load_usersdash_accounts(SERVER)

        remote_by_internal = {
            r.get("internal_id"): r for r in remote_items if r.get("internal_id")
        }
        remote_by_name = {r.get("name"): r for r in remote_items if r.get("name")}

        changes: list[dict] = []

        field_map = [
            ("email", "email", "email"),
            ("password", "passwd", "password"),
            ("igg_id", "igg", "igg_id"),
            ("server", "server", "server"),
            ("telegram", "tg_tag", "telegram"),
            ("next_payment_at", "pay_until", "next_payment_at"),
            ("tariff", "tariff_rub", "tariff"),
        ]

        for loc in local_items:
            lid = str(loc.get("id") or "")
            lname = loc.get("name") or ""

            rem = None
            if lid and lid in remote_by_internal:
                rem = remote_by_internal[lid]
            elif lname and lname in remote_by_name:
                rem = remote_by_name[lname]

            if not rem:
                continue

            for field, local_key, remote_key in field_map:
                lv_raw = loc.get(local_key, "")
                rv_raw = rem.get(remote_key, "")

                if field == "next_payment_at":
                    lv = _normalize_date_str(lv_raw)
                    rv = _normalize_date_str(rv_raw)
                elif field == "tariff":
                    lv = "" if lv_raw in (None, "") else str(lv_raw)
                    rv = "" if rv_raw in (None, "") else str(rv_raw)
                else:
                    lv = str(lv_raw or "")
                    rv = str(rv_raw or "")

                if lv == rv:
                    continue

                changes.append({
                    "id": lid,
                    "name": lname,
                    "field": field,
                    "local": lv,
                    "remote": rv,
                    "usersdash_id": rem.get("usersdash_id"),
                })

        return jsonify({"ok": True, "changes": changes, "errors": errors})
    except Exception as exc:
        app.logger.exception("api_usersdash_sync_preview failed")
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/usersdash_sync_apply", methods=["POST"])
def api_usersdash_sync_apply():
    try:
        payload = request.get_json(silent=True) or {}
        changes = payload.get("changes") or []
        if not isinstance(changes, list) or not changes:
            return jsonify({"ok": False, "error": "Нет изменений для применения"}), 400

        updates: dict[str, dict] = {}
        for ch in changes:
            acc_id = str(ch.get("id") or ch.get("internal_id") or "").strip()
            field = ch.get("field")
            remote_val = ch.get("remote")
            if not acc_id or not field:
                continue

            rec = updates.setdefault(acc_id, {"id": acc_id})

            if field == "email":
                rec["email"] = remote_val or ""
            elif field == "password":
                rec["passwd"] = remote_val or ""
            elif field == "igg_id":
                rec["igg"] = remote_val or ""
            elif field == "server":
                rec["server"] = remote_val or ""
            elif field == "telegram":
                rec["tg_tag"] = remote_val or ""
            elif field == "next_payment_at":
                rec["pay_until"] = _normalize_date_str(remote_val)
            elif field == "tariff":
                try:
                    rec["tariff_rub"] = int(remote_val)
                except Exception:
                    rec["tariff_rub"] = None

        if not updates:
            return jsonify({"ok": False, "error": "Нет валидных изменений"}), 400

        conn = open_db(RESOURCES_DB)
        c = conn.cursor()

        # Текущие значения для аккуратного мерджа
        existing: dict[str, dict] = {}
        placeholders = ",".join("?" for _ in updates)
        if placeholders:
            for row in c.execute(
                f"SELECT id, email, passwd, igg, pay_until, tariff_rub, server, tg_tag "
                f"FROM account_meta WHERE id IN ({placeholders})",
                tuple(updates.keys()),
            ):
                existing[str(row[0])] = {
                    "email": row[1] or "",
                    "passwd": row[2] or "",
                    "igg": row[3] or "",
                    "pay_until": row[4] or "",
                    "tariff_rub": row[5],
                    "server": row[6] or "",
                    "tg_tag": row[7] or "",
                }

        merged_rows = []
        profile_payload = []
        for acc_id, rec in updates.items():
            base = existing.get(acc_id, {
                "email": "",
                "passwd": "",
                "igg": "",
                "pay_until": "",
                "tariff_rub": None,
                "server": "",
                "tg_tag": "",
            })

            merged = {**base}
            for k, v in rec.items():
                if k == "id":
                    continue
                merged[k] = v if v is not None else ""

            merged_rows.append({"id": acc_id, **merged})
            profile_payload.append({
                "id": acc_id,
                "email": merged.get("email", ""),
                "passwd": merged.get("passwd", ""),
                "igg": merged.get("igg", ""),
            })

        cols = ["email","passwd","igg","pay_until","tariff_rub","server","tg_tag"]
        placeholders = ",".join("?" for _ in range(len(cols) + 1))
        for rec in merged_rows:
            vals = [rec.get(k) for k in cols]
            c.execute(
                f"""
                   INSERT INTO account_meta(id,{','.join(cols)})
                   VALUES({placeholders})
                   ON CONFLICT(id) DO UPDATE SET
                     {', '.join([f'{k}=excluded.{k}' for k in cols])}
                """,
                [rec["id"], *vals],
            )

        conn.commit()
        conn.close()

        try:
            apply_profile_updates(profile_payload)
        except FileNotFoundError:
            return jsonify({"ok": False, "error": "PROFILE_PATH не найден"}), 404

        return jsonify({"ok": True, "updated": len(merged_rows)})
    except Exception as exc:
        app.logger.exception("api_usersdash_sync_apply failed")
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/payalert")
def api_payalert():
    return jsonify(_get_pay_alerts())


@app.route("/api/payalert/extend/<acc_id>", methods=["POST"])
def api_payalert_extend(acc_id):
    conn = open_db(RESOURCES_DB)
    c    = conn.cursor()
    row  = c.execute(
            "SELECT pay_until FROM account_meta WHERE id=?",
            (acc_id,)).fetchone()
    today = date.today()
    if row and row[0]:
        try:
            cur = datetime.strptime(row[0], "%Y-%m-%d").date()
        except ValueError:
            cur = today
    else:
        cur = today

    new = cur + timedelta(days=30)
    c.execute("UPDATE account_meta SET pay_until=? WHERE id=?",
              (new.strftime("%Y-%m-%d"), acc_id))
    conn.commit(); conn.close()
    return jsonify({"status":"ok","new_date":new.strftime("%Y-%m-%d")})


@app.route("/api/accounts_meta", methods=["GET"])
def api_acc_meta_get():
    conn = open_db(RESOURCES_DB)
    rows = conn.execute("SELECT * FROM account_meta").fetchall()
    conn.close()
    return jsonify([{k[0]:row[idx] for idx,k in enumerate(conn.execute('PRAGMA table_info(account_meta)'))} for row in rows])

@app.route("/api/accounts_meta", methods=["PUT"])
def api_acc_meta_put():
    payload = request.json         # [{id,email,passwd,igg,pay_until,tariff_rub}, ...]
    if not isinstance(payload,list): return jsonify({"err":"bad"}),400
    conn = open_db(RESOURCES_DB); c = conn.cursor()
    for rec in payload:
        if "id" not in rec: continue
        cols = ["email","passwd","igg","pay_until","tariff_rub"]
        vals = [rec.get(k) for k in cols]
        c.execute(f"""
           INSERT INTO account_meta(id,{','.join(cols)})
           VALUES(?,?,?,?,?,?)
           ON CONFLICT(id) DO UPDATE SET
             {', '.join([f'{k}=excluded.{k}' for k in cols])}
        """, [rec["id"],*vals])
    conn.commit(); conn.close()
    return jsonify({"status":"ok"})

@app.route("/api/income")
def api_income():
    today = date.today()

    # последний день текущего месяца
    month_end = (today.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)

    conn = open_db(RESOURCES_DB)
    c    = conn.cursor()

    rows = c.execute("SELECT pay_until, tariff_rub FROM account_meta").fetchall()

    # есть ли таблица expenses → рассчитываем общую сумму расходов
    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='expenses'")
    if c.fetchone():
        exp = c.execute("SELECT COALESCE(SUM(amount),0) FROM expenses").fetchone()[0] or 0
    else:
        exp = 0

    conn.close()

    # общий доход (с учётом возможных NULL)
    total = sum((r[1] or 0) for r in rows)

    # доход, ещё оставшийся до конца текущего месяца
    left = 0
    for pay_until, tariff in rows:
        if not tariff:               # тариф может быть NULL
            continue
        try:
            if pay_until:
                pu_date = datetime.strptime(pay_until, "%Y-%m-%d").date()
                if today <= pu_date <= month_end:
                    left += tariff
        except ValueError:
            # неверный формат даты — игнорируем
            pass

    # вычитаем расходы из обеих сумм
    return jsonify({"total": total - exp, "left": left})

@app.route("/api/expenses", methods=["GET","POST","PUT"])
def api_expenses():
    conn = open_db(RESOURCES_DB)

    # гарантируем, что таблица есть ──────────────▼
    conn.execute("""CREATE TABLE IF NOT EXISTS expenses(
                      id INTEGER PRIMARY KEY AUTOINCREMENT,
                      amount INTEGER NOT NULL,
                      dt TEXT NOT NULL)""")

    if request.method == "POST":
        amt = int(request.json.get("amount",0))
        if amt <= 0:
            return jsonify({"err":"amount"}), 400
        conn.execute("INSERT INTO expenses(amount,dt) VALUES(?,?)",
                     (amt, datetime.now().isoformat()))
        conn.commit()

    elif request.method == "PUT":           # «записать новое значение»
        amt = int(request.json.get("amount",0))
        if amt < 0:
            return jsonify({"err":"negative"}), 400
        conn.execute("DELETE FROM expenses")
        if amt:
            conn.execute("INSERT INTO expenses(amount,dt) VALUES(?,?)",
                         (amt, datetime.now().isoformat()))
        conn.commit()

    total = conn.execute(
        "SELECT COALESCE(SUM(amount),0) FROM expenses").fetchone()[0]
    conn.close()
    return jsonify({"total": total})

@app.route("/api/taskState")
def api_task_state():
    tn  = request.args.get("name","")
    val = _task_enabled(tn)
    return jsonify({"name": tn, "enabled": val})



# === ДО ВСТАВКИ НАЙДИ БЛОК С ДРУГИМИ @app.route("/api/…") И ВСТАВЬ РЯДОМ ===
from datetime import datetime, timedelta, timezone
import re

# Парсим dt из cached_logs (формат: 'YYYY-MM-DD HH:MM:SS.mmm +HH:MM')
_DT_RE = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} [\+\-]\d{2}:\d{2})")
def _parse_dt_iso_with_tz(dt_str: str) -> datetime | None:
    try:
        # Совпадает с тем, что записываем в cached_logs (первые 23+6 символов)
        return datetime.strptime(dt_str.strip(), "%Y-%m-%d %H:%M:%S.%f %z")
    except Exception:
        return None

def _format_hms(total_seconds: int) -> str:
    if total_seconds < 0: total_seconds = 0
    h = total_seconds // 3600
    m = (total_seconds % 3600) // 60
    s = total_seconds % 60
    return f"{h:02d}:{m:02d}:{s:02d}"

def _housekeep_cached_logs(conn, keep_days: int = 14):
    """
    Мягкая чистка: удаляем логи старше N дней.
    Хранить дольше смысла нет — "время круга" считаем в окне (по умолчанию 24ч).
    """
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=keep_days)).astimezone()
        # dt в базе в текстовом виде, но ISO-морфный и лексикографически сравним.
        # Преобразуем к тому же формату, что сохраняем в cached_logs.dt:
        #   "YYYY-MM-DD HH:MM:SS.mmm +03:00"
        cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + cutoff.strftime(" %z")
        conn.execute("DELETE FROM cached_logs WHERE dt < ?", (cutoff_str,))
        conn.commit()
    except Exception as e:
        print("[housekeep_cached_logs] warn:", e)


def _load_gather_state() -> dict[str, t.Any]:
    """Возвращает сохранённое состояние проблем с плитками (мягко)."""

    state = _safe_json_load(GATHER_TILES_STATE_PATH)
    return state if isinstance(state, dict) else {}


def _save_gather_state(state: dict[str, t.Any]) -> None:
    """Пишет состояние проблем с плитками на диск с защитой от ошибок."""

    try:
        os.makedirs(os.path.dirname(GATHER_TILES_STATE_PATH) or ".", exist_ok=True)
        with open(GATHER_TILES_STATE_PATH, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception as exc:
        print(f"[gather-watch] warn: cannot save state: {exc}")


def _cycle_has_gather_issue(lines: list[str]) -> bool:
    """Проверяет, есть ли в рамках цикла подряд >= N ошибок поиска плиток."""

    streak = 0
    for line in lines:
        low = line.lower()
        if any(pat in low for pat in GATHER_TILES_PATTERNS):
            streak += 1
            if streak >= GATHER_TILES_STREAK:
                return True
        else:
            streak = 0
    return False


def _extract_last_cycles(rows: list[tuple[str, str, str | None]]):
    """
    Возвращает (предыдущий цикл, последний цикл, dt_последнего Account Done).

    rows — список (dt, raw_line, nickname) в хронологическом порядке.
    Если не хватает завершённых циклов — возвращает (None, None, None).
    """

    done_idx = [i for i, (_, raw, _) in enumerate(rows) if "account done" in raw.lower()]
    if len(done_idx) < 2:
        return None, None, None

    last_end = done_idx[-1]
    prev_end = done_idx[-2]

    prev_start = done_idx[-3] + 1 if len(done_idx) >= 3 else 0

    prev_cycle = [raw for _, raw, _ in rows[prev_start : prev_end + 1]]
    last_cycle = [raw for _, raw, _ in rows[prev_end + 1 : last_end + 1]]
    last_done_dt = rows[last_end][0]

    return prev_cycle, last_cycle, last_done_dt


def _collect_gather_watch() -> list[dict[str, t.Any]]:
    """Ищет повторяющиеся ошибки поиска ресурсных плиток в последних циклах."""

    active = load_active_names()
    if not active:
        return []

    state_before = _load_gather_state()
    new_state: dict[str, t.Any] = {}
    alerts: list[dict[str, t.Any]] = []

    conn = open_db(LOGS_DB)
    c = conn.cursor()

    try:
        for acc_id, nick in active:
            rows = c.execute(
                """
                  SELECT dt, raw_line, COALESCE(nickname, '')
                  FROM cached_logs
                  WHERE acc_id = ?
                  ORDER BY id DESC
                  LIMIT ?
                """,
                (acc_id, GATHER_TILES_LOG_LIMIT),
            ).fetchall()

            if not rows:
                continue

            rows = list(reversed(rows))
            prev_cycle, last_cycle, last_done_dt = _extract_last_cycles(rows)
            if not last_cycle or not last_done_dt:
                continue

            last_issue = _cycle_has_gather_issue(last_cycle)
            prev_issue = _cycle_has_gather_issue(prev_cycle or []) if prev_cycle is not None else False

            prev_state = state_before.get(acc_id) if isinstance(state_before, dict) else None

            persistent = False
            if prev_state:
                if prev_state.get("last_done") != last_done_dt:
                    if prev_state.get("had_issue") and last_issue:
                        persistent = True
            elif prev_issue and last_issue:
                persistent = True

            new_state[acc_id] = {"last_done": last_done_dt, "had_issue": last_issue}

            if persistent:
                alerts.append(
                    {
                        "acc_id": acc_id,
                        "nickname": nick or rows[-1][2] or acc_id,
                        "summary": (
                            "🏕️ Недостаточно свободных ресурсных точек рядом с фермой "
                            "(повторяется)"
                        ),
                        "kind": "gather_tiles",
                        "total": 1,
                    }
                )
    finally:
        try:
            _save_gather_state(new_state)
        finally:
            conn.close()

    return alerts


def _load_inactive_watch() -> list[dict[str, t.Any]]:
    """Подмешиваем список неактивных аккаунтов (dayGain=0 > THRESH)."""

    path = Path(__file__).with_name("inactive15.json")
    data = _safe_json_load(path)

    if not isinstance(data, list):
        return []

    alerts: list[dict[str, t.Any]] = []
    for item in data:
        if not isinstance(item, dict):
            continue

        nick = item.get("nickname") or item.get("name")
        hours = item.get("hours")
        tag = item.get("tag") or "0gain🍽️"

        if not nick or hours is None:
            continue

        try:
            hours_num = float(hours)
        except (TypeError, ValueError):
            continue

        hours_txt = f"{hours_num:.1f}".replace(".0", "")
        alerts.append(
            {
                "nickname": nick,
                "summary": f"{tag} {hours_txt}ч без прироста",
                "total": 1,
                "kind": "inactive",
                "hours": hours_num,
                "tag": tag,
            }
        )

    return alerts

def _compute_cycle_stats(window_hours: int = 24,
                         min_gap_minutes: int = 5,
                         max_gap_hours: int = 3) -> dict:
    """
    Считает среднее время круга:
      • берём по каждому активному аккаунту точки 'Account Done'
      • сортируем по времени, берём соседние интервалы
      • игнорируем интервалы < min_gap_minutes (глюки) и > max_gap_hours (аномалии/простои)
      • усредняем по всем валидным интервалам (взвешенно по числу интервалов)
    Возвращает JSON-словарь для фронта.
    """
    acts = load_profiles()
    active_ids = [(a["Id"], a.get("Name","")) for a in acts]
    if not active_ids:
        return {
            "avg_cycle_seconds": None,
            "avg_cycle_hms": "—",
            "accounts_used": 0,
            "intervals_used": 0,
            "window_hours": window_hours,
            "min_gap_minutes": min_gap_minutes,
            "max_gap_hours": max_gap_hours,
            "per_account": []
        }

    now_utc = datetime.now(timezone.utc)
    window_start = (now_utc - timedelta(hours=window_hours)).astimezone()
    window_str = window_start.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + window_start.strftime(" %z")

    conn = open_db(LOGS_DB)
    c = conn.cursor()

    # Раз в час делаем чистку старых записей (нечасто, дёшево)
    try:
        if datetime.now().minute == 0:
            _housekeep_cached_logs(conn, keep_days=14)
    except Exception as e:
        print("[cycle_stats] housekeeping skipped:", e)

    total_secs = 0
    total_intervals = 0
    per_acc = []

    min_gap = timedelta(minutes=min_gap_minutes)
    max_gap = timedelta(hours=max_gap_hours)

    for acc_id, nickname in active_ids:
        # Берём ТОЛЬКО сегодня/нужное окно, и только строки с "Account Done"
        rows = c.execute("""
          SELECT dt, raw_line
          FROM cached_logs
          WHERE acc_id=? AND dt >= ? AND raw_line LIKE '%Account Done%'
          ORDER BY id ASC
        """, (acc_id, window_str)).fetchall()

        times = []
        for dt_text, raw in rows:
            # подстрахуемся: проверим префикс даты точно в raw_line
            # но лучше парсить dt из столбца dt (он уже с таймзоной)
            d = _parse_dt_iso_with_tz(dt_text)
            if d is not None:
                times.append(d)

        if len(times) < 2:
            continue

        # находим интервалы между соседними "Account Done"
        good = []
        prev = times[0]
        for cur in times[1:]:
            delta = cur - prev
            prev = cur
            if delta < min_gap or delta > max_gap:
                # игнорим глюки (<5 мин) и нехарактерные длинные простои
                continue
            good.append(delta.total_seconds())

        if not good:
            continue

        acc_avg = sum(good) / len(good)
        per_acc.append({
            "id": acc_id,
            "nickname": nickname,
            "intervals": len(good),
            "avg_seconds": int(acc_avg),
            "avg_hms": _format_hms(int(acc_avg))
        })
        total_secs += sum(good)
        total_intervals += len(good)

    if total_intervals == 0:
        return {
            "avg_cycle_seconds": None,
            "avg_cycle_hms": "—",
            "accounts_used": 0,
            "intervals_used": 0,
            "window_hours": window_hours,
            "min_gap_minutes": min_gap_minutes,
            "max_gap_hours": max_gap_hours,
            "per_account": []
        }

    global_avg = int(total_secs / total_intervals)
    return {
        "avg_cycle_seconds": global_avg,
        "avg_cycle_hms": _format_hms(global_avg),
        "accounts_used": len(per_acc),
        "intervals_used": total_intervals,
        "window_hours": window_hours,
        "min_gap_minutes": min_gap_minutes,
        "max_gap_hours": max_gap_hours,
        "per_account": per_acc
    }

@app.route("/api/cycle_time")
def api_cycle_time():
    """
    Возвращает оценку "времени круга" по cached_logs:
      { avg_cycle_seconds, avg_cycle_hms, accounts_used, intervals_used, window_hours, ... }
    Параметры (query):
      window_hours=24  — окно анализа
      min_gap_minutes=5 — игнорировать интервалы меньше (защита от глюков)
      max_gap_hours=6   — отсекать слишком длинные простои
    """
    try:
        wh = int(request.args.get("window_hours", 24))
    except:
        wh = 24
    try:
        mg = int(request.args.get("min_gap_minutes", 5))
    except:
        mg = 5
    try:
        mx = int(request.args.get("max_gap_hours", 6))
    except:
        mx = 6

    stats = _compute_cycle_stats(window_hours=wh, min_gap_minutes=mg, max_gap_hours=mx)
    return jsonify(stats)
# ВРемя круга ВСЁ

import math
from collections import defaultdict

def _utcnow_local() -> datetime:
    return datetime.now(timezone.utc).astimezone()

def _range_start_end(range_key: str) -> tuple[str, str]:
    """
    Возвращаем (start_iso, end_iso) для окна 'day'|'week'|'month'
    в формате 'YYYY-MM-DD HH:MM:SS.mmm +HH:MM' (лексикографически сравним).
    """
    now = _utcnow_local()
    if range_key == "day":
        start = now - timedelta(days=1)
    elif range_key == "week":
        start = now - timedelta(days=7)
    else:
        start = now - timedelta(days=30)
    start_s = start.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + start.strftime(" %z")
    end_s   = now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + now.strftime(" %z")
    return start_s, end_s

def _estimate_account_cycle(acc_id: str,
                            min_gap_minutes: int = 5,
                            max_gap_hours: int = 6,
                            window_hours: int = 48) -> tuple[int|None, datetime|None]:
    """
    Оцениваем средний цикл аккаунта (сек) и ищем последний Account Done.
    """
    conn = open_db(LOGS_DB)
    c = conn.cursor()
    window_start = (_utcnow_local() - timedelta(hours=window_hours)).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + _utcnow_local().strftime(" %z")
    rows = c.execute("""
      SELECT dt
      FROM cached_logs
      WHERE acc_id = ? AND dt >= ? AND raw_line LIKE '%Account Done%'
      ORDER BY id ASC
    """, (acc_id, window_start)).fetchall()

    times = []
    last_done = None
    for (dt_text,) in rows:
        d = _parse_dt_iso_with_tz(dt_text)
        if d:
            times.append(d)
            last_done = d

    if len(times) < 2:
        return (None, last_done)

    min_gap = timedelta(minutes=min_gap_minutes)
    max_gap = timedelta(hours=max_gap_hours)
    good = []
    p = times[0]
    for cur in times[1:]:
        delta = cur - p
        p = cur
        if delta < min_gap or delta > max_gap:
            continue
        good.append(delta.total_seconds())

    if not good:
        return (None, last_done)

    return (int(sum(good)/len(good)), last_done)

def _group_key(ts: datetime, range_key: str) -> str:
    if range_key == "day":
        return ts.strftime("%Y-%m-%d %H:00")  # почасовые бины
    else:
        return ts.strftime("%Y-%m-%d")        # по дням
# Конец НОВЫХ ПОМОЩНИКОВ


@app.route("/api/account_stats")
def api_account_stats():
    """
    Агрегаты/график по аккаунту с учётом локальной зоны пользователя.

    Параметры:
      acc_id=<id>            (обяз.)
      range=day|week|month   (по умолчанию day)
      mode=normal|losses     (по умолчанию normal)
      tz_offset=<минуты>     (минуты смещения от UTC по данным браузера, как в JS: new Date().getTimezoneOffset(); для МСК будет -180)

    Определения:
      - "Сутки": от локальной полуночи пользователя до "сейчас".
      - "Неделя": сумма net-приростов по каждому календарному дню за последние 7 локальных дней.
      - "Месяц": сумма net-приростов по дням за последние 30 локальных дней.
      - "Обычный": положительные дельты (max(delta,0)); "Минусы": |отрицательные дельты|.
      - Точки графика: "Сутки" — по часам, "Неделя/Месяц" — по дням.
    """
    acc_id = request.args.get("acc_id", "").strip()
    if not acc_id:
        return jsonify({"error": "acc_id required"}), 400

    range_key = (request.args.get("range", "day") or "day").lower()
    if range_key not in ("day", "week", "month"):
        range_key = "day"

    mode = (request.args.get("mode", "normal") or "normal").lower()
    if mode not in ("normal", "losses"):
        mode = "normal"

    # смещение зоны пользователя (минуты)
    try:
        tz_off_min = int(request.args.get("tz_offset", "0"))
    except Exception:
        tz_off_min = 0

    # хелперы преобразования времени
    from datetime import timezone
    user_tz = timezone(timedelta(minutes=-tz_off_min))  # в JS offset отрицательный для восточных зон; приводим к tzinfo

    def to_dt(dt_text: str) -> datetime | None:
        d = _parse_dt_iso_with_tz(dt_text)
        return d

    def to_user_local(d: datetime) -> datetime:
        # d — aware (с TZ из лога), приводим к зоне пользователя
        return d.astimezone(user_tz)

    # Вытаскиваем снапшоты за последние 35 суток (чтобы хватило для month)
    conn = open_db(LOGS_DB)
    c = conn.cursor()
    cutoff = (datetime.now(timezone.utc).astimezone() - timedelta(days=35))
    cutoff_s = cutoff.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + cutoff.strftime(" %z")
    rows = c.execute("""
      SELECT dt, food, wood, stone, gold
      FROM resource_snapshots
      WHERE acc_id = ? AND dt >= ?
      ORDER BY id ASC
    """, (acc_id, cutoff_s)).fetchall()

    # Преобразуем и упорядочим
    snaps = []
    for dt_text, f, w, s, g in rows:
        d = to_dt(dt_text)
        if not d:
            continue
        snaps.append((d, int(f or 0), int(w or 0), int(s or 0), int(g or 0)))
    if len(snaps) < 2:
        # попробуем вернуть хотя бы метаданные + пустые ряды
        avg_sec, last_done = _estimate_account_cycle(acc_id)
        return jsonify({
            "acc_id": acc_id,
            "nickname": next((a.get("Name","") for a in load_profiles() if a["Id"]==acc_id), None),
            "range": range_key,
            "mode": mode,
            "points": [],
            "totals": {"day":{"food":0,"wood":0,"stone":0,"gold":0},
                       "week":{"food":0,"wood":0,"stone":0,"gold":0},
                       "month":{"food":0,"wood":0,"stone":0,"gold":0}},
            "forecast_month": {"food":0,"wood":0,"stone":0,"gold":0,"ok": False},
            "last_done_iso": last_done.strftime("%Y-%m-%dT%H:%M:%S%z") if last_done else None,
            "next_eta_iso": (last_done + timedelta(seconds=avg_sec)).strftime("%Y-%m-%dT%H:%M:%S%z") if last_done and avg_sec else None,
            "next_eta_seconds": avg_sec,
            "cycle_avg_seconds": avg_sec,
            "available_days": 0
        })

    # Строим дельты между соседними снапшотами
    deltas = []  # (ts_user_local, dF, dW, dS, dG)
    prev = snaps[0]
    for cur in snaps[1:]:
        (dp, fp, wp, sp, gp) = prev
        (dc, fc, wc, sc, gc) = cur
        dd = dc  # время текущей точки
        # дельта как изменение остатков между соседними снапшотами
        dF, dW, dS, dG = (fc - fp), (wc - wp), (sc - sp), (gc - gp)
        # в нужном режиме
        if mode == "normal":
            dF = max(dF, 0); dW = max(dW, 0); dS = max(dS, 0); dG = max(dG, 0)
        else:
            dF = abs(min(dF, 0)); dW = abs(min(dW, 0)); dS = abs(min(dS, 0)); dG = abs(min(dG, 0))
        deltas.append((to_user_local(dd), dF, dW, dS, dG))
        prev = cur

    # Границы периодов (локальные для пользователя)
    now_u = datetime.now(user_tz)
    # полуночь сегодняшняя
    today_start = now_u.replace(hour=0, minute=0, second=0, microsecond=0)
    # массив стартов для 7 и 30 суток назад
    week_starts = [ (today_start - timedelta(days=i)) for i in range(7, -1, -1) ]  # 8 меток от -7 до 0
    month_starts = [ (today_start - timedelta(days=i)) for i in range(30, -1, -1) ]  # 31 метка

    # Агрегируем по бинам:
    #  - день: каждый bin = 1 час от today_start
    #  - неделя/месяц: bin = календарный день
    from collections import defaultdict
    day_bins = defaultdict(lambda: [0,0,0,0])
    week_bins = defaultdict(lambda: [0,0,0,0])
    month_bins = defaultdict(lambda: [0,0,0,0])

    for ts, f, w, s, g in deltas:
        # сутки (с полуночи по часам)
        if ts >= today_start and ts <= now_u:
            key_h = ts.replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:00")
            day_bins[key_h][0]+=f; day_bins[key_h][1]+=w; day_bins[key_h][2]+=s; day_bins[key_h][3]+=g
        # неделя (календарные дни)
        if ts >= (today_start - timedelta(days=7)) and ts <= now_u:
            key_d = ts.replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%d")
            week_bins[key_d][0]+=f; week_bins[key_d][1]+=w; week_bins[key_d][2]+=s; week_bins[key_d][3]+=g
        # месяц (календарные дни)
        if ts >= (today_start - timedelta(days=30)) and ts <= now_u:
            key_m = ts.replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%d")
            month_bins[key_m][0]+=f; month_bins[key_m][1]+=w; month_bins[key_m][2]+=s; month_bins[key_m][3]+=g

    # Точки для графика под выбранный диапазон
    points = []
    if range_key == "day":
        keys_sorted = sorted(day_bins.keys())
        for k in keys_sorted:
            F,W,S,G = day_bins[k]
            points.append({"ts": k, "food":F, "wood":W, "stone":S, "gold":G})
    elif range_key == "week":
        keys_sorted = sorted(week_bins.keys())
        for k in keys_sorted:
            F,W,S,G = week_bins[k]
            points.append({"ts": k, "food":F, "wood":W, "stone":S, "gold":G})
    else:
        keys_sorted = sorted(month_bins.keys())
        for k in keys_sorted:
            F,W,S,G = month_bins[k]
            points.append({"ts": k, "food":F, "wood":W, "stone":S, "gold":G})

    # TOTАLS:
    #  - day: просто сумма day_bins
    #  - week: сумма по каждому дню недели (уже бины)
    #  - month: сумма по каждому дню месяца (уже бины)
    def totals_of(bins_dict):
        t = [0,0,0,0]
        for v in bins_dict.values():
            t[0]+=v[0]; t[1]+=v[1]; t[2]+=v[2]; t[3]+=v[3]
        return {"food":t[0], "wood":t[1], "stone":t[2], "gold":t[3]}

    totals_day   = totals_of(day_bins)
    totals_week  = totals_of(week_bins)
    totals_month = totals_of(month_bins)

    # FORECAST: если есть >= 5-7 дней покрытия — пропорция на 30 дней
    covered_days = len(week_bins.keys())
    forecast_ok = covered_days >= 5
    k = 30/7
    forecast = {"food":0,"wood":0,"stone":0,"gold":0,"ok": False}
    if forecast_ok:
        forecast = {
            "food":  int(totals_week["food"]  * k),
            "wood":  int(totals_week["wood"]  * k),
            "stone": int(totals_week["stone"] * k),
            "gold":  int(totals_week["gold"]  * k),
            "ok": True
        }

    # последний Account Done и ETA следующего (как было)
    avg_sec, last_done = _estimate_account_cycle(acc_id)
    next_eta_iso = None
    if last_done and avg_sec:
        next_eta_iso = (last_done + timedelta(seconds=avg_sec)).strftime("%Y-%m-%dT%H:%M:%S%z")

    # available_days — span по снапшотам (UTC-независимо)
    try:
        d0 = snaps[0][0]; d1 = snaps[-1][0]
        available_days = max(0, int((d1 - d0).total_seconds() // 86400))
    except:
        available_days = 0

    nickname = None
    try:
        for a in load_profiles():
            if a["Id"] == acc_id:
                nickname = a.get("Name","")
                break
    except:
        pass

    return jsonify({
        "acc_id": acc_id,
        "nickname": nickname,
        "range": range_key,
        "mode": mode,
        "points": points,
        "totals": {
            "day":   totals_day,
            "week":  totals_week,
            "month": totals_month
        },
        "forecast_month": forecast,
        "last_done_iso": last_done.strftime("%Y-%m-%dT%H:%M:%S%z") if last_done else None,
        "next_eta_iso": next_eta_iso,
        "next_eta_seconds": avg_sec,
        "cycle_avg_seconds": avg_sec,
        "available_days": available_days
    })

# 1.4. API: агрегаты и ряд для графика
# После твоего /api/cycle_time добавь новый маршрут(они выше)

@app.route("/manage")
def manage_page():
    """Отдаём manage.html"""
    return render_template("manage.html")

@app.route("/api/manage/accounts", methods=["GET"])
def api_manage_accounts():
    """Возвращаем список всех аккаунтов из PROFILE_PATH"""
    if not os.path.exists(PROFILE_PATH):
        return jsonify([])
    with open(PROFILE_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    # data — массив: [{ "Name":"...", "Id":"...", "Active":..., ...}, ...]
    # Возвращаем как есть
    return jsonify(data)


def _parse_json_field(raw_value, default):
    """
    Аккуратно парсим JSON из строки, возвращаем default при ошибке.
    Если значение уже dict/list — возвращаем как есть.
    """
    if isinstance(raw_value, (dict, list)):
        return raw_value
    try:
        return json.loads(raw_value)
    except Exception:
        return default

def _strip_start(logs):
    """
    Для промежуточных FIX’ов обрезает всё от запуска GnBots.exe и дальше,
    чтобы старт происходил только в самом конце.
    """
    out = []
    for line in logs:
        if line.startswith("Запускаем GnBots.exe"):
            break
        out.append(line)
    return out

# --- Маршруты/утилиты для работы со схемой и шаблонами ---

@app.route("/api/schema/get", methods=["GET"])
def api_schema_get():
    """Отдать текущую авто-схему (schema_cache.json)."""
    return jsonify(schema_load())

@app.route("/api/schema/rebuild", methods=["POST"])
def api_schema_rebuild():
    """Полностью перестроить схему по всем профилям в settings/profiles."""
    schema = {}
    changed = False
    for name in os.listdir(PROFILES_DIR):
        if not name.lower().endswith(".json"):
            continue
        full = os.path.join(PROFILES_DIR, name)
        prof = _json_read_or(full, {})
        steps = prof.get("Data") or []
        before = json.dumps(schema, ensure_ascii=False, sort_keys=True)
        schema_learn_from_steps(steps)
        schema = schema_load()
        after = json.dumps(schema, ensure_ascii=False, sort_keys=True)
        if before != after:
            changed = True
    return jsonify({"ok": True, "changed": changed, "size": len(schema)})


@app.route("/api/templates/check", methods=["GET"])
def api_templates_check():
    """Сверяет все шаблоны со схемой. ?refresh=1 — принудительный пересчёт."""

    refresh = request.args.get("refresh") == "1"

    payload = load_template_gap_cache()
    if refresh or not payload.get("checked_at"):
        payload = run_templates_schema_audit()
    else:
        payload["ok"] = not payload.get("gaps")

    return jsonify(payload)


@app.route("/api/templates/fix", methods=["POST"])
def api_templates_fix():
    """
    POST /api/templates/fix?dry_run=true|false

    Дополняет шаблоны недостающими ключами из схемы. В режиме dry-run возвращает diff
    без записи файлов, в обычном режиме перезаписывает шаблоны и сохраняет бэкап.
    """

    payload = request.get_json(silent=True) or {}
    dry_run_param = request.args.get("dry_run", payload.get("dry_run"))
    if isinstance(dry_run_param, bool):
        dry_run = dry_run_param
    else:
        dry_run = str(dry_run_param).lower() == "true"

    schema = schema_load()
    if not schema:
        return jsonify({"error": "schema_not_found"}), 400

    changes: list[dict] = []
    updated: list[str] = []
    backups: list[dict] = []
    skipped: list[dict] = []

    for name in sorted(os.listdir(TEMPLATES_DIR)):
        if not name.lower().endswith(".json"):
            continue

        full_path = os.path.join(TEMPLATES_DIR, name)
        template_steps = _json_read_or(full_path, None)
        ok, err = _validate_template_steps(template_steps)
        if not ok:
            skipped.append({"template": name, "reason": err})
            continue

        gaps = find_template_schema_gaps(template_steps, schema)
        if not gaps:
            continue

        fixed_steps = template_inflate_with_schema(template_steps, schema)
        diff = _diff_template_with_schema(template_steps, fixed_steps)
        changes.append({"template": name, "gaps": gaps, "diff": diff})

        if dry_run:
            continue

        try:
            backup_path = _backup_template_file(full_path)
            safe_write_json(full_path, fixed_steps)
            updated.append(name)
            if backup_path:
                backups.append({"template": name, "backup": backup_path})
        except Exception:
            skipped.append({"template": name, "reason": "write_failed"})
            app.logger.exception("Не удалось сохранить шаблон %s", name)

    audit = run_templates_schema_audit(schema) if not dry_run else load_template_gap_cache()

    if updated:
        app.logger.info("Templates fixed: %s", ", ".join(updated))

    return jsonify(
        {
            "ok": True,
            "dry_run": dry_run,
            "changes": changes,
            "updated": updated,
            "backups": backups,
            "skipped": skipped,
            "audit": audit,
        }
    )

@app.route("/api/templates/list", methods=["GET"])
def api_templates_list():
    aliases = _load_template_aliases()
    alias_targets: dict[str, list[str]] = {}
    for alias, target in aliases.items():
        alias_targets.setdefault(target, []).append(alias)

    arr = []
    base = []
    for name in sorted(os.listdir(TEMPLATES_DIR)):
        if not name.lower().endswith(".json"):
            continue
        full = os.path.join(TEMPLATES_DIR, name)
        steps = _json_read_or(full, [])
        steps_count = len(steps) if isinstance(steps, list) else None
        arr.append(
            {
                "name": name,
                "label": os.path.splitext(name)[0],
                "steps_count": steps_count,
                "aliases": alias_targets.get(name, []),
            }
        )

    return jsonify({"templates": arr, "aliases": aliases})


def _normalized_template_path(raw_name: str) -> t.Tuple[str, str]:
    """
    Проверяем имя шаблона и возвращаем (полный путь, безопасное имя).

    Исключаем попытки выхода из каталога с шаблонами и приводим имя к *.json.
    """
    if not raw_name:
        raise ValueError("empty template name")

    name = os.path.basename(raw_name.strip())
    if not name or name.startswith("."):
        raise ValueError("invalid template name")
    if not name.lower().endswith(".json"):
        name = f"{name}.json"

    full = os.path.abspath(os.path.join(TEMPLATES_DIR, name))
    base_dir = os.path.abspath(TEMPLATES_DIR)
    if not full.startswith(base_dir + os.sep):
        raise ValueError("invalid template name")

    return full, name


_ensure_builtin_templates()


@app.route("/api/templates/<path:template_name>", methods=["GET"])
def api_templates_get(template_name: str):
    """Возвращает содержимое выбранного шаблона."""
    try:
        aliases = _load_template_aliases()
        canon_name = _resolve_template_name(template_name, aliases)
        full_path, safe_name = _normalized_template_path(canon_name)
    except ValueError:
        return jsonify({"error": "invalid template name"}), 400

    if not os.path.isfile(full_path):
        return jsonify({"error": "template not found"}), 404

    steps = _json_read_or(full_path, [])
    if not isinstance(steps, list):
        return jsonify({"error": "invalid template format"}), 400

    aliases = _load_template_aliases()
    alias_names = [k for k, v in aliases.items() if _canonical_template_name(v) == safe_name]

    return jsonify(
        {
            "name": safe_name,
            "steps": steps,
            "steps_count": len(steps),
            "aliases": alias_names,
        }
    )


@app.route("/api/templates/<path:template_name>", methods=["PUT"])
def api_templates_put(template_name: str):
    """Создаёт или обновляет шаблон шагов."""
    try:
        full_path, safe_name = _normalized_template_path(
            _resolve_template_name(template_name)
        )
    except ValueError:
        return jsonify({"error": "invalid template name"}), 400

    payload = request.get_json(silent=True) or {}
    steps = payload.get("steps")
    if not isinstance(steps, list):
        return jsonify({"error": "steps must be an array"}), 400

    safe_write_json(full_path, steps)
    return jsonify({"ok": True, "name": safe_name, "steps_count": len(steps)})


@app.route("/api/templates/<path:template_name>", methods=["DELETE"])
def api_templates_delete(template_name: str):
    """Удаляет указанный шаблон."""
    try:
        aliases = _load_template_aliases()
        canon_name = _resolve_template_name(template_name, aliases)
        full_path, safe_name = _normalized_template_path(canon_name)
    except ValueError:
        return jsonify({"error": "invalid template name"}), 400

    if not os.path.isfile(full_path):
        return jsonify({"error": "template not found"}), 404

    os.remove(full_path)
    aliases = {k: v for k, v in aliases.items() if _canonical_template_name(v) != safe_name}
    _save_template_aliases(aliases)
    return jsonify({"ok": True, "name": safe_name, "aliases": aliases})


@app.route("/api/templates/<path:template_name>/rename", methods=["PATCH"])
def api_templates_rename(template_name: str):
    payload = request.get_json(silent=True) or {}
    new_name = (payload.get("new_name") or "").strip()
    if not new_name:
        return jsonify({"error": "new_name is required"}), 400

    aliases = _load_template_aliases()
    try:
        src_path, safe_src = _normalized_template_path(
            _resolve_template_name(template_name, aliases)
        )
        dst_path, safe_dst = _normalized_template_path(new_name)
    except ValueError:
        return jsonify({"error": "invalid template name"}), 400

    if not os.path.isfile(src_path):
        return jsonify({"error": "template not found"}), 404

    if os.path.exists(dst_path):
        return jsonify({"error": "target exists"}), 409

    os.rename(src_path, dst_path)

    updated_aliases = {}
    for alias, target in aliases.items():
        target_canon = _canonical_template_name(target)
        if target_canon == safe_src:
            updated_aliases[alias] = safe_dst
        else:
            updated_aliases[alias] = target_canon

    # сохраняем совместимость: старое имя становится алиасом
    updated_aliases[safe_src] = safe_dst
    _save_template_aliases(updated_aliases)

    return jsonify({"ok": True, "name": safe_dst, "aliases": updated_aliases})

@app.route("/api/templates/rehydrate", methods=["POST"])
def api_templates_rehydrate():
    """Дополнить ВСЕ шаблоны недостающими ключами из schema_cache.json."""
    schema = schema_load()
    updated = []
    for name in os.listdir(TEMPLATES_DIR):
        if not name.lower().endswith(".json"):
            continue
        p = os.path.join(TEMPLATES_DIR, name)
        tpl = _json_read_or(p, None)
        ok, _ = _validate_template_steps(tpl)
        if not ok:
            continue
        new_tpl = template_inflate_with_schema(tpl, schema)
        if new_tpl != tpl:
            safe_write_json(p, new_tpl)
            updated.append(name)
    return jsonify({"ok": True, "updated": updated})

@app.route("/api/manage/account/<acc_id>/settings", methods=["GET"])
def api_manage_account_settings(acc_id):
    # Читаем общий JSON
    if not os.path.exists(PROFILE_PATH):
        return jsonify({"error": "profile not found"}), 404
    with open(PROFILE_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Находим аккаунт
    acc = next((a for a in data if a.get("Id") == acc_id), None)
    if not acc:
        return jsonify({"error": "acc not found"}), 404

    # Парсим строку Data в JSON
    settings = _parse_json_field(acc.get("Data", "[]"), [])
    menu     = _parse_json_field(acc.get("MenuData", "{}"), {})
    try:
        schema_learn_from_steps(settings)
    except Exception:
        app.logger.exception("schema learn failed")
    return jsonify({"Data": settings, "MenuData": menu})

@app.route("/api/manage/account/<acc_id>/settings/<int:step_idx>", methods=["PUT"])
def api_manage_account_setting_step(acc_id, step_idx):
    """
    PUT /api/manage/account/<acc_id>/settings/<step_idx>
    payload может содержать:
      - Config: {ключ: новое_значение, …}
      - IsActive: true|false
      - ScheduleRules: [ …новый массив правил… ]
    """
    payload     = request.get_json(silent=True) or {}
    cfg_updates = payload.get("Config", {})
    new_active  = payload.get("IsActive", None)
    new_rules   = payload.get("ScheduleRules", None)

    # 1) читаем текущий профиль
    with open(PROFILE_PATH, "r", encoding="utf-8") as f:
        all_accs = json.load(f)

    # 2) ищем нужный аккаунт
    for acc in all_accs:
        if acc.get("Id") == acc_id:
            data_list = _parse_json_field(acc.get("Data", "[]"), [])
            if not isinstance(data_list, list):
                return jsonify({"error": "invalid data format"}), 400

            # проверяем step_idx
            if step_idx < 0 or step_idx >= len(data_list):
                return jsonify({"error": "step_idx out of range"}), 400

            step = data_list[step_idx]

            # 3a) обновляем активность
            if new_active is not None:
                step["IsActive"] = bool(new_active)

            # 3b) обновляем конфиг
            conf = step.get("Config", {})
            for key, val in cfg_updates.items():
                if isinstance(conf.get(key), dict) and "value" in conf[key]:
                    conf[key]["value"] = val
                else:
                    conf[key] = val

            # 3c) обновляем расписание, если передали
            if new_rules is not None:
                step["ScheduleRules"] = new_rules

            # 4) сохраняем обратно в JSON-профиль (компактно, без пробелов)
            acc["Data"] = json.dumps(
                data_list,
                ensure_ascii=False,
                separators=(',', ':')   # ← убираем лишние пробелы
            )
            break
    else:
        return jsonify({"error": "acc not found"}), 404

    # 5) перезаписываем файл
    with open(PROFILE_PATH, "w", encoding="utf-8") as f:
        json.dump(all_accs, f, indent=2, ensure_ascii=False)

    return jsonify({"status": "ok"})



@app.route("/api/fix/batch", methods=["POST"])
def api_fix_batch():
    """
    Принимает JSON вида {"acc_ids": ["id1","id2",...]},
    прогоняет do_fix_logic для каждого acc_id, но старт GnBots.exe
    (последняя часть логов) оставляет только для последнего аккаунта.
    """
    data = request.get_json() or {}
    ids  = data.get("acc_ids", [])
    backup_dir = (data.get("backup_dir") or "").strip()

    cfg_override = data.get("cfg_src_override") or (
        os.path.join(BACKUP_CONFIG_DST_ROOT, backup_dir) if backup_dir else None
    )

    if not ids:
        return jsonify({"error": "acc_ids missing or empty"}), 400

    batch_logs = []
    total = len(ids)
    for idx, acc_id in enumerate(ids):
        # собственно фиксим
        single = do_fix_logic(acc_id,
                      only_config=False,
                      cfg_src_override=cfg_override)
        # для всех кроме последнего — отрезаем часть со стартом
        if idx < total - 1:
            single = _strip_start(single)
        batch_logs.extend(single)

    return jsonify({"logs": batch_logs})


# ───── вставьте рядом с другими /api/fix/... роутами ─────
@app.route("/api/fix/replace_configs", methods=["POST"])
def api_replace_configs():
    """
    JSON {"backup_dir":"ДД__ММ__ГГГГ"}  – копирует ВСЕ config’ы.
             "" или параметра нет → стандартный SRC_VMS\\config
    """
    data = request.get_json() or {}
    bdir = data.get("backup_dir", "") or None
    logs = emergency_replace_configs(bdir)
    return jsonify({"logs": logs})


@app.route("/api/check_ld", methods=["POST"])
def api_check_ld():
    # полный путь до скрипта
    script_path = os.path.join(BASE_DIR, "LD_check.py")
    logs = []

    if not os.path.isfile(script_path):
        return jsonify({"logs": [f"LD_check.py не найден по пути {script_path}"]}), 404

    try:
        # запускаем скрипт по абсолютному пути
        proc = subprocess.Popen(
            ["python", script_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True
        )
        # читаем построчно
        for line in proc.stdout:
            logs.append(line.rstrip())
        proc.wait()
    except Exception as e:
        logs.append(f"Error running LD_check.py: {e}")

    return jsonify({"logs": logs})


@app.route("/api/crashedEmus")
def api_crashed_emu():
    path = r"C:\LDPlayer\ldChecker\crashed.json"
    if not os.path.exists(path):
        return jsonify([])
    with open(path, "r", encoding="utf-8") as f:
        arr = json.load(f)
    return jsonify(arr)  # например ["leidian5.config", "leidian36.config"]




@app.route("/api/paths", methods=["GET"])
def api_get_paths():
    return jsonify({
        "paths": CONFIG,
        "servers": get_configured_servers(),
    })

@app.route("/api/paths", methods=["PUT"])
def api_put_paths():
    data = request.get_json() or {}
    paths = data.get("paths", {}) or {}
    servers = data.get("servers", []) or []

    for k in list(CONFIG):
        if k in paths and isinstance(paths[k], str):
            CONFIG[k] = paths[k]

    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(CONFIG, f, ensure_ascii=False, indent=2)

    # Обновляем адреса серверов (шифруем перед записью)
    global SERVERS
    SERVERS = save_server_links(servers)

    return jsonify({"status": "ok", "paths": CONFIG, "servers": SERVERS})

# === I18N endpoints (Flask) =======================================
from flask import Blueprint, request, jsonify
from pathlib import Path
import json, tempfile, os

i18n_bp = Blueprint('i18n', __name__)

I18N_DIR = Path("./static/i18n")
I18N_DIR.mkdir(parents=True, exist_ok=True)
def _i18n_path(lang:str)->Path:
    safe = "".join(c for c in lang if c.isalnum() or c in ('-','_')).strip() or "ru"
    return I18N_DIR / f"{safe}.json"

@i18n_bp.route("/api/manage/i18n", methods=["GET"])
def get_i18n():
    lang = request.args.get("lang", "ru")
    p = _i18n_path(lang)
    if not p.exists():
        return jsonify({"script_labels":{}, "config_labels":{}, "option_labels":{}, "order_map":{}})
    try:
        with p.open("r", encoding="utf-8") as f:
            data = json.load(f)
        for k in ("script_labels","config_labels","option_labels","order_map"):
            data.setdefault(k, {})
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": f"i18n read error: {e}"}), 500

@i18n_bp.route("/api/manage/i18n", methods=["PUT"])
def put_i18n():
    lang = request.args.get("lang", "ru")
    p = _i18n_path(lang)
    payload = request.get_json(force=True, silent=True) or {}
    for k in ("script_labels","config_labels","option_labels","order_map"):
        payload.setdefault(k, {})
    fd, tmp_name = tempfile.mkstemp(prefix="i18n_", suffix=".json", dir=str(I18N_DIR))
    os.close(fd)
    try:
        with open(tmp_name, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        os.replace(tmp_name, p)
        return jsonify({"ok": True})
    except Exception as e:
        try: os.remove(tmp_name)
        except: pass
        return jsonify({"error": f"i18n write error: {e}"}), 500

# === регистрация блюпринта ===
# app.register_blueprint(i18n_bp)


# ДОБАВИТЬ где-нибудь рядом с другими /api/… роутами
@app.route("/api/backup_dirs")
def api_backup_dirs():
    """
    Возвращает массив из максимум 5 последних папок-дат
    внутри C:\\LD_backup\\configs\\
    """
    root = BACKUP_CONFIG_DST_ROOT
    if not os.path.isdir(root):
        return jsonify([])

    def is_date_dir(d):
        try:
            datetime.strptime(d, "%d__%m__%Y")
            return True
        except ValueError:
            return False

    dirs = [d for d in os.listdir(root)
            if os.path.isdir(os.path.join(root, d)) and is_date_dir(d)]
    dirs.sort(key=lambda s: datetime.strptime(s, "%d__%m__%Y"), reverse=True)
    return jsonify(dirs[:5])


@app.route("/api/forceRefreshToday", methods=["POST"])
def api_force_refresh_today():
    """
    Полностью перечитывает сегодняшние логи, игнорируя offsets,
    и снова парсит их в базу.
    """
    today_str = datetime.now().strftime("%Y%m%d")

    # 1) Сбрасываем offsets для любых файлов, у которых fname.startswith("bot" + today_str)
    conn_log = open_db(LOGS_DB)
    c_log = conn_log.cursor()
    # Получим все offsets
    rows = c_log.execute("SELECT filename FROM files_offset").fetchall()
    for (fname,) in rows:
        if fname.startswith("bot"+today_str) and fname.endswith(".txt"):
            c_log.execute("DELETE FROM files_offset WHERE filename=?", (fname,))
    conn_log.commit()
    conn_log.close()

    # 2) parse_logs() заново всё перечитает
    parse_logs()

    return {"status":"ok","message":"All logs for today re-read.", "timestamp": datetime.now().isoformat()}

@app.route("/api/manage/account/<acc_id>", methods=["PUT"])
def api_manage_account_update(acc_id):
    """Получаем {Active:true/false}, записываем в JSON."""
    req_data = request.json
    new_active = bool(req_data.get("Active", False))

    # Читаем JSON
    if not os.path.exists(PROFILE_PATH):
        return jsonify({"error":"profile not found"}),404
    with open(PROFILE_PATH,"r",encoding="utf-8") as f:
        data = json.load(f)  # массив

    # Ищем нужный аккаунт
    found = None
    for acc in data:
        if acc.get("Id")==acc_id:
            found=acc
            break
    if not found:
        return jsonify({"error":"acc not found"}),404

    # меняем
    found["Active"] = new_active

    # сохраняем
    with open(PROFILE_PATH,"w",encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

    return jsonify({"status":"ok","acc_id":acc_id,"Active":new_active})


@app.route("/api/resources")
def api_resources():
    acts= load_profiles()
    active_ids= {a["Id"] for a in acts}
    inst_map= {a["Id"]: a.get("InstanceId",-1) for a in acts}

    conn= open_db(RESOURCES_DB)
    c= conn.cursor()
    rows= c.execute("SELECT id,nickname,food,wood,stone,gold,gems,last_updated FROM resources").fetchall()
    conn.close()

    today_str= datetime.now().strftime("%Y-%m-%d")
    conn= open_db(RESOURCES_DB)
    c= conn.cursor()
    base_rows= c.execute("""
      SELECT id,food,wood,stone,gold,gems
      FROM daily_baseline
      WHERE baseline_date=?
    """,(today_str,)).fetchall()
    base_map={}
    for br in base_rows:
        base_map[br[0]]= (br[1],br[2],br[3],br[4],br[5])
    conn.close()

    totf=0; totw=0; tots=0; totg=0; totm=0
    accounts=[]
    for (acc_id,nick,f,w,s,g,m,lastupd) in rows:
        if acc_id not in active_ids:
            continue
        totf+=f; totw+=w; tots+=s; totg+=g; totm+=m
        bf=bw=bs=bg=bgems=0
        if acc_id in base_map:
            bf,bw,bs,bg,bgems= base_map[acc_id]
        gf= f-bf; gw= w-bw; gs= s-bs; gg= g-bg
        dayGain= gf+gw+gs+gg

        # Формируем food_view...
        def format_view(curVal,diffVal):
            base= shorten_number(curVal)
            if diffVal==0: 
                return base
            sign="+" if diffVal>0 else "-"
            dif= shorten_number(abs(diffVal))
            return f"{base}<span class='gainValue'>{sign}{dif}</span>"

        fv= format_view(f, gf)
        wv= format_view(w, gw)
        sv= format_view(s, gs)
        gv= format_view(g, gg)
        mv= shorten_number(m)
        #   —‑ тариф из account_meta
        if 'meta_map' not in globals():
            connT = open_db(RESOURCES_DB)
            meta_map = dict(connT.execute("SELECT id, tariff_rub FROM account_meta").fetchall())
            connT.close()
        tariff = meta_map.get(acc_id, 0)
        tariff_view = "0₽" if tariff is None else f"{tariff:,}₽".replace(",", " ")


        accounts.append({
          "id": acc_id,
          "nickname": nick,
          "instanceId": inst_map.get(acc_id,-1),

          "food_raw": f,
          "wood_raw": w,
          "stone_raw": s,
          "gold_raw": g,
        #   "gems_raw": m,

          "food_view": fv,
          "wood_view": wv,
          "stone_view": sv,
          "gold_view": gv,
        #   "gems_view": mv,
          "tariff_raw": tariff,
          "tariff_view": tariff_view,


          "today_gain": shorten_number(dayGain),
          "last_updated": lastupd
        })

    return jsonify({
      "accounts": accounts,
      "account_count": len(accounts),
      "totals":{
        "food": shorten_number(totf),
        "wood": shorten_number(totw),
        "stone": shorten_number(tots),
        "gold": shorten_number(totg),
        "gems": shorten_number(totm)
      }
    })

@app.route("/api/stop", methods=["POST"])
def api_stop():
    logs= do_stop_logic()
    return {"status":"ok","logs": logs}

@app.route("/api/reboot", methods=["POST"])
def api_reboot():
    logs= do_reboot_logic()
    return {"status":"ok","logs": logs}

@app.route("/api/serverStop", methods=["POST"])
def api_server_stop():
    name = request.args.get("name", "")
    server = next((s for s in get_configured_servers() if s["name"] == name), None)
    if not server:
        return jsonify({"error": "server not found"}), 404
    return jsonify({"error": "SSH управление отключено"}), 400

@app.route("/api/serverReboot", methods=["POST"])
def api_server_reboot():
    name = request.args.get("name", "")
    server = next((s for s in get_configured_servers() if s["name"] == name), None)
    if not server:
        return jsonify({"error": "server not found"}), 404
    return jsonify({"error": "SSH управление отключено"}), 400

@app.route("/api/rs_cleanup", methods=["POST"])
def api_rs_cleanup():
    """
    Удаляет снапшоты за последние N дней (по умолчанию 7), чтобы пересчитать их заново из логов.
    Вызывай перед 'Перечитать логи'.
    """
    try:
        days = int(request.args.get("days", 7))
    except:
        days = 7
    conn = open_db(LOGS_DB)
    cut = (datetime.now(timezone.utc).astimezone() - timedelta(days=days))
    cut_s = cut.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + cut.strftime(" %z")
    conn.execute("DELETE FROM resource_snapshots WHERE dt >= ?", (cut_s,))
    conn.commit()
    conn.close()
    return jsonify({"ok": True, "deleted_since": cut_s, "days": days})


# ── после других @app.route("/api/manage/...") ──
@app.route("/api/manage/copy_settings", methods=["POST"])
def api_copy_settings():
    """
    JSON: {source_id:str, dest_ids:[str,…]}
    Копирует ДАННЫЕ (Data) источника во все dest_ids.
    """
    payload   = request.get_json() or {}
    src_id    = payload.get("source_id")
    dest_ids  = payload.get("dest_ids") or []
    if not src_id or not dest_ids:
        return jsonify({"err":"bad"}),400

    with open(PROFILE_PATH, "r", encoding="utf-8") as f:
        prof = json.load(f)

    # находим источник
    src = next((a for a in prof if a.get("Id")==src_id), None)
    if not src:
        return jsonify({"err":"src not found"}),404

    # Нормализуем JSON-текст Data источника (убираем лишние пробелы).
    src_data_raw = src.get("Data", "[]")
    try:
        src_data_norm = json.dumps(
            json.loads(src_data_raw),
            ensure_ascii=False,
            separators=(',', ':')  # ← компактный вид: без пробелов
        )
    except Exception:
        # Если вдруг не распарсилось — переносим как есть.
        src_data_norm = src_data_raw

    for acc in prof:
        if acc.get("Id") in dest_ids:
            # копируем ТОЛЬКО Data (шаги), MenuData не трогаем
            acc["Data"] = src_data_norm

    with open(PROFILE_PATH, "w", encoding="utf-8") as f:
        json.dump(prof, f, ensure_ascii=False, indent=2)

    return jsonify({"status":"ok"})


@app.route("/api/problems/summary")
def api_problems_summary():
    """Возвращает компактную сводку ошибок из LD_problems и наблюдений по логам."""

    data = _safe_json_load(LD_PROBLEMS_SUMMARY_PATH) or {}
    gather_alerts = _collect_gather_watch()
    inactive_alerts = _load_inactive_watch()

    accounts = []
    if isinstance(data.get("accounts"), list):
        accounts.extend(data.get("accounts") or [])

    accounts.extend(gather_alerts)
    accounts.extend(inactive_alerts)

    payload = {
        "server": data.get("server") or SERVER_NAME,
        "generated_at": data.get("generated_at"),
        "total_accounts": data.get("total_accounts", 0),
        "total_problems": data.get("total_problems", 0) + len(inactive_alerts),
        "accounts": accounts,
    }

    if not payload["generated_at"] and accounts:
        payload["generated_at"] = datetime.now().isoformat()

    return jsonify(payload)


@app.route("/api/logstatus")
def api_logstatus():
    acts = load_profiles()
    active_ids = {acc["Id"] for acc in acts}

    conn = open_db(RESOURCES_DB)
    c = conn.cursor()
    rows = c.execute("SELECT id, nickname, food, wood, stone, gold, gems FROM resources").fetchall()
    conn.close()

    today_str = datetime.now().strftime("%Y-%m-%d")
    conn = open_db(RESOURCES_DB)
    c = conn.cursor()
    base_rows = c.execute("""
      SELECT id, food, wood, stone, gold, gems
      FROM daily_baseline
      WHERE baseline_date=?
    """,(today_str,)).fetchall()
    base_map={}
    for (bid, bf, bw, bs, bg, bgm) in base_rows:
        base_map[bid] = (bf, bw, bs, bg, bgm)
    conn.close()

    status = {}
    for (acc_id, nick, f, w, s, g, m) in rows:
        if acc_id not in active_ids:
            continue
        
        bf=bw=bs=bg=bgem=0
        if acc_id in base_map:
            bf,bw,bs,bg,bgem = base_map[acc_id]
        gf = f - bf
        gw = w - bw
        gs = s - bs
        gg = g - bg
        dayGain = gf+gw+gs+gg
        zeroGain = (dayGain == 0)

        status[acc_id] = {
          "nickname": nick,
          "zeroGain": zeroGain,
          "hasNoMoreMarches": False
        }

    connL = open_db(LOGS_DB)
    cL = connL.cursor()
    tday = datetime.now().strftime("%Y-%m-%d")
    for acid in status.keys():
        rows_m = cL.execute("""
          SELECT raw_line
          FROM cached_logs
          WHERE acc_id=?
          ORDER BY id DESC
          LIMIT 1000
        """,(acid,)).fetchall()
        foundNoMore=False
        foundUpdate=False
        for (line,) in rows_m:
            if line.startswith(tday):
                ll = line.lower()
                if ("no more marches left" in ll) or ("reached maximum of marches" in ll):
                    foundNoMore = True
                if "update the game" in ll:
                    foundUpdate = True    
                    break
        status[acid]["hasNoMoreMarches"] = foundNoMore
        status[acid]["hasUpdateGame"] = foundUpdate
    connL.close()

    return jsonify(status)


# Принимаем и GET, и POST. Читаем JSON-тело или query-параметры.
@app.route("/api/fix/do", methods=["GET", "POST"])
def api_fix_do():
    data = request.get_json(silent=True) or request.args

    acc_id_raw   = (data.get("acc_id") or "").strip()
    only_raw     = str(data.get("config_only", "0")).strip().lower()
    backup_dir   = (data.get("backup_dir") or "").strip()
    # Если явно передали cfg_src_override — используем его; иначе построим из backup_dir
    cfg_override = data.get("cfg_src_override") or (
        os.path.join(BACKUP_CONFIG_DST_ROOT, backup_dir) if backup_dir else None
    )

    only_config = (only_raw in ("1", "true", "yes", "on"))
    if not acc_id_raw:
        return jsonify({"error": "acc_id required"}), 400

    logs = do_fix_logic(
        acc_id_raw,
        only_config=only_config,
        cfg_src_override=cfg_override
    )
    return jsonify({"ok": True, "logs": logs})


@app.route("/api/fix/config_batch", methods=["POST"])
def api_fix_config_batch():
    data       = request.get_json() or {}
    ids        = data.get("acc_ids", [])
    backup_dir = (data.get("backup_dir") or "").strip()

    cfg_override = data.get("cfg_src_override") or (
        os.path.join(BACKUP_CONFIG_DST_ROOT, backup_dir) if backup_dir else None
    )

    if not ids:
        return jsonify({"error": "acc_ids missing or empty"}), 400

    out = []
    for acc_id in ids:
        out += do_fix_logic(
            acc_id,
            only_config=True,
            cfg_src_override=cfg_override
        )
    return jsonify({"ok": True, "logs": out})


@app.route("/api/logs")
def api_logs():
    acc_id= request.args.get("acc_id")
    if not acc_id:
        return {"error":"no acc_id"},400
    conn= open_db(LOGS_DB)
    c= conn.cursor()
    rows= c.execute("""
      SELECT dt, raw_line
      FROM cached_logs
      WHERE acc_id=?
      ORDER BY id DESC
      LIMIT 300
    """,(acc_id,)).fetchall()
    conn.close()

    lines=[]
    for (dt_part, ls) in rows:
        if "[DBG]" in ls:
            continue
        lines.append(transformLogLine(dt_part, ls))
    lines.reverse()
    if len(lines)>290:
        lines= lines[-290:]
    return {"acc_id":acc_id,"logs": lines}

@app.route("/api/schema", methods=["GET"])
def api_get_schema():
    schema = _ensure_schema()
    age = int(time.time()) - _schema_cache["built_at"]
    return jsonify({"schema": schema, "built_at": _schema_cache["built_at"], "age_sec": age})

@app.route("/api/schema/refresh", methods=["POST"])
def api_refresh_schema():
    _ensure_schema(force=True)
    return jsonify({"ok": True, "built_at": _schema_cache["built_at"]})


if __name__=="__main__":
    if not os.path.exists(RESOURCES_DB):
        print("Создаём базу ресурсов:", RESOURCES_DB)
    init_resources_db()

    if not os.path.exists(LOGS_DB):
        print("Создаём базу логов:", LOGS_DB)
    init_logs_db()

    health_check()
    init_resources_db()
    init_accounts_db()

    sync_account_meta()
    parse_logs()

    ensure_today_backups()      # ➟ создаст бэкапы, если их ещё нет за сегодня
    run_templates_schema_audit()
    _schedule_daily_backups()   # ➟ запустит фоновый планировщик на полуночь
    _schedule_pay_notifications()  # 09:00 & 18:00 Telegram-оповещения
    _schedule_inactive_checker()   # ← запуск «монитора 15 ч»



    LAST_UPDATE_TIME= datetime.now(timezone.utc)
    app.run(debug=True, host="0.0.0.0", port=5001)
