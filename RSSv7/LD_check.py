# LD_check.py — автофикc «Починить всё» при обнаружении слётов
# Запуск от имени администратора сохранён.

import os
import re
import sys
import json
import time
import ctypes
import asyncio
import platform
import requests                     # ← для вызова /api/fix/config_batch
from urllib.parse import urlparse, urlunparse
from typing import Dict, Tuple, Optional, List

# Telegram (asynchronous API)
from telegram import Bot
from telegram.error import TelegramError

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

# ─────────────────────────────────────────────────────────────
# Заголовок окна и автоповышение до Админа
# ─────────────────────────────────────────────────────────────
title = "LD_Check"
if sys.platform == "win32":
    ctypes.windll.kernel32.SetConsoleTitleW(title)

def is_admin() -> bool:
    try:
        return bool(ctypes.windll.shell32.IsUserAnAdmin())
    except Exception:
        return False

if sys.platform == "win32" and not is_admin():
    script = os.path.abspath(sys.argv[0])
    params = " ".join([f'"{script}"'] + [f'"{arg}"' for arg in sys.argv[1:]])
    ctypes.windll.shell32.ShellExecuteW(None, "runas", sys.executable, params, None, 1)
    sys.exit(0)


def _ensure_utf8_stdio() -> None:
    """
    Переконфигурируем stdout/stderr в UTF-8 с заменой, чтобы не падать на cp1252/cp866.
    """
    for name in ("stdout", "stderr"):
        stream = getattr(sys, name, None)
        if not stream:
            continue
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, ValueError):
            try:
                fd = stream.fileno()
                setattr(
                    sys,
                    name,
                    open(fd, mode=stream.mode, encoding="utf-8", errors="replace", buffering=1),
                )
            except Exception:
                pass


_ensure_utf8_stdio()

# ─────────────────────────────────────────────────────────────
# Пути/конфиги (оставил ваши дефолты)
# ─────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "config.json")
DEFAULT_CONFIG_FOLDER = r'C:\LDPlayer\LDPlayer9\vms\config'
DEFAULT_PROFILE_FILE = r'C:/Program Files/GnBots/profiles/FRESH_NOX.json'
crashed_file = r'C:\LDPlayer\ldChecker\crashed.json'  # для UI (цвет кнопок)
crashed_alert_state_file = os.getenv(
    "LDCHECK_ALERT_STATE_FILE",
    r"C:\LDPlayer\ldChecker\crashed_alert_state.json",
)
ALERT_AFTER_RUNS = int(os.getenv("LDCHECK_ALERT_AFTER_RUNS", "2"))
ALERT_REMINDER_HOURS = int(os.getenv("LDCHECK_ALERT_REMINDER_HOURS", "24"))


def _load_rss_config(path: str) -> Dict[str, str]:
    """Читает config.json из rsscounter и возвращает словарь (или пустой)."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        print(f"[INFO] config.json не найден по пути {path}, беру дефолты.")
    except Exception as err:
        print(f"[WARN] Не удалось прочитать config.json: {err}")
    return {}


rss_config = _load_rss_config(CONFIG_PATH)
config_folder_from_cfg = rss_config.get("DST_VMS")
config_folder = os.getenv("LDCHECK_CONFIG_FOLDER")
if not config_folder and config_folder_from_cfg:
    config_folder = os.path.join(config_folder_from_cfg, "config")
config_folder = config_folder or DEFAULT_CONFIG_FOLDER

profile_file = os.getenv("LDCHECK_PROFILE_FILE") or rss_config.get("PROFILE_PATH")
profile_file = profile_file or DEFAULT_PROFILE_FILE

# [SECURITY] Telegram-секреты читаем только из обязательных env-переменных.
TELEGRAM_TOKEN_ENV = "RSSV7_LD_CHECK_BOT_TOKEN"
TELEGRAM_CHAT_ID_ENV = "RSSV7_LD_CHECK_CHAT_ID"


def require_env(name: str) -> str:
    """[SECURITY] Возвращает обязательную env-переменную или бросает понятную ошибку."""
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Не задана обязательная переменная окружения: {name}")
    return value


telegram_token: str | None = None
chat_id: str | None = None


def get_telegram_config() -> tuple[str, str]:
    """[SECURITY] Ленивая загрузка Telegram-конфига без падения при импорте модуля."""
    return require_env(TELEGRAM_TOKEN_ENV), require_env(TELEGRAM_CHAT_ID_ENV)

def normalize_api_base(raw_url: str) -> str:
    """Возвращает корневой URL RSSv7 для вызовов API автофикса."""
    value = (raw_url or "http://127.0.0.1:5001").strip().rstrip("/")
    parsed = urlparse(value)
    if not parsed.scheme or not parsed.netloc:
        return value

    # В DASH_API иногда ошибочно кладут ссылку на страницу /fix или конкретный API-route.
    # Для автофикса нужен именно корень backend, иначе POST может уйти в HTML-страницу и дать 405.
    path = parsed.path.rstrip("/")
    if path in ("/fix", "/api/fix/config_batch") or path.startswith("/api/fix/"):
        cleaned = parsed._replace(path="", params="", query="", fragment="")
        return urlunparse(cleaned).rstrip("/")

    return value


# Бэкенд Dashboard (куда «эмулируем клик»)
API_BASE = normalize_api_base(os.getenv("DASH_API", "http://127.0.0.1:5001"))

# URL-ы серверов (для кликабельной ссылки в Telegram)
SERVERS: Dict[str, str] = {
    "208": "https://hotly-large-coral.cloudpub.ru/",
    "F99": "https://tastelessly-quickened-chub.cloudpub.ru/",
    "R9": "https://creakily-big-spaniel.cloudpub.ru/",
    "RSS":  "https://fiendishly-awake-stickleback.cloudpub.ru/",
}
FIX_PAGE = "fix"                                  # страница фикса
ENV_SERVER_NAME = (os.getenv("SERVER_NAME") or "").strip().upper()

# ─────────────────────────────────────────────────────────────
# Хелперы
# ─────────────────────────────────────────────────────────────
def load_profiles(path: str):
    """Читает профили ботов из profiles.json."""
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data if isinstance(data, list) else [data]
    except Exception as e:
        print(f"[ERR] Не смог загрузить профили: {e}")
        return []

def build_maps(profiles) -> Tuple[Dict[str, str], set, Dict[str, str]]:
    """
    Возвращает три структуры:
      inst2name:   str(InstanceId) → Name
      active_ids:  множество активных InstanceId (строк)
      inst2acc:    str(InstanceId) → Id (GUID аккаунта)
    """
    inst2name: Dict[str, str] = {}
    active_ids = set()
    inst2acc: Dict[str, str] = {}

    for rec in profiles:
        if not isinstance(rec, dict):
            continue
        inst_id = rec.get('InstanceId')
        name    = rec.get('Name')
        acc_id  = rec.get('Id')
        if inst_id is None:
            continue
        sid = str(inst_id)
        if name is not None:
            inst2name[sid] = str(name)
        if rec.get('Active'):
            active_ids.add(sid)
        if acc_id:
            inst2acc[sid] = str(acc_id)

    return inst2name, active_ids, inst2acc

def find_key_recursive(d, target_key):
    """Ищет ключ target_key в глубине JSON-структуры (точное имя)."""
    if isinstance(d, dict):
        for k, v in d.items():
            if k == target_key:
                return v
            if isinstance(v, (dict, list)):
                res = find_key_recursive(v, target_key)
                if res is not None:
                    return res
    elif isinstance(d, list):
        for item in d:
            res = find_key_recursive(item, target_key)
            if res is not None:
                return res
    return None

def extract_instance_id(fname: str) -> Optional[str]:
    """Из «leidian36.config» вернёт «36», иначе None."""
    m = re.search(r"leidian(\d+)\.config", fname, re.IGNORECASE)
    return m.group(1) if m else None

def normalize_base_url(u: str) -> str:
    return (u or "").rstrip("/") + "/"

def detect_server_name_fallback() -> str:
    host = platform.node().upper()
    for key in SERVERS.keys():
        if key in host:
            return key
    return "UNKNOWN srv"

def resolve_server_name() -> str:
    return ENV_SERVER_NAME or detect_server_name_fallback()

def make_fix_url(server_name: str) -> Optional[str]:
    base = SERVERS.get(server_name.upper())
    if not base:
        return None
    return normalize_base_url(base) + FIX_PAGE

def health_check(verbose: bool=False) -> None:
    global telegram_token, chat_id
    if telegram_token is None or chat_id is None:
        telegram_token, chat_id = get_telegram_config()

    problems = []
    warnings = []
    if not os.path.isdir(config_folder):
        problems.append(f"CONFIG_FOLDER not found: {config_folder}")
    if not os.path.isfile(profile_file):
        problems.append(f"PROFILE not found: {profile_file}")
    if not telegram_token or len(telegram_token) < 20:
        problems.append(f"TELEGRAM_TOKEN is empty/invalid (проверь {TELEGRAM_TOKEN_ENV})")
    if not chat_id or not str(chat_id).strip():
        problems.append(f"TELEGRAM_CHAT_ID is empty (проверь {TELEGRAM_CHAT_ID_ENV})")

    srv = resolve_server_name()
    if not make_fix_url(srv):
        warnings.append(f"Server URL not resolved for '{srv}' (будет использоваться заглушка FIX)")

    if problems:
        print("[HEALTH-CHECK FAIL]")
        for p in problems:
            print(" -", p)
        sys.exit(1)
    if warnings:
        print("[HEALTH-CHECK WARN]")
        for w in warnings:
            print(" -", w)
    if verbose:
        print(f"[HEALTH-OK] server={srv} fix_url={make_fix_url(srv)}")
        print(f"[HEALTH-OK] config={config_folder}")
        print(f"[HEALTH-OK] profile={profile_file}")

# ─────────────────────────────────────────────────────────────
# Сканер конфигов (общий для обычного и «тихого» прохода)
# ─────────────────────────────────────────────────────────────
def collect_crashed(inst2name: Dict[str, str], active_inst_ids: set) -> Tuple[List[str], List[str]]:
    """
    Возвращает:
      crashed_files — имена файлов (leidianXX.config) для UI,
      crashed_names — человеко-читаемые имена для Telegram.
    """
    crashed_files: List[str] = []
    crashed_names: List[str] = []

    for root, _, files in os.walk(config_folder):
        for fname in files:
            if not fname.endswith('.config'):
                continue
            if fname.lower() == 'leidians.config':
                continue

            inst_id = extract_instance_id(fname)
            if not inst_id or inst_id not in active_inst_ids:
                continue

            fpath = os.path.join(root, fname)
            try:
                with open(fpath, 'r', encoding='utf-8') as f:
                    cfg = json.load(f)
            except Exception as e:
                print(f"[ERR] Ошибка чтения {fpath}: {e}")
                continue

            player_name = find_key_recursive(cfg, 'statusSettings.playerName') or ''
            player_name = str(player_name).strip()
            is_bad = (not player_name) or (player_name.lower() == 'ldplayer')

            if is_bad:
                crashed_files.append(fname)
                crashed_names.append(inst2name.get(inst_id, f'inst{inst_id}'))

    return crashed_files, crashed_names

def write_crashed_file(files: List[str]) -> None:
    try:
        os.makedirs(os.path.dirname(crashed_file), exist_ok=True)
        with open(crashed_file, 'w', encoding='utf-8') as f:
            json.dump(files, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[ERR] Не могу записать {crashed_file}: {e}")


def _load_crashed_alert_state() -> dict:
    try:
        with open(crashed_alert_state_file, encoding="utf-8") as src:
            data = json.load(src)
        return data if isinstance(data, dict) else {}
    except (FileNotFoundError, ValueError, OSError):
        return {}


def _save_crashed_alert_state(state: dict) -> None:
    try:
        os.makedirs(os.path.dirname(crashed_alert_state_file) or ".", exist_ok=True)
        with open(crashed_alert_state_file, "w", encoding="utf-8") as out:
            json.dump(state, out, ensure_ascii=False, indent=2)
    except OSError as err:
        print(f"[WARN] Не удалось сохранить состояние алертов: {err}")


async def notify_persistent_crashes(
    bot: Bot,
    server_name: str,
    fix_url: str,
    files: List[str],
    names: List[str],
) -> None:
    """Шлёт один алерт только по слётам, пережившим автофикс несколько раз."""

    previous = _load_crashed_alert_state()
    now = time.time()
    reminder_seconds = max(1, ALERT_REMINDER_HOURS) * 3600
    current = {}
    due_names = []

    for file_name, human_name in zip(files, names):
        old = previous.get(file_name, {}) if isinstance(previous.get(file_name), dict) else {}
        consecutive = int(old.get("consecutive") or 0) + 1
        last_alert_at = float(old.get("last_alert_at") or 0)
        due = consecutive >= max(1, ALERT_AFTER_RUNS) and now - last_alert_at >= reminder_seconds
        current[file_name] = {
            "name": human_name,
            "first_seen_at": old.get("first_seen_at") or now,
            "last_seen_at": now,
            "last_alert_at": now if due else last_alert_at,
            "consecutive": consecutive,
        }
        if due:
            due_names.append(human_name)

    _save_crashed_alert_state(current)

    if not due_names:
        return

    unique_names = sorted(set(due_names))
    summary = ", ".join(unique_names)
    try:
        await bot.send_message(
            chat_id=chat_id,
            text=(
                f"{server_name}⚠️ Эмуляторы не восстановились после автофикса "
                f"в {max(1, ALERT_AFTER_RUNS)} проверках: {summary}\n"
                f"🔧 FIX: {fix_url}"
            ),
        )
    except TelegramError as err:
        print(f"[TG error] {err}")

# ─────────────────────────────────────────────────────────────
# Основная логика: проверить, отписать в TG, автофикс (кнопка!), перескан
# ─────────────────────────────────────────────────────────────
async def check_all_configs_and_notify():
    health_check()  # лог и стоп при критичных проблемах
    bot = Bot(token=telegram_token)

    profiles = load_profiles(profile_file)
    inst2name, active_inst_ids, inst2acc = build_maps(profiles)
    if not active_inst_ids:
        print('[INFO] Нет активных аккаунтов в профиле. Нечего проверять.')
        return

    server_name = resolve_server_name()
    msg_prefix = server_name
    fix_url = make_fix_url(server_name) or "FIX"

    # 1) Сканируем конфиги
    crashed_files, crashed_names = collect_crashed(inst2name, active_inst_ids)

    # 2) Пишем файл для UI
    write_crashed_file(crashed_files)

    # Технические слёты сначала пытаемся исправить молча. Telegram нужен только
    # если один и тот же слёт пережил несколько проверок и автофикс не помог.
    if crashed_names:
        # АВТОФИКС = эмуляция кнопки «Починить всё»
        #    Собираем acc_id по инстансам из профиля и шлём в бекенд.
        acc_ids: List[str] = []
        for fname in crashed_files:
            inst = extract_instance_id(fname)
            if inst and inst in inst2acc:
                acc_ids.append(inst2acc[inst])

        if acc_ids:
            try:
                # ЭТО и есть «нажатие кнопки Починить всё» (фронт бьёт в тот же роут)
                # /api/fix/config_batch → копирует ТОЛЬКО .config’и (без полного переноса ВМ)
                url = f"{API_BASE}/api/fix/config_batch"
                print(f"[AUTO-FIX] POST {url}")
                resp = requests.post(url, json={"acc_ids": acc_ids}, timeout=600)
                if resp.ok:
                    print(f"[AUTO-FIX] OK: fixed {len(acc_ids)} accounts")
                else:
                    print(f"[AUTO-FIX] HTTP {resp.status_code}: {resp.text}")
            except Exception as e:
                print(f"[AUTO-FIX] error: {e}")

        # Тихий повторный скан обновляет UI и только затем решает, нужен ли алерт.
        time.sleep(2)
        unresolved_files, unresolved_names = collect_crashed(inst2name, active_inst_ids)
        write_crashed_file(unresolved_files)
        if not unresolved_files:
            print("[VERIFY] Повторная проверка: слётов не обнаружено (ок).")
        else:
            print(f"[VERIFY] После автофикса всё ещё слетевшие: {unresolved_files}")
        await notify_persistent_crashes(
            bot,
            msg_prefix,
            fix_url,
            unresolved_files,
            unresolved_names,
        )

    else:
        _save_crashed_alert_state({})
        print('[INFO] Слетевших активных эмуляторов не найдено.')

# ─────────────────────────────────────────────────────────────
# Точка входа
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    _load_root_env()
    asyncio.run(check_all_configs_and_notify())

