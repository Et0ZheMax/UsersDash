#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
LD_problems.py — мониторинг логов GnBots/LDPlayer

Функции
───────
1. Мгновенные алерты (regex_list).
2. «Кластеры» ошибок за окно времени (cluster_regex_list), теперь с указанием фразы ошибки.
3. ≥4 Game Crash за 10 мин.
4. Health-check.
5. Подстановка имён аккаунтов вместо raw-ID.
"""

import os
import json
import asyncio
from datetime import datetime, timedelta
from collections import Counter, defaultdict
from pathlib import Path
import re
import shutil
import socket
import ctypes
import sys
import tempfile
import time
from telegram import Bot
from telegram.error import TelegramError, TimedOut

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

# ─────────────────── Путь до локальной конфигурации ───────────────
BASE_DIR    = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "config.json"


def _load_config(path: Path) -> dict:
    """Пытаемся прочитать config.json рядом со скриптом."""

    try:
        with open(path, "r", encoding="utf-8") as cfg:
            data = json.load(cfg)
        print(f"⚙️  Загружен config.json: {path}")
        return data
    except FileNotFoundError:
        print("⚠️  config.json не найден, используем значения по умолчанию.")
    except Exception as exc:
        print(f"⚠️  Не удалось прочитать config.json: {exc}")
    return {}


CONFIG = _load_config(CONFIG_PATH)


# ────────────── Заголовок консольного окна (Windows) ──────────────
title = "LD_problems"
if sys.platform == "win32":
    ctypes.windll.kernel32.SetConsoleTitleW(title)

# ─────────────────────── ⚙️  Настройки ────────────────────────────
DEFAULT_LOG_FOLDER    = r"C:\Program Files\GnBots\logs"
DEFAULT_PROFILE_FILE  = r"C:/Program Files/GnBots/profiles/FRESH_NOX.json"

LOG_FOLDER      = os.getenv("LDP_LOG_FOLDER") or CONFIG.get("LOGS_DIR", DEFAULT_LOG_FOLDER)
PROBLEMS_FILE   = r"C:\LDPlayer\ldChecker\problems.json"
SUMMARY_FILE    = os.getenv("LDP_SUMMARY_FILE", r"C:\LDPlayer\ldChecker\problems_summary.json")
TELEGRAM_STATE_FILE = os.getenv(
    "LDP_TELEGRAM_STATE_FILE",
    r"C:\LDPlayer\ldChecker\problems_telegram_state.json",
)
PROFILE_FILE    = os.getenv("LDP_PROFILE_FILE") or CONFIG.get("PROFILE_PATH", DEFAULT_PROFILE_FILE)
SERVER_NAME     = os.getenv("SERVER_NAME") or CONFIG.get("SERVER_NAME") or socket.gethostname()

PROBLEM_LABELS = {
    "login": "Login🔑",
    "update": "UPD🔄",
    "restart": "Restart X4❌",
    "launch_restart": "Launch restart🔁",
    "crash": "Crash💥",
    "idle": "Idle⌛",
    "no_tasks": "No tasks🤷🏼‍♀️📋",
    "broken_acc": "Broken acc🪫",
    "account_switch": "Account switch⚙️",
    "other": "Other⚠️",
}

# Технические рестарты/слёты остаются в JSON и веб-сводке, но не засоряют Telegram.
# В Telegram попадают только проблемы, которые напрямую означают отсутствие работы.
TELEGRAM_CRITICAL_KINDS = {
    value.strip()
    for value in os.getenv(
        "LDP_TELEGRAM_CRITICAL_KINDS",
        "update,idle,no_tasks,broken_acc,account_switch",
    ).split(",")
    if value.strip()
}
TELEGRAM_COOLDOWN_HOURS = int(os.getenv("LDP_TELEGRAM_COOLDOWN_HOURS", "12"))
ACCOUNT_SWITCH_AUTOFIX = os.getenv("LDP_ACCOUNT_SWITCH_AUTOFIX", "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}

RECOVERABLE_PROBLEM_KINDS = {
    "login",
    "update",
    "restart",
    "launch_restart",
    "crash",
    "idle",
    "no_tasks",
}
HEALTHY_ACTIVITY_MARKERS = (
    "cityresourcesamount:",
    "gather: resources ",
    "gather: farming lowest resource",
)
EVENT_TIMESTAMP_RE = re.compile(
    r"^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})(?:\.(\d{1,6}))?"
)
ACCOUNT_DONE_RE = re.compile(r"account done \[(\d{2}):(\d{2}):(\d{2})\]", re.I)

# [SECURITY] Telegram-токен читаем только из обязательной env-переменной без fallback.
TELEGRAM_TOKEN_ENV = "RSSV7_LD_PROBLEMS_BOT_TOKEN"


def require_env(name: str) -> str:
    """[SECURITY] Возвращает обязательную env-переменную или бросает понятную ошибку."""
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Не задана обязательная переменная окружения: {name}")
    return value


telegram_token: str | None = None
chat_id: str | None = None
SERVER_LABEL    = (SERVER_NAME or "LD").strip() or "LD"


def get_telegram_config() -> tuple[str, str]:
    """[SECURITY] Ленивая загрузка Telegram-конфига без падения при импорте модуля."""
    token = require_env(TELEGRAM_TOKEN_ENV)
    # [SECURITY][LEGACY] Для совместимости разрешён fallback в локальный config.json,
    # т.к. на части хостов chat_id исторически задаётся только там.
    resolved_chat_id = os.getenv("RSSV7_LD_PROBLEMS_CHAT_ID") or CONFIG.get("TELEGRAM_CHAT_ID", "275483461")
    if not str(resolved_chat_id).strip():
        raise RuntimeError("Не задан chat_id: RSSV7_LD_PROBLEMS_CHAT_ID")
    return token, str(resolved_chat_id)

# 1️⃣  Мгновенные шаблоны
regex_list = [
    re.compile(r'Account expired'),
    re.compile(r'No\s+account selected'),
    re.compile(r'Game doesn'),
    re.compile(r'Write gmail'),
    re.compile(r'Update the Game'),
    re.compile(r'no actions'),
    re.compile(r'Found\s+0\s+active\s+Actions'),
    re.compile(r'Ignoring'),
    re.compile(r'Launch:\s*Many restarts detected', re.I),
]

# 2️⃣  Шаблоны для «кластеров»
cluster_regex_list = [
    re.compile(r'Account expired'),
    re.compile(r'crashed'),
    re.compile(r'Booting timeout. Restarting'),
    re.compile(r'Launch:\s*Restarting Game', re.I),
    re.compile(r'Launch:\s*We will now restart the instance', re.I),
]

CLUSTER_WINDOW_MIN = 25
CLUSTER_MIN_COUNT  = 4

MAX_SAFE_LEN       = 3500
MAX_LINES_PER_MSG  = 50
DEBUG_MISS_ID      = True   # печатать неизвестные ID (один раз за запуск)

# ────────────────── 🩺 Health-check ──────────────────────────────
def health_check() -> None:
    global telegram_token, chat_id
    if telegram_token is None or chat_id is None:
        telegram_token, chat_id = get_telegram_config()

    issues = []
    if not os.path.isdir(LOG_FOLDER):
        issues.append(f"Папка логов не найдена: {LOG_FOLDER}")
    if not os.path.isfile(PROFILE_FILE):
        issues.append(f"Файл профилей не найден: {PROFILE_FILE}")
    if not telegram_token or telegram_token.startswith("000"):
        issues.append(f"Некорректный Telegram-токен (проверьте {TELEGRAM_TOKEN_ENV})")
    if issues:
        print("❌ Health-check:")
        for m in issues:
            print("   •", m)
        sys.exit(1)
    print("✅ Health-check OK")

# ───────────── Вспомогательные утилиты ───────────────────────────
def norm_id(raw: str | None) -> str:
    """Приводит ID к нижнему регистру, оставляя только 0-9 a-f."""
    return re.sub(r'[^0-9a-f]', '', (raw or '').lower())

def extract_mapping_recursive(obj, mapping: dict[str, str]) -> None:
    """Рекурсивно собирает пары norm_id → name из произвольного JSON."""
    if isinstance(obj, dict):
        lower = {k.lower(): k for k in obj.keys()}
        id_key   = next((lower[k] for k in ('id', 'accountid', 'account_id') if k in lower), None)
        name_key = lower.get('name')
        if id_key and name_key:
            nid = norm_id(obj[id_key])
            if nid:
                mapping[nid] = str(obj[name_key])
        for v in obj.values():
            extract_mapping_recursive(v, mapping)
    elif isinstance(obj, list):
        for v in obj:
            extract_mapping_recursive(v, mapping)

def load_account_mapping() -> dict[str, str]:
    mapping: dict[str, str] = {}
    try:
        with open(PROFILE_FILE, encoding="utf-8") as pf:
            data = json.load(pf)
        extract_mapping_recursive(data, mapping)
        print(f"🔄 Найдено {len(mapping)} аккаунтов в профиле.")
    except Exception as e:
        print(f"⚠️  Не удалось загрузить профили: {e}")
    return mapping


def _name_number(name: str) -> int | None:
    """Возвращает числовой суффикс ника для безопасного упорядочивания пар."""

    match = re.search(r"(\d+)$", str(name or "").strip())
    return int(match.group(1)) if match else None


def _option_values(envelope: dict) -> list[str]:
    """Читает enum, сохраняя штатную опечатку GnBots `optons`."""

    raw = envelope.get("optons")
    if not isinstance(raw, list):
        raw = envelope.get("options")
    return [str(value) for value in raw] if isinstance(raw, list) else []


def analyze_account_switch_profiles(
    profiles: list,
    *,
    auto_fix: bool,
) -> tuple[list[dict], bool]:
    """Проверяет Account Switch и готовит только однозначные исправления."""

    problems: list[dict] = []
    entries: list[dict] = []
    changed = False

    for profile_index, profile in enumerate(profiles):
        if not isinstance(profile, dict):
            continue
        name = str(profile.get("Name") or f"профиль #{profile_index}")
        try:
            steps = json.loads(profile.get("Data") or "[]")
        except (TypeError, ValueError) as exc:
            problems.append(
                {
                    "account": name,
                    "detail": f"Data не разбирается как JSON: {exc}",
                    "fixed": False,
                }
            )
            continue
        if not isinstance(steps, list):
            problems.append(
                {
                    "account": name,
                    "detail": "Data не является списком шагов",
                    "fixed": False,
                }
            )
            continue

        switches = [
            step
            for step in steps
            if isinstance(step, dict)
            and step.get("ScriptId") == "vikingbot.base.accountswitch"
        ]
        if not switches:
            continue
        if len(switches) != 1:
            problems.append(
                {
                    "account": name,
                    "detail": f"найдено шагов Account Switch: {len(switches)}, ожидается 1",
                    "fixed": False,
                }
            )
            continue

        switch = switches[0]
        config = switch.get("Config")
        if not isinstance(config, dict):
            problems.append(
                {"account": name, "detail": "у Account Switch нет Config", "fixed": False}
            )
            continue

        mode = config.get("mode")
        account = config.get("account")
        if not isinstance(mode, dict) or not isinstance(account, dict):
            problems.append(
                {
                    "account": name,
                    "detail": "поля mode/account имеют неожиданную структуру",
                    "fixed": False,
                }
            )
            continue

        mode_value = str(mode.get("value") or "").strip()
        mode_options = _option_values(mode)
        mode_fixed = False
        if mode_value not in mode_options and mode_value not in {"Player", "IGG_ID"}:
            if auto_fix and mode_value.lower() == "layer" and "Player" in mode_options:
                mode["value"] = "Player"
                mode_fixed = True
                changed = True
            problems.append(
                {
                    "account": name,
                    "detail": f"Mode={mode_value or '<пусто>'}, ожидается Player/IGG_ID",
                    "fixed": mode_fixed,
                }
            )

        position = str(account.get("value") or "").strip()
        position_options = _option_values(account)
        entries.append(
            {
                "name": name,
                "instance": str(
                    profile.get("InstanceId")
                    if profile.get("InstanceId") is not None
                    else "?"
                ),
                "profile": profile,
                "steps": steps,
                "account": account,
                "position": position,
                "position_invalid": not position or (
                    bool(position_options) and position not in position_options
                ),
                "profile_index": profile_index,
                "changed": mode_fixed,
            }
        )

    grouped: defaultdict[str, list[dict]] = defaultdict(list)
    for entry in entries:
        grouped[entry["instance"]].append(entry)

    reported_positions: set[int] = set()
    for instance, group in grouped.items():
        if len(group) < 2:
            continue
        positions = [entry["position"] for entry in group]
        duplicates = (
            len(set(positions)) != len(positions)
            or any(entry["position_invalid"] for entry in group)
        )
        if not duplicates:
            continue
        reported_positions.update(id(entry) for entry in group)

        names = [entry["name"] for entry in group]
        fixed = False
        target_positions: dict[str, str] = {}
        if len(group) == 2:
            first_num = _name_number(group[0]["name"])
            second_num = _name_number(group[1]["name"])
            if (
                first_num is not None
                and second_num is not None
                and abs(first_num - second_num) == 1
            ):
                ordered = sorted(group, key=lambda entry: _name_number(entry["name"]) or 0)
                target_positions = {ordered[0]["name"]: "1", ordered[1]["name"]: "2"}
            elif sum(value in {"1", "2"} for value in positions) == 1:
                selected = next(value for value in positions if value in {"1", "2"})
                complement = "2" if selected == "1" else "1"
                target_positions = {
                    entry["name"]: (
                        entry["position"]
                        if entry["position"] in {"1", "2"}
                        else complement
                    )
                    for entry in group
                }

        if auto_fix and target_positions:
            for entry in group:
                target = target_positions[entry["name"]]
                if entry["position"] != target:
                    entry["account"]["value"] = target
                    entry["changed"] = True
                    changed = True
            fixed = True

        before = " / ".join(value or "<пусто>" for value in positions)
        after = ""
        if fixed:
            after = " -> " + " / ".join(target_positions[name] for name in names)
        problems.append(
            {
                "account": f"LD {instance}: {' / '.join(names)}",
                "detail": f"позиции {before}{after}",
                "fixed": fixed,
            }
        )

    for entry in entries:
        if entry["position_invalid"] and id(entry) not in reported_positions:
            problems.append(
                {
                    "account": entry["name"],
                    "detail": (
                        f"Account Position={entry['position'] or '<пусто>'} "
                        "не входит в допустимые options"
                    ),
                    "fixed": False,
                }
            )

    if changed:
        for entry in entries:
            if entry["changed"]:
                entry["profile"]["Data"] = json.dumps(
                    entry["steps"], ensure_ascii=False, separators=(",", ":")
                )

    return problems, changed


def _write_profiles_safely(path: Path, profiles: list, original: bytes) -> Path:
    """Атомарно записывает профиль, если GnBots не изменил его во время проверки."""

    if path.read_bytes() != original:
        raise RuntimeError("профиль изменился во время проверки")

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = path.with_name(f"{path.name}.before_accountswitch_autofix_{stamp}")
    shutil.copy2(path, backup)
    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{path.name}.accountswitch.", suffix=".tmp", dir=str(path.parent)
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as out:
            json.dump(profiles, out, ensure_ascii=False, indent=2)
            out.flush()
            os.fsync(out.fileno())
        shutil.copystat(path, tmp_name)
        if path.read_bytes() != original:
            raise RuntimeError("профиль изменился перед атомарной записью")
        os.replace(tmp_name, path)
    finally:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)
    return backup


def check_account_switch_profile(*, auto_fix: bool = True) -> list[dict]:
    """Возвращает проблемы Account Switch для Telegram и блока «Наблюдение»."""

    path = Path(PROFILE_FILE)
    try:
        original = path.read_bytes()
        profiles = json.loads(original.decode("utf-8-sig"))
        if not isinstance(profiles, list):
            raise ValueError("корень профиля не является списком")
    except Exception as exc:
        return [
            {
                "file": "ACCOUNT_SWITCH_CONFIG",
                "account": "GnBots profile",
                "line": f"Account Switch config: не удалось прочитать профиль: {exc}",
            }
        ]

    problems, changed = analyze_account_switch_profiles(profiles, auto_fix=auto_fix)
    write_error = ""
    backup: Path | None = None
    if changed:
        try:
            backup = _write_profiles_safely(path, profiles, original)
        except Exception as exc:
            write_error = str(exc)

    records = []
    for problem in problems:
        if problem.get("fixed") and not write_error:
            status = "автоисправлено"
        elif problem.get("fixed"):
            status = f"автоисправление не записано: {write_error}"
        else:
            status = "требуется ручная проверка"
        records.append(
            {
                "file": "ACCOUNT_SWITCH_CONFIG",
                "account": problem["account"],
                "line": f"Account Switch config: {problem['detail']}; {status}",
            }
        )

    if backup is not None:
        print(f"🛠️  Account Switch автоисправлен, backup: {backup}")
    return records

def split_into_messages(lines: list[str]) -> list[str]:
    msgs, cur, length = [], [], 0
    for l in lines:
        if length + len(l) + 1 > MAX_SAFE_LEN or len(cur) >= MAX_LINES_PER_MSG:
            msgs.append("\n".join(cur))
            cur, length = [], 0
        cur.append(l)
        length += len(l) + 1
    if cur:
        msgs.append("\n".join(cur))
    return msgs

async def safe_send(bot: Bot, text: str) -> None:
    """Отправка сообщения в TG с защитой от Flood-limit и таймаутов."""
    retries = 0
    while True:
        try:
            await bot.send_message(chat_id=chat_id, text=text)
            return
        except TimedOut:
            retries += 1
            if retries > 3:
                print("Telegram-error: Timed out (превышено число попыток)")
                return
            await asyncio.sleep(min(5 * retries, 20))
        except TelegramError as e:
            m = str(e)
            if "Flood control exceeded" in m:
                delay = int(re.search(r"Retry in (\d+)", m).group(1))
                await asyncio.sleep(delay)
            elif "Message is too long" in m:
                for part in split_into_messages(text.split("\n")):
                    await safe_send(bot, part)
                return
            else:
                print("Telegram-error:", e)
                return

def prettify(raw: str, account: str) -> str:
    """Готовит читабельную строку для TG."""
    try:
        dt = raw[5:16]  # 'MM-DD HH:MM'
    except Exception:
        dt = "-- --:--"
    desc = raw.rsplit("|", 1)[-1].strip()
    return f"🔹 {account}: {dt} {desc}"


def _classify_problem(raw_line: str) -> tuple[str, str]:
    """Возвращает (ключ, компактная метка) для строки ошибки."""

    lower = raw_line.lower()

    if "account expired" in lower or "write gmail" in lower or "no account selected" in lower:
        return "login", PROBLEM_LABELS["login"]
    if "update the game" in lower:
        return "update", PROBLEM_LABELS["update"]
    if "booting timeout" in lower:
        return "restart", PROBLEM_LABELS["restart"]
    if (
        "launch: restarting game" in lower
        or "launch: many restarts detected" in lower
        or "launch: we will now restart the instance" in lower
    ):
        return "launch_restart", PROBLEM_LABELS["launch_restart"]
    if "crash" in lower:
        return "crash", PROBLEM_LABELS["crash"]
    if "no actions" in lower:
        return "idle", PROBLEM_LABELS["idle"]
    if "found 0 active actions" in lower:
        return "no_tasks", PROBLEM_LABELS["no_tasks"]
    if "broken acc" in lower or "broken_acc" in lower:
        return "broken_acc", PROBLEM_LABELS["broken_acc"]
    if "account switch config:" in lower:
        return "account_switch", PROBLEM_LABELS["account_switch"]

    return "other", PROBLEM_LABELS["other"]


def _format_summary(counter: Counter) -> str:
    """Собирает компактную строку вида "Login🔑(2) + Restart X4❌"."""

    parts = []
    for key, count in sorted(counter.items()):
        label = PROBLEM_LABELS.get(key, PROBLEM_LABELS["other"])
        suffix = f"({count})" if count > 1 else ""
        parts.append(f"{label}{suffix}")
    return " + ".join(parts)


def _save_summary(per_account: dict[str, Counter], total_problems: int) -> None:
    """Сохраняет агрегированную статистику в JSON для веб-интерфейса."""

    accounts: list[dict] = []
    for acc, counter in sorted(per_account.items()):
        problems = []
        for key, cnt in sorted(counter.items()):
            label = PROBLEM_LABELS.get(key, PROBLEM_LABELS["other"])
            problems.append({"kind": key, "label": label, "count": cnt})
        summary = _format_summary(counter)
        accounts.append(
            {
                "nickname": acc,
                "summary": summary,
                "total": sum(counter.values()),
                "problems": problems,
            }
        )

    payload = {
        "server": SERVER_NAME,
        "generated_at": datetime.now().isoformat(),
        "total_accounts": len(accounts),
        "total_problems": total_problems,
        "accounts": accounts,
    }

    try:
        os.makedirs(os.path.dirname(SUMMARY_FILE) or ".", exist_ok=True)
        with open(SUMMARY_FILE, "w", encoding="utf-8") as out:
            json.dump(payload, out, ensure_ascii=False, indent=2)
    except Exception as exc:
        print(f"⚠️  Не удалось сохранить сводку наблюдения: {exc}")


def _extract_event_timestamp(raw_line: str) -> datetime | None:
    """Извлекает локальное время события из обычной или синтетической строки лога."""

    match = EVENT_TIMESTAMP_RE.match(raw_line or "")
    if not match:
        return None
    date_part, time_part, fraction = match.groups()
    value = f"{date_part} {time_part}"
    fmt = "%Y-%m-%d %H:%M:%S"
    if fraction:
        value += f".{fraction}"
        fmt += ".%f"
    try:
        return datetime.strptime(value, fmt)
    except ValueError:
        return None


def _is_healthy_activity(raw_line: str) -> bool:
    """Определяет строку, которая подтверждает работу аккаунта после ошибки входа/запуска."""

    lower = (raw_line or "").lower()
    if any(marker in lower for marker in HEALTHY_ACTIVITY_MARKERS):
        return True

    match = ACCOUNT_DONE_RE.search(lower)
    if not match:
        return False
    hours, minutes, seconds = (int(part) for part in match.groups())
    return hours * 3600 + minutes * 60 + seconds >= 60


def _current_problem_records(
    records: list[dict],
    healthy_after: dict[str, datetime],
) -> list[dict]:
    """Убирает уже восстановившиеся ошибки и дубли одного события из разных лог-файлов."""

    current: list[dict] = []
    seen: set[tuple] = set()
    for record in records:
        account = str(record.get("account") or "Unknown")
        raw_line = str(record.get("line") or "")
        kind, _ = _classify_problem(raw_line)
        event_at = _extract_event_timestamp(raw_line)
        recovered_at = healthy_after.get(account)

        if (
            kind in RECOVERABLE_PROBLEM_KINDS
            and event_at is not None
            and recovered_at is not None
            and event_at <= recovered_at
        ):
            continue

        if event_at is not None:
            logical_key = (account, kind, event_at.replace(microsecond=0))
        else:
            logical_key = (account, kind, record.get("file"), raw_line)
        if logical_key in seen:
            continue
        seen.add(logical_key)
        current.append(record)

    return current


def _load_telegram_state() -> dict[str, float]:
    try:
        with open(TELEGRAM_STATE_FILE, encoding="utf-8") as src:
            data = json.load(src)
        return {
            str(key): float(value)
            for key, value in data.items()
            if isinstance(value, (int, float))
        }
    except (FileNotFoundError, ValueError, TypeError, OSError):
        return {}


def _save_telegram_state(state: dict[str, float]) -> None:
    try:
        os.makedirs(os.path.dirname(TELEGRAM_STATE_FILE) or ".", exist_ok=True)
        cutoff = time.time() - 7 * 24 * 3600
        compact = {key: value for key, value in state.items() if value >= cutoff}
        with open(TELEGRAM_STATE_FILE, "w", encoding="utf-8") as out:
            json.dump(compact, out, ensure_ascii=False, indent=2)
    except OSError as exc:
        print(f"⚠️  Не удалось сохранить Telegram-state: {exc}")


def _select_telegram_alerts(records: list[dict]) -> list[dict]:
    """Оставляет по одному критичному событию на аккаунт/тип с cooldown."""

    latest: dict[tuple[str, str], dict] = {}
    for rec in records:
        kind, _ = _classify_problem(rec["line"])
        if kind in TELEGRAM_CRITICAL_KINDS:
            latest[(rec["account"], kind)] = rec

    state = _load_telegram_state()
    now = time.time()
    cooldown = max(1, TELEGRAM_COOLDOWN_HOURS) * 3600
    selected = []
    for (account, kind), rec in latest.items():
        state_key = f"{account}\x1f{kind}"
        if now - state.get(state_key, 0) >= cooldown:
            selected.append(rec)
            state[state_key] = now

    _save_telegram_state(state)
    return selected

def deduplicate(recs: list[dict]) -> list[dict]:
    seen, out = set(), []
    for r in recs:
        k = (r["account"], r["file"], r["line"])
        if k not in seen:
            seen.add(k)
            out.append(r)
    return out


BROKEN_ACC_LOOP = ["prep", "done", "stop"]
BROKEN_ACC_DOUBLE_LOOP = BROKEN_ACC_LOOP * 2


def get_broken_acc_event(lower_line: str, has_account_id: bool) -> str | None:
    """Возвращает шаг broken acc или маркер строки, которая разрывает цикл."""
    if "preparing account" in lower_line:
        return "prep"
    if "account done" in lower_line:
        return "done"
    if "stopping emulator" in lower_line:
        return "stop"
    if has_account_id:
        return "other"
    return None


def has_broken_acc_pattern(events: list[str]) -> bool:
    """Проверяет два подряд идущих пустых цикла подготовки без промежуточных действий."""
    for i in range(len(events) - len(BROKEN_ACC_DOUBLE_LOOP) + 1):
        if events[i : i + len(BROKEN_ACC_DOUBLE_LOOP)] == BROKEN_ACC_DOUBLE_LOOP:
            return True
    return False


# ─────────────────────── Основная логика ─────────────────────────
async def check_logs_and_notify() -> None:
    global telegram_token, chat_id
    if telegram_token is None or chat_id is None:
        telegram_token, chat_id = get_telegram_config()

    bot    = Bot(token=telegram_token)
    id_map = load_account_mapping()
    today  = datetime.now().strftime("%Y-%m-%d")
    today_started_at = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp()

    config_records = check_account_switch_profile(auto_fix=ACCOUNT_SWITCH_AUTOFIX)
    found: list[dict] = list(config_records)
    crash_events: defaultdict[str, list[datetime]] = defaultdict(list)

    # 🆕 Теперь храним ts + desc, а не только ts
    cluster_dict: defaultdict[str, list[dict]] = defaultdict(list)
    prep_done_stop_events: defaultdict[str, list[str]] = defaultdict(list)
    healthy_after: dict[str, datetime] = {}

    unknown_ids: set[str] = set()   # для отладки

    # ───────── Сканирование файлов ─────────
    for root, _, files in os.walk(LOG_FOLDER):
        for fname in files:
            if not fname.lower().endswith((".log", ".txt")):
                continue
            path = os.path.join(root, fname)
            try:
                if os.path.getmtime(path) < today_started_at:
                    continue
            except OSError:
                continue
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    if today not in line:
                        continue

                    lower_line = line.lower()
                    id_match = re.search(r"\|([0-9a-f\-]{8,32})\|", line, re.I)
                    acct_id_raw = id_match.group(1) if id_match else ""
                    acct_id = norm_id(acct_id_raw)
                    acct = id_map.get(acct_id, acct_id or "Unknown")

                    event_at = _extract_event_timestamp(line)
                    if event_at is not None and _is_healthy_activity(line):
                        previous = healthy_after.get(acct)
                        if previous is None or event_at > previous:
                            healthy_after[acct] = event_at

                    broken_acc_event = get_broken_acc_event(lower_line, bool(acct_id))
                    if broken_acc_event:
                        prep_done_stop_events[acct].append(broken_acc_event)

                    # Строки с областью `Main` дублируют соседнее событие аккаунта,
                    # но не содержат его ID и раньше создавали ложную карточку Unknown.
                    matched_instant = bool(acct_id) and any(rgx.search(line) for rgx in regex_list)
                    matched_cluster = bool(acct_id) and any(rgx.search(line) for rgx in cluster_regex_list)

                    if matched_instant or matched_cluster:
                        if acct == "Unknown" and DEBUG_MISS_ID and acct_id and acct_id not in unknown_ids:
                            print(f"⚠️  Не найден ник для ID: {acct_id_raw}")
                            unknown_ids.add(acct_id)

                    # 1️⃣  Мгновенный алерт
                    if matched_instant:
                        found.append({"file": path, "account": acct, "line": line.rstrip()})

                    # 1a. 🆕 Кластерная статистика (ts + desc)
                    if matched_cluster:
                        ts_match = re.match(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", line)
                        ts = (
                            datetime.strptime(ts_match.group(1), "%Y-%m-%d %H:%M:%S")
                            if ts_match else datetime.now()
                        )
                        desc = line.rsplit("|", 1)[-1].strip()
                        cluster_dict[acct].append({"ts": ts, "desc": desc})

                    # 2️⃣  Game Crash
                    if "Launch: We detected a Game Crash" in line:
                        m = re.match(
                            r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ [+-]\d{2}:\d{2}).*\|([0-9a-f\-]{8,32})\|",
                            line,
                        )
                        if m:
                            ts_str, acct_id_cr = m.groups()
                            acct_name = id_map.get(norm_id(acct_id_cr), "Unknown")
                            try:
                                ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S.%f %z")
                            except ValueError:
                                continue
                            crash_events[acct_name].append(ts)

    for acct, events in prep_done_stop_events.items():
        if has_broken_acc_pattern(events):
            found.append(
                {
                    "file": "BROKEN_ACC_PATTERN",
                    "account": acct,
                    "line": "BROKEN ACC: повторяется цикл Preparing Account -> Account Done -> Stopping Emulator",
                }
            )

    # ───────── Кластеры Crash ──────────
    for acct, tl in crash_events.items():
        tl.sort()
        for i in range(len(tl) - 3):
            if tl[i + 3] - tl[i] <= timedelta(minutes=10):
                found.append({
                    "file": "CRASH",
                    "account": acct,
                    "line": f"{tl[i].isoformat()} | CRASH>3 (до {tl[i+3].time()})",
                })
                break

    # ───────── Кластеры ошибок (с указанием фразы) ─────────
    for acct, events in cluster_dict.items():
        events.sort(key=lambda e: e["ts"])
        for i in range(len(events) - (CLUSTER_MIN_COUNT - 1)):
            if events[i + CLUSTER_MIN_COUNT - 1]["ts"] - events[i]["ts"] <= timedelta(minutes=CLUSTER_WINDOW_MIN):
                window = events[i : i + CLUSTER_MIN_COUNT]
                # 🆕 самая частая фраза в окне
                common_desc = Counter(ev["desc"] for ev in window).most_common(1)[0][0]
                found.append({
                    "file": "CLUSTER",
                    "account": acct,
                    "line": (
                        f"{events[i]['ts'].isoformat()} | "
                        f"MULTI>{CLUSTER_MIN_COUNT} '{common_desc}' "
                        f"за {CLUSTER_WINDOW_MIN}м"
                    ),
                })
                break

    # ───────── Дельта с прошлым состоянием ─────────
    try:
        old = json.load(open(PROBLEMS_FILE, encoding="utf-8")) if os.path.exists(PROBLEMS_FILE) else []
    except Exception:
        old = []

    old_keys = {(r["account"], r["file"], r["line"]) for r in old if isinstance(r, dict)}
    new = [r for r in deduplicate(found) if (r["account"], r["file"], r["line"]) not in old_keys]

    all_found = deduplicate(found)
    current_found = _current_problem_records(all_found, healthy_after)
    per_account_all: dict[str, Counter] = defaultdict(Counter)
    for rec in current_found:
        kind, _ = _classify_problem(rec["line"])
        per_account_all[rec["account"]][kind] += 1

    if not new and not config_records:
        _save_summary(per_account_all, len(current_found))
        print("Новых проблем нет.")
        return

    telegram_records = _select_telegram_alerts(deduplicate(new + config_records))
    per_account: dict[str, Counter] = defaultdict(Counter)
    for rec in telegram_records:
        kind, _ = _classify_problem(rec["line"])
        per_account[rec["account"]][kind] += 1

    if telegram_records:
        summary_lines = [
            f"🔴 {acc}: {_format_summary(counter)}"
            for acc, counter in sorted(per_account.items())
        ]
        header = f"{len(per_account)} аккаунт(а) требуют внимания"
        await safe_send(
            bot,
            f"{SERVER_LABEL}🚨 КРИТИЧЕСКИЕ проблемы: {header}\n" + "\n".join(summary_lines),
        )
    else:
        print("Новые события только технические; Telegram-уведомление подавлено.")

    _save_summary(per_account_all, len(current_found))

    try:
        with open(PROBLEMS_FILE, "w", encoding="utf-8") as f:
            json.dump(old + new, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print("Ошибка записи history:", e)

# ───────────────────── Точка входа ─────────────────────
async def main() -> None:
    _load_root_env()
    health_check()
    try:
        await check_logs_and_notify()
    except Exception as e:
        print("Неожиданная ошибка:", e)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Прерывание пользователем.")
