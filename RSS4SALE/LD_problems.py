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
import re
import ctypes
import sys
from telegram import Bot
from telegram.error import TelegramError

# ────────────── Заголовок консольного окна (Windows) ──────────────
title = "LD_problems"
if sys.platform == "win32":
    ctypes.windll.kernel32.SetConsoleTitleW(title)

# ─────────────────────── ⚙️  Настройки ────────────────────────────
LOG_FOLDER      = r"C:\Program Files (x86)\GnBots\logs"
PROBLEMS_FILE   = r"C:\LDPlayer\ldChecker\problems.json"
PROFILE_FILE    = r'C:\Program Files (x86)\GnBots\profiles\TIME_ONLY_RSS.json'

telegram_token  = os.getenv("LDP_TG_TOKEN", "7219135420:AAFCxU_xj7Hzn-slUG1iY88qle7LZlFIzzk")
chat_id         = "-1002237965982"

# 1️⃣  Мгновенные шаблоны
regex_list = [
    re.compile(r'Account expired'),
    re.compile(r'No\s+account selected'),
    re.compile(r'Game doesn'),
    re.compile(r'Write gmail'),
    re.compile(r'Update the Game'),
]

# 2️⃣  Шаблоны для «кластеров»
cluster_regex_list = [
    re.compile(r'Account expired'),
    re.compile(r'crashed'),
]

CLUSTER_WINDOW_MIN = 25
CLUSTER_MIN_COUNT  = 4

MAX_SAFE_LEN       = 3500
MAX_LINES_PER_MSG  = 50
DEBUG_MISS_ID      = True   # печатать неизвестные ID (один раз за запуск)

# ────────────────── 🩺 Health-check ──────────────────────────────
def health_check() -> None:
    issues = []
    if not os.path.isdir(LOG_FOLDER):
        issues.append(f"Папка логов не найдена: {LOG_FOLDER}")
    if not os.path.isfile(PROFILE_FILE):
        issues.append(f"Файл профилей не найден: {PROFILE_FILE}")
    if not telegram_token or telegram_token.startswith("000"):
        issues.append("Некорректный Telegram-токен (проверьте LDP_TG_TOKEN)")
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
    """Отправка сообщения в TG с защитой от Flood-limit и длины."""
    while True:
        try:
            await bot.send_message(chat_id=chat_id, text=text)
            return
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

def deduplicate(recs: list[dict]) -> list[dict]:
    seen, out = set(), []
    for r in recs:
        k = (r["account"], r["file"], r["line"])
        if k not in seen:
            seen.add(k)
            out.append(r)
    return out

# ─────────────────────── Основная логика ─────────────────────────
async def check_logs_and_notify() -> None:
    bot    = Bot(token=telegram_token)
    id_map = load_account_mapping()
    today  = datetime.now().strftime("%Y-%m-%d")

    found: list[dict] = []
    crash_events: defaultdict[str, list[datetime]] = defaultdict(list)

    # 🆕 Теперь храним ts + desc, а не только ts
    cluster_dict: defaultdict[str, list[dict]] = defaultdict(list)

    unknown_ids: set[str] = set()   # для отладки

    # ───────── Сканирование файлов ─────────
    for root, _, files in os.walk(LOG_FOLDER):
        for fname in files:
            if not fname.lower().endswith((".log", ".txt")):
                continue
            path = os.path.join(root, fname)
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    if today not in line:
                        continue

                    matched_instant = any(rgx.search(line) for rgx in regex_list)
                    matched_cluster = any(rgx.search(line) for rgx in cluster_regex_list)

                    if matched_instant or matched_cluster:
                        id_match = re.search(r"\|([0-9a-f\-]{8,32})\|", line, re.I)
                        acct_id_raw = id_match.group(1) if id_match else ""
                        acct_id = norm_id(acct_id_raw)
                        acct = id_map.get(acct_id, acct_id or "Unknown")

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

    if not new:
        print("Новых проблем нет.")
        return

    details = [prettify(r["line"], r["account"]) for r in new]
    counts  = Counter(r["account"] for r in new)

    for part in split_into_messages(details):
        await safe_send(bot, "RSS🚨 Найдены проблемы:\n" + part)

    summary = "\n".join(f"{a}: {c}" for a, c in counts.items())
    await safe_send(bot, f"RSS📊 Сводка: {len(counts)} аккаунтов, {len(new)} проблем\n{summary}")

    try:
        with open(PROBLEMS_FILE, "w", encoding="utf-8") as f:
            json.dump(old + new, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print("Ошибка записи history:", e)

# ───────────────────── Точка входа ─────────────────────
async def main() -> None:
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
