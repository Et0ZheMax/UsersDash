import os
import json
import asyncio
from datetime import datetime, timedelta
from telegram import Bot
import re
from collections import Counter, defaultdict
from telegram.error import TelegramError
import ctypes
import sys

# Пример: задаём своему скрипту заголовок «MyUniqueScript»
title = "LD_problems"
if sys.platform == "win32":
    ctypes.windll.kernel32.SetConsoleTitleW(title)


# ─────────────────── Параметры ───────────────────
# Путь к папке с логами бота
log_folder = r"C:\Program Files (x86)\GnBots\logs"

# Файл для сохранения найденных проблем
problems_file = r"C:\LDPlayer\ldChecker\problems.json"

# Путь к папке с профилями аккаунтов
profile_file = r'C:/Program Files (x86)/GnBots/profiles/FRESH_NOX.json'

telegram_token = '7460479135:AAEUcUZdO01AEOVxgA0xlV8ZoLOmZcKw-Uc'
chat_id        = '275483461'

# ваши «старые» паттерны ошибок
regex_list = [
    re.compile(r'Account expired'),
    re.compile(r'No\s+account selected'),
    re.compile(r'Game doesn'),
    re.compile(r'Write gmail'),
    re.compile(r'Update the Game'),
    # re.compile(r'identify Game'),
    #re.compile(r'New Game detected'),
    
]


# ───────────── Вспомогательные функции ───────────
def load_account_mapping() -> dict[str, str]:
    """Читает JSON-профиль и строит словарь id(lower) → Name."""
    mapping: dict[str, str] = {}
    try:
        with open(profile_file, 'r', encoding='utf-8') as pf:
            data = json.load(pf)
        records = data if isinstance(data, list) else [data]
        for rec in records:
            if not isinstance(rec, dict):
                continue
            # подхватываем возможные варианты ключей
            lower = {k.lower(): k for k in rec.keys()}
            acct_id = None
            for cand in ('id', 'accountid', 'account_id'):
                if cand in lower:
                    acct_id = str(rec[lower[cand]]).lower()
                    break
            name_key = lower.get('name')
            name = rec.get(name_key) if name_key else None
            if acct_id and name:
                mapping[acct_id] = name
    except Exception as e:
        print(f"⚠️  Не удалось загрузить профили ({e})")
    return mapping


MAX_SAFE_LEN = 3500
MAX_LINES_PER_MSG = 50

def split_into_messages(lines):
    msgs, cur, length = [], [], 0
    for l in lines:
        if length + len(l) + 1 > MAX_SAFE_LEN or len(cur) >= MAX_LINES_PER_MSG:
            msgs.append("\n".join(cur)); cur, length = [], 0
        cur.append(l); length += len(l) + 1
    if cur:
        msgs.append("\n".join(cur))
    return msgs

async def safe_send(bot: Bot, text: str):
    while True:
        try:
            await bot.send_message(chat_id=chat_id, text=text)
            return
        except TelegramError as err:
            msg = str(err)
            if 'Flood control exceeded' in msg:
                m = re.search(r'Retry in (\d+) seconds', msg)
                delay = int(m.group(1)) if m else 10
                await asyncio.sleep(delay)
            elif 'Message is too long' in msg:
                for chunk in split_into_messages(text.split('\n')):
                    await safe_send(bot, chunk)
                return
            else:
                print(f"Telegram-ошибка: {err}")
                return

def format_problem_line(raw_line: str, account: str) -> str:
    try:
        date_part = raw_line[5:10]; time_part = raw_line[11:16]
        dt_str = f"{date_part} {time_part}"
    except Exception:
        dt_str = "-- --:--"
    desc = '|' + raw_line.rsplit('|', 1)[-1].strip() if '|' in raw_line else raw_line.strip()
    return f"🔹 {account}: {dt_str} {desc}"

def deduplicate(records):
    seen, uniq = set(), []
    for r in records:
        key = (r['account'], r['file'], r['line'])
        if key not in seen:
            seen.add(key); uniq.append(r)
    return uniq


# ───────────────────── Основное ───────────────────
async def check_logs_and_notify():
    bot = Bot(token=telegram_token)
    id_map = load_account_mapping()
    today_str = datetime.now().strftime('%Y-%m-%d')

    found        = []                    # «старые» проблемы
    crash_events = defaultdict(list)     # тайм-стемпы Game Crash

    # ── Сканирование логов ─────────────────────────
    for root, _, files in os.walk(log_folder):
        for fname in files:
            if not (fname.lower().endswith(('.log', '.txt')) or today_str in fname):
                continue
            path = os.path.join(root, fname)
            with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    if today_str not in line:
                        continue

                    # 1) ваши прежние regex-проблемы
                    if any(rgx.search(line) for rgx in regex_list):
                        m = re.search(r"\|([0-9a-f]{8,32})\|", line, re.I)
                        acct_id = m.group(1).lower() if m else ''
                        acct = id_map.get(acct_id, acct_id or 'Unknown')
                        found.append({'file': path, 'account': acct, 'line': line.rstrip()})

                    # 2) сбор Game Crash
                    if "Launch: We detected a Game Crash" in line:
                        m = re.match(
                            r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ [+-]\d{2}:\d{2}).*\|([0-9a-f]{8,32})\|",
                            line
                        )
                        if m:
                            ts_str, acct_id = m.groups()
                            try:
                                ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S.%f %z")
                            except ValueError:
                                continue
                            acct = id_map.get(acct_id.lower(), acct_id.lower())
                            crash_events[acct].append(ts)

    # ── Детект ≥4 крэшей за 10 мин ────────────────
    for acct, times in crash_events.items():
        times.sort()
        for i in range(len(times) - 3):
            if times[i+3] - times[i] <= timedelta(minutes=10):
                start, end = times[i], times[i+3]
                raw_line = f"{start.isoformat()} | CRASH>3 (до {end.time()})"
                found.append({'file': 'CRASH', 'account': acct, 'line': raw_line})
                break  # одно уведомление на аккаунт

    # ── Дельта с прошлым состоянием ───────────────
    try:
        old = json.load(open(problems_file, 'r', encoding='utf-8')) if os.path.exists(problems_file) else []
    except Exception:
        old = []

    old_keys = {(r['account'], r['file'], r['line']) for r in old if isinstance(r, dict)}
    new_entries = [r for r in deduplicate(found) if (r['account'], r['file'], r['line']) not in old_keys]

    if not new_entries:
        print("Новых проблем нет."); return

    # ── Отправка уведомлений ─────────────────────
    detail_lines = [format_problem_line(r['line'], r['account']) for r in new_entries]
    counts = Counter(r['account'] for r in new_entries)

    for chunk in split_into_messages(detail_lines):
        await safe_send(bot, f"F99🚨 Найдены проблемы:\n{chunk}")

    summary = "\n".join(f"{acct}: {cnt}" for acct, cnt in counts.items())
    await safe_send(bot, f"F99📊 Сводка: {len(counts)} аккаунтов, {len(new_entries)} проблем\n{summary}")

    # ── Сохраняем состояние ──────────────────────
    try:
        with open(problems_file, 'w', encoding='utf-8') as out:
            json.dump(old + new_entries, out, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Ошибка сохранения {problems_file}: {e}")


# ────────────────────── Запуск ────────────────────
async def main():
    try:
        await check_logs_and_notify()
    except asyncio.CancelledError:
        pass
    except Exception as e:
        print(f"Непредвиденная ошибка: {e}")

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Прерывание пользователем.")
