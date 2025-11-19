import os
import json
import asyncio
from datetime import datetime
from telegram import Bot
import re 
from collections import Counter
from telegram.error import TelegramError


# Путь к папке с логами бота
log_folder = r"C:\Program Files (x86)\GnBots\logs"

# Файл для сохранения найденных проблем
problems_file = r"C:\LDPlayer\ldChecker\problems.json"

# Путь к папке с профилями аккаунтов
profile_file = r'C:/Program Files (x86)/GnBots/profiles/FRESH_NOX.json'

# Ваш Telegram токен и ID чата
telegram_token = '7460479135:AAEUcUZdO01AEOVxgA0xlV8ZoLOmZcKw-Uc'
chat_id = '275483461'

# === Шаблоны для поиска проблем ===
patterns = [
    r'Account expired',
    r'No\s+account selected',
    r'Game doesn',
    r'Write gmail'
]
regex_list = [re.compile(p, re.IGNORECASE) for p in patterns]

# --- Telegram ограничения ---
MAX_SAFE_LEN = 3500  # до лимита 4096
MAX_LINES_PER_MSG = 50


# -------------------------------------------------
# Вспомогательные функции
# -------------------------------------------------

def load_account_mapping():
    """Читает FRESH_NOX.json и строит словарь id(lower) -> Name"""
    mapping: dict[str, str] = {}
    try:
        with open(profile_file, 'r', encoding='utf-8') as pf:
            data = json.load(pf)
            records = data if isinstance(data, list) else [data]
            for rec in records:
                if not isinstance(rec, dict):
                    continue
                lower_keys = {k.lower(): k for k in rec.keys()}
                acct_id = None
                for cand in ('id', 'accountid', 'account_id'):
                    if cand in lower_keys:
                        acct_id = str(rec[lower_keys[cand]]).lower()
                        break
                acct_name = None
                for cand in ('name', 'accountname', 'profilename', 'title', 'label', 'nickname', 'login'):
                    if cand in lower_keys:
                        acct_name = str(rec[lower_keys[cand]])
                        break
                if acct_id and acct_name:
                    mapping[acct_id] = acct_name
    except Exception as e:
        print(f"Ошибка загрузки профиля: {e}")
    return mapping


def format_problem_line(raw_line: str, account: str) -> str:
    """Возвращает строку вида '🔹 Name: MM-DD HH:MM |описание'"""
    try:
        date_part = raw_line[5:10]   # MM-DD
        time_part = raw_line[11:16]  # HH:MM
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
            seen.add(key)
            uniq.append(r)
    return uniq


def split_into_messages(lines):
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
                print(f"Flood control: ждём {delay}s")
                await asyncio.sleep(delay)
            elif 'Message is too long' in msg:
                for chunk in split_into_messages(text.split('\n')):
                    await safe_send(bot, chunk)
                return
            else:
                print(f"Ошибка Telegram: {err}")
                return


# -------------------------------------------------
# Основная логика
# -------------------------------------------------

async def check_logs_and_notify():
    bot = Bot(token=telegram_token)
    id_map = load_account_mapping()
    today_str = datetime.now().strftime('%Y-%m-%d')
    found = []

    # --- Сканирование логов ---
    for root, _, files in os.walk(log_folder):
        for file in files:
            if file.lower().endswith(('.log', '.txt')) or today_str in file:
                path = os.path.join(root, file)
                with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                    for line in f:
                        if today_str not in line:
                            continue
                        if not any(rgx.search(line) for rgx in regex_list):
                            continue
                        id_match = re.search(r"\|([0-9a-f]{8,32})\|", line, re.IGNORECASE)
                        acct_id = id_match.group(1).lower() if id_match else None
                        acct_name = id_map.get(acct_id, acct_id or 'Unknown')
                        found.append({'file': path, 'account': acct_name, 'line': line.rstrip()})

    unique = deduplicate(found)

    # --- Дельта со старым состоянием ---
    try:
        old = json.load(open(problems_file, 'r', encoding='utf-8')) if os.path.exists(problems_file) else []
    except Exception:
        old = []
    old_keys = {(r['account'], r['file'], r['line']) for r in old if isinstance(r, dict)}

    new_entries = [r for r in unique if (r['account'], r['file'], r['line']) not in old_keys]
    if not new_entries:
        print('Новых проблем не найдено.')
        return

    # --- Подробные строки ---
    detail_lines = [format_problem_line(r['line'], r['account']) for r in new_entries]

    # --- Сводка по аккаунтам ---
    counts = Counter(r['account'] for r in new_entries)
    summary_lines = [f"{name}: {cnt}" for name, cnt in counts.items()]
    summary_text = "\n".join(summary_lines)
    summary_header = f"📊 Сводка: {len(counts)} аккаунтов, {len(new_entries)} проблем"

    # --- Отправка сообщений ---
    for chunk in split_into_messages(detail_lines):
        await safe_send(bot, f"🚨 Найдены новые проблемы:\n{chunk}")

    await safe_send(bot, f"{summary_header}\n{summary_text}")

    # --- Сохранение всех проблем ---
    try:
        with open(problems_file, 'w', encoding='utf-8') as out:
            json.dump(old + new_entries, out, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Ошибка сохранения {problems_file}: {e}")


async def main():
    try:
        await check_logs_and_notify()
    except asyncio.CancelledError:
        print('Задача отменена.')
    except Exception as e:
        print(f"Непредвиденная ошибка: {e}")


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print('Прерывание пользователем.')
