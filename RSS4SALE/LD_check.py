import os
import json
import asyncio
import re
from telegram import Bot
from telegram.error import TelegramError
import ctypes
import sys

# Пример: задаём своему скрипту заголовок «MyUniqueScript»
title = "LD_Check"
if sys.platform == "win32":
    ctypes.windll.kernel32.SetConsoleTitleW(title)
# -------------------------------------------------
# Пути и конфиги
# -------------------------------------------------
config_folder = r'C:\LDPlayer\LDPlayer9\vms\config'
profile_file = r'C:/Program Files (x86)/GnBots/profiles/TIME_ONLY_RSS.json'
crashed_file = r'C:\LDPlayer\ldChecker\crashed.json'

telegram_token = os.getenv(
    'LDC_TG_TOKEN',
    '7219135420:AAFCxU_xj7Hzn-slUG1iY88qle7LZlFIzzk'
)
chat_id = '-1002237965982'
# ID темы (thread) в чате, куда нужно отправить сообщение
message_thread_id = 4274
# -------------------------------------------------
# Helper-функции
# -------------------------------------------------

def load_profiles(path: str):
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data if isinstance(data, list) else [data]
    except Exception as e:
        print(f"Не смог загрузить профили: {e}")
        return []


def build_active_maps(profiles):
    """Возвращает two dicts: instId→Name и set of active instIds"""
    id2name = {}
    active_ids = set()
    for rec in profiles:
        if not isinstance(rec, dict):
            continue
        inst_id = rec.get('InstanceId')
        name = rec.get('Name')
        if inst_id is None or name is None:
            continue
        id2name[str(inst_id)] = name
        if rec.get('Active'):
            active_ids.add(str(inst_id))
    return id2name, active_ids


def find_key_recursive(d, target_key):
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


def extract_instance_id(fname: str):
    m = re.search(r"leidian(\d+)\.config", fname, re.IGNORECASE)
    return m.group(1) if m else None


async def check_all_configs_and_notify():
    issues = []
    if not os.path.isdir(config_folder):
        issues.append(f"Папка конфигов не найдена: {config_folder}")
    if not os.path.isfile(profile_file):
        issues.append(f"Файл профилей не найден: {profile_file}")
    if not telegram_token or telegram_token.startswith('000'):
        issues.append('Некорректный Telegram-токен (переменная LDC_TG_TOKEN)')

    if issues:
        print('❌ Настройки LD_check:')
        for msg in issues:
            print(f'   • {msg}')
        return

    bot = Bot(token=telegram_token)

    profiles = load_profiles(profile_file)
    id2name, active_inst_ids = build_active_maps(profiles)
    if not active_inst_ids:
        print('Нет активных аккаунтов.')
        return

    crashed_files = []   # для UI (цвет кнопки) – сохраняем как leidianXX.config
    crashed_names = []   # для Telegram-сводки

    for root, _, files in os.walk(config_folder):
        for fname in files:
            if not fname.endswith('.config'):
                continue
            if fname.lower() == 'leidians.config':
                continue
            inst_id = extract_instance_id(fname)
            if inst_id is None or inst_id not in active_inst_ids:
                continue  # не активный – пропуск

            fpath = os.path.join(root, fname)
            try:
                with open(fpath, 'r', encoding='utf-8') as f:
                    cfg = json.load(f)
            except Exception as e:
                print(f"Ошибка чтения {fpath}: {e}")
                continue

            player_name = find_key_recursive(cfg, 'statusSettings.playerName') or ''
            player_name = str(player_name).strip()
            is_bad = (not player_name) or (player_name.lower() == 'ldplayer')
            if is_bad:
                crashed_files.append(fname)
                crashed_names.append(id2name.get(inst_id, f'inst{inst_id}'))
                try:
                    await bot.send_message(
                        chat_id=chat_id,
                        text=f"🚨RSS: Слетевший эмулятор – {id2name.get(inst_id, inst_id)} ({fname})",
                        message_thread_id=message_thread_id
                    )
                except TelegramError as err:
                    print(f"TG error: {err}")

    # Записываем список слетевших конфигов
    try:
        with open(crashed_file, 'w', encoding='utf-8') as f:
            json.dump(crashed_files, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Не могу записать {crashed_file}: {e}")

    # Отправляем сводку в ту же тему
    if crashed_names:
        uniq = sorted(set(crashed_names))
        summary = ", ".join(uniq)
        await bot.send_message(
            chat_id=chat_id,
            text=f"📊RSS: Сводка – всего {len(uniq)} слетевших эмуляторов: {summary}",
            message_thread_id=message_thread_id
        )
    else:
        print('Слетевших активных эмуляторов не найдено.')


async def main():
    await check_all_configs_and_notify()

if __name__ == '__main__':
    asyncio.run(main())
