#!/usr/bin/env python3
# check_and_reboot.py — проверка GnBots.exe и окон dnplayer.exe;
# при нарушении условий — уведомление в Telegram + ребут (kill & restart)

import os
import sys
import psutil
import subprocess
import time
import asyncio
from telegram import Bot
from telegram.error import TelegramError, RetryAfter
import ctypes
# ставим или обновляем библиотеку бота КОПИРУЙ
# py -3.13 -m pip install --upgrade python-telegram-bot

# Пример: задаём своему скрипту заголовок «MyUniqueScript»
title = "Gn_Ld_Check"
if sys.platform == "win32":
    ctypes.windll.kernel32.SetConsoleTitleW(title)

# -------------------------------------------------
# Настройки — отредактируйте под свои значения
# -------------------------------------------------
TELEGRAM_TOKEN    = '7460479135:AAEUcUZdO01AEOVxgA0xlV8ZoLOmZcKw-Uc'
CHAT_ID           = '275483461'
THRESHOLD_WINDOWS = 6
GNBOTS_SHORTCUT   = r'C:\Users\Administrator\Desktop\GnBots.lnk'

# -------------------------------------------------
# Инициализация бота
# -------------------------------------------------
_bot = Bot(token=TELEGRAM_TOKEN)

async def safe_send(bot: Bot, text: str, thread_id: int | None = None):
    """
    Безопасно отправляет сообщение, обрабатывая flood‑limit и ошибки.
    """
    while True:
        try:
            await bot.send_message(chat_id=CHAT_ID, text=text, message_thread_id=thread_id)
            return
        except RetryAfter as e:
            wait = e.retry_after + 1
            print(f"[INFO] Flood‑limit, ждём {wait}s…")
            await asyncio.sleep(wait)
        except TelegramError as e:
            print(f"[WARN] TelegramError при отправке: {e}")
            return

# -------------------------------------------------
# Утилиты для процессов
# -------------------------------------------------
def is_process_running(name: str) -> bool:
    """Проверяет, запущен ли процесс с точным именем name."""
    for proc in psutil.process_iter(['name']):
        if proc.info['name'] and proc.info['name'].lower() == name.lower():
            return True
    return False

def count_processes(name: str) -> int:
    """Считает количество процессов с точным именем name."""
    return sum(
        1 for proc in psutil.process_iter(['name'])
        if proc.info['name'] and proc.info['name'].lower() == name.lower()
    )

def kill_process(name: str, soft_timeout: int = 5, hard_timeout: int = 5) -> list[int]:
    """
    Мягко terminate(), затем kill(), затем taskkill,
    все процессы с именем name. Возвращает список затронутых PID.
    """
    killed = []
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
                    subprocess.run(
                        ['taskkill', '/F', '/T', '/PID', str(pid)],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL
                    )
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
            killed.append(pid)
    return killed

# -------------------------------------------------
# Основная асинхронная логика
# -------------------------------------------------
async def check_and_reboot():
    problems = []

    # Проверка GnBots.exe
    if not is_process_running("GnBots.exe"):
        problems.append("❗ GnBots.exe не запущен")

    # Проверка окон dnplayer.exe
    count = count_processes("dnplayer.exe")
    if count < THRESHOLD_WINDOWS:
        problems.append(f"❗ Окон dnplayer.exe: {count} (требуется ≥{THRESHOLD_WINDOWS})")

    if not problems:
        print(f"[OK] Всё в порядке: GnBots запущен, dnplayer окон — {count}")
        return

    # Формируем текст алерта и отправляем
    alert_text = "🚨F99 Обнаружены проблемы:\n" + "\n".join(problems)
    await safe_send(_bot, alert_text)

    # Логика ребута
    await safe_send(_bot, "🔄F99 Начинаю ребут: убиваем процессы и перезапускаем через ярлык")

    kd = kill_process("dnplayer.exe", soft_timeout=5, hard_timeout=3)
    kb = kill_process("GnBots.exe",     soft_timeout=5, hard_timeout=3)
    kh = kill_process("Ld9BoxHeadless.exe", soft_timeout=5, hard_timeout=3)

    time.sleep(2)

    try:
        os.startfile(GNBOTS_SHORTCUT)
        await safe_send(
            _bot,
            f"✅F99 Ребут завершён.\n"
            f"Убиты PID: dnplayer={kd}, GnBots={kb}, Headless={kh}.\n"
            f"Запущен ярлык: {os.path.basename(GNBOTS_SHORTCUT)}"
        )
    except Exception as e:
        await safe_send(_bot, f"❗ Не удалось запустить ярлык: {e}")

# -------------------------------------------------
# Запуск
# -------------------------------------------------
if __name__ == "__main__":
    # для корректной работы относительных путей
    os.chdir(os.path.dirname(__file__))
    try:
        asyncio.run(check_and_reboot())
    except KeyboardInterrupt:
        print("Прерывание пользователем.")
