#!/usr/bin/env python3
import os
import socket
import sys
import time
import traceback

import requests

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

# [SECURITY] Токен и chat_id теперь читаются только из env; без fallback-значений.
TELEGRAM_TOKEN_ENV = "RSSV7_BOL_DETECT_SSH_BOT_TOKEN"
TELEGRAM_CHAT_ID_ENV = "RSSV7_BOL_DETECT_SSH_CHAT_ID"


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

# Параметры проверки
host = "tcp.cloudpub.ru"
port = 53826
timeout = 5.0  # секунд
interval = 300  # проверять каждые 300 секунд (5 минут)


def check_tcp(host: str, port: int, timeout: float = 5.0) -> float:
    """
    Пытается установить TCP-соединение и возвращает время в мс.
    Если не удалось — выбрасывает исключение.
    """
    start = time.time()
    with socket.create_connection((host, port), timeout=timeout):
        pass
    return (time.time() - start) * 1000.0


def send_telegram(token: str, chat_id: str, message: str) -> None:
    """
    Отправляет текстовое сообщение в Telegram через Bot API.
    """
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": message, "parse_mode": "Markdown"}
    resp = requests.post(url, data=payload, timeout=10)
    resp.raise_for_status()


def main():
    _load_root_env()
    global telegram_token, chat_id
    telegram_token, chat_id = get_telegram_config()

    print(f"Старт проверки {host}:{port} каждые {interval} секунд.")
    while True:
        try:
            rtt = check_tcp(host, port, timeout)
        except Exception as e:
            # Не отправляем уведомление при падении, просто логируем
            print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} ❌ {host}:{port} недоступен ({e})")
        else:
            msg = (
                f"✅ Сервер *{host}:{port}* доступен!\n"
                f"⏱ Время отклика: {rtt:.1f} ms\n"
                f"_(проверка {time.strftime('%Y-%m-%d %H:%M:%S')})_"
            )
            try:
                send_telegram(telegram_token, chat_id, msg)
                print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} уведомление отправлено.")
            except Exception:
                print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} ⚠️ Ошибка при отправке в Telegram:")
                traceback.print_exc()

        # Ждём перед следующей проверкой
        time.sleep(interval)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nПрервано пользователем, выхожу.")
        sys.exit(0)
