#!/usr/bin/env python3
import socket
import time
import requests
import sys
import traceback

# Настройки Telegram
telegram_token = '7460479135:AAEUcUZdO01AEOVxgA0xlV8ZoLOmZcKw-Uc'
chat_id        = '275483461'

# Параметры проверки
host    = 'tcp.cloudpub.ru'
port    = 53826
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
    url = f'https://api.telegram.org/bot{token}/sendMessage'
    payload = {
        'chat_id': chat_id,
        'text': message,
        'parse_mode': 'Markdown'
    }
    resp = requests.post(url, data=payload, timeout=10)
    resp.raise_for_status()

def main():
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
            except Exception as e:
                print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} ⚠️ Ошибка при отправке в Telegram:")
                traceback.print_exc()

        # Ждём перед следующей проверкой
        time.sleep(interval)

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\nПрервано пользователем, выхожу.")
        sys.exit(0)
