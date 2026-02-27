# config.py
# Конфигурация приложения: пути, настройки БД, секретный ключ.

import os
from pathlib import Path

from UsersDash.telegram_settings import load_telegram_settings


def _get_int_env(name: str, default: int) -> int:
    """Аккуратно читает целое число из переменных окружения."""

    raw_value = os.environ.get(name)
    if raw_value is None or raw_value == "":
        return default
    try:
        return int(raw_value)
    except ValueError:
        return default


class Config:
    """
    Базовая конфигурация приложения.
    При желании можно расширить (под разные окружения).
    """
    # Корневая директория проекта (папка, где лежит app.py)
    BASE_DIR = Path(__file__).resolve().parent

    # Папка для БД и прочих данных
    DATA_DIR = BASE_DIR / "data"

    # Путь к SQLite БД
    SQLALCHEMY_DATABASE_URI = f"sqlite:///{DATA_DIR / 'app.db'}"
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    # Секретный ключ для сессий (обязательно поменяй на свой)
    SECRET_KEY = os.environ.get("MULTIDASH_SECRET_KEY", "change-me-please")

    # Доп. настройки можно добавить сюда (например, DEBUG из переменных окружения)

    # Настройки Telegram для уведомлений
    TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_IDS = load_telegram_settings()

    # Настройки Telegram-бота для клиентов
    TELEGRAM_ADMIN_CHAT_IDS = os.environ.get("TELEGRAM_ADMIN_CHAT_IDS", "")
    TELEGRAM_BIND_CODE = os.environ.get("TELEGRAM_BIND_CODE", "")
    TELEGRAM_REMINDER_DAYS = _get_int_env("TELEGRAM_REMINDER_DAYS", 3)
    TELEGRAM_REMINDER_HOUR = _get_int_env("TELEGRAM_REMINDER_HOUR", 10)


    # Настройки Telegram-бота продления аренды
    RENTAL_REMINDER_DAYS = os.environ.get("RENTAL_REMINDER_DAYS", "3,1,0,-1")
    RENTAL_PENDING_ADMIN_REMINDER_HOURS = _get_int_env("RENTAL_PENDING_ADMIN_REMINDER_HOURS", 12)
