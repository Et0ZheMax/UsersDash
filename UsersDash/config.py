# config.py
# Конфигурация приложения: пути, настройки БД, секретный ключ.

import os
from pathlib import Path

from UsersDash.telegram_settings import load_telegram_settings


def _load_local_env_files() -> None:
    """Подгружает `.env` из репозитория без перезаписи уже заданных ENV.

    Поддерживается минимальный формат `KEY=VALUE` с опциональным префиксом `export`.
    Значения в кавычках очищаются от внешних `"`/`'`.
    """

    base_dir = Path(__file__).resolve().parent
    candidates = (base_dir.parent / ".env", base_dir / ".env")

    for env_path in candidates:
        if not env_path.exists():
            continue

        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[len("export "):].strip()
            if "=" not in line:
                continue

            key, value = line.split("=", 1)
            key = key.strip()
            if not key or key in os.environ:
                continue

            value = value.strip()
            if value and value[0] == value[-1] and value[0] in {'"', "'"}:
                value = value[1:-1]
            os.environ[key] = value


_load_local_env_files()


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
