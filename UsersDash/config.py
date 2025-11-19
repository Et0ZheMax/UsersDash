# config.py
# Конфигурация приложения: пути, настройки БД, секретный ключ.

import os
from pathlib import Path


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
