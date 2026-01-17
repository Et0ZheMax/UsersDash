"""CLI для запуска Telegram-бота обслуживания клиентов."""

from __future__ import annotations

import time
from datetime import datetime
from pathlib import Path

from flask import Flask

from UsersDash.config import Config
from UsersDash.models import db
from UsersDash.services.telegram_client_bot import (
    fetch_updates,
    handle_update,
    load_bot_config,
    send_payment_reminders,
)


def create_bot_app() -> Flask:
    """Создаёт минимальный Flask-контекст для доступа к БД."""

    app = Flask(__name__)
    app.config.from_object(Config)

    data_dir: Path = Config.DATA_DIR
    data_dir.mkdir(parents=True, exist_ok=True)

    db.init_app(app)
    with app.app_context():
        db.create_all()

    return app


def run_bot() -> None:
    """Запускает long polling Telegram-бота."""

    app = create_bot_app()
    config = load_bot_config(app.config)
    offset: int | None = None
    last_reminder_date: datetime.date | None = None

    print("[telegram_bot] Запуск бота...")

    while True:
        try:
            updates = fetch_updates(config.token, offset, config.poll_timeout)
        except Exception as exc:  # pragma: no cover - защитный контур
            print(f"[telegram_bot] Ошибка получения обновлений: {exc}")
            time.sleep(5)
            continue

        for update in updates:
            offset = update.update_id + 1
            try:
                handle_update(app, config, update)
            except Exception as exc:  # pragma: no cover - защита от падений
                print(f"[telegram_bot] Ошибка обработки апдейта: {exc}")

        now = datetime.utcnow()
        if now.hour >= config.reminder_hour and last_reminder_date != now.date():
            try:
                with app.app_context():
                    send_payment_reminders(config, now)
                last_reminder_date = now.date()
            except Exception as exc:  # pragma: no cover - защита от падений
                print(f"[telegram_bot] Ошибка отправки напоминаний: {exc}")

        time.sleep(1)


if __name__ == "__main__":
    run_bot()
