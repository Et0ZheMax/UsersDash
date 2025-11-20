# reset_admin_password.py
# Одноразовый скрипт для сброса пароля админа в UsersDash.
# Запускать из папки UsersDash: python reset_admin_password.py

import os
import sys
import ctypes

from werkzeug.security import generate_password_hash

from UsersDash.app import create_app
from UsersDash.models import db, User


def is_admin() -> bool:
    """
    Проверяет, запущен ли процесс с правами администратора (только Windows).
    На других ОС просто возвращает False, но без критичности.
    """
    if os.name != "nt":
        return False
    try:
        return bool(ctypes.windll.shell32.IsUserAnAdmin())
    except Exception:
        return False


def relaunch_as_admin_if_needed():
    """
    Если мы на Windows и процесс не с админ-правами — пытаемся перезапустить
    текущий скрипт от имени администратора.
    """
    if os.name != "nt":
        return

    if is_admin():
        return

    try:
        params = " ".join([f'"{arg}"' for arg in sys.argv])
        ctypes.windll.shell32.ShellExecuteW(
            None,
            "runas",
            sys.executable,
            params,
            None,
            1,
        )
        sys.exit(0)
    except Exception as exc:
        print(f"[WARN] Не удалось перезапустить от имени администратора: {exc}")


def health_check(app):
    """
    Минималистичный health-check: убеждаемся, что БД доступна и модель User работает.
    """
    print("========== RESET-ADMIN HEALTH-CHECK ==========")
    try:
        from sqlalchemy import text
        with app.app_context():
            db.session.execute(text("SELECT 1"))
            count_users = User.query.count()
            print(f"[OK] Подключение к БД есть. Пользователей: {count_users}")
    except Exception as exc:
        print(f"[ERROR] Ошибка health-check: {exc}")
    print("==============================================")


def main():
    # Перезапуск от имени администратора (на Windows)
    relaunch_as_admin_if_needed()

    app = create_app()
    health_check(app)

    # Новый пароль можно задать тут
    NEW_USERNAME = "admin"          # кого сбрасываем
    NEW_PASSWORD = "11111112koT"          # НОВЫЙ пароль (потом сменишь в интерфейсе)

    with app.app_context():
        admin = User.query.filter_by(username=NEW_USERNAME).first()

        if admin is None:
            print(f"[WARN] Пользователь '{NEW_USERNAME}' не найден. Создаём нового админа.")
            admin = User(
                username=NEW_USERNAME,
                password_hash=generate_password_hash(NEW_PASSWORD),
                role="admin",
                is_active=True,
            )
            db.session.add(admin)
        else:
            print(f"[INFO] Нашли пользователя '{NEW_USERNAME}', обновляем пароль.")
            admin.password_hash = generate_password_hash(NEW_PASSWORD)

        db.session.commit()
        print("[DONE] Пароль успешно сброшен.")
        print(f"ЛОГИН: {NEW_USERNAME}")
        print(f"ПАРОЛЬ: {NEW_PASSWORD}")
        print("Обязательно зайди в админку и поменяй пароль на свой в настройках пользователя.")


if __name__ == "__main__":
    main()
