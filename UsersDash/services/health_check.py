from pathlib import Path
from typing import Optional

from flask import Flask
from sqlalchemy import text

from UsersDash.config import Config
from UsersDash.models import Account, FarmData, Server, User, db
from UsersDash.services.remote_api import ping_server


def _resolve_db_path(app: Flask) -> Optional[Path]:
    """Возвращает путь к файлу БД, если он указан в конфигурации SQLite."""
    uri = app.config.get("SQLALCHEMY_DATABASE_URI", "")
    if not uri.startswith("sqlite:///"):
        return None

    db_path = uri.replace("sqlite:///", "", 1)
    db_path = Path(db_path)
    if not db_path.is_absolute():
        db_path = (Config.BASE_DIR / db_path).resolve()
    return db_path


def run_health_check(app: Flask):
    """
    Выполняет базовые проверки:
      - существует ли файл БД (для SQLite);
      - выполняется ли простой запрос к БД и подсчёт сущностей;
      - есть ли администратор;
      - доступны ли серверы по API.
    Все ошибки логируются и не останавливают приложение.
    """
    print("========== MULTI-TENANT DASHBOARD HEALTH-CHECK ==========")

    try:
        db_path = _resolve_db_path(app)
        if db_path:
            if db_path.exists():
                print(f"[OK] БД найдена: {db_path}")
            else:
                print(f"[WARN] Файл БД не найден: {db_path}")
        else:
            print("[WARN] Не удалось определить путь к БД из SQLALCHEMY_DATABASE_URI.")

        with app.app_context():
            db.session.execute(text("SELECT 1"))
            print("[OK] Подключение к БД установлено.")

            user_count = User.query.count()
            server_count = Server.query.count()
            account_count = Account.query.count()
            farmdata_count = FarmData.query.count()

            print(f"[OK] Пользователей в БД: {user_count}")
            print(f"[OK] Серверов: {server_count}")
            print(f"[OK] Аккаунтов (ферм): {account_count}")
            print(f"[OK] FarmData-записей: {farmdata_count}")

            admin_exists = User.query.filter_by(role="admin").first() is not None
            if admin_exists:
                print("[OK] Найден хотя бы один администратор.")
            else:
                print("[WARN] Администраторов не найдено — будет создан дефолтный.")

            if server_count > 0:
                print("[INFO] Проверка доступности серверов по API:")
                for srv in Server.query.all():
                    ok, msg = ping_server(srv)
                    status = "OK" if ok else "FAIL"
                    print(f"  - {srv.name} ({srv.host}): {status} - {msg}")
            else:
                print("[INFO] Серверов пока нет — проверять нечего.")

    except Exception as exc:
        print(f"[ERROR] Ошибка при выполнении health-check: {exc}")

    print("==========================================================")
