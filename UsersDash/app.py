# app.py
# Основной вход Flask-приложения с мультитенантной архитектурой UsersDash.
# Запускается от имени администратора (на Windows) и выполняет health-check при старте.

import os
import sys
import ctypes
import traceback
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path

# Обеспечиваем возможность запускать приложение напрямую (python app.py)
# даже если корневая папка проекта не лежит в PYTHONPATH (актуально на Windows).
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from flask import Flask, g, redirect, url_for
from flask_login import LoginManager, current_user
from flask_sqlalchemy import SQLAlchemy  # только для типов, основная инстанция в models.py

from UsersDash.config import Config
from UsersDash.models import db, User, Server, Account, FarmData
from UsersDash.auth import auth_bp
from UsersDash.admin_views import admin_bp
from UsersDash.client_views import client_bp
from UsersDash.api_views import api_bp
from UsersDash.services.db_backup import backup_database, ensure_backup_dir
from UsersDash.services.farmdata_status import collect_farmdata_status


# -------------------------------------------------
# Функции для запуска с правами администратора (Windows)
# -------------------------------------------------


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
        # Если что-то пошло не так — считаем, что прав нет
        return False


def relaunch_as_admin_if_needed():
    """
    Если мы на Windows и процесс не с админ-правами — пытаемся перезапустить
    текущий скрипт от имени администратора.
    На других ОС ничего не делает.
    """
    if os.name != "nt":
        return

    if is_admin():
        # Уже админ — ничего не делаем
        return

    # Перезапускаем текущий интерпретатор с теми же аргументами
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
        # Завершаем текущий процесс, дальше будет работать новый
        sys.exit(0)
    except Exception as exc:
        # Не удалось перезапуститься — логируем и продолжаем без повышения
        print(f"[WARN] Не удалось перезапустить от имени администратора: {exc}")


# -------------------------------------------------
# Инициализация Flask-приложения и LoginManager
# -------------------------------------------------


login_manager = LoginManager()
login_manager.login_view = "auth.login"
login_manager.login_message = "Пожалуйста, войдите в систему."
login_manager.login_message_category = "warning"


@login_manager.user_loader
def load_user(user_id: str):
    """
    Функция, которую использует Flask-Login для загрузки пользователя по ID.
    """
    if not user_id:
        return None
    try:
        # SQLAlchemy 2.x по-прежнему поддерживает User.query.get, но можно и через сессию
        return User.query.get(int(user_id))
    except Exception as exc:
        print(f"[login_manager.user_loader] ERROR: {exc}")
        return None


# -------------------------------------------------
# Health-check при старте: проверяем БД и базовые сущности
# -------------------------------------------------


def run_health_check(app: Flask):
    """
    Выполняет базовые проверки:
      - существует ли файл БД;
      - можно ли выполнить простые запросы к таблицам;
      - есть ли хотя бы один админ.
    Все ошибки не валят приложение, а только логируются.
    """
    print("========== MULTI-TENANT DASHBOARD HEALTH-CHECK ==========")

    try:
        db_path = None
        uri = app.config.get("SQLALCHEMY_DATABASE_URI", "")
        if uri.startswith("sqlite:///"):
            db_path = uri.replace("sqlite:///", "", 1)
            if not os.path.isabs(db_path):
                # Привязываем к BASE_DIR на всякий случай
                db_path = str((Config.BASE_DIR / db_path).resolve())

        if db_path:
            if os.path.exists(db_path):
                print(f"[OK] БД найдена: {db_path}")
            else:
                print(f"[WARN] Файл БД не найден: {db_path}")
        else:
            print("[WARN] Не удалось определить путь к БД из SQLALCHEMY_DATABASE_URI.")

        # Пробуем выполнить простые запросы
        from sqlalchemy import text

        with app.app_context():
            db.session.execute(text("SELECT 1"))
            print("[OK] Подключение к БД установлено.")

            # Считаем сущности. Если структуры таблиц не совпадают с моделями,
            # это может выбросить OperationalError (мы его отловим).
            users_count = User.query.count()
            servers_count = Server.query.count()
            accounts_count = Account.query.count()
            farmdata_count = FarmData.query.count()

            print(f"[OK] Пользователей в БД: {users_count}")
            print(f"[OK] Серверов: {servers_count}")
            print(f"[OK] Аккаунтов (ферм): {accounts_count}")
            print(f"[OK] FarmData-записей: {farmdata_count}")

            # Проверка наличия хотя бы одного админа
            admin_exists = User.query.filter_by(role="admin").first() is not None
            if admin_exists:
                print("[OK] Найден хотя бы один администратор.")
            else:
                print("[WARN] Администраторов не найдено — будет создан дефолтный.")

    except Exception as exc:
        # Любая ошибка health-check не должна останавливать приложение
        print(f"[ERROR] Ошибка при выполнении health-check: {exc}")

    print("==========================================================")


# -------------------------------------------------
# Служебные функции: создание дефолтного админа и директорий
# -------------------------------------------------


def ensure_data_dir():
    """
    Создаёт директорию для БД и данных, если её ещё нет.
    """
    data_dir: Path = Config.DATA_DIR
    if not data_dir.exists():
        data_dir.mkdir(parents=True, exist_ok=True)


def ensure_default_admin():
    """
    Если в системе нет ни одного пользователя с ролью 'admin' — создаёт дефолтного
    администратора admin / admin.
    Пароль настоятельно рекомендуется сменить после первого входа.
    """
    from werkzeug.security import generate_password_hash

    if User.query.filter_by(role="admin").first():
        return

    admin = User(
        username="admin",
        password_hash=generate_password_hash("admin"),
        role="admin",
        is_active=True,
    )
    db.session.add(admin)
    db.session.commit()
    print("[INFO] Создан дефолтный администратор: логин 'admin', пароль 'admin'.")


def _run_midnight_backup(app: Flask):
    """Фоновая задача: ежедневный бэкап БД в 00:00."""

    def worker():
        while True:
            now = datetime.now()
            next_run = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            sleep_seconds = max(60, (next_run - now).total_seconds())
            time.sleep(sleep_seconds)

            try:
                with app.app_context():
                    path = backup_database("daily")
                    print(f"[backup] Ежедневный бэкап сохранён: {path}")
            except Exception as exc:
                print(f"[backup] Не удалось сделать ежедневный бэкап: {exc}")
                traceback.print_exc()

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()
    return thread


# -------------------------------------------------
# Фабрика приложения
# -------------------------------------------------


def create_app() -> Flask:
    """
    Фабрика Flask-приложения.
    """
    app = Flask(__name__)
    app.config.from_object(Config)

    # Убедимся, что папки для БД и бэкапов существуют
    ensure_data_dir()
    ensure_backup_dir()

    # Инициализируем расширения
    db.init_app(app)
    login_manager.init_app(app)

    # Создаём таблицы, если их ещё нет
    with app.app_context():
        db.create_all()
        ensure_default_admin()

    # Регистрируем blueprints
    app.register_blueprint(auth_bp)
    app.register_blueprint(admin_bp, url_prefix="/admin")
    app.register_blueprint(client_bp)
    app.register_blueprint(api_bp, url_prefix="/api")

    @app.context_processor
    def inject_farmdata_flags():
        farmdata_required = False
        farmdata_status = None

        try:
            if current_user.is_authenticated and getattr(current_user, "role", None) != "admin":
                if not hasattr(g, "farmdata_status_cache"):
                    g.farmdata_status_cache = collect_farmdata_status(current_user.id)

                farmdata_status = g.farmdata_status_cache
                farmdata_required = bool(farmdata_status.get("has_issues"))
        except Exception as exc:
            print(f"[context_processor] farmdata flags failed: {exc}")

        return {
            "farmdata_required": farmdata_required,
            "farmdata_status": farmdata_status,
        }

    # ---------- Маршрут по умолчанию ----------

    @app.route("/")
    def index():
        """
        Корневая страница:
          - неавторизованный пользователь -> /auth/login
          - клиент -> /dashboard
          - админ -> /admin (admin_dashboard)
        """
        if not current_user.is_authenticated:
            return redirect(url_for("auth.login"))

        if current_user.role == "admin":
            # ИСПРАВЛЕНО: правильный endpoint главной страницы админки
            return redirect(url_for("admin.admin_dashboard"))

        # по умолчанию считаем, что это клиент
        return redirect(url_for("client.dashboard"))

    # Выполняем health-check после инициализации
    run_health_check(app)

    # Фоновый бэкап БД каждый день в 00:00
    _run_midnight_backup(app)

    return app


# -------------------------------------------------
# Точка входа
# -------------------------------------------------


if __name__ == "__main__":
    # Пытаемся перезапустить приложение от имени администратора (Windows)
    relaunch_as_admin_if_needed()

    # Создаём и запускаем Flask-приложение
    app = create_app()
    # Для разработки — debug=True. В проде лучше выключить.
    app.run(host="0.0.0.0", port=5555, debug=True)
else:
    # Экземпляр для WSGI/CLI-запуска (gunicorn, flask run --app UsersDash.app и т.п.)
    app = create_app()
