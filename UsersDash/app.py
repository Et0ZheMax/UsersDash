# app.py
# Основной вход Flask-приложения с мультитенантной архитектурой UsersDash.
# Запускается от имени администратора (на Windows) и выполняет health-check при старте.

import os
import sys
import ctypes
from ctypes import wintypes
import signal
import subprocess
import traceback
import threading
import time
import atexit
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

# Обеспечиваем возможность запускать приложение напрямую (python app.py)
# даже если корневая папка проекта не лежит в PYTHONPATH (актуально на Windows).
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from flask import Flask, g, redirect, send_from_directory, url_for
from flask_login import LoginManager, current_user
from flask_sqlalchemy import SQLAlchemy  # только для типов, основная инстанция в models.py
from sqlalchemy import inspect, text

from UsersDash.config import Config
from UsersDash.models import db, User
from UsersDash.auth import auth_bp
from UsersDash.admin_views import admin_bp
from UsersDash.client_views import client_bp
from UsersDash.api_views import api_bp
from UsersDash.services.db_backup import backup_database, ensure_backup_dir
from UsersDash.services.farmdata_status import collect_farmdata_status
from UsersDash.services.health_check import run_health_check


_RENTAL_BOT_THREAD: threading.Thread | None = None
_RENTAL_BOT_PID_FILE = Path(tempfile.gettempdir()) / "usersdash_rental_bot.pid"

# Константы WinAPI для управления процессами.
_WIN_PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
_WIN_PROCESS_TERMINATE = 0x0001
_WIN_SYNCHRONIZE = 0x00100000
_WIN_STILL_ACTIVE = 259


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
        return db.session.get(User, int(user_id))
    except Exception as exc:
        print(f"[login_manager.user_loader] ERROR: {exc}")
        return None


def _process_is_alive(pid: int) -> bool:
    """Проверяет, существует ли процесс с заданным PID."""

    try:
        pid = int(pid)
    except Exception:
        return False

    if pid <= 0:
        return False

    # На Windows os.kill(pid, 0) часто ведёт себя нестабильно (например, WinError 87),
    # поэтому используем WinAPI: OpenProcess + GetExitCodeProcess + CloseHandle.
    if os.name == "nt":
        try:
            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            open_process = kernel32.OpenProcess
            get_exit_code_process = kernel32.GetExitCodeProcess
            close_handle = kernel32.CloseHandle

            open_process.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
            open_process.restype = wintypes.HANDLE
            get_exit_code_process.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
            get_exit_code_process.restype = wintypes.BOOL
            close_handle.argtypes = [wintypes.HANDLE]
            close_handle.restype = wintypes.BOOL

            handle = open_process(_WIN_PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
            if not handle:
                try:
                    import csv
                    from io import StringIO

                    result = subprocess.run(
                        ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
                        capture_output=True,
                        text=True,
                        timeout=10,
                        check=False,
                    )
                    stdout = (result.stdout or "").strip()
                    if not stdout or "no tasks are running" in stdout.lower():
                        return False

                    for row in csv.reader(StringIO(stdout)):
                        if len(row) < 2:
                            continue
                        try:
                            if int(row[1].strip()) == pid:
                                return True
                        except Exception:
                            continue
                    return False
                except Exception:
                    return False

            try:
                exit_code = wintypes.DWORD(0)
                if not get_exit_code_process(handle, ctypes.byref(exit_code)):
                    return False
                return exit_code.value == _WIN_STILL_ACTIVE
            finally:
                close_handle(handle)
        except Exception:
            return False

    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    except Exception:
        return False
    return True


def _terminate_process(pid: int, timeout_sec: float = 5.0) -> bool:
    """Завершает процесс: на Windows принудительно (WinAPI/taskkill), на POSIX SIGTERM→SIGKILL."""

    try:
        pid = int(pid)
    except Exception:
        return True

    if pid <= 0:
        return True

    if not _process_is_alive(pid):
        return True

    if os.name == "nt":
        try:
            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            open_process = kernel32.OpenProcess
            terminate_process = kernel32.TerminateProcess
            close_handle = kernel32.CloseHandle

            open_process.argtypes = [ctypes.c_uint32, ctypes.c_int, ctypes.c_uint32]
            open_process.restype = ctypes.c_void_p
            terminate_process.argtypes = [ctypes.c_void_p, ctypes.c_uint32]
            terminate_process.restype = ctypes.c_int
            close_handle.argtypes = [ctypes.c_void_p]
            close_handle.restype = ctypes.c_int

            handle = open_process(_WIN_PROCESS_TERMINATE | _WIN_SYNCHRONIZE, False, pid)
            if handle:
                try:
                    if not terminate_process(handle, 1):
                        print(f"[rental-bot] Не удалось завершить процесс {pid} через WinAPI.")
                    else:
                        deadline = time.time() + max(0.1, timeout_sec)
                        while time.time() < deadline:
                            if not _process_is_alive(pid):
                                return True
                            time.sleep(0.2)
                finally:
                    close_handle(handle)
            else:
                print(f"[rental-bot] OpenProcess не открыл PID {pid}, пробуем taskkill.")
        except Exception as exc:
            print(f"[rental-bot] Ошибка WinAPI при завершении PID {pid}: {exc}")

        try:
            result = subprocess.run(
                ["taskkill", "/PID", str(pid), "/T", "/F"],
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
            if result.returncode not in (0,):
                stderr = (result.stderr or "").strip()
                stdout = (result.stdout or "").strip()
                details = stderr or stdout or "без деталей"
                print(f"[rental-bot] taskkill вернул код {result.returncode} для PID {pid}: {details}")
        except Exception as exc:
            print(f"[rental-bot] Не удалось запустить taskkill для PID {pid}: {exc}")

        return not _process_is_alive(pid)

    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return True
    except Exception as exc:
        print(f"[rental-bot] Не удалось отправить SIGTERM процессу {pid}: {exc}")
        return False

    deadline = time.time() + max(0.1, timeout_sec)
    while time.time() < deadline:
        if not _process_is_alive(pid):
            return True
        time.sleep(0.2)

    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        return True
    except Exception as exc:
        print(f"[rental-bot] Не удалось принудительно завершить процесс {pid}: {exc}")
        return False

    return not _process_is_alive(pid)


def _cleanup_previous_rental_bot_session() -> None:
    """Закрывает предыдущую сессию rental-бота по PID-файлу и фиксирует текущий PID."""

    current_pid = os.getpid()
    if _RENTAL_BOT_PID_FILE.exists():
        previous_pid_raw = ""
        previous_pid = 0

        try:
            previous_pid_raw = _RENTAL_BOT_PID_FILE.read_text(encoding="utf-8").strip()
        except Exception as exc:
            print(f"[rental-bot] Не удалось прочитать PID-файл {_RENTAL_BOT_PID_FILE}: {exc}")

        if previous_pid_raw:
            try:
                previous_pid = int(previous_pid_raw)
            except Exception:
                print(f"[rental-bot] Некорректный PID в файле: {previous_pid_raw!r}. Пропускаем очистку.")
                previous_pid = 0

        if previous_pid <= 0:
            # Нечего завершать: PID отсутствует, битый или заведомо невалидный.
            previous_pid = 0
        elif previous_pid == current_pid:
            print("[rental-bot] PID в файле совпадает с текущим процессом. Пропускаем очистку.")
        else:
            try:
                previous_alive = _process_is_alive(previous_pid)
            except Exception as exc:
                print(f"[rental-bot] Ошибка проверки предыдущего PID {previous_pid}: {exc}")
                previous_alive = False

            if previous_alive:
                print(
                    "[rental-bot] Обнаружена предыдущая сессия UsersDash "
                    f"(PID {previous_pid}). Завершаем её перед автозапуском бота."
                )
                try:
                    terminated = _terminate_process(previous_pid)
                except Exception as exc:
                    print(f"[rental-bot] Ошибка завершения предыдущего PID {previous_pid}: {exc}")
                    terminated = False

                if terminated:
                    print(f"[rental-bot] Предыдущая сессия PID {previous_pid} завершена.")
                else:
                    print(
                        "[rental-bot] Не удалось завершить предыдущую сессию. "
                        "Возможен конфликт polling у Telegram."
                    )

    try:
        _RENTAL_BOT_PID_FILE.write_text(str(current_pid), encoding="utf-8")
    except Exception as exc:
        print(f"[rental-bot] Не удалось обновить PID-файл {_RENTAL_BOT_PID_FILE}: {exc}")


def _clear_rental_bot_pid_file() -> None:
    """Удаляет PID-файл, если он принадлежит текущему процессу."""

    if not _RENTAL_BOT_PID_FILE.exists():
        return
    try:
        pid = int(_RENTAL_BOT_PID_FILE.read_text(encoding="utf-8").strip())
    except Exception:
        pid = 0
    if pid == os.getpid():
        try:
            _RENTAL_BOT_PID_FILE.unlink(missing_ok=True)
        except Exception:
            pass


atexit.register(_clear_rental_bot_pid_file)


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


def ensure_blocked_for_payment_column():
    """Добавляет колонку blocked_for_payment в accounts, если её нет (SQLite)."""

    try:
        engine = db.engine
        if engine.url.get_backend_name() != "sqlite":
            return

        inspector = inspect(engine)
        columns = [col["name"] for col in inspector.get_columns("accounts")]
        if "blocked_for_payment" in columns:
            return

        with engine.connect() as conn:
            conn.execute(
                text(
                    "ALTER TABLE accounts "
                    "ADD COLUMN blocked_for_payment BOOLEAN DEFAULT 0 NOT NULL"
                )
            )
        print("[MIGRATE] Добавлена колонка accounts.blocked_for_payment")
    except Exception as exc:
        print(f"[MIGRATE] Не удалось добавить колонку blocked_for_payment: {exc}")


def ensure_farm_data_account_id_column() -> None:
    """Добавляет колонку account_id в farm_data и синхронизирует данные (SQLite)."""

    try:
        engine = db.engine
        if engine.url.get_backend_name() != "sqlite":
            return

        inspector = inspect(engine)
        columns = [col["name"] for col in inspector.get_columns("farm_data")]
        if "account_id" in columns:
            return

        merge_fields = (
            "email",
            "login",
            "password",
            "igg_id",
            "server",
            "telegram_tag",
        )

        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE farm_data ADD COLUMN account_id INTEGER"))

            conn.execute(
                text(
                    """
                    UPDATE farm_data
                    SET account_id = (
                        SELECT accounts.id
                        FROM accounts
                        WHERE accounts.owner_id = farm_data.user_id
                          AND accounts.name = farm_data.farm_name
                    )
                    WHERE account_id IS NULL
                    """
                )
            )

            conn.execute(
                text(
                    """
                    UPDATE farm_data
                    SET user_id = (
                        SELECT accounts.owner_id
                        FROM accounts
                        WHERE accounts.id = farm_data.account_id
                    )
                    WHERE account_id IS NOT NULL
                    """
                )
            )

            conn.execute(
                text(
                    """
                    UPDATE farm_data
                    SET farm_name = (
                        SELECT accounts.name
                        FROM accounts
                        WHERE accounts.id = farm_data.account_id
                    )
                    WHERE account_id IS NOT NULL
                    """
                )
            )

            duplicates = conn.execute(
                text(
                    """
                    SELECT account_id
                    FROM farm_data
                    WHERE account_id IS NOT NULL
                    GROUP BY account_id
                    HAVING COUNT(*) > 1
                    """
                )
            ).fetchall()

            for row in duplicates:
                account_id = row[0]
                rows = conn.execute(
                    text(
                        """
                        SELECT id, email, login, password, igg_id, server, telegram_tag,
                               created_at, updated_at
                        FROM farm_data
                        WHERE account_id = :account_id
                        ORDER BY COALESCE(updated_at, created_at) DESC, id DESC
                        """
                    ),
                    {"account_id": account_id},
                ).mappings().all()

                if not rows:
                    continue

                keep_row = rows[0]
                updates: dict[str, str | None] = {}
                for field in merge_fields:
                    if keep_row[field] not in (None, ""):
                        continue
                    for candidate in rows[1:]:
                        value = candidate[field]
                        if value not in (None, ""):
                            updates[field] = value
                            break

                if updates:
                    assignments = ", ".join([f"{field} = :{field}" for field in updates])
                    updates["id"] = keep_row["id"]
                    conn.execute(
                        text(f"UPDATE farm_data SET {assignments} WHERE id = :id"),
                        updates,
                    )

                for dup_row in rows[1:]:
                    conn.execute(
                        text("DELETE FROM farm_data WHERE id = :id"),
                        {"id": dup_row["id"]},
                    )

            null_count = conn.execute(
                text("SELECT COUNT(*) FROM farm_data WHERE account_id IS NULL")
            ).scalar_one()
            if null_count:
                print(f"[MIGRATE] Внимание: записей farm_data без account_id: {null_count}")

            indexes = conn.execute(text("PRAGMA index_list(farm_data)")).fetchall()
            for index in indexes:
                if index[1] == "uq_farm_data_user_farm":
                    conn.execute(text("DROP INDEX uq_farm_data_user_farm"))
                    break

            conn.execute(
                text(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS uq_farm_data_account_id
                    ON farm_data (account_id)
                    """
                )
            )

        print("[MIGRATE] Добавлена колонка farm_data.account_id и выполнена синхронизация.")
    except Exception as exc:
        print(f"[MIGRATE] Не удалось обновить farm_data.account_id: {exc}")


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


def _run_rental_bot_worker() -> threading.Thread | None:
    """Запускает rental Telegram-бота в отдельном daemon-потоке."""

    global _RENTAL_BOT_THREAD

    if _RENTAL_BOT_THREAD and _RENTAL_BOT_THREAD.is_alive():
        return _RENTAL_BOT_THREAD

    rental_token = (os.environ.get("RENTAL_TELEGRAM_BOT_TOKEN") or "").strip()
    if not rental_token:
        print("[rental-bot] Автозапуск пропущен: отсутствует RENTAL_TELEGRAM_BOT_TOKEN.")
        return None

    _cleanup_previous_rental_bot_session()

    def worker():
        try:
            from UsersDash.bot.rental_bot import main as rental_bot_main

            rental_bot_main()
        except Exception as exc:
            print(f"[rental-bot] Критическая ошибка фонового запуска: {exc}")
            traceback.print_exc()

    thread = threading.Thread(target=worker, daemon=True, name="usersdash-rental-bot")
    thread.start()
    _RENTAL_BOT_THREAD = thread
    print("[rental-bot] Автозапуск выполнен вместе с UsersDash.")
    return thread


# -------------------------------------------------
# Фабрика приложения
# -------------------------------------------------


def create_app(enable_background_workers: bool = True) -> Flask:
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
        ensure_blocked_for_payment_column()
        ensure_farm_data_account_id_column()
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

    @app.route("/favicon.ico")
    def favicon():
        """Единая иконка для всех страниц и будущих разделов."""

        return send_from_directory(
            app.static_folder, "usersdash.png", mimetype="image/png"
        )

    # Выполняем health-check после инициализации
    run_health_check(app)

    if enable_background_workers:
        # Фоновый бэкап БД каждый день в 00:00
        _run_midnight_backup(app)
        # Автозапуск Telegram-бота продления аренды вместе с приложением
        _run_rental_bot_worker()

    return app


# -------------------------------------------------
# Точка входа
# -------------------------------------------------



if __name__ == "__main__":
    # Пытаемся перезапустить приложение от имени администратора (Windows)
    relaunch_as_admin_if_needed()

    # Создаём и запускаем Flask-приложение
    # В debug-режиме Werkzeug создаёт родительский и дочерний процессы.
    # Фоновые потоки запускаем только в дочернем, чтобы не плодить дубли.
    run_background_workers = os.environ.get("WERKZEUG_RUN_MAIN") == "true"
    app = create_app(enable_background_workers=run_background_workers)
    # Для разработки — debug=True. В проде лучше выключить.
    app.run(host="0.0.0.0", port=5555, debug=True)
else:
    # Экземпляр для WSGI/CLI-запуска (gunicorn, flask run --app UsersDash.app и т.п.)
    app = create_app()
