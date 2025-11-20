# services/health_check.py
# Health-check: проверка БД, наличия админа, серверов и пр.
# При старте выводит лог в консоль, не падает при единичных ошибках.

from UsersDash.models import User, Server, Account
from UsersDash.services.remote_api import ping_server


def run_health_check(app):
    """
    Выполняет базовую проверку состояния приложения и выводит лог в консоль.
    Здесь можно и нужно расширять проверки (доступ к логам, к БД ресурсов и т.п.).
    """
    print("========== MULTI-TENANT DASHBOARD HEALTH-CHECK ==========")
    try:
        user_count = User.query.count()
        server_count = Server.query.count()
        account_count = Account.query.count()

        print(f"[OK] Подключение к БД успешно.")
        print(f"[INFO] Пользователей в системе: {user_count}")
        print(f"[INFO] Серверов зарегистрировано: {server_count}")
        print(f"[INFO] Аккаунтов/ферм в БД: {account_count}")

        # Проверим, есть ли хотя бы один админ
        admin_exists = User.query.filter_by(role="admin").first() is not None
        if admin_exists:
            print("[OK] Найден хотя бы один администратор.")
        else:
            print("[WARN] Администратор не найден (но ensure_default_admin должен был его создать).")

        # Дополнительно проверим доступность серверов по API
        if server_count > 0:
            print("[INFO] Проверка доступности серверов по API:")
            for srv in Server.query.all():
                ok, msg = ping_server(srv)
                status = "OK" if ok else "FAIL"
                print(f"  - {srv.name} ({srv.host}): {status} - {msg}")
        else:
            print("[INFO] Серверов пока нет — проверять нечего.")

    except Exception as exc:
        # Ловим любые ошибки и выводим в консоль, чтобы само приложение не падало
        print(f"[ERROR] Ошибка при выполнении health-check: {exc}")

    print("==========================================================")
