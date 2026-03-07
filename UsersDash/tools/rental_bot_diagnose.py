"""Диагностика расхождений очереди подтверждений и проблем с открытием меню в rental-боте."""

from __future__ import annotations

from pathlib import Path

from sqlalchemy import distinct, inspect, text
from sqlalchemy.exc import SQLAlchemyError

from UsersDash.bot.rental_bot import create_flask_context, render_client_dashboard
from UsersDash.models import RenewalBatchRequest, RenewalRequest, TelegramSubscriber, db
from UsersDash.services.rental_bot import get_multi_pending_statuses

SINGLE_PENDING_STATUSES = (
    "payment_pending_confirmation",
    "payment_data_collecting",
    "needs_info",
)


def _sqlite_file_info(db_uri: str) -> tuple[Path | None, bool, int | None]:
    """Возвращает путь к SQLite-файлу, факт существования и размер в байтах."""

    if not db_uri.startswith("sqlite:///"):
        return None, False, None

    raw_path = db_uri.replace("sqlite:///", "", 1)
    db_path = Path(raw_path)
    if not db_path.is_absolute():
        db_path = (Path.cwd() / db_path).resolve()
    exists = db_path.exists()
    size = db_path.stat().st_size if exists else None
    return db_path, exists, size


def main() -> int:
    """Запускает самодиагностику и печатает понятный отчёт."""

    app = create_flask_context()
    print("===== rental bot diagnose =====")

    with app.app_context():
        db_uri = app.config.get("SQLALCHEMY_DATABASE_URI", "")
        print(f"SQLALCHEMY_DATABASE_URI: {db_uri}")

        sqlite_path, sqlite_exists, sqlite_size = _sqlite_file_info(db_uri)
        if sqlite_path is not None:
            print(f"SQLite path: {sqlite_path}")
            print(f"SQLite exists: {'yes' if sqlite_exists else 'no'}")
            print(f"SQLite size: {sqlite_size if sqlite_size is not None else 0} bytes")

        try:
            db.session.execute(text("SELECT 1"))
        except SQLAlchemyError as exc:
            print(f"Ошибка подключения к БД: {exc}")
            print("===== diagnose completed =====")
            return 0

        inspector = inspect(db.engine)
        has_single = inspector.has_table("renewal_requests")
        has_multi = inspector.has_table("renewal_batch_requests")
        print(f"Таблица renewal_requests: {'yes' if has_single else 'no'}")
        print(f"Таблица renewal_batch_requests: {'yes' if has_multi else 'no'}")

        single_distinct: list[str] = []
        multi_distinct: list[str] = []
        single_pending_count = 0
        multi_pending_count = 0
        multi_pending_statuses: list[str] = []

        if has_single:
            single_distinct = sorted(
                status
                for (status,) in db.session.query(distinct(RenewalRequest.status)).all()
                if status
            )
            single_pending_count = RenewalRequest.query.filter(
                RenewalRequest.status.in_(SINGLE_PENDING_STATUSES)
            ).count()

        if has_multi:
            multi_distinct = sorted(
                status
                for (status,) in db.session.query(distinct(RenewalBatchRequest.status)).all()
                if status
            )
            multi_pending_statuses = get_multi_pending_statuses()
            multi_pending_count = RenewalBatchRequest.query.filter(
                RenewalBatchRequest.status.in_(multi_pending_statuses)
            ).count()

        site_like_count = single_pending_count + multi_pending_count

        print(f"RenewalRequest distinct statuses: {single_distinct}")
        print(f"RenewalBatchRequest distinct statuses: {multi_distinct}")
        print(f"Single pending statuses: {list(SINGLE_PENDING_STATUSES)}")
        print(f"Multi pending statuses (adaptive): {multi_pending_statuses}")
        print(f"single_pending_count: {single_pending_count}")
        print(f"multi_pending_count: {multi_pending_count}")
        print(f"site_like_count: {site_like_count}")

        print("\nПоследние 5 RenewalRequest:")
        if not has_single:
            print("  — таблица отсутствует")
        else:
            latest_single = RenewalRequest.query.order_by(RenewalRequest.created_at.desc()).limit(5).all()
            if not latest_single:
                print("  — нет записей")
            for row in latest_single:
                print(
                    f"  id={row.id} status={row.status} created_at={row.created_at} "
                    f"user_id={row.user_id} account_id={row.account_id}"
                )

        print("\nПоследние 5 заявок по нескольким фермам:")
        if not has_multi:
            print("  — таблица отсутствует")
        else:
            latest_multi = RenewalBatchRequest.query.order_by(RenewalBatchRequest.created_at.desc()).limit(5).all()
            if not latest_multi:
                print("  — нет записей")
            for row in latest_multi:
                print(
                    f"  id={row.id} status={row.status} created_at={row.created_at} "
                    f"user_id={row.user_id} mode={row.mode or '—'}"
                )

        print("\nПроверка длины текста меню:")
        has_subscribers = inspector.has_table("telegram_subscribers")
        profile = None
        menu_too_long = False
        if not has_subscribers:
            print("  Таблица telegram_subscribers отсутствует, проверить длину меню нельзя.")
        else:
            profile = TelegramSubscriber.query.filter_by(is_active=True).order_by(TelegramSubscriber.id.asc()).first()
            if not profile:
                print("  Нет активного TelegramSubscriber, проверить длину меню нельзя.")
            else:
                dashboard_text = render_client_dashboard(profile)
                text_len = len(dashboard_text)
                menu_too_long = text_len > 4096
                print(f"  subscriber_id={profile.id}, user_id={profile.user_id}, text_len={text_len}")
                print(f"  превышение лимита 4096: {'yes' if menu_too_long else 'no'}")

        print("\nИтог диагностики:")
        if not has_single and not has_multi:
            print("  база не содержит таблиц очереди заявок — миграции не применены или выбрана не та БД")
        elif site_like_count == 0:
            print("  в базе нет pending заявок")
        else:
            if single_pending_count == 0 and multi_pending_count > 0:
                print(
                    "  бот ранее мог показывать 0, потому что не учитывал заявки по нескольким фермам "
                    "в общей очереди"
                )
            else:
                print("  в базе есть pending заявки; если бот показывал 0, проверьте фильтр статусов и подключение к БД")

        if menu_too_long:
            print("  меню падало из-за лимита Telegram (сообщение > 4096 символов)")
        else:
            print("  длина меню в пределах лимита; ошибка меню связана с исключением при построении/отправке")

    print("===== diagnose completed =====")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
