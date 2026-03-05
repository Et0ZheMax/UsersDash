"""Aiogram-бот для уведомлений об аренде и заявок на продление."""

from __future__ import annotations

import asyncio
import logging
import os
from collections import defaultdict
import json
import time
from pathlib import Path
from dataclasses import dataclass
from datetime import date, datetime, timedelta

from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command, CommandStart
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from flask import Flask
from sqlalchemy import inspect, text

from UsersDash.config import Config
from UsersDash.models import (
    Account,
    FarmData,
    RenewalBatchItem,
    RenewalBatchRequest,
    RenewalAdminAction,
    RenewalRequest,
    RenewalRequestMessage,
    RentalNotificationLog,
    TelegramSubscriber,
    User,
    db,
)
from UsersDash.services.rental_bot import (
    BatchValidationError,
    NotificationCandidate,
    RentalBotError,
    TokenValidationError,
    admin_dashboard_snapshot,
    bind_telegram_chat,
    collect_notification_candidates,
    confirm_batch_request,
    confirm_renewal_request,
    create_notification_batch,
    ensure_batch_editable,
    create_renewal_request,
    get_batch_for_user,
    get_bot_settings,
    log_notification_result,
    mark_batch_mode,
    reject_batch_request,
    reject_renewal_request,
    notification_stage,
    set_batch_selected_accounts,
    submit_batch_request,
    to_utc_naive,
    unresolved_batch_requests,
    unresolved_requests,
    utcnow,
)
from UsersDash.services.tariffs import get_tariff_name_by_price


class PaymentFSM(StatesGroup):
    """FSM для приёма данных об оплате от клиента."""

    waiting_amount = State()
    waiting_method = State()
    waiting_comment = State()


class BatchPaymentFSM(StatesGroup):
    """FSM для сценариев оплаты по нескольким фермам."""

    waiting_amount = State()
    waiting_method = State()
    waiting_comment = State()
    waiting_manual_comment = State()



class AdminClarifyFSM(StatesGroup):
    """FSM для вопроса администратора по заявке."""

    waiting_question = State()


class ClientClarifyFSM(StatesGroup):
    """FSM для ответа клиента на уточнение."""

    waiting_answer = State()


class AdminSettingsFSM(StatesGroup):
    """FSM редактирования настроек бота администратором."""

    waiting_value = State()


@dataclass(slots=True)
class RuntimeConfig:
    """Runtime-конфиг Telegram-бота."""

    token: str
    admin_chat_ids: set[int]
    reminder_days: list[int]


logger = logging.getLogger(__name__)


def build_runtime_config() -> RuntimeConfig:
    """Собирает настройки запуска из ENV."""

    token = (os.environ.get("RENTAL_TELEGRAM_BOT_TOKEN") or "").strip()
    if not token:
        raise RuntimeError(
            "Не задан RENTAL_TELEGRAM_BOT_TOKEN. "
            "Для rental-бота используйте отдельный токен, без fallback на TELEGRAM_BOT_TOKEN."
        )

    raw_admin_ids = os.environ.get("TELEGRAM_ADMIN_CHAT_IDS", "")
    admin_ids: set[int] = set()
    for raw_item in raw_admin_ids.split(","):
        item = raw_item.strip()
        if not item:
            continue
        try:
            admin_ids.add(int(item))
        except ValueError:
            logger.warning("Пропущен невалидный TELEGRAM_ADMIN_CHAT_IDS элемент: %s", item)

    raw_days = os.environ.get("RENTAL_REMINDER_DAYS", "3,1,0,-1")
    reminder_days = [int(item.strip()) for item in raw_days.split(",") if item.strip()]
    return RuntimeConfig(token=token, admin_chat_ids=admin_ids, reminder_days=reminder_days)


def build_user_keyboard(batch_id: int, admin_contact: str | None) -> InlineKeyboardMarkup:
    """Кнопки для сценариев оплаты по нескольким фермам клиента."""

    rows = [
        [InlineKeyboardButton(text="✅ Я оплатил всё", callback_data=f"batch_full:{batch_id}")],
        [InlineKeyboardButton(text="☑️ Я оплатил часть", callback_data=f"batch_partial:{batch_id}:0")],
        [InlineKeyboardButton(text="✍️ Есть изменения", callback_data=f"batch_change:{batch_id}")],
    ]
    if admin_contact:
        rows.append([InlineKeyboardButton(text="Связаться с администратором", url=admin_contact)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_dashboard_keyboard(admin_contact: str | None) -> InlineKeyboardMarkup:
    """Компактное меню для уже привязанного клиента."""

    rows = [
        [InlineKeyboardButton(text="💳 Оплата", callback_data="menu:payment")],
        [InlineKeyboardButton(text="🔁 Реквизиты и как оплатить", callback_data="menu:payment_info")],
        [InlineKeyboardButton(text="🧾 Мои заявки", callback_data="menu:my_requests")],
        [InlineKeyboardButton(text="⏸ Пауза напоминаний", callback_data="menu:pause")],
        [InlineKeyboardButton(text="✍️ Есть изменения", callback_data="menu:change")],
        [InlineKeyboardButton(text="📄 Мои фермы", callback_data="menu:farms")],
        [InlineKeyboardButton(text="🆘 Поддержка", callback_data="menu:support")],
    ]
    if admin_contact:
        rows.append([InlineKeyboardButton(text="Связаться с администратором", url=admin_contact)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _account_status_label(account: Account) -> str:
    if account.blocked_for_payment:
        return "Заблокирована"
    if not account.is_active:
        return "Неактивна"
    return "Активна"


def _collect_user_accounts(user_id: int) -> list[tuple[Account, FarmData | None]]:
    """Возвращает фермы пользователя вместе с FarmData."""

    rows = (
        Account.query.outerjoin(FarmData, FarmData.account_id == Account.id)
        .filter(Account.owner_id == user_id)
        .order_by(Account.name.asc())
        .with_entities(Account, FarmData)
        .all()
    )
    return rows


def render_client_dashboard(profile: TelegramSubscriber) -> str:
    """Краткая сводка клиента для /start без токена."""

    accounts = _collect_user_accounts(profile.user_id)
    if not accounts:
        return (
            "✅ Telegram уже привязан к вашему кабинету UsersDash.\n"
            "Фермы пока не найдены. Когда администратор добавит фермы, они появятся здесь."
        )

    total = len(accounts)
    active = 0
    limited = 0
    problematic = 0
    due_candidates: list[tuple[datetime, Account, str]] = []

    for account, _farm_data in accounts:
        status = _account_status_label(account)
        if status == "Активна":
            active += 1
        else:
            limited += 1
        if status == "Заблокирована":
            problematic += 1

        due_at = to_utc_naive(account.next_payment_at)
        if due_at is not None:
            due_candidates.append((due_at, account, status))

    due_candidates.sort(key=lambda item: item[0])
    nearest_due = due_candidates[0][0].strftime("%d.%m.%Y") if due_candidates else "—"

    lines = [
        "✅ Telegram уже привязан к вашему кабинету UsersDash.",
        f"Ферм: {total} • Активных: {active} • Ограниченных: {limited} • Проблемных: {problematic}",
        f"Ближайшая оплата: {nearest_due}",
    ]
    if not due_candidates:
        lines.append("\nБлижайших оплат пока нет.")
        return "\n".join(lines)

    lines.append("\nБлижайшие к оплате фермы:")
    for idx, (due_at, account, status) in enumerate(due_candidates[:5], start=1):
        tariff_name = get_tariff_name_by_price(account.next_payment_tariff) if account.next_payment_tariff else None
        amount = account.next_payment_amount if account.next_payment_amount is not None else "—"
        lines.append(
            f"{idx}. {account.name}\n"
            f"   • Статус: {status}\n"
            f"   • Тариф: {tariff_name or 'Не указан'}\n"
            f"   • Оплатить до: {due_at.strftime('%d.%m.%Y')}\n"
            f"   • Сумма: {amount} ₽"
        )
    return "\n".join(lines)


def render_client_farms_list(profile: TelegramSubscriber, limit: int = 12) -> str:
    """Возвращает компактный список ферм клиента."""

    accounts = _collect_user_accounts(profile.user_id)
    if not accounts:
        return "Фермы пока не найдены."

    lines = ["📄 Мои фермы:"]
    for idx, (account, _farm_data) in enumerate(accounts[:limit], start=1):
        status = _account_status_label(account)
        tariff_name = get_tariff_name_by_price(account.next_payment_tariff) if account.next_payment_tariff else None
        due_at = to_utc_naive(account.next_payment_at)
        due_text = due_at.strftime("%d.%m.%Y") if due_at else "—"
        amount = account.next_payment_amount if account.next_payment_amount is not None else "—"
        lines.append(
            f"{idx}. {account.name}\n"
            f"   • Статус: {status}\n"
            f"   • Тариф: {tariff_name or 'Не указан'}\n"
            f"   • Оплатить до: {due_text}\n"
            f"   • Сумма: {amount} ₽"
        )

    remaining = len(accounts) - min(len(accounts), limit)
    if remaining > 0:
        lines.append(f"… и ещё {remaining}")

    return "\n".join(lines)


def build_partial_selection_keyboard(batch: RenewalBatchRequest, page: int, page_size: int = 6) -> InlineKeyboardMarkup:
    """Рисует страницу multi-select по фермам заявки по нескольким фермам."""

    items = list(batch.items.order_by("id").all())
    total_pages = max(1, (len(items) + page_size - 1) // page_size)
    page = max(0, min(page, total_pages - 1))
    start = page * page_size
    chunk = items[start:start + page_size]

    rows: list[list[InlineKeyboardButton]] = []
    for item in chunk:
        marker = "✅" if item.selected_for_renewal else "⬜"
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{marker} {item.account_name_snapshot}",
                    callback_data=f"batch_toggle:{batch.id}:{item.id}:{page}",
                )
            ]
        )

    nav_row: list[InlineKeyboardButton] = []
    if page > 0:
        nav_row.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"batch_partial:{batch.id}:{page - 1}"))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton(text="➡️ Далее", callback_data=f"batch_partial:{batch.id}:{page + 1}"))
    if nav_row:
        rows.append(nav_row)

    rows.append([
        InlineKeyboardButton(
            text="☑️ Выбрать всё на странице",
            callback_data=f"batch_select_page:{batch.id}:{page}",
        )
    ])

    rows.append([InlineKeyboardButton(text="Готово", callback_data=f"batch_partial_done:{batch.id}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def render_grouped_notification(candidates: list[NotificationCandidate]) -> str:
    """Рендерит агрегированное уведомление по нескольким фермам клиента."""

    total_amount = sum(item.account.next_payment_amount or 0 for item in candidates)
    header = [
        "⏳ Напоминание по оплате аренды:",
        f"Ферм к оплате: {len(candidates)}",
        f"Общий итог: {total_amount} ₽",
    ]
    for idx, candidate in enumerate(candidates, start=1):
        account = candidate.account
        amount = account.next_payment_amount or 0
        tariff_price = account.next_payment_tariff
        tariff_name = get_tariff_name_by_price(tariff_price) if tariff_price is not None else None
        tariff_name = tariff_name or "Индивидуальный"
        due_on = account.next_payment_at.strftime("%d.%m.%Y") if account.next_payment_at else "—"
        status = "Активна" if account.is_active and not account.blocked_for_payment else "Ограничена"
        header.append(
            f"\n{idx}. {account.name} (ID: {account.id})\n"
            f"   • Оплатить до: {due_on}\n"
            f"   • Сумма: {amount} ₽\n"
            f"   • Тариф: {tariff_name}\n"
            f"   • Статус: {status}"
        )
    return "\n".join(header)


def render_batch_notification(batch: RenewalBatchRequest) -> str:
    """Рендерит агрегированное уведомление по snapshot-данным batch."""

    items = RenewalBatchItem.query.filter_by(batch_request_id=batch.id).order_by(RenewalBatchItem.id.asc()).all()
    if not items:
        return (
            "⏳ Напоминание по оплате аренды:\n"
            "Для этой платёжной сессии пока нет актуальных ферм. "
            "Обновите данные или свяжитесь с администратором."
        )

    total_amount = sum(item.amount_rub_snapshot or 0 for item in items)
    visible_items = items[:8]
    lines = [
        "⏳ Напоминание по оплате аренды:",
        f"Ферм к оплате: {len(items)}",
        f"Общий итог: {total_amount} ₽",
    ]
    for idx, item in enumerate(visible_items, start=1):
        tariff_name = get_tariff_name_by_price(item.tariff_snapshot) if item.tariff_snapshot is not None else None
        due_text = item.due_at_snapshot.strftime("%d.%m.%Y") if item.due_at_snapshot else "—"
        status = "Активна"
        if item.blocked_snapshot:
            status = "Ограничена"
        elif item.is_active_snapshot is False:
            status = "Неактивна"
        lines.append(
            f"\n{idx}. {item.account_name_snapshot} (ID: {item.account_id})\n"
            f"   • Оплатить до: {due_text}\n"
            f"   • Сумма: {item.amount_rub_snapshot or 0} ₽\n"
            f"   • Тариф: {tariff_name or 'Индивидуальный'}\n"
            f"   • Статус: {status}"
        )

    remaining = len(items) - len(visible_items)
    if remaining > 0:
        lines.append(f"\n… и ещё {remaining}")

    return "\n".join(lines)


def build_admin_keyboard(request_id: int) -> InlineKeyboardMarkup:
    """Кнопки подтверждения заявки для админа."""

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"admin_confirm:{request_id}")],
            [InlineKeyboardButton(text="❌ Отклонить", callback_data=f"admin_reject:{request_id}")],
        ]
    )


def build_admin_batch_keyboard(batch_id: int, mode: str | None) -> InlineKeyboardMarkup:
    """Кнопки подтверждения заявки по нескольким фермам для админа."""

    if mode == "full":
        confirm_text = "✅ Подтвердить всё"
    elif mode == "partial":
        confirm_text = "✅ Подтвердить выбранные"
    else:
        confirm_text = "✅ Взять в работу"

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=confirm_text, callback_data=f"admin_batch_confirm:{batch_id}")],
            [InlineKeyboardButton(text="❌ Отклонить", callback_data=f"admin_batch_reject:{batch_id}")],
        ]
    )


def create_flask_context() -> Flask:
    """Поднимает минимальный Flask app context для SQLAlchemy."""

    app = Flask(__name__)
    app.config.from_object(Config)
    db.init_app(app)
    return app



INLINE_RATE_LIMIT_SECONDS = 1.2
_inline_hits: dict[tuple[int, str], float] = {}


def _is_admin(chat_id: int, cfg: RuntimeConfig) -> bool:
    return chat_id in cfg.admin_chat_ids


def _safe_callback_int(callback_data: str, index: int, sep: str = ":") -> int | None:
    parts = callback_data.split(sep)
    if len(parts) <= index:
        return None
    try:
        return int(parts[index])
    except (TypeError, ValueError):
        return None


def _admin_actor_id() -> int | None:
    admin_user = User.query.filter_by(role="admin", is_active=True).order_by(User.id.asc()).first()
    return admin_user.id if admin_user else None


def _resolve_audit_request_id(target_user_id: int | None = None) -> int | None:
    query = RenewalRequest.query
    if target_user_id is not None:
        target = query.filter_by(user_id=target_user_id).order_by(RenewalRequest.created_at.desc()).first()
        if target:
            return target.id
    fallback = query.order_by(RenewalRequest.created_at.desc()).first()
    return fallback.id if fallback else None


def _append_admin_audit(action_type: str, *, actor_user_id: int | None, request_id: int | None = None, details: dict | None = None) -> None:
    if not actor_user_id:
        logger.warning("Не удалось записать audit %s: actor_user_id отсутствует", action_type)
        return
    if not request_id:
        logger.warning("Не удалось записать audit %s: renewal_request_id отсутствует", action_type)
        return
    db.session.add(
        RenewalAdminAction(
            renewal_request_id=request_id,
            actor_user_id=actor_user_id,
            action_type=action_type,
            details_json=json.dumps(details or {}, ensure_ascii=False),
        )
    )


def _status_after_clarify(row: RenewalRequest) -> str:
    prev = (row.status_before_needs_info or "").strip()
    if prev and prev != "needs_info":
        return prev
    if row.request_type == "change":
        return "payment_data_collecting"
    return "payment_pending_confirmation"


def _is_duplicate_notification(candidate: NotificationCandidate) -> bool:
    stage = notification_stage(candidate.days_left)
    return (
        RentalNotificationLog.query.filter_by(
            account_id=candidate.account.id,
            user_id=candidate.user.id,
            due_on=candidate.due_on,
            stage=stage,
        )
        .filter(RentalNotificationLog.status.in_(["sent", "delivered"]))
        .first()
        is not None
    )


def _limit_inline(chat_id: int, action: str) -> bool:
    now_ts = time.monotonic()
    key = (chat_id, action)
    prev = _inline_hits.get(key)
    if prev and now_ts - prev < INLINE_RATE_LIMIT_SECONDS:
        return True
    _inline_hits[key] = now_ts
    return False


def _request_type_label(row: RenewalRequest) -> str:
    mapping = {
        "payment": "оплата",
        "change": "изменение",
    }
    request_type = (row.request_type or "").strip().lower()
    return mapping.get(request_type, "другое")


def _status_label(status: str) -> str:
    mapping = {
        "payment_pending_confirmation": "на проверке",
        "payment_data_collecting": "на проверке",
        "payment_confirmed": "подтверждено",
        "rejected": "отклонено",
        "needs_info": "нужно уточнение",
    }
    return mapping.get(status, status)


def parse_reminder_days(raw: str) -> list[int]:
    days = []
    for item in raw.split(','):
        part = item.strip()
        if not part:
            continue
        days.append(int(part))
    if not days:
        raise ValueError("Список дней пуст")
    return sorted(set(days), reverse=True)


def _settings_payment_details(settings) -> str:
    return (settings.payment_details_text or "").strip() or "Реквизиты пока не заданы."


def _settings_payment_instruction(settings) -> str:
    return (settings.payment_instruction_text or settings.payment_instructions or "").strip() or "Инструкция пока не задана."


def _pause_text(profile: TelegramSubscriber) -> str:
    pause_until = to_utc_naive(profile.pause_until)
    if pause_until and pause_until > utcnow():
        return f"пауза до {pause_until:%d.%m.%Y %H:%M}"
    if not profile.reminders_enabled:
        return "выключены"
    return "активны"


def run_startup_health_check(app: Flask, cfg: RuntimeConfig) -> None:
    print("===== RENTAL BOT HEALTH-CHECK =====")
    critical_ok = True

    if not cfg.token:
        print("[CRITICAL] Не задан RENTAL_TELEGRAM_BOT_TOKEN")
        critical_ok = False
    else:
        print("[OK] Токен бота задан")

    if not cfg.admin_chat_ids:
        print("[WARN] TELEGRAM_ADMIN_CHAT_IDS пуст")
    else:
        print(f"[OK] ADMIN_IDS: {len(cfg.admin_chat_ids)}")

    with app.app_context():
        try:
            db.session.execute(text("SELECT 1"))
            print("[OK] Подключение к БД")
        except Exception as exc:  # pragma: no cover
            print(f"[CRITICAL] Ошибка подключения к БД: {exc}")
            critical_ok = False
            print("===================================")
            return

        inspector = inspect(db.engine)
        critical_tables = [
            "telegram_subscribers",
            "telegram_bot_settings",
            "renewal_requests",
            "rental_notification_logs",
        ]
        for table_name in critical_tables:
            if inspector.has_table(table_name):
                print(f"[OK] Таблица {table_name} есть")
            else:
                print(f"[CRITICAL] Нет таблицы {table_name}")
                critical_ok = False

        optional_warnings: list[str] = []
        if not inspector.has_table("renewal_request_messages"):
            optional_warnings.append("Нет таблицы renewal_request_messages")

        if inspector.has_table("telegram_subscribers"):
            subscriber_columns = {item['name'] for item in inspector.get_columns("telegram_subscribers")}
            for col in ("reminders_enabled", "pause_until"):
                if col not in subscriber_columns:
                    optional_warnings.append(f"Нет колонки telegram_subscribers.{col}")

        if inspector.has_table("telegram_bot_settings"):
            settings_columns = {item['name'] for item in inspector.get_columns("telegram_bot_settings")}
            for col in ("payment_details_text", "payment_instruction_text", "support_contact", "reminder_days", "reminders_enabled"):
                if col not in settings_columns:
                    optional_warnings.append(f"Нет колонки telegram_bot_settings.{col}")

        if inspector.has_table("renewal_requests"):
            request_columns = {item['name'] for item in inspector.get_columns("renewal_requests")}
            for col in ("request_type", "status_before_needs_info"):
                if col not in request_columns:
                    optional_warnings.append(f"Нет колонки renewal_requests.{col}")

        for warning in optional_warnings:
            print(f"[WARN] {warning}. Подсказка: запустите migrate_add_rental_bot_admin_features.py")

    logs_dir = Path("logs")
    try:
        logs_dir.mkdir(exist_ok=True)
        check_file = logs_dir / "rental_bot_health.log"
        check_file.write_text(f"health-check {utcnow().isoformat()}\n", encoding="utf-8")
        print(f"[OK] Проверка записи логов: {check_file}")
    except Exception as exc:  # pragma: no cover
        print(f"[WARN] Нет доступа на запись логов: {exc}")

    print("[OK] CRITICAL проверки пройдены" if critical_ok else "[WARN] Есть ошибки в CRITICAL проверках")
    print("===================================")


def _admin_request_keyboard(request_id: int, include_open: bool = True) -> InlineKeyboardMarkup:
    rows = [[
        InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"admin_confirm:{request_id}"),
        InlineKeyboardButton(text="❌ Отклонить", callback_data=f"admin_reject:{request_id}"),
    ], [InlineKeyboardButton(text="❓ Уточнить", callback_data=f"admin_clarify:{request_id}")]]
    if include_open:
        rows.append([InlineKeyboardButton(text="🔎 Открыть", callback_data=f"admin_open:{request_id}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _pause_keyboard(is_active_pause: bool) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text=f"{days} дн.", callback_data=f"pause_set:{days}") for days in (1, 3, 7)],
            [InlineKeyboardButton(text=f"{days} дн.", callback_data=f"pause_set:{days}") for days in (14, 30)]]
    if is_active_pause:
        rows.append([InlineKeyboardButton(text="▶️ Снять паузу", callback_data="pause_clear")])
    return InlineKeyboardMarkup(inline_keyboard=rows)



def create_dispatcher(app: Flask, cfg: RuntimeConfig, bot: Bot) -> Dispatcher:
    """Конфигурирует роутер и хендлеры aiogram."""

    router = Router()
    dp = Dispatcher()
    dp.include_router(router)

    @router.message(CommandStart(deep_link=True))
    async def on_start_with_token(message: Message) -> None:
        deep_arg = (message.text or "").split(maxsplit=1)
        if len(deep_arg) < 2 or not deep_arg[1].startswith("bind_"):
            await message.answer("Привет! Для привязки перейдите по персональной ссылке из UsersDash.")
            return

        token = deep_arg[1].replace("bind_", "", 1)
        with app.app_context():
            try:
                profile = bind_telegram_chat(
                    raw_token=token,
                    chat_id=str(message.chat.id),
                    username=message.from_user.username if message.from_user else None,
                    first_name=message.from_user.first_name if message.from_user else None,
                    last_name=message.from_user.last_name if message.from_user else None,
                )
            except TokenValidationError as exc:
                await message.answer(f"Не удалось привязать Telegram: {exc}")
                return

            settings = get_bot_settings()
            dashboard_text = render_client_dashboard(profile)
            keyboard = build_dashboard_keyboard(settings.admin_contact)
        await message.answer("✅ Telegram успешно привязан. Теперь вы будете получать напоминания об аренде.")
        await message.answer(dashboard_text, reply_markup=keyboard)

    @router.message(CommandStart())
    async def on_start(message: Message) -> None:
        with app.app_context():
            profile = TelegramSubscriber.query.filter_by(chat_id=str(message.chat.id)).first()
            if not profile:
                await message.answer(
                    "Привет! Я бот продления аренды Viking Rise.\n"
                    "Для безопасной привязки используйте персональную ссылку из UsersDash."
                )
                return

            profile.last_interaction_at = utcnow()
            db.session.commit()
            settings = get_bot_settings()
            await message.answer(
                render_client_dashboard(profile),
                reply_markup=build_dashboard_keyboard(settings.admin_contact),
            )

    @router.callback_query(F.data == "menu:farms")
    async def on_menu_farms(callback: CallbackQuery) -> None:
        with app.app_context():
            profile = TelegramSubscriber.query.filter_by(chat_id=str(callback.message.chat.id)).first()
            if not profile:
                await callback.answer("Чат не привязан", show_alert=True)
                return
            profile.last_interaction_at = utcnow()
            db.session.commit()
            await callback.message.answer(render_client_farms_list(profile))
        await callback.answer()

    @router.callback_query(F.data == "menu:payment")
    async def on_menu_payment(callback: CallbackQuery) -> None:
        with app.app_context():
            profile = TelegramSubscriber.query.filter_by(chat_id=str(callback.message.chat.id)).first()
            if not profile:
                await callback.answer("Чат не привязан", show_alert=True)
                return

            accounts = Account.query.filter_by(owner_id=profile.user_id).filter(Account.next_payment_at.isnot(None)).all()
            if not accounts:
                await callback.message.answer("У вас пока нет ферм с указанной датой оплаты.")
                await callback.answer()
                return

            owner = User.query.get(profile.user_id)
            today = utcnow().date()
            candidates: list[NotificationCandidate] = []
            for account in accounts:
                due_at = to_utc_naive(account.next_payment_at)
                if due_at is None:
                    continue
                candidates.append(
                    NotificationCandidate(
                        account=account,
                        user=owner,
                        subscriber=profile,
                        telegram_tag=None,
                        days_left=(due_at.date() - today).days,
                        due_on=due_at.date(),
                    )
                )
            if not candidates:
                await callback.message.answer("У вас пока нет ферм с корректной датой оплаты.")
                await callback.answer()
                return

            candidates.sort(key=lambda item: to_utc_naive(item.account.next_payment_at) or datetime.max)
            settings = get_bot_settings()
            batch = create_notification_batch(user_id=profile.user_id, subscriber_id=profile.id, candidates=candidates)
            await callback.message.answer(
                render_batch_notification(batch),
                reply_markup=build_user_keyboard(batch.id, settings.admin_contact),
            )
        await callback.answer()

    @router.callback_query(F.data == "menu:change")
    async def on_menu_change(callback: CallbackQuery, state: FSMContext) -> None:
        with app.app_context():
            profile = TelegramSubscriber.query.filter_by(chat_id=str(callback.message.chat.id)).first()
            if not profile:
                await callback.answer("Чат не привязан", show_alert=True)
                return

            accounts = Account.query.filter_by(owner_id=profile.user_id).all()
            owner = User.query.get(profile.user_id)
            candidates = [
                NotificationCandidate(
                    account=account,
                    user=owner,
                    subscriber=profile,
                    telegram_tag=None,
                    days_left=0,
                    due_on=(to_utc_naive(account.next_payment_at) or utcnow()).date(),
                )
                for account in accounts
            ]
            if not candidates:
                await callback.message.answer("Фермы пока не найдены. Опишите изменения администратору вручную.")
                await callback.answer()
                return

            batch = create_notification_batch(user_id=profile.user_id, subscriber_id=profile.id, candidates=candidates)
            ensure_batch_editable(batch)
            mark_batch_mode(batch, "manual_change")

        await state.set_data({"batch_id": batch.id, "mode": "manual_change"})
        await state.set_state(BatchPaymentFSM.waiting_manual_comment)
        await callback.message.answer(
            "Опишите изменения: какие фермы продлеваете, какие отключить или что нужно скорректировать."
        )
        await callback.answer()

    @router.callback_query(F.data == "menu:payment_info")
    async def on_menu_payment_info(callback: CallbackQuery) -> None:
        with app.app_context():
            profile = TelegramSubscriber.query.filter_by(chat_id=str(callback.message.chat.id)).first()
            if not profile:
                await callback.answer("Чат не привязан", show_alert=True)
                return
            settings = get_bot_settings()
            text = (
                "🔁 Реквизиты и как оплатить\n\n"
                f"{_settings_payment_details(settings)}\n\n"
                f"{_settings_payment_instruction(settings)}\n\n"
                "Если вы оплатили — нажмите «Я оплатил(а)» и прикрепите чек/скрин."
            )
            await callback.message.answer(text)
        await callback.answer()

    @router.callback_query(F.data == "menu:my_requests")
    async def on_menu_my_requests(callback: CallbackQuery) -> None:
        with app.app_context():
            profile = TelegramSubscriber.query.filter_by(chat_id=str(callback.message.chat.id)).first()
            if not profile:
                await callback.answer("Чат не привязан", show_alert=True)
                return
            rows = (
                RenewalRequest.query.filter_by(user_id=profile.user_id)
                .order_by(RenewalRequest.created_at.desc())
                .limit(5)
                .all()
            )
            if not rows:
                await callback.message.answer("У вас пока нет заявок.")
                await callback.answer()
                return
            lines = ["🧾 Ваши последние заявки:"]
            kb = InlineKeyboardBuilder()
            has_buttons = False
            for row in rows:
                kb.button(text=f"🔎 Открыть #{row.id}", callback_data=f"myreq_open:{row.id}")
                has_buttons = True
                lines.append(
                    f"#{row.id} • {_request_type_label(row)} • {_status_label(row.status)} • {row.created_at:%d.%m.%Y %H:%M}"
                )
                if row.status == "needs_info":
                    kb.button(text=f"✍️ Ответить #{row.id}", callback_data=f"client_reply:{row.id}")
            kb.adjust(1)
            await callback.message.answer("\n".join(lines), reply_markup=kb.as_markup() if has_buttons else None)
        await callback.answer()

    @router.callback_query(F.data == "menu:pause")
    async def on_menu_pause(callback: CallbackQuery) -> None:
        with app.app_context():
            profile = TelegramSubscriber.query.filter_by(chat_id=str(callback.message.chat.id)).first()
            if not profile:
                await callback.answer("Чат не привязан", show_alert=True)
                return
            pause_until = to_utc_naive(profile.pause_until)
            active_pause = bool(pause_until and pause_until > utcnow())
            await callback.message.answer(
                f"Текущий статус напоминаний: {_pause_text(profile)}.\nВыберите паузу:",
                reply_markup=_pause_keyboard(active_pause),
            )
        await callback.answer()

    @router.callback_query(F.data.startswith("pause_set:"))
    async def on_pause_set(callback: CallbackQuery) -> None:
        days = _safe_callback_int(callback.data or "", 1)
        if days is None:
            await callback.answer("Некорректные данные", show_alert=True)
            return
        if _limit_inline(callback.message.chat.id, f"pause_set:{days}"):
            await callback.answer("Слишком часто. Попробуйте через секунду.", show_alert=True)
            return
        with app.app_context():
            profile = TelegramSubscriber.query.filter_by(chat_id=str(callback.message.chat.id)).first()
            if not profile:
                await callback.answer("Чат не привязан", show_alert=True)
                return
            profile.pause_until = utcnow() + timedelta(days=days)
            db.session.commit()
            await callback.message.answer(f"Ок, напоминания на паузе до {profile.pause_until:%d.%m.%Y %H:%M}.")
        await callback.answer("Пауза включена")

    @router.callback_query(F.data == "pause_clear")
    async def on_pause_clear(callback: CallbackQuery) -> None:
        with app.app_context():
            profile = TelegramSubscriber.query.filter_by(chat_id=str(callback.message.chat.id)).first()
            if not profile:
                await callback.answer("Чат не привязан", show_alert=True)
                return
            profile.pause_until = None
            db.session.commit()
            await callback.message.answer("Пауза снята. Напоминания снова активны.")
        await callback.answer("Готово")

    @router.callback_query(F.data == "menu:support")
    async def on_menu_support(callback: CallbackQuery) -> None:
        with app.app_context():
            settings = get_bot_settings()
            contact = (settings.support_contact or settings.admin_contact or "").strip()
            if not contact:
                await callback.message.answer("Контакт поддержки пока не настроен. Напишите администратору UsersDash.")
            else:
                await callback.message.answer(f"🆘 Поддержка: {contact}")
        await callback.answer()

    @router.callback_query(F.data.startswith("client_reply:"))
    async def on_client_reply(callback: CallbackQuery, state: FSMContext) -> None:
        request_id = _safe_callback_int(callback.data or "", 1)
        if request_id is None:
            await callback.answer("Некорректный request_id", show_alert=True)
            return
        with app.app_context():
            profile = TelegramSubscriber.query.filter_by(chat_id=str(callback.message.chat.id)).first()
            row = RenewalRequest.query.get(request_id)
            if not profile or not row or row.user_id != profile.user_id:
                await callback.answer("Заявка недоступна", show_alert=True)
                return
        await state.set_state(ClientClarifyFSM.waiting_answer)
        await state.set_data({"clarify_request_id": request_id, "clarify_messages_count": 0})
        await callback.message.answer("Напишите ответ одним сообщением. Можно приложить скрин в следующем сообщении.")
        await callback.answer()

    @router.callback_query(F.data.startswith("myreq_open:"))
    async def on_myreq_open(callback: CallbackQuery) -> None:
        request_id = _safe_callback_int(callback.data or "", 1)
        if request_id is None:
            await callback.answer("Некорректный запрос", show_alert=True)
            return
        with app.app_context():
            profile = TelegramSubscriber.query.filter_by(chat_id=str(callback.message.chat.id)).first()
            row = RenewalRequest.query.get(request_id)
            if not profile or not row or row.user_id != profile.user_id:
                await callback.answer("Заявка недоступна", show_alert=True)
                return
            last_msg = (
                RenewalRequestMessage.query.filter_by(renewal_request_id=row.id)
                .order_by(RenewalRequestMessage.created_at.desc())
                .first()
            )
            details = [
                f"🔎 Заявка #{row.id}",
                f"Тип: {row.request_type or _request_type_label(row)}",
                f"Статус: {_status_label(row.status)}",
                f"Сумма: {row.amount_rub or '—'}",
                f"Метод: {row.payment_method or '—'}",
                f"Комментарий: {row.comment or '—'}",
            ]
            if last_msg:
                details.append(f"Последнее уточнение: {last_msg.message_text or 'вложение'}")
            await callback.message.answer("\n".join(details))
        await callback.answer()

    @router.message(Command("status"))
    async def on_status(message: Message) -> None:
        with app.app_context():
            profile = TelegramSubscriber.query.filter_by(chat_id=str(message.chat.id)).first()
            if not profile:
                await message.answer("Чат не привязан. Откройте deep-link из UsersDash.")
                return

            accounts = Account.query.filter_by(owner_id=profile.user_id, is_active=True).all()
            if not accounts:
                await message.answer("У вас пока нет активных аренд в системе.")
                return

            lines = ["Ваши аренды:"]
            for account in accounts:
                due = account.next_payment_at.strftime("%d.%m.%Y") if account.next_payment_at else "не задано"
                lines.append(f"• {account.name} — оплачено до {due}")
            await message.answer("\n".join(lines))

    @router.message(Command("admin_pending"))
    @router.message(Command("admin_user"))
    @router.message(Command("admin_settings"))
    @router.message(Command("admin_audit"))
    @router.message(Command("admin"))
    async def on_admin_command(message: Message, state: FSMContext) -> None:
        if not _is_admin(message.chat.id, cfg):
            await message.answer("Недостаточно прав.")
            return
        parts = (message.text or "").split()
        cmd = ((parts[0] or "") if parts else "").split("@")[0].lower()

        if cmd == "/admin_pending":
            subcmd = "pending"
            args = parts[1:]
        elif cmd == "/admin_user":
            subcmd = "user"
            args = parts[1:]
        elif cmd == "/admin_settings":
            subcmd = "settings"
            args = parts[1:]
        elif cmd == "/admin_audit":
            subcmd = "audit"
            args = parts[1:]
        else:
            if len(parts) < 2:
                await message.answer(
                    "Команды: /admin_pending [N], /admin_user <id|@username>, /admin_settings, /admin_audit <user_id>"
                )
                return
            subcmd = parts[1].lower()
            args = parts[2:]

        with app.app_context():
            if subcmd == "pending":
                limit = 20
                if args and args[0].isdigit():
                    limit = max(1, min(int(args[0]), 50))
                rows = (
                    RenewalRequest.query.filter(RenewalRequest.status.in_(["payment_pending_confirmation", "payment_data_collecting", "needs_info"]))
                    .order_by(RenewalRequest.created_at.desc())
                    .limit(limit)
                    .all()
                )
                if not rows:
                    await message.answer("Нет заявок на проверке.")
                    return
                for row in rows:
                    user_label = f"{row.user_id}"
                    if row.subscriber and row.subscriber.username:
                        user_label = f"@{row.subscriber.username} ({row.user_id})"
                    text = (
                        f"Заявка #{row.id}\n"
                        f"Пользователь: {user_label}\n"
                        f"Сумма: {row.amount_rub or '—'}\n"
                        f"Дата: {row.created_at:%d.%m.%Y %H:%M}\n"
                        f"Тип: {_request_type_label(row)}\n"
                        f"Статус: {_status_label(row.status)}\n"
                        f"Описание: {(row.comment or '—')[:180]}"
                    )
                    await message.answer(text, reply_markup=_admin_request_keyboard(row.id))
                return

            if subcmd == "user" and len(args) >= 1:
                lookup = args[0].strip()
                user = None
                if lookup.startswith("@"):
                    username = lookup[1:]
                    profile = TelegramSubscriber.query.filter_by(username=username).first()
                    if profile:
                        user = User.query.get(profile.user_id)
                elif lookup.isdigit():
                    user = User.query.get(int(lookup))
                if not user:
                    await message.answer("Пользователь не найден.")
                    return
                profile = TelegramSubscriber.query.filter_by(user_id=user.id).first()
                reqs = RenewalRequest.query.filter_by(user_id=user.id).order_by(RenewalRequest.created_at.desc()).limit(5).all()
                lines = [
                    f"👤 Пользователь #{user.id}",
                    f"Контакт: @{profile.username if profile and profile.username else '—'}",
                    f"Напоминания: {_pause_text(profile) if profile else 'Telegram не привязан'}",
                    "Последние заявки:",
                ]
                lines.extend([f"• #{r.id} {_status_label(r.status)} ({r.created_at:%d.%m %H:%M})" for r in reqs] or ["• нет"])
                kb = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="Напоминания ON/OFF", callback_data=f"admin_user_toggle:{user.id}")],
                    [InlineKeyboardButton(text="Пауза 7 дней", callback_data=f"admin_user_pause:{user.id}:7")],
                    [InlineKeyboardButton(text="Снять паузу", callback_data=f"admin_user_unpause:{user.id}")],
                ])
                await message.answer("\n".join(lines), reply_markup=kb)
                return

            if subcmd == "settings":
                settings = get_bot_settings()
                days = settings.reminder_days or ",".join(str(x) for x in cfg.reminder_days)
                text = (
                    "⚙️ Настройки бота:\n"
                    f"Реквизиты: {(_settings_payment_details(settings))[:120]}\n"
                    f"Инструкция: {(_settings_payment_instruction(settings))[:120]}\n"
                    f"Дни напоминаний: {days}\n"
                    f"Напоминания: {'ON' if settings.reminders_enabled else 'OFF'}\n"
                    f"Поддержка: {settings.support_contact or settings.admin_contact or '—'}"
                )
                kb = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="✏️ Реквизиты", callback_data="admin_settings:payment_details_text")],
                    [InlineKeyboardButton(text="✏️ Инструкция", callback_data="admin_settings:payment_instruction_text")],
                    [InlineKeyboardButton(text="⏰ Дни напоминаний", callback_data="admin_settings:reminder_days")],
                    [InlineKeyboardButton(text="🔔 Напоминания ON/OFF", callback_data="admin_settings_toggle_reminders")],
                ])
                await message.answer(text, reply_markup=kb)
                return

            if subcmd == "audit" and len(args) >= 1 and args[0].isdigit():
                user_id = int(args[0])
                rows = (
                    RenewalAdminAction.query.join(RenewalRequest, RenewalRequest.id == RenewalAdminAction.renewal_request_id)
                    .filter(RenewalRequest.user_id == user_id)
                    .order_by(RenewalAdminAction.created_at.desc())
                    .limit(10)
                    .all()
                )
                if not rows:
                    await message.answer("Действий админа по пользователю пока нет.")
                    return
                lines = [f"Audit по user_id={user_id}:"]
                for row in rows:
                    actor = User.query.get(row.actor_user_id)
                    lines.append(f"• {row.created_at:%d.%m %H:%M} — {row.action_type} (admin={actor.username if actor else row.actor_user_id})")
                await message.answer("\n".join(lines))
                return

        await message.answer("Неизвестная команда. Используйте: /admin_pending | /admin_user | /admin_settings | /admin_audit")

    @router.callback_query(F.data.startswith("admin_open:"))
    async def on_admin_open(callback: CallbackQuery) -> None:
        if not _is_admin(callback.message.chat.id, cfg):
            await callback.answer("Недостаточно прав", show_alert=True)
            return
        request_id = _safe_callback_int(callback.data or "", 1)
        if request_id is None:
            await callback.answer("Некорректный request_id", show_alert=True)
            return
        with app.app_context():
            row = RenewalRequest.query.get(request_id)
            if not row:
                await callback.answer("Заявка не найдена", show_alert=True)
                return
            last_msg = (
                RenewalRequestMessage.query.filter_by(renewal_request_id=row.id)
                .order_by(RenewalRequestMessage.created_at.desc())
                .first()
            )
            extra = f"\nПоследнее уточнение: {last_msg.message_text or 'вложение'}" if last_msg else ""
            await callback.message.answer(
                f"🔎 Заявка #{row.id}\nСтатус: {_status_label(row.status)}\n"
                f"Сумма: {row.amount_rub or '—'}\nМетод: {row.payment_method or '—'}\nКомментарий: {row.comment or '—'}{extra}",
                reply_markup=_admin_request_keyboard(row.id, include_open=False),
            )
        await callback.answer()

    @router.callback_query(F.data.startswith("renew:"))
    async def on_renew_click(callback: CallbackQuery) -> None:
        account_id = int(callback.data.split(":", 1)[1])
        with app.app_context():
            profile = TelegramSubscriber.query.filter_by(chat_id=str(callback.message.chat.id)).first()
            if not profile:
                await callback.message.answer("Чат не привязан. Откройте deep-link из UsersDash.")
                await callback.answer()
                return

            account = Account.query.get(account_id)
            if not account or account.owner_id != profile.user_id:
                await callback.message.answer("Ферма не найдена или недоступна для вашего аккаунта.")
                await callback.answer()
                return

            amount = account.next_payment_amount if account.next_payment_amount is not None else "не указана"
            settings = get_bot_settings()

            await callback.message.answer(
                "💳 Продление аренды\n"
                f"Ферма: {account.name}\n"
                f"Стоимость: {amount} {'₽' if isinstance(amount, int) else ''}\n"
                f"Срок продления: {settings.renew_duration_days} дней\n\n"
                f"{settings.payment_instructions or 'Реквизиты уточняйте у администратора.'}"
            )
        await callback.answer()

    @router.callback_query(F.data.startswith("paid:"))
    async def on_paid_click(callback: CallbackQuery, state: FSMContext) -> None:
        account_id = int(callback.data.split(":", 1)[1])
        with app.app_context():
            profile = TelegramSubscriber.query.filter_by(chat_id=str(callback.message.chat.id)).first()
            account = Account.query.get(account_id)
            if not profile or not account or account.owner_id != profile.user_id:
                await callback.message.answer("Ферма не найдена или недоступна для вашего аккаунта.")
                await callback.answer()
                return

        await state.set_data({"account_id": account_id})
        await state.set_state(PaymentFSM.waiting_amount)
        await callback.message.answer("Введите сумму оплаты в рублях (только число).")
        await callback.answer()

    @router.callback_query(F.data.startswith("batch_full:"))
    async def on_batch_full(callback: CallbackQuery, state: FSMContext) -> None:
        batch_id = _safe_callback_int(callback.data or "", 1)
        if batch_id is None:
            await callback.answer("Некорректный id", show_alert=True)
            return
        with app.app_context():
            profile = TelegramSubscriber.query.filter_by(chat_id=str(callback.message.chat.id)).first()
            if not profile:
                await callback.message.answer("Чат не привязан. Откройте deep-link из UsersDash.")
                await callback.answer()
                return
            try:
                batch = get_batch_for_user(batch_id, profile.user_id)
                ensure_batch_editable(batch)
                mark_batch_mode(batch, "full")
                set_batch_selected_accounts(
                    batch=batch,
                    selected_account_ids={item.account_id for item in batch.items},
                )
            except BatchValidationError as exc:
                await callback.message.answer(str(exc))
                await callback.answer()
                return

        await state.set_data({"batch_id": batch_id, "mode": "full"})
        await state.set_state(BatchPaymentFSM.waiting_amount)
        await callback.message.answer("Укажите общую сумму оплаты по всем фермам (только число).")
        await callback.answer()

    @router.callback_query(F.data.startswith("batch_partial:"))
    async def on_batch_partial(callback: CallbackQuery) -> None:
        parts = (callback.data or "").split(":", 2)
        if len(parts) != 3:
            await callback.answer("Некорректные данные", show_alert=True)
            return
        try:
            batch_id = int(parts[1])
            page = int(parts[2])
        except ValueError:
            await callback.answer("Некорректные данные", show_alert=True)
            return
        with app.app_context():
            profile = TelegramSubscriber.query.filter_by(chat_id=str(callback.message.chat.id)).first()
            if not profile:
                await callback.message.answer("Чат не привязан. Откройте deep-link из UsersDash.")
                await callback.answer()
                return
            try:
                batch = get_batch_for_user(batch_id, profile.user_id)
                ensure_batch_editable(batch)
                mark_batch_mode(batch, "partial")
            except BatchValidationError as exc:
                await callback.message.answer(str(exc))
                await callback.answer()
                return

            selected_count = RenewalBatchItem.query.filter_by(
                batch_request_id=batch.id,
                selected_for_renewal=True,
            ).count()
            keyboard = build_partial_selection_keyboard(batch, page=page)
            await callback.message.answer(
                f"Выберите оплаченные фермы (отмечено: {selected_count}).",
                reply_markup=keyboard,
            )
        await callback.answer()

    @router.callback_query(F.data.startswith("batch_toggle:"))
    async def on_batch_toggle(callback: CallbackQuery) -> None:
        parts = (callback.data or "").split(":", 3)
        if len(parts) != 4:
            await callback.answer("Некорректные данные", show_alert=True)
            return
        try:
            batch_id = int(parts[1])
            item_id = int(parts[2])
            page = int(parts[3])
        except ValueError:
            await callback.answer("Некорректные данные", show_alert=True)
            return
        with app.app_context():
            profile = TelegramSubscriber.query.filter_by(chat_id=str(callback.message.chat.id)).first()
            if not profile:
                await callback.answer("Чат не привязан", show_alert=True)
                return
            try:
                batch = get_batch_for_user(batch_id, profile.user_id)
                ensure_batch_editable(batch)
            except BatchValidationError as exc:
                await callback.answer(str(exc), show_alert=True)
                return

            item = RenewalBatchItem.query.filter_by(
                id=item_id,
                batch_request_id=batch.id,
            ).first()
            if not item:
                await callback.answer("Элемент не найден.", show_alert=True)
                return

            item.selected_for_renewal = not item.selected_for_renewal
            db.session.commit()
            selected_count = RenewalBatchItem.query.filter_by(
                batch_request_id=batch.id,
                selected_for_renewal=True,
            ).count()
            keyboard = build_partial_selection_keyboard(batch, page=page)
            await callback.message.edit_text(
                f"Выберите оплаченные фермы (отмечено: {selected_count}).",
                reply_markup=keyboard,
            )
        await callback.answer()

    @router.callback_query(F.data.startswith("batch_select_page:"))
    async def on_batch_select_page(callback: CallbackQuery) -> None:
        parts = (callback.data or "").split(":", 2)
        if len(parts) != 3:
            await callback.answer("Некорректные данные", show_alert=True)
            return
        try:
            batch_id = int(parts[1])
            page = int(parts[2])
        except ValueError:
            await callback.answer("Некорректные данные", show_alert=True)
            return
        with app.app_context():
            profile = TelegramSubscriber.query.filter_by(chat_id=str(callback.message.chat.id)).first()
            if not profile:
                await callback.answer("Чат не привязан", show_alert=True)
                return
            try:
                batch = get_batch_for_user(batch_id, profile.user_id)
                ensure_batch_editable(batch)
            except BatchValidationError as exc:
                await callback.answer(str(exc), show_alert=True)
                return

            items = (
                RenewalBatchItem.query.filter_by(batch_request_id=batch.id)
                .order_by(RenewalBatchItem.id.asc())
                .all()
            )
            page_size = 6
            total_pages = max(1, (len(items) + page_size - 1) // page_size)
            page = max(0, min(page, total_pages - 1))
            start = page * page_size
            page_items = items[start:start + page_size]
            if not page_items:
                await callback.answer("На этой странице нет ферм для выбора.", show_alert=True)
                return

            for item in page_items:
                item.selected_for_renewal = True
            db.session.commit()

            selected_count = RenewalBatchItem.query.filter_by(
                batch_request_id=batch.id,
                selected_for_renewal=True,
            ).count()
            keyboard = build_partial_selection_keyboard(batch, page=page)
            await callback.message.edit_text(
                f"Выберите оплаченные фермы (отмечено: {selected_count}).",
                reply_markup=keyboard,
            )
        await callback.answer()

    @router.callback_query(F.data.startswith("batch_partial_done:"))
    async def on_batch_partial_done(callback: CallbackQuery, state: FSMContext) -> None:
        batch_id = _safe_callback_int(callback.data or "", 1)
        if batch_id is None:
            await callback.answer("Некорректный id", show_alert=True)
            return
        with app.app_context():
            profile = TelegramSubscriber.query.filter_by(chat_id=str(callback.message.chat.id)).first()
            if not profile:
                await callback.answer("Чат не привязан", show_alert=True)
                return
            try:
                batch = get_batch_for_user(batch_id, profile.user_id)
                ensure_batch_editable(batch)
            except BatchValidationError as exc:
                await callback.answer(str(exc), show_alert=True)
                return

            selected_count = RenewalBatchItem.query.filter_by(
                batch_request_id=batch.id,
                selected_for_renewal=True,
            ).count()
            if selected_count == 0:
                await callback.answer("Выберите хотя бы одну ферму.", show_alert=True)
                return

        await state.set_data({"batch_id": batch_id, "mode": "partial"})
        await state.set_state(BatchPaymentFSM.waiting_amount)
        await callback.message.answer("Укажите сумму оплаченной части (только число).")
        await callback.answer()

    @router.callback_query(F.data.startswith("batch_change:"))
    async def on_batch_change(callback: CallbackQuery, state: FSMContext) -> None:
        batch_id = _safe_callback_int(callback.data or "", 1)
        if batch_id is None:
            await callback.answer("Некорректный id", show_alert=True)
            return
        with app.app_context():
            profile = TelegramSubscriber.query.filter_by(chat_id=str(callback.message.chat.id)).first()
            if not profile:
                await callback.answer("Чат не привязан", show_alert=True)
                return
            try:
                batch = get_batch_for_user(batch_id, profile.user_id)
                ensure_batch_editable(batch)
                mark_batch_mode(batch, "manual_change")
            except BatchValidationError as exc:
                await callback.answer(str(exc), show_alert=True)
                return

        await state.set_data({"batch_id": batch_id, "mode": "manual_change"})
        await state.set_state(BatchPaymentFSM.waiting_manual_comment)
        await callback.message.answer(
            "Опишите изменения: какие фермы продлеваете, какие отключить или что нужно скорректировать."
        )
        await callback.answer()

    @router.message(Command("cancel"))
    async def on_cancel(message: Message, state: FSMContext) -> None:
        await state.clear()
        await message.answer("Действие отменено.")

    @router.message(PaymentFSM.waiting_amount)
    async def on_payment_amount(message: Message, state: FSMContext) -> None:
        if not message.text or not message.text.strip().isdigit():
            await message.answer("Нужна сумма числом, например: 1500")
            return
        data = await state.get_data()
        data["amount_rub"] = int(message.text.strip())
        await state.set_data(data)
        await state.set_state(PaymentFSM.waiting_method)
        await message.answer("Укажите способ оплаты (СБП / карта / крипто / другое).")

    @router.message(PaymentFSM.waiting_method)
    async def on_payment_method(message: Message, state: FSMContext) -> None:
        if not message.text:
            await message.answer("Напишите способ оплаты текстом.")
            return
        data = await state.get_data()
        data["payment_method"] = message.text.strip()[:64]
        await state.set_data(data)
        await state.set_state(PaymentFSM.waiting_comment)
        await message.answer("Пришлите комментарий или номер операции (можно '-', если нечего добавить).")

    @router.message(PaymentFSM.waiting_comment)
    async def on_payment_comment(message: Message, state: FSMContext) -> None:
        data = await state.get_data()
        account_id = int(data["account_id"])
        with app.app_context():
            profile = TelegramSubscriber.query.filter_by(chat_id=str(message.chat.id)).first()
            if not profile:
                await message.answer("Чат не привязан. Откройте deep-link из UsersDash.")
                await state.clear()
                return

            account = Account.query.get(account_id)
            if not account or account.owner_id != profile.user_id:
                await message.answer("Ферма не найдена или недоступна для вашего аккаунта.")
                await state.clear()
                return

            request_row = create_renewal_request(
                user_id=profile.user_id,
                account_id=account_id,
                subscriber_id=profile.id,
                amount_rub=int(data.get("amount_rub", 0)) or None,
                payment_method=str(data.get("payment_method") or "") or None,
                comment=(message.text or "").strip() or None,
                receipt_file_id=data.get("receipt_file_id"),
                request_type="payment",
            )
            sent_to_admin = await notify_admins_about_request(bot, cfg.admin_chat_ids, request_row.id)
            if not cfg.admin_chat_ids:
                logger.warning(
                    "Список TELEGRAM_ADMIN_CHAT_IDS пуст. "
                    "Заявка %s сохранена без Telegram-уведомления админам.",
                    request_row.id,
                )
                await message.answer(
                    f"✅ Заявка #{request_row.id} сохранена и будет обработана через UsersDash."
                )
            elif sent_to_admin:
                await message.answer(
                    f"✅ Заявка #{request_row.id} создана и отправлена администратору на подтверждение."
                )
            else:
                await message.answer(
                    f"✅ Заявка #{request_row.id} сохранена. Сейчас не удалось уведомить администраторов в Telegram, "
                    "заявка доступна в UsersDash."
                )

        await state.clear()

    @router.message(BatchPaymentFSM.waiting_amount)
    async def on_batch_amount(message: Message, state: FSMContext) -> None:
        if not message.text or not message.text.strip().isdigit():
            await message.answer("Нужна сумма числом, например: 15000")
            return
        data = await state.get_data()
        data["amount_rub"] = int(message.text.strip())
        await state.set_data(data)
        await state.set_state(BatchPaymentFSM.waiting_method)
        await message.answer("Укажите способ оплаты (СБП / карта / крипто / другое).")

    @router.message(BatchPaymentFSM.waiting_method)
    async def on_batch_method(message: Message, state: FSMContext) -> None:
        if not message.text:
            await message.answer("Напишите способ оплаты текстом.")
            return
        data = await state.get_data()
        data["payment_method"] = message.text.strip()[:64]
        await state.set_data(data)
        await state.set_state(BatchPaymentFSM.waiting_comment)
        await message.answer("Добавьте комментарий/номер операции (можно '-').")

    @router.message(BatchPaymentFSM.waiting_comment)
    async def on_batch_comment(message: Message, state: FSMContext) -> None:
        data = await state.get_data()
        batch_id = int(data["batch_id"])
        with app.app_context():
            profile = TelegramSubscriber.query.filter_by(chat_id=str(message.chat.id)).first()
            if not profile:
                await message.answer("Чат не привязан. Откройте deep-link из UsersDash.")
                await state.clear()
                return
            try:
                batch = get_batch_for_user(batch_id, profile.user_id)
                submit_batch_request(
                    batch=batch,
                    amount_rub=int(data.get("amount_rub", 0)) or None,
                    payment_method=str(data.get("payment_method") or "") or None,
                    comment=(message.text or "").strip() or None,
                    receipt_file_id=data.get("receipt_file_id"),
                )
            except BatchValidationError as exc:
                await message.answer(f"Не удалось отправить заявку: {exc}")
                await state.clear()
                return

            sent_to_admin = await notify_admins_about_batch_request(bot, cfg.admin_chat_ids, batch.id)
            if not cfg.admin_chat_ids:
                logger.warning(
                    "Список TELEGRAM_ADMIN_CHAT_IDS пуст. "
                    "Заявка по нескольким фермам %s сохранена без Telegram-уведомления админам.",
                    batch.id,
                )
                await message.answer("✅ Заявка по нескольким фермам сохранена и будет обработана через UsersDash.")
            elif sent_to_admin:
                await message.answer("✅ Заявка по нескольким фермам отправлена администратору на подтверждение.")
            else:
                await message.answer(
                    "✅ Заявка по нескольким фермам сохранена. Сейчас не удалось уведомить администраторов в Telegram, "
                    "заявка доступна в UsersDash."
                )
        await state.clear()

    @router.message(BatchPaymentFSM.waiting_manual_comment)
    async def on_batch_manual_comment(message: Message, state: FSMContext) -> None:
        data = await state.get_data()
        batch_id = int(data["batch_id"])
        with app.app_context():
            profile = TelegramSubscriber.query.filter_by(chat_id=str(message.chat.id)).first()
            if not profile:
                await message.answer("Чат не привязан. Откройте deep-link из UsersDash.")
                await state.clear()
                return
            try:
                batch = get_batch_for_user(batch_id, profile.user_id)
                submit_batch_request(
                    batch=batch,
                    amount_rub=None,
                    payment_method=None,
                    comment=(message.text or "").strip() or "Запрошена ручная обработка",
                    receipt_file_id=None,
                )
            except BatchValidationError as exc:
                await message.answer(f"Не удалось отправить заявку: {exc}")
                await state.clear()
                return

            await notify_admins_about_batch_request(bot, cfg.admin_chat_ids, batch.id)
            await message.answer("✅ Запрос на изменения отправлен администратору.")
        await state.clear()

    @router.message(F.photo)
    async def on_photo(message: Message, state: FSMContext) -> None:
        current_state = await state.get_state()
        if current_state not in {PaymentFSM.waiting_comment.state, BatchPaymentFSM.waiting_comment.state}:
            return
        data = await state.get_data()
        data["receipt_file_id"] = message.photo[-1].file_id
        await state.set_data(data)
        await message.answer("Скрин сохранён. Теперь отправьте комментарий или '-' для завершения заявки.")

    @router.callback_query(F.data.startswith("admin_user_toggle:"))
    async def on_admin_user_toggle(callback: CallbackQuery) -> None:
        if not _is_admin(callback.message.chat.id, cfg):
            await callback.answer("Недостаточно прав", show_alert=True)
            return
        user_id = _safe_callback_int(callback.data or "", 1)
        if user_id is None:
            await callback.answer("Некорректный user_id", show_alert=True)
            return
        with app.app_context():
            profile = TelegramSubscriber.query.filter_by(user_id=user_id).first()
            if not profile:
                await callback.answer("Telegram не привязан", show_alert=True)
                return
            old_value = profile.reminders_enabled
            profile.reminders_enabled = not profile.reminders_enabled
            actor_user_id = _admin_actor_id()
            recent_request = RenewalRequest.query.filter_by(user_id=user_id).order_by(RenewalRequest.created_at.desc()).first()
            _append_admin_audit(
                "user_toggle",
                actor_user_id=actor_user_id,
                request_id=(recent_request.id if recent_request else _resolve_audit_request_id(int(user_id))),
                details={"user_id": user_id, "old": old_value, "new": profile.reminders_enabled},
            )
            db.session.commit()
            await callback.message.answer(f"Напоминания для user_id={user_id}: {'ON' if profile.reminders_enabled else 'OFF'}")
        await callback.answer("Готово")

    @router.callback_query(F.data.startswith("admin_user_pause:"))
    async def on_admin_user_pause(callback: CallbackQuery) -> None:
        if not _is_admin(callback.message.chat.id, cfg):
            await callback.answer("Недостаточно прав", show_alert=True)
            return
        parts = (callback.data or "").split(":", 2)
        if len(parts) != 3 or not parts[1].isdigit() or not parts[2].isdigit():
            await callback.answer("Некорректные данные", show_alert=True)
            return
        _, user_id, days = parts
        with app.app_context():
            profile = TelegramSubscriber.query.filter_by(user_id=int(user_id)).first()
            if not profile:
                await callback.answer("Telegram не привязан", show_alert=True)
                return
            profile.pause_until = utcnow() + timedelta(days=int(days))
            actor_user_id = _admin_actor_id()
            recent_request = RenewalRequest.query.filter_by(user_id=int(user_id)).order_by(RenewalRequest.created_at.desc()).first()
            _append_admin_audit(
                "user_pause",
                actor_user_id=actor_user_id,
                request_id=(recent_request.id if recent_request else _resolve_audit_request_id(int(user_id))),
                details={"user_id": int(user_id), "pause_until": profile.pause_until.isoformat()},
            )
            db.session.commit()
            await callback.message.answer(f"Пауза установлена до {profile.pause_until:%d.%m.%Y %H:%M}")
        await callback.answer("Готово")

    @router.callback_query(F.data.startswith("admin_user_unpause:"))
    async def on_admin_user_unpause(callback: CallbackQuery) -> None:
        if not _is_admin(callback.message.chat.id, cfg):
            await callback.answer("Недостаточно прав", show_alert=True)
            return
        user_id = _safe_callback_int(callback.data or "", 1)
        if user_id is None:
            await callback.answer("Некорректный user_id", show_alert=True)
            return
        with app.app_context():
            profile = TelegramSubscriber.query.filter_by(user_id=user_id).first()
            if not profile:
                await callback.answer("Telegram не привязан", show_alert=True)
                return
            profile.pause_until = None
            actor_user_id = _admin_actor_id()
            recent_request = RenewalRequest.query.filter_by(user_id=user_id).order_by(RenewalRequest.created_at.desc()).first()
            _append_admin_audit(
                "user_unpause",
                actor_user_id=actor_user_id,
                request_id=(recent_request.id if recent_request else _resolve_audit_request_id(int(user_id))),
                details={"user_id": user_id},
            )
            db.session.commit()
            await callback.message.answer("Пауза снята")
        await callback.answer("Готово")

    @router.callback_query(F.data.startswith("admin_settings:"))
    async def on_admin_settings_edit(callback: CallbackQuery, state: FSMContext) -> None:
        if not _is_admin(callback.message.chat.id, cfg):
            await callback.answer("Недостаточно прав", show_alert=True)
            return
        parts = (callback.data or "").split(":", 1)
        if len(parts) != 2:
            await callback.answer("Некорректные данные", show_alert=True)
            return
        field = parts[1]
        await state.set_state(AdminSettingsFSM.waiting_value)
        await state.set_data({"settings_field": field})
        await callback.message.answer("Введите новое значение одним сообщением. Для отмены: /cancel")
        await callback.answer()

    @router.callback_query(F.data == "admin_settings_toggle_reminders")
    async def on_admin_settings_toggle_reminders(callback: CallbackQuery) -> None:
        if not _is_admin(callback.message.chat.id, cfg):
            await callback.answer("Недостаточно прав", show_alert=True)
            return
        with app.app_context():
            settings = get_bot_settings()
            old_value = settings.reminders_enabled
            settings.reminders_enabled = not settings.reminders_enabled
            actor_user_id = _admin_actor_id()
            recent_request = RenewalRequest.query.order_by(RenewalRequest.created_at.desc()).first()
            _append_admin_audit(
                "settings_toggle",
                actor_user_id=actor_user_id,
                request_id=(recent_request.id if recent_request else _resolve_audit_request_id()),
                details={"old": old_value, "new": settings.reminders_enabled},
            )
            db.session.commit()
            await callback.message.answer(f"Глобальные напоминания: {'ON' if settings.reminders_enabled else 'OFF'}")
        await callback.answer("Сохранено")

    @router.message(AdminSettingsFSM.waiting_value)
    async def on_admin_settings_value(message: Message, state: FSMContext) -> None:
        if not _is_admin(message.chat.id, cfg):
            await message.answer("Недостаточно прав.")
            await state.clear()
            return
        raw = (message.text or "").strip()
        data = await state.get_data()
        field = data.get("settings_field")
        if not field:
            await state.clear()
            return
        with app.app_context():
            settings = get_bot_settings()
            if field == "reminder_days":
                try:
                    days = parse_reminder_days(raw)
                except Exception:
                    await message.answer("Формат неверный. Пример: 3,1,0,-1")
                    return
                settings.reminder_days = ",".join(str(x) for x in days)
            elif field in {"payment_details_text", "payment_instruction_text"}:
                setattr(settings, field, raw)
            else:
                await message.answer("Это поле нельзя редактировать через бот.")
                await state.clear()
                return
            actor_user_id = _admin_actor_id()
            recent_request = RenewalRequest.query.order_by(RenewalRequest.created_at.desc()).first()
            _append_admin_audit(
                "settings_update",
                actor_user_id=actor_user_id,
                request_id=(recent_request.id if recent_request else _resolve_audit_request_id()),
                details={"field": field, "value": raw[:300]},
            )
            db.session.commit()
        await state.clear()
        await message.answer("Сохранено.")

    @router.callback_query(F.data.startswith("admin_clarify:"))
    async def on_admin_clarify(callback: CallbackQuery, state: FSMContext) -> None:
        if not _is_admin(callback.message.chat.id, cfg):
            await callback.answer("Недостаточно прав", show_alert=True)
            return
        request_id = _safe_callback_int(callback.data or "", 1)
        if request_id is None:
            await callback.answer("Некорректный request_id", show_alert=True)
            return
        await state.set_state(AdminClarifyFSM.waiting_question)
        await state.set_data({"clarify_request_id": request_id, "clarify_messages_count": 0})
        await callback.message.answer(f"Введите вопрос для клиента по заявке #{request_id}.")
        await callback.answer()

    @router.message(AdminClarifyFSM.waiting_question)
    async def on_admin_clarify_question(message: Message, state: FSMContext) -> None:
        if not _is_admin(message.chat.id, cfg):
            await message.answer("Недостаточно прав.")
            await state.clear()
            return
        question = (message.text or "").strip()
        data = await state.get_data()
        request_id = int(data.get("clarify_request_id", 0))
        with app.app_context():
            row = RenewalRequest.query.get(request_id)
            if not row:
                await message.answer("Заявка не найдена.")
                await state.clear()
                return
            if row.status != "needs_info":
                row.status_before_needs_info = row.status
            row.status = "needs_info"
            actor_user_id = _admin_actor_id()
            _append_admin_audit(
                "clarify",
                actor_user_id=actor_user_id,
                request_id=row.id,
                details={"question": question},
            )
            db.session.add(RenewalRequestMessage(
                renewal_request_id=row.id,
                sender_role="admin",
                sender_user_id=None,
                message_text=question,
            ))
            db.session.commit()
            if row.subscriber and row.subscriber.chat_id:
                kb = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="✍️ Ответить", callback_data=f"client_reply:{row.id}")],
                    [InlineKeyboardButton(text="📎 Прикрепить чек/скрин", callback_data=f"client_reply:{row.id}")],
                ])
                await bot.send_message(int(row.subscriber.chat_id), f"Админ просит уточнить: {question}", reply_markup=kb)
        await state.clear()
        await message.answer("Вопрос отправлен клиенту.")

    async def _finalize_client_clarify(message: Message, state: FSMContext) -> None:
        data = await state.get_data()
        request_id = int(data.get("clarify_request_id", 0))
        with app.app_context():
            profile = TelegramSubscriber.query.filter_by(chat_id=str(message.chat.id)).first()
            row = RenewalRequest.query.get(request_id)
            if not profile or not row or row.user_id != profile.user_id:
                await message.answer("Заявка не найдена или недоступна.")
                await state.clear()
                return

            row.status = _status_after_clarify(row)
            row.status_before_needs_info = None
            db.session.commit()
            last_msg = (
                RenewalRequestMessage.query.filter_by(renewal_request_id=row.id, sender_role="client")
                .order_by(RenewalRequestMessage.created_at.desc())
                .first()
            )
            admin_text = f"✍️ Уточнение по заявке #{row.id} от клиента\n{(last_msg.message_text if last_msg else None) or 'Приложен файл.'}"
            for admin_chat_id in cfg.admin_chat_ids:
                try:
                    await bot.send_message(
                        admin_chat_id,
                        admin_text,
                        reply_markup=InlineKeyboardMarkup(
                            inline_keyboard=[[
                                InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"admin_confirm:{row.id}"),
                                InlineKeyboardButton(text="❌ Отклонить", callback_data=f"admin_reject:{row.id}"),
                                InlineKeyboardButton(text="🔎 Открыть", callback_data=f"admin_open:{row.id}"),
                            ]]
                        ),
                    )
                except Exception as exc:
                    logger.warning("Не удалось отправить уточнение админу %s: %s", admin_chat_id, exc)
        await state.clear()
        await message.answer("Спасибо! Уточнение отправлено администратору.")

    @router.message(Command("done"), ClientClarifyFSM.waiting_answer)
    async def on_client_clarify_done(message: Message, state: FSMContext) -> None:
        await _finalize_client_clarify(message, state)

    @router.message(ClientClarifyFSM.waiting_answer)
    async def on_client_clarify_answer(message: Message, state: FSMContext) -> None:
        data = await state.get_data()
        request_id = int(data.get("clarify_request_id", 0))
        text_value = (message.text or message.caption or "").strip()
        attachment_file_id = (message.photo[-1].file_id if message.photo else None)
        if not attachment_file_id and message.document:
            attachment_file_id = message.document.file_id
        with app.app_context():
            profile = TelegramSubscriber.query.filter_by(chat_id=str(message.chat.id)).first()
            row = RenewalRequest.query.get(request_id)
            if not profile or not row or row.user_id != profile.user_id:
                await message.answer("Заявка не найдена или недоступна.")
                await state.clear()
                return
            db.session.add(RenewalRequestMessage(
                renewal_request_id=row.id,
                sender_role="client",
                sender_user_id=row.user_id,
                message_text=text_value or None,
                attachment_file_id=attachment_file_id,
            ))
            db.session.commit()

        count = int(data.get("clarify_messages_count", 0)) + 1
        data["clarify_messages_count"] = count
        await state.set_data(data)
        if count >= 2 or attachment_file_id:
            await _finalize_client_clarify(message, state)
            return
        await message.answer("Ответ сохранён. Прикрепите чек/скрин (фото или файл) или отправьте /done.")


    @router.message(F.text)
    async def on_text_shortcuts(message: Message, state: FSMContext) -> None:
        normalized = (message.text or "").strip().lower()
        if normalized in {"продлить", "продление"}:
            await message.answer("Напишите /status и нажмите «Продлить» под нужной арендой.")
            return
        if normalized in {"оплатил", "я оплатил", "оплата"}:
            await message.answer("Напишите /status и нажмите «Я уже оплатил» под нужной арендой.")

    @router.callback_query(F.data.startswith("admin_confirm:"))
    async def on_admin_confirm(callback: CallbackQuery) -> None:
        request_id = _safe_callback_int(callback.data or "", 1)
        if request_id is None:
            await callback.answer("Некорректный request_id", show_alert=True)
            return
        if callback.message.chat.id not in cfg.admin_chat_ids:
            await callback.answer("Недостаточно прав", show_alert=True)
            return
        if _limit_inline(callback.message.chat.id, f"{(callback.data or '').split(':', 1)[0]}:{request_id}"):
            await callback.answer("Слишком часто. Подождите секунду.", show_alert=True)
            return

        with app.app_context():
            admin_user = User.query.filter_by(role="admin", is_active=True).order_by(User.id.asc()).first()
            if not admin_user:
                await callback.message.answer("В UsersDash не найден активный администратор.")
                return
            try:
                row = confirm_renewal_request(request_id, admin_user.id)
                _append_admin_audit(
                    "confirm",
                    actor_user_id=admin_user.id,
                    request_id=row.id,
                    details={"status": row.status},
                )
                db.session.commit()
            except RentalBotError as exc:
                await callback.message.answer(f"Не удалось подтвердить заявку: {exc}")
                await callback.answer("Ошибка")
                return

            await callback.message.answer(
                f"Заявка #{row.id} подтверждена. "
                f"Аренда продлена до {row.confirmed_paid_until:%d.%m.%Y}."
            )
            if row.subscriber and row.subscriber.chat_id:
                await bot.send_message(
                    chat_id=int(row.subscriber.chat_id),
                    text=(
                        f"✅ Оплата по заявке #{row.id} подтверждена. "
                        f"Аренда продлена до {row.confirmed_paid_until:%d.%m.%Y}."
                    ),
                )
        await callback.answer("Подтверждено")

    @router.callback_query(F.data.startswith("admin_reject:"))
    async def on_admin_reject(callback: CallbackQuery) -> None:
        request_id = _safe_callback_int(callback.data or "", 1)
        if request_id is None:
            await callback.answer("Некорректный request_id", show_alert=True)
            return
        if callback.message.chat.id not in cfg.admin_chat_ids:
            await callback.answer("Недостаточно прав", show_alert=True)
            return
        if _limit_inline(callback.message.chat.id, f"admin_reject:{request_id}"):
            await callback.answer("Слишком часто. Подождите секунду.", show_alert=True)
            return

        with app.app_context():
            admin_user = User.query.filter_by(role="admin", is_active=True).order_by(User.id.asc()).first()
            if not admin_user:
                await callback.message.answer("В UsersDash не найден активный администратор.")
                return
            try:
                reject_reason = "Проверьте реквизиты и пришлите корректные данные"
                row = reject_renewal_request(
                    request_id,
                    admin_user.id,
                    reason=reject_reason,
                )
                _append_admin_audit(
                    "reject",
                    actor_user_id=admin_user.id,
                    request_id=row.id,
                    details={"reason": reject_reason},
                )
                db.session.commit()
            except RentalBotError as exc:
                await callback.message.answer(f"Не удалось отклонить заявку: {exc}")
                await callback.answer("Ошибка")
                return

            await callback.message.answer(f"Заявка #{row.id} отклонена.")
            if row.subscriber and row.subscriber.chat_id:
                await bot.send_message(
                    chat_id=int(row.subscriber.chat_id),
                    text=(
                        f"❌ Оплата по заявке #{row.id} отклонена: {row.rejection_reason}. "
                        "Пожалуйста, отправьте уточнённые данные повторно."
                    ),
                )
        await callback.answer("Отклонено")

    @router.callback_query(F.data.startswith("admin_batch_confirm:"))
    async def on_admin_batch_confirm(callback: CallbackQuery) -> None:
        batch_id = _safe_callback_int(callback.data or "", 1)
        if batch_id is None:
            await callback.answer("Некорректный id", show_alert=True)
            return
        if callback.message.chat.id not in cfg.admin_chat_ids:
            await callback.answer("Недостаточно прав", show_alert=True)
            return

        with app.app_context():
            admin_user = User.query.filter_by(role="admin", is_active=True).order_by(User.id.asc()).first()
            if not admin_user:
                await callback.message.answer("В UsersDash не найден активный администратор.")
                return
            try:
                row = confirm_batch_request(batch_id, admin_user.id)
            except BatchValidationError as exc:
                await callback.message.answer(f"Не удалось подтвердить заявку: {exc}")
                await callback.answer("Ошибка")
                return

            confirmed_items = RenewalBatchItem.query.filter_by(
                batch_request_id=row.id,
                result_status="confirmed",
            ).count()
            await callback.message.answer(
                f"Заявка по нескольким фермам #{row.id} подтверждена. Продлено ферм: {confirmed_items}."
            )
            if row.subscriber and row.subscriber.chat_id:
                await bot.send_message(
                    chat_id=int(row.subscriber.chat_id),
                    text=(
                        f"✅ Заявка по нескольким фермам #{row.id} подтверждена. "
                        f"Продлено ферм: {confirmed_items}."
                    ),
                )
        await callback.answer("Подтверждено")

    @router.callback_query(F.data.startswith("admin_batch_reject:"))
    async def on_admin_batch_reject(callback: CallbackQuery) -> None:
        batch_id = _safe_callback_int(callback.data or "", 1)
        if batch_id is None:
            await callback.answer("Некорректный id", show_alert=True)
            return
        if callback.message.chat.id not in cfg.admin_chat_ids:
            await callback.answer("Недостаточно прав", show_alert=True)
            return

        with app.app_context():
            admin_user = User.query.filter_by(role="admin", is_active=True).order_by(User.id.asc()).first()
            if not admin_user:
                await callback.message.answer("В UsersDash не найден активный администратор.")
                return
            try:
                row = reject_batch_request(
                    batch_id,
                    admin_user.id,
                    reason="Проверьте данные платежа и отправьте уточнение",
                )
            except BatchValidationError as exc:
                await callback.message.answer(f"Не удалось отклонить заявку: {exc}")
                await callback.answer("Ошибка")
                return

            await callback.message.answer(f"Заявка по нескольким фермам #{row.id} отклонена.")
            if row.subscriber and row.subscriber.chat_id:
                await bot.send_message(
                    chat_id=int(row.subscriber.chat_id),
                    text=(
                        f"❌ Заявка по нескольким фермам #{row.id} отклонена: {row.rejection_reason}. "
                        "Пожалуйста, отправьте корректные данные повторно."
                    ),
                )
        await callback.answer("Отклонено")

    return dp


async def notify_admins_about_request(bot: Bot, admin_chat_ids: set[int], request_id: int) -> bool:
    """Уведомляет админов о новой заявке на подтверждение оплаты."""

    row = RenewalRequest.query.get(request_id)
    if not row:
        return False

    if not admin_chat_ids:
        logger.warning("Список admin_chat_ids пуст, заявка %s не будет отправлена в Telegram.", request_id)
        return False

    text = (
        f"🧾 Новая заявка на подтверждение оплаты #{row.id}\n"
        f"Аккаунт ID: {row.account_id}\n"
        f"Клиент ID: {row.user_id}\n"
        f"Сумма: {row.amount_rub or 'не указана'}\n"
        f"Метод: {row.payment_method or 'не указан'}"
    )
    keyboard = build_admin_keyboard(row.id)
    delivered = False
    for admin_chat_id in admin_chat_ids:
        try:
            await bot.send_message(chat_id=admin_chat_id, text=text, reply_markup=keyboard)
            delivered = True
        except Exception as exc:  # pragma: no cover
            logger.warning(
                "Не удалось отправить заявку %s админу %s: %s",
                row.id,
                admin_chat_id,
                exc,
            )

    if delivered:
        row.last_admin_reminder_at = utcnow()
        db.session.commit()

    return delivered


async def notify_admins_about_batch_request(bot: Bot, admin_chat_ids: set[int], batch_id: int) -> bool:
    """Уведомляет админов о новой заявке по нескольким фермам."""

    row = RenewalBatchRequest.query.get(batch_id)
    if not row:
        return False

    if not admin_chat_ids:
        logger.warning("Список admin_chat_ids пуст, заявка по нескольким фермам %s не будет отправлена в Telegram.", batch_id)
        return False

    selected_items = [item for item in row.items if item.selected_for_renewal]
    skipped_items = [item for item in row.items if not item.selected_for_renewal]
    selected_lines = [f"• {item.account_name_snapshot} (ID: {item.account_id})" for item in selected_items]
    skipped_lines = [f"• {item.account_name_snapshot} (ID: {item.account_id})" for item in skipped_items]
    selected_text = "\n".join(selected_lines) if selected_lines else "—"
    skipped_text = "\n".join(skipped_lines) if skipped_lines else "—"

    text = (
        f"🧾 Новая заявка по нескольким фермам #{row.id}\n"
        f"Клиент ID: {row.user_id}\n"
        f"Режим: {row.mode or 'не выбран'}\n"
        f"Сумма: {row.total_amount_rub or 'не указана'}\n"
        f"Метод: {row.payment_method or 'не указан'}\n"
        f"Комментарий: {row.comment or '—'}\n\n"
        f"Выбраны к продлению:\n{selected_text}\n\n"
        f"Не выбраны:\n{skipped_text}"
    )
    keyboard = build_admin_batch_keyboard(row.id, row.mode)

    delivered = False
    for admin_chat_id in admin_chat_ids:
        try:
            await bot.send_message(chat_id=admin_chat_id, text=text, reply_markup=keyboard)
            delivered = True
        except Exception as exc:  # pragma: no cover
            logger.warning(
                "Не удалось отправить заявку %s админу %s: %s",
                row.id,
                admin_chat_id,
                exc,
            )

    if delivered:
        row.last_admin_reminder_at = utcnow()
        db.session.commit()

    return delivered


async def run_notifications_job(app: Flask, bot: Bot, cfg: RuntimeConfig) -> None:
    """Фоновая задача отправки напоминаний."""

    with app.app_context():
        settings = get_bot_settings()
        if not settings.reminders_enabled:
            logger.info("Глобальные напоминания выключены в настройках.")
            return
        reminder_days = cfg.reminder_days
        if settings.reminder_days:
            try:
                reminder_days = parse_reminder_days(settings.reminder_days)
            except ValueError:
                logger.warning("Некорректные reminder_days в настройках: %s", settings.reminder_days)
        candidates = collect_notification_candidates(reminder_days)
        candidates = [
            item for item in candidates
            if item.subscriber.reminders_enabled and not (
                to_utc_naive(item.subscriber.pause_until) and to_utc_naive(item.subscriber.pause_until) > utcnow()
            )
        ]
        grouped: dict[tuple[int, str], list[NotificationCandidate]] = defaultdict(list)
        for candidate in candidates:
            key = (candidate.user.id, candidate.subscriber.chat_id)
            grouped[key].append(candidate)

        for group_candidates in grouped.values():
            group_candidates.sort(key=lambda item: (item.due_on, item.account.id))
            group_candidates = [candidate for candidate in group_candidates if not _is_duplicate_notification(candidate)]
            if not group_candidates:
                continue
            account_ids = [item.account.id for item in group_candidates]
            try:
                batch = create_notification_batch(
                    user_id=group_candidates[0].user.id,
                    subscriber_id=group_candidates[0].subscriber.id,
                    candidates=group_candidates,
                )
                text = render_batch_notification(batch)
                keyboard = build_user_keyboard(batch.id, settings.admin_contact)
                msg = await bot.send_message(
                    chat_id=int(group_candidates[0].subscriber.chat_id),
                    text=text,
                    reply_markup=keyboard,
                )
                for candidate in group_candidates:
                    log_notification_result(
                        account_id=candidate.account.id,
                        user_id=candidate.user.id,
                        subscriber_id=candidate.subscriber.id,
                        due_on=candidate.due_on,
                        days_left=candidate.days_left,
                        status="delivered",
                        message_id=str(msg.message_id),
                        payload={
                            "chat_id": candidate.subscriber.chat_id,
                            "grouped_accounts": account_ids,
                            "telegram_tag": candidate.telegram_tag,
                            "batch_id": batch.id,
                        },
                    )
            except Exception as exc:  # pragma: no cover
                for candidate in group_candidates:
                    log_notification_result(
                        account_id=candidate.account.id,
                        user_id=candidate.user.id,
                        subscriber_id=candidate.subscriber.id,
                        due_on=candidate.due_on,
                        days_left=candidate.days_left,
                        status="failed",
                        error_text=str(exc),
                        payload={
                            "telegram_tag": candidate.telegram_tag,
                            "grouped_accounts": account_ids,
                        },
                    )

        for row in unresolved_requests(limit=20):
            row_created_at = to_utc_naive(row.created_at)
            age_hours = int((utcnow() - row_created_at).total_seconds() / 3600) if row_created_at else 0
            if age_hours < settings.pending_admin_reminder_hours:
                continue

            last_admin_reminder_at = to_utc_naive(row.last_admin_reminder_at)
            if last_admin_reminder_at:
                remind_delta = utcnow() - last_admin_reminder_at
                if remind_delta.total_seconds() < settings.pending_admin_reminder_hours * 3600:
                    continue

            await notify_admins_about_request(bot, cfg.admin_chat_ids, row.id)

        for batch in unresolved_batch_requests(limit=20):
            batch_created_at = to_utc_naive(batch.created_at)
            age_hours = int((utcnow() - batch_created_at).total_seconds() / 3600) if batch_created_at else 0
            if age_hours < settings.pending_admin_reminder_hours:
                continue

            last_admin_reminder_at = to_utc_naive(batch.last_admin_reminder_at)
            if last_admin_reminder_at:
                remind_delta = utcnow() - last_admin_reminder_at
                if remind_delta.total_seconds() < settings.pending_admin_reminder_hours * 3600:
                    continue

            await notify_admins_about_batch_request(bot, cfg.admin_chat_ids, batch.id)


def build_scheduler(app: Flask, bot: Bot, cfg: RuntimeConfig) -> AsyncIOScheduler:
    """Создаёт APScheduler для регулярных задач бота."""

    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.add_job(run_notifications_job, "cron", minute="*/15", args=[app, bot, cfg], id="rental_notifications")
    return scheduler


async def run_bot() -> None:
    """Запускает aiogram-бота и планировщик уведомлений."""

    app = create_flask_context()
    cfg = build_runtime_config()
    run_startup_health_check(app, cfg)
    bot = Bot(token=cfg.token)
    dispatcher = create_dispatcher(app, cfg, bot)

    scheduler = build_scheduler(app, bot, cfg)
    scheduler.start()

    try:
        await dispatcher.start_polling(bot)
    finally:
        scheduler.shutdown(wait=False)
        await bot.session.close()


def main() -> None:
    """CLI entrypoint."""

    asyncio.run(run_bot())


if __name__ == "__main__":
    main()
