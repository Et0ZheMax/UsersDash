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
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandStart
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)
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
    bind_telegram_chat,
    collect_notification_candidates,
    confirm_batch_request,
    confirm_renewal_request,
    create_notification_batch,
    ensure_batch_editable,
    create_renewal_request,
    get_batch_for_user,
    get_bot_settings,
    get_multi_pending_statuses,
    get_admin_pending_overview,
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
    waiting_change_kind = State()
    waiting_tariff_choice = State()
    waiting_change_scope = State()
    waiting_change_farm_selection = State()



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


STAGE_LABELS: dict[str, str] = {
    "before_3d": "за 3 дня",
    "before_1d": "за 1 день",
    "on_expiry": "в день окончания",
    "expired_plus": "просрочено+",
}

STATUS_LABELS: dict[str, str] = {
    "sent": "отправлено",
    "delivered": "доставлено",
    "failed": "ошибка",
}


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
        [InlineKeyboardButton(text="✅ Ок, увидел(а)", callback_data="notif_ack")],
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


def build_change_kind_keyboard(batch_id: int) -> InlineKeyboardMarkup:
    """Возвращает клавиатуру выбора типа изменений для batch-заявки."""

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="❌ Не продлеваю в этом месяце",
                    callback_data=f"change_kind:cancel:{batch_id}",
                )
            ],
            [InlineKeyboardButton(text="🔄 Смена тарифа", callback_data=f"change_kind:tariff:{batch_id}")],
            [InlineKeyboardButton(text="✍️ Другое изменение", callback_data=f"change_kind:other:{batch_id}")],
        ]
    )


def build_change_scope_keyboard(batch_id: int) -> InlineKeyboardMarkup:
    """Возвращает клавиатуру выбора охвата изменений по фермам."""

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🌐 Применить ко всем фермам",
                    callback_data=f"change_scope:all:{batch_id}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="🎯 Выбрать конкретные фермы",
                    callback_data=f"change_scope:custom:{batch_id}:0",
                )
            ],
        ]
    )


def build_tariff_change_keyboard(batch_id: int) -> InlineKeyboardMarkup:
    """Возвращает клавиатуру выбора нового тарифа."""

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Премиум — 1399 ₽", callback_data=f"change_tariff:premium:{batch_id}")],
            [InlineKeyboardButton(text="Расширенный — 999 ₽", callback_data=f"change_tariff:extended:{batch_id}")],
            [InlineKeyboardButton(text="Только фарм — 499 ₽", callback_data=f"change_tariff:farm:{batch_id}")],
        ]
    )


def build_change_farms_keyboard(batch: RenewalBatchRequest, page: int = 0, page_size: int = 8) -> InlineKeyboardMarkup:
    """Рисует страницу выбора ферм для изменений клиента."""

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
                    callback_data=f"change_toggle:{batch.id}:{item.id}:{page}",
                )
            ]
        )

    nav_row: list[InlineKeyboardButton] = []
    if page > 0:
        nav_row.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"change_page:{batch.id}:{page - 1}"))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton(text="➡️ Далее", callback_data=f"change_page:{batch.id}:{page + 1}"))
    if nav_row:
        rows.append(nav_row)

    rows.append([
        InlineKeyboardButton(
            text="☑️ Выбрать всё на странице",
            callback_data=f"change_select_page:{batch.id}:{page}",
        )
    ])
    rows.append([InlineKeyboardButton(text="Готово", callback_data=f"change_done:{batch.id}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)




def build_reply_keyboard(is_admin: bool) -> ReplyKeyboardMarkup:
    """Возвращает постоянную Reply-клавиатуру внизу чата."""

    rows = [[KeyboardButton(text="🏠 Меню")]]
    if is_admin:
        rows = [
            [KeyboardButton(text="🛠 Админ-меню")],
            [KeyboardButton(text="📥 Очередь оплат"), KeyboardButton(text="📣 Отправки")],
            [KeyboardButton(text="🏠 Меню")],
        ]
    return ReplyKeyboardMarkup(
        keyboard=rows,
        resize_keyboard=True,
        is_persistent=True,
        input_field_placeholder="Выберите действие",
    )


def build_admin_menu_keyboard() -> InlineKeyboardMarkup:
    """Inline-панель быстрого доступа к админ-разделам."""

    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📥 Очередь оплат", callback_data="admin_menu:pending")],
        [InlineKeyboardButton(text="📣 Отправки уведомлений", callback_data="admin_menu:sent")],
        [InlineKeyboardButton(text="⚙️ Настройки", callback_data="admin_menu:settings")],
    ])



async def safe_send_text(target: Message, text: str, *, reply_markup=None, chunk_limit: int = 3500) -> None:
    """Безопасно отправляет длинный текст в Telegram с разбиением на части."""

    payload = text or ""
    if len(payload) <= chunk_limit:
        try:
            await target.answer(payload, reply_markup=reply_markup)
        except TelegramBadRequest as exc:
            logger.warning("Не удалось отправить сообщение: %s", exc)
            await target.answer("Не удалось отправить сообщение: текст слишком длинный для Telegram.")
        return

    chunks: list[str] = []
    current: list[str] = []
    current_len = 0
    for line in payload.splitlines() or [payload]:
        piece = line if line else " "
        line_len = len(piece) + 1
        if current and current_len + line_len > chunk_limit:
            chunks.append("\n".join(current))
            current = [piece]
            current_len = line_len
            continue
        if not current and line_len > chunk_limit:
            for idx in range(0, len(piece), chunk_limit):
                chunks.append(piece[idx:idx + chunk_limit])
            current = []
            current_len = 0
            continue
        current.append(piece)
        current_len += line_len
    if current:
        chunks.append("\n".join(current))

    if not chunks:
        chunks = [payload[:chunk_limit]]

    for idx, chunk in enumerate(chunks):
        try:
            await target.answer(chunk, reply_markup=reply_markup if idx == 0 else None)
        except TelegramBadRequest as exc:
            logger.warning("Не удалось отправить chunk %s/%s: %s", idx + 1, len(chunks), exc)
            await target.answer("Не удалось отправить сообщение: текст слишком длинный для Telegram.")
            break


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

    db_uri = app.config.get("SQLALCHEMY_DATABASE_URI", "")
    print(f"[INFO] SQLALCHEMY_DATABASE_URI: {db_uri}")
    if db_uri.startswith("sqlite:///"):
        db_rel_path = db_uri.replace("sqlite:///", "", 1)
        db_abs_path = Path(db_rel_path)
        if not db_abs_path.is_absolute():
            db_abs_path = (Path.cwd() / db_abs_path).resolve()
        exists = db_abs_path.exists()
        print(f"[INFO] SQLite path: {db_abs_path}")
        print(f"[INFO] SQLite exists: {'yes' if exists else 'no'}")

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
            for col in (
                "payment_details_text",
                "payment_instruction_text",
                "support_contact",
                "reminder_days",
                "reminders_enabled",
            ):
                if col not in settings_columns:
                    optional_warnings.append(f"Нет колонки telegram_bot_settings.{col}")

        if inspector.has_table("renewal_requests"):
            request_columns = {item['name'] for item in inspector.get_columns("renewal_requests")}
            for col in ("request_type", "status_before_needs_info"):
                if col not in request_columns:
                    optional_warnings.append(f"Нет колонки renewal_requests.{col}")

        for warning in optional_warnings:
            print(f"[WARN] {warning}. Подсказка: запустите migrate_add_rental_bot_admin_features.py")

        single_statuses = ["payment_pending_confirmation", "payment_data_collecting", "needs_info"]
        multi_statuses = get_multi_pending_statuses()
        single_count = RenewalRequest.query.filter(RenewalRequest.status.in_(single_statuses)).count()
        multi_count = RenewalBatchRequest.query.filter(RenewalBatchRequest.status.in_(multi_statuses)).count()
        site_like_count = single_count + multi_count
        print(f"[INFO] Pending RenewalRequest: {single_count} (statuses={single_statuses})")
        print(f"[INFO] Pending заявки по нескольким фермам: {multi_count} (statuses={multi_statuses})")
        print(f"[INFO] Site-like pending total: {site_like_count}")

        if single_count > 0:
            sample_single = (
                RenewalRequest.query.filter(RenewalRequest.status.in_(single_statuses))
                .order_by(RenewalRequest.created_at.desc())
                .limit(2)
                .all()
            )
            for row in sample_single:
                print(f"[INFO] RenewalRequest sample: id={row.id}, status={row.status}, created_at={row.created_at}")

        if multi_count > 0:
            sample_multi = (
                RenewalBatchRequest.query.filter(RenewalBatchRequest.status.in_(multi_statuses))
                .order_by(RenewalBatchRequest.created_at.desc())
                .limit(2)
                .all()
            )
            for row in sample_multi:
                print(
                    f"[INFO] Заявка по нескольким фермам sample: "
                    f"id={row.id}, status={row.status}, created_at={row.created_at}"
                )

        bot_like_count = RenewalRequest.query.filter(
            RenewalRequest.status.in_(["payment_pending_confirmation", "payment_data_collecting", "needs_info"])
        ).count()
        if bot_like_count == 0 and site_like_count > 0:
            print(
                "[HINT] В очереди сайта есть заявки, но одиночных заявок нет: "
                "вероятно, раньше бот не учитывал заявки по нескольким фермам или статусы отличаются."
            )

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

    async def _admin_show_pending(message: Message, limit: int = 20) -> None:
        """Показывает администратору общую очередь подтверждений как на сайте."""

        try:
            prepared_single: list[tuple[str, InlineKeyboardMarkup]] = []
            prepared_multi: list[tuple[str, InlineKeyboardMarkup]] = []
            with app.app_context():
                overview = get_admin_pending_overview(limit_single=limit, limit_multi=limit)
                single_rows = overview["single"]
                multi_rows = overview["multi"]
                counts = overview["counts"]

                for request_row in single_rows:
                    account = request_row.account
                    subscriber = request_row.subscriber
                    farm_name = account.name if account and account.name else "—"
                    due_at = to_utc_naive(account.next_payment_at) if account else None
                    due_text = due_at.strftime("%d.%m.%Y") if due_at else "—"
                    amount_due = (
                        f"{account.next_payment_amount} ₽"
                        if account and account.next_payment_amount is not None
                        else "—"
                    )
                    user_label = str(request_row.user_id)
                    username = (subscriber.username or "").strip() if subscriber else ""
                    if username:
                        user_label = f"@{username} ({request_row.user_id})"

                    comment = (request_row.comment or "—").strip()
                    if len(comment) > 220:
                        comment = f"{comment[:220]}…"

                    row_text = (
                        f"Заявка #{request_row.id}\n"
                        f"Ферма: {farm_name}\n"
                        f"Оплатить до: {due_text}\n"
                        f"Сумма к оплате: {amount_due}\n"
                        f"Клиент: {user_label}\n"
                        f"Статус: {_status_label(request_row.status)}\n"
                        f"Клиент указал сумму: {request_row.amount_rub if request_row.amount_rub is not None else '—'}\n"
                        f"Метод: {request_row.payment_method or '—'}\n"
                        f"Дата заявки: {request_row.created_at:%d.%m.%Y %H:%M}\n"
                        f"Комментарий: {comment}"
                    )
                    prepared_single.append((row_text, _admin_request_keyboard(request_row.id)))

                for batch in multi_rows:
                    subscriber = batch.subscriber
                    user_label = str(batch.user_id)
                    username = (subscriber.username or "").strip() if subscriber else ""
                    if username:
                        user_label = f"@{username} ({batch.user_id})"

                    mode_map = {
                        "full": "оплачено всё",
                        "partial": "оплачена часть",
                        "manual_change": "запрошены изменения",
                    }
                    mode_text = mode_map.get((batch.mode or "").strip(), batch.mode or "—")
                    method = (batch.payment_method or "—").strip() or "—"
                    comment = (batch.comment or "—").strip()
                    if len(comment) > 220:
                        comment = f"{comment[:220]}…"

                    items_lines: list[str] = []
                    for item in batch.items.order_by(RenewalBatchItem.id.asc()).all()[:10]:
                        selected = "✅" if item.selected_for_renewal else "▫️"
                        due_at = to_utc_naive(item.due_at_snapshot)
                        due_text = due_at.strftime("%d.%m.%Y") if due_at else "—"
                        amount = f"{item.amount_rub_snapshot} ₽" if item.amount_rub_snapshot is not None else "—"
                        items_lines.append(f"{selected} {item.account_name_snapshot} • до {due_text} • {amount}")
                    if not items_lines:
                        items_lines.append("—")

                    row_text = (
                        f"Заявка #{batch.id}\n"
                        f"Клиент: {user_label}\n"
                        f"Режим: {mode_text}\n"
                        f"Сумма: {batch.total_amount_rub if batch.total_amount_rub is not None else '—'} ₽\n"
                        f"Метод: {method}\n"
                        f"Комментарий: {comment}\n"
                        f"Дата заявки: {batch.created_at:%d.%m.%Y %H:%M}\n"
                        f"Выбранные фермы:\n" + "\n".join(items_lines)
                    )
                    prepared_multi.append((row_text, build_admin_batch_keyboard(batch.id, batch.mode)))

            await message.answer(
                "📥 Очередь подтверждений\n"
                f"Заявки (1 ферма): {counts['single']}\n"
                f"Заявки (несколько ферм): {counts['multi']}\n"
                f"Итого: {counts['total']}"
            )

            await message.answer("🧾 Заявки (1 ферма)")
            if not prepared_single:
                await message.answer("Нет заявок на проверке.")
            for row_text, row_keyboard in prepared_single:
                await message.answer(row_text, reply_markup=row_keyboard)

            await message.answer("🧾 Заявки (несколько ферм)")
            if not prepared_multi:
                await message.answer("Нет заявок на проверке.")
            for row_text, row_keyboard in prepared_multi:
                await message.answer(row_text, reply_markup=row_keyboard)
        except Exception:
            logger.exception("Ошибка формирования очереди оплат для chat_id=%s", message.chat.id)
            await message.answer("Не удалось показать очередь оплат.")

    async def _admin_show_notifications(message: Message, limit: int = 30) -> None:
        """Показывает отчёт по отправкам уведомлений без N+1 запросов."""

        try:
            lines: list[str] = []
            with app.app_context():
                logs = (
                    RentalNotificationLog.query.order_by(RentalNotificationLog.created_at.desc())
                    .limit(limit)
                    .all()
                )
                if not logs:
                    await message.answer("Отправок пока нет.")
                    return

                account_ids = {row.account_id for row in logs if row.account_id}
                subscriber_ids = {row.subscriber_id for row in logs if row.subscriber_id}
                user_ids = {row.user_id for row in logs if row.user_id}

                accounts_by_id: dict[int, Account] = {}
                if account_ids:
                    accounts_by_id = {
                        row.id: row for row in Account.query.filter(Account.id.in_(account_ids)).all()
                    }

                subs_by_id: dict[int, TelegramSubscriber] = {}
                subs_by_user_id: dict[int, TelegramSubscriber] = {}
                subscribers: list[TelegramSubscriber] = []
                if subscriber_ids:
                    subscribers.extend(
                        TelegramSubscriber.query.filter(TelegramSubscriber.id.in_(subscriber_ids)).all()
                    )
                if user_ids:
                    subscribers.extend(
                        TelegramSubscriber.query.filter(TelegramSubscriber.user_id.in_(user_ids)).all()
                    )
                for sub in subscribers:
                    if sub.id and sub.id not in subs_by_id:
                        subs_by_id[sub.id] = sub
                    if sub.user_id and sub.user_id not in subs_by_user_id:
                        subs_by_user_id[sub.user_id] = sub

                # Формируем готовые строки внутри app_context, чтобы снаружи только отправлять чанки.
                lines = ["📣 Последние отправки уведомлений:"]
                for row in logs:
                    account_name = accounts_by_id.get(row.account_id).name if row.account_id in accounts_by_id else "—"
                    due_on = row.due_on.strftime("%d.%m.%Y") if row.due_on else "—"
                    created_at = row.created_at.strftime("%d.%m %H:%M") if row.created_at else "—"
                    subscriber = subs_by_id.get(row.subscriber_id) or subs_by_user_id.get(row.user_id)
                    username = (subscriber.username or "").strip() if subscriber else ""
                    user_label = f"@{username} ({row.user_id})" if username else str(row.user_id or "—")
                    stage_label = STAGE_LABELS.get(row.stage or "", row.stage or "—")
                    status_code = row.status or "sent"
                    status_label = STATUS_LABELS.get(status_code, status_code)
                    ack = f"подтверждено ({row.acked_at:%d.%m %H:%M})" if row.acked_at else "нет"
                    lines.append(
                        f"• {created_at} | {user_label} | {account_name} | срок: {due_on} "
                        f"| этап: {stage_label} | доставка: {status_label} | подтверждение: {ack}"
                    )

            chunk: list[str] = []
            chunk_chars = 0
            for line in lines:
                if chunk and chunk_chars + len(line) + 1 > 3500:
                    await message.answer("\n".join(chunk))
                    chunk = []
                    chunk_chars = 0
                chunk.append(line)
                chunk_chars += len(line) + 1
            if chunk:
                await message.answer("\n".join(chunk))
        except Exception:
            logger.exception("Ошибка формирования отчёта отправок для chat_id=%s", message.chat.id)
            await message.answer("Не удалось показать отчёт по отправкам.")

    @router.message(CommandStart(deep_link=True))
    async def on_start_with_token(message: Message) -> None:
        deep_arg = (message.text or "").split(maxsplit=1)
        if len(deep_arg) < 2 or not deep_arg[1].startswith("bind_"):
            await message.answer(
                "Привет! Для привязки перейдите по персональной ссылке из UsersDash.",
                reply_markup=build_reply_keyboard(is_admin=_is_admin(message.chat.id, cfg)),
            )
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
                await message.answer(
                    f"Не удалось привязать Telegram: {exc}",
                    reply_markup=build_reply_keyboard(is_admin=_is_admin(message.chat.id, cfg)),
                )
                return

            settings = get_bot_settings()
            dashboard_text = render_client_dashboard(profile)
            keyboard = build_dashboard_keyboard(settings.admin_contact)
        await message.answer("✅ Telegram успешно привязан. Теперь вы будете получать напоминания об аренде.")
        await message.answer(
            "Меню доступно по кнопке «🏠 Меню».",
            reply_markup=build_reply_keyboard(is_admin=_is_admin(message.chat.id, cfg)),
        )
        await message.answer(dashboard_text, reply_markup=keyboard)

    @router.message(CommandStart())
    async def on_start(message: Message) -> None:
        with app.app_context():
            profile = TelegramSubscriber.query.filter_by(chat_id=str(message.chat.id)).first()
            if not profile:
                await message.answer(
                    "Привет! Я бот продления аренды Viking Rise.\n"
                    "Для безопасной привязки используйте персональную ссылку из UsersDash.",
                    reply_markup=build_reply_keyboard(is_admin=_is_admin(message.chat.id, cfg)),
                )
                return

            profile.last_interaction_at = utcnow()
            db.session.commit()
            settings = get_bot_settings()
            await message.answer(
                "Меню доступно по кнопке «🏠 Меню».",
                reply_markup=build_reply_keyboard(is_admin=_is_admin(message.chat.id, cfg)),
            )
            await message.answer(
                render_client_dashboard(profile),
                reply_markup=build_dashboard_keyboard(settings.admin_contact),
            )



    @router.message(F.text == "🏠 Меню")
    async def on_reply_menu(message: Message) -> None:
        """Открывает клиентский dashboard по постоянной кнопке Reply-клавиатуры."""

        try:
            with app.app_context():
                is_admin = _is_admin(message.chat.id, cfg)
                profile = TelegramSubscriber.query.filter_by(chat_id=str(message.chat.id)).first()
                if not profile:
                    await message.answer(
                        "Чат пока не привязан к UsersDash. Откройте персональную ссылку из кабинета, "
                        "после этого меню заработает полностью.",
                        reply_markup=build_reply_keyboard(is_admin=is_admin),
                    )
                    return

                profile.last_interaction_at = utcnow()
                db.session.commit()
                settings = get_bot_settings()
                dashboard_text = render_client_dashboard(profile)
                logger.info(
                    "Reply меню chat_id=%s, user_id=%s, dashboard_len=%s",
                    message.chat.id,
                    profile.user_id,
                    len(dashboard_text),
                )
                dashboard_keyboard = build_dashboard_keyboard(settings.admin_contact)

            await safe_send_text(
                message,
                dashboard_text,
                reply_markup=dashboard_keyboard,
                chunk_limit=3500,
            )
            await message.answer(
                "Выберите действие:",
                reply_markup=build_reply_keyboard(is_admin=is_admin),
            )
        except Exception:
            logger.exception("Ошибка обработки Reply-кнопки меню для chat_id=%s", message.chat.id)
            await message.answer(
                "Не удалось открыть меню. Попробуйте чуть позже.",
                reply_markup=build_reply_keyboard(is_admin=_is_admin(message.chat.id, cfg)),
            )

    @router.message(F.text == "🛠 Админ-меню")
    async def on_reply_admin_menu(message: Message) -> None:
        """Открывает inline-меню администратора по Reply-кнопке."""

        if not _is_admin(message.chat.id, cfg):
            await message.answer("Эта кнопка доступна только администраторам.")
            return
        try:
            await message.answer("🛠 Админ-меню:", reply_markup=build_admin_menu_keyboard())
            await message.answer(
                "Быстрые кнопки снизу тоже доступны.",
                reply_markup=build_reply_keyboard(is_admin=True),
            )
        except Exception:
            logger.exception("Ошибка открытия Reply админ-меню для chat_id=%s", message.chat.id)
            await message.answer("Не удалось открыть админ-меню. Попробуйте позже.")

    @router.message(F.text == "📥 Очередь оплат")
    async def on_reply_admin_pending(message: Message) -> None:
        """Показывает очередь оплат по Reply-кнопке администратора."""

        if not _is_admin(message.chat.id, cfg):
            await message.answer("Недостаточно прав.")
            return
        await _admin_show_pending(message)

    @router.message(F.text == "📣 Отправки")
    async def on_reply_admin_sent(message: Message) -> None:
        """Показывает отчёт по отправкам по Reply-кнопке администратора."""

        if not _is_admin(message.chat.id, cfg):
            await message.answer("Недостаточно прав.")
            return
        await _admin_show_notifications(message)

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
        await state.set_state(BatchPaymentFSM.waiting_change_kind)
        await callback.message.answer(
            "Выберите, что хотите изменить:",
            reply_markup=build_change_kind_keyboard(batch.id),
        )
        await callback.answer()

    @router.callback_query(F.data.startswith("change_kind:"))
    async def on_change_kind(callback: CallbackQuery, state: FSMContext) -> None:
        parts = (callback.data or "").split(":", 2)
        if len(parts) != 3:
            await callback.answer("Некорректные данные", show_alert=True)
            return
        _, kind, raw_batch_id = parts
        if not raw_batch_id.isdigit():
            await callback.answer("Некорректный id", show_alert=True)
            return
        batch_id = int(raw_batch_id)

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

        data = await state.get_data()
        data.update({"batch_id": batch_id, "mode": "manual_change", "change_kind": kind})
        await state.set_data(data)

        if kind == "tariff":
            await state.set_state(BatchPaymentFSM.waiting_tariff_choice)
            await callback.message.answer(
                "Выберите новый тариф:\n"
                "• Премиум — 1399 ₽\n"
                "• Расширенный — 999 ₽\n"
                "• Только фарм — 499 ₽\n\n"
                "Для 4+ ферм действует скидка (подробнее в лс админу).",
                reply_markup=build_tariff_change_keyboard(batch_id),
            )
            await callback.answer()
            return

        await state.set_state(BatchPaymentFSM.waiting_change_scope)
        if kind == "cancel":
            prompt = "Укажите охват отказа от продления в этом месяце:"
        else:
            prompt = "Укажите, к каким фермам относятся изменения:"
        await callback.message.answer(prompt, reply_markup=build_change_scope_keyboard(batch_id))
        await callback.answer()

    @router.callback_query(F.data.startswith("change_tariff:"))
    async def on_change_tariff(callback: CallbackQuery, state: FSMContext) -> None:
        parts = (callback.data or "").split(":", 2)
        if len(parts) != 3:
            await callback.answer("Некорректные данные", show_alert=True)
            return
        _, tariff_code, raw_batch_id = parts
        if not raw_batch_id.isdigit():
            await callback.answer("Некорректный id", show_alert=True)
            return
        batch_id = int(raw_batch_id)

        tariffs = {
            "premium": "Премиум — 1399 ₽",
            "extended": "Расширенный — 999 ₽",
            "farm": "Только фарм — 499 ₽",
        }
        selected_tariff = tariffs.get(tariff_code)
        if not selected_tariff:
            await callback.answer("Неизвестный тариф", show_alert=True)
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

        data = await state.get_data()
        data.update(
            {
                "batch_id": batch_id,
                "mode": "manual_change",
                "change_kind": "tariff",
                "tariff_target": selected_tariff,
            }
        )
        await state.set_data(data)
        await state.set_state(BatchPaymentFSM.waiting_change_scope)
        await callback.message.answer(
            f"Вы выбрали тариф: {selected_tariff}.\nТеперь укажите, куда применить смену тарифа:",
            reply_markup=build_change_scope_keyboard(batch_id),
        )
        await callback.answer()

    @router.callback_query(F.data.startswith("change_scope:"))
    async def on_change_scope(callback: CallbackQuery, state: FSMContext) -> None:
        parts = (callback.data or "").split(":")
        if len(parts) < 3:
            await callback.answer("Некорректные данные", show_alert=True)
            return
        _, scope, raw_batch_id = parts[:3]
        page = 0
        if len(parts) > 3 and parts[3].isdigit():
            page = int(parts[3])
        if not raw_batch_id.isdigit():
            await callback.answer("Некорректный id", show_alert=True)
            return
        batch_id = int(raw_batch_id)

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

            all_item_ids = {item.account_id for item in batch.items}
            if scope == "all":
                set_batch_selected_accounts(batch=batch, selected_account_ids=all_item_ids)
            elif scope == "custom":
                set_batch_selected_accounts(batch=batch, selected_account_ids=set())
            else:
                await callback.answer("Неизвестный вариант", show_alert=True)
                return

            mark_batch_mode(batch, "manual_change")

        data = await state.get_data()
        data.update({"batch_id": batch_id, "mode": "manual_change", "change_scope": scope})
        await state.set_data(data)

        if scope == "all":
            await state.set_state(BatchPaymentFSM.waiting_manual_comment)
            await callback.message.answer(
                "Добавьте комментарий для администратора (или отправьте '-')."
            )
            await callback.answer("Выбраны все фермы")
            return

        with app.app_context():
            profile = TelegramSubscriber.query.filter_by(chat_id=str(callback.message.chat.id)).first()
            batch = get_batch_for_user(batch_id, profile.user_id)
            keyboard = build_change_farms_keyboard(batch, page=page)
        await state.set_state(BatchPaymentFSM.waiting_change_farm_selection)
        await callback.message.answer(
            "Выберите фермы, к которым применить изменение:",
            reply_markup=keyboard,
        )
        await callback.answer()

    @router.callback_query(F.data.startswith("change_page:"))
    async def on_change_page(callback: CallbackQuery, state: FSMContext) -> None:
        parts = (callback.data or "").split(":")
        if len(parts) != 3 or not parts[1].isdigit() or not parts[2].isdigit():
            await callback.answer("Некорректные данные", show_alert=True)
            return
        batch_id = int(parts[1])
        page = int(parts[2])
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
            keyboard = build_change_farms_keyboard(batch, page=page)
        await callback.message.edit_reply_markup(reply_markup=keyboard)
        await callback.answer()

    @router.callback_query(F.data.startswith("change_toggle:"))
    async def on_change_toggle(callback: CallbackQuery) -> None:
        parts = (callback.data or "").split(":")
        if len(parts) != 4 or not all(part.isdigit() for part in parts[1:]):
            await callback.answer("Некорректные данные", show_alert=True)
            return
        batch_id, item_id, page = map(int, parts[1:])
        with app.app_context():
            profile = TelegramSubscriber.query.filter_by(chat_id=str(callback.message.chat.id)).first()
            if not profile:
                await callback.answer("Чат не привязан", show_alert=True)
                return
            batch = get_batch_for_user(batch_id, profile.user_id)
            ensure_batch_editable(batch)
            item = RenewalBatchItem.query.filter_by(batch_request_id=batch.id, id=item_id).first()
            if not item:
                await callback.answer("Ферма не найдена", show_alert=True)
                return
            item.selected_for_renewal = not item.selected_for_renewal
            db.session.commit()
            keyboard = build_change_farms_keyboard(batch, page=page)
        await callback.message.edit_reply_markup(reply_markup=keyboard)
        await callback.answer("Обновлено")

    @router.callback_query(F.data.startswith("change_select_page:"))
    async def on_change_select_page(callback: CallbackQuery) -> None:
        parts = (callback.data or "").split(":")
        if len(parts) != 3 or not parts[1].isdigit() or not parts[2].isdigit():
            await callback.answer("Некорректные данные", show_alert=True)
            return
        batch_id = int(parts[1])
        page = int(parts[2])
        page_size = 8
        with app.app_context():
            profile = TelegramSubscriber.query.filter_by(chat_id=str(callback.message.chat.id)).first()
            if not profile:
                await callback.answer("Чат не привязан", show_alert=True)
                return
            batch = get_batch_for_user(batch_id, profile.user_id)
            ensure_batch_editable(batch)
            items = list(batch.items.order_by(RenewalBatchItem.id.asc()).all())
            start = page * page_size
            chunk = items[start:start + page_size]
            for item in chunk:
                item.selected_for_renewal = True
            db.session.commit()
            keyboard = build_change_farms_keyboard(batch, page=page)
        await callback.message.edit_reply_markup(reply_markup=keyboard)
        await callback.answer("Страница отмечена")

    @router.callback_query(F.data.startswith("change_done:"))
    async def on_change_done(callback: CallbackQuery, state: FSMContext) -> None:
        parts = (callback.data or "").split(":", 1)
        if len(parts) != 2 or not parts[1].isdigit():
            await callback.answer("Некорректные данные", show_alert=True)
            return
        batch_id = int(parts[1])
        with app.app_context():
            profile = TelegramSubscriber.query.filter_by(chat_id=str(callback.message.chat.id)).first()
            if not profile:
                await callback.answer("Чат не привязан", show_alert=True)
                return
            batch = get_batch_for_user(batch_id, profile.user_id)
            ensure_batch_editable(batch)
            selected_count = RenewalBatchItem.query.filter_by(
                batch_request_id=batch.id,
                selected_for_renewal=True,
            ).count()
            if selected_count == 0:
                await callback.answer("Выберите хотя бы одну ферму.", show_alert=True)
                return
            mark_batch_mode(batch, "manual_change")

        await state.set_state(BatchPaymentFSM.waiting_manual_comment)
        await callback.message.answer("Добавьте комментарий для администратора (или отправьте '-').")
        await callback.answer("Готово")

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



    @router.callback_query(F.data == "notif_ack")
    async def on_notif_ack(callback: CallbackQuery) -> None:
        """Фиксирует ACK клиента для уведомления по message_id."""

        try:
            if not callback.message:
                await callback.answer("Не удалось определить сообщение.", show_alert=True)
                return
            message_id = str(callback.message.message_id)
            updated = 0
            with app.app_context():
                profile = TelegramSubscriber.query.filter_by(chat_id=str(callback.message.chat.id)).first()
                if not profile:
                    await callback.answer("Чат не привязан", show_alert=True)
                    return
                updated = (
                    RentalNotificationLog.query.filter_by(subscriber_id=profile.id, message_id=message_id)
                    .filter(RentalNotificationLog.acked_at.is_(None))
                    .update({"acked_at": utcnow()}, synchronize_session=False)
                )
                db.session.commit()
            if updated == 0:
                await callback.answer("Уже отмечено или запись не найдена")
                return
            await callback.answer("Отмечено 👍")
        except Exception:
            logger.exception(
                "Ошибка ACK уведомления для chat_id=%s",
                callback.message.chat.id if callback.message else "?",
            )
            await callback.answer("Не удалось сохранить отметку", show_alert=True)

    @router.callback_query(F.data.startswith("admin_menu:"))
    async def on_admin_menu_callback(callback: CallbackQuery) -> None:
        """Обрабатывает быстрые кнопки inline админ-меню."""

        if not callback.message:
            await callback.answer("Сообщение недоступно", show_alert=True)
            return
        if not _is_admin(callback.message.chat.id, cfg):
            await callback.answer("Недостаточно прав", show_alert=True)
            return
        action = (callback.data or "").split(":", 1)[1] if ":" in (callback.data or "") else ""
        try:
            with app.app_context():
                if action == "pending":
                    await _admin_show_pending(callback.message)
                elif action == "sent":
                    await _admin_show_notifications(callback.message)
                elif action == "settings":
                    settings = get_bot_settings()
                    days = settings.reminder_days or ",".join(str(x) for x in cfg.reminder_days)
                    text = (
                        "⚙️ Настройки бота\n"
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
                    await callback.message.answer(text, reply_markup=kb)
                else:
                    await callback.answer("Неизвестный пункт меню", show_alert=True)
                    return
            await callback.answer()
        except Exception:
            logger.exception("Ошибка обработки admin_menu callback=%s", callback.data)
            await callback.answer("Не удалось выполнить действие", show_alert=True)

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
                    "Команды: /admin_pending [N], /admin_user <id|@username>, /admin_settings, /admin_audit <user_id>",
                    reply_markup=build_admin_menu_keyboard(),
                )
                return
            subcmd = parts[1].lower()
            args = parts[2:]

        with app.app_context():
            if subcmd == "pending":
                limit = 20
                if args and args[0].isdigit():
                    limit = max(1, min(int(args[0]), 50))
                await _admin_show_pending(message, limit=limit)
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
        await state.set_state(BatchPaymentFSM.waiting_change_kind)
        await callback.message.answer(
            "Выберите, что хотите изменить:",
            reply_markup=build_change_kind_keyboard(batch_id),
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
        change_kind = str(data.get("change_kind") or "other")
        change_scope = str(data.get("change_scope") or "all")
        tariff_target = str(data.get("tariff_target") or "").strip()
        with app.app_context():
            profile = TelegramSubscriber.query.filter_by(chat_id=str(message.chat.id)).first()
            if not profile:
                await message.answer("Чат не привязан. Откройте deep-link из UsersDash.")
                await state.clear()
                return
            try:
                batch = get_batch_for_user(batch_id, profile.user_id)
                selected_items = [item for item in batch.items if item.selected_for_renewal]
                selected_names = ", ".join(item.account_name_snapshot for item in selected_items[:12])
                if len(selected_items) > 12:
                    selected_names = f"{selected_names}, … и ещё {len(selected_items) - 12}"
                if not selected_names:
                    selected_names = "—"
                extra_lines = [
                    f"Тип изменения: {change_kind}",
                    f"Охват: {'все фермы' if change_scope == 'all' else 'выбранные фермы'}",
                    f"Фермы: {selected_names}",
                ]
                if tariff_target:
                    extra_lines.append(f"Новый тариф: {tariff_target}")
                    extra_lines.append(
                        "Примечание: при установке от 4-х ферм действует скидка "
                        "(подробнее в лс админу)."
                    )
                user_comment = (message.text or "").strip()
                comment_payload = "\n".join(extra_lines)
                if user_comment and user_comment != "-":
                    comment_payload = f"{comment_payload}\nКомментарий клиента: {user_comment}"
                submit_batch_request(
                    batch=batch,
                    amount_rub=None,
                    payment_method=None,
                    comment=comment_payload,
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
