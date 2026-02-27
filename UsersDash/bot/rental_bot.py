"""Aiogram-бот для уведомлений об аренде и заявок на продление."""

from __future__ import annotations

import asyncio
import logging
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime

from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from flask import Flask

from UsersDash.config import Config
from UsersDash.models import (
    Account,
    RenewalBatchItem,
    RenewalBatchRequest,
    RenewalRequest,
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
    """FSM для batch-сценариев оплаты."""

    waiting_amount = State()
    waiting_method = State()
    waiting_comment = State()
    waiting_manual_comment = State()


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
    """Кнопки для batch-сценариев клиента."""

    rows = [
        [InlineKeyboardButton(text="✅ Я оплатил всё", callback_data=f"batch_full:{batch_id}")],
        [InlineKeyboardButton(text="☑️ Я оплатил часть", callback_data=f"batch_partial:{batch_id}:0")],
        [InlineKeyboardButton(text="✍️ Есть изменения", callback_data=f"batch_change:{batch_id}")],
    ]
    if admin_contact:
        rows.append([InlineKeyboardButton(text="Связаться с администратором", url=admin_contact)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_partial_selection_keyboard(batch: RenewalBatchRequest, page: int, page_size: int = 6) -> InlineKeyboardMarkup:
    """Рисует страницу multi-select по фермам batch-заявки."""

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
    """Кнопки подтверждения batch-заявки для админа."""

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
                bind_telegram_chat(
                    raw_token=token,
                    chat_id=str(message.chat.id),
                    username=message.from_user.username if message.from_user else None,
                    first_name=message.from_user.first_name if message.from_user else None,
                    last_name=message.from_user.last_name if message.from_user else None,
                )
            except TokenValidationError as exc:
                await message.answer(f"Не удалось привязать Telegram: {exc}")
                return

        await message.answer("✅ Telegram успешно привязан. Теперь вы будете получать напоминания об аренде.")

    @router.message(CommandStart())
    async def on_start(message: Message) -> None:
        await message.answer(
            "Привет! Я бот продления аренды Viking Rise.\n"
            "Для безопасной привязки используйте персональную ссылку из UsersDash."
        )

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

    @router.callback_query(F.data.startswith("renew:"))
    async def on_renew_click(callback: CallbackQuery) -> None:
        account_id = int(callback.data.split(":", 1)[1])
        with app.app_context():
            profile = TelegramSubscriber.query.filter_by(chat_id=str(callback.message.chat.id)).first()
            if not profile:
                await callback.message.answer("Чат не привязан. Откройте deep-link из UsersDash.")
                await callback.answer()
                return

            settings = get_bot_settings()
            account = Account.query.get(account_id)
            if not account or account.owner_id != profile.user_id:
                await callback.message.answer("Ферма не найдена или недоступна для вашего аккаунта.")
                await callback.answer()
                return

            await callback.message.answer(
                "💳 Продление аренды\n"
                f"Ферма: {account.name}\n"
                f"Стоимость: {account.next_payment_amount or settings.renewal_price_rub} ₽\n"
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
        batch_id = int(callback.data.split(":", 1)[1])
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
        _, raw_batch_id, raw_page = callback.data.split(":", 2)
        batch_id = int(raw_batch_id)
        page = int(raw_page)
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
        _, raw_batch_id, raw_item_id, raw_page = callback.data.split(":", 3)
        batch_id = int(raw_batch_id)
        item_id = int(raw_item_id)
        page = int(raw_page)
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
        _, raw_batch_id, raw_page = callback.data.split(":", 2)
        batch_id = int(raw_batch_id)
        page = int(raw_page)
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
        batch_id = int(callback.data.split(":", 1)[1])
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
        batch_id = int(callback.data.split(":", 1)[1])
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
                await message.answer(f"Не удалось отправить batch-заявку: {exc}")
                await state.clear()
                return

            sent_to_admin = await notify_admins_about_batch_request(bot, cfg.admin_chat_ids, batch.id)
            if not cfg.admin_chat_ids:
                logger.warning(
                    "Список TELEGRAM_ADMIN_CHAT_IDS пуст. "
                    "Batch-заявка %s сохранена без Telegram-уведомления админам.",
                    batch.id,
                )
                await message.answer("✅ Batch-заявка сохранена и будет обработана через UsersDash.")
            elif sent_to_admin:
                await message.answer("✅ Batch-заявка отправлена администратору на подтверждение.")
            else:
                await message.answer(
                    "✅ Batch-заявка сохранена. Сейчас не удалось уведомить администраторов в Telegram, "
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
                await message.answer(f"Не удалось отправить batch-заявку: {exc}")
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
        request_id = int(callback.data.split(":", 1)[1])
        if callback.message.chat.id not in cfg.admin_chat_ids:
            await callback.answer("Недостаточно прав", show_alert=True)
            return

        with app.app_context():
            admin_user = User.query.filter_by(role="admin", is_active=True).order_by(User.id.asc()).first()
            if not admin_user:
                await callback.message.answer("В UsersDash не найден активный администратор.")
                return
            try:
                row = confirm_renewal_request(request_id, admin_user.id)
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
        request_id = int(callback.data.split(":", 1)[1])
        if callback.message.chat.id not in cfg.admin_chat_ids:
            await callback.answer("Недостаточно прав", show_alert=True)
            return

        with app.app_context():
            admin_user = User.query.filter_by(role="admin", is_active=True).order_by(User.id.asc()).first()
            if not admin_user:
                await callback.message.answer("В UsersDash не найден активный администратор.")
                return
            try:
                row = reject_renewal_request(
                    request_id,
                    admin_user.id,
                    reason="Проверьте реквизиты и пришлите корректные данные",
                )
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
        batch_id = int(callback.data.split(":", 1)[1])
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
                await callback.message.answer(f"Не удалось подтвердить batch-заявку: {exc}")
                await callback.answer("Ошибка")
                return

            confirmed_items = RenewalBatchItem.query.filter_by(
                batch_request_id=row.id,
                result_status="confirmed",
            ).count()
            await callback.message.answer(
                f"Batch-заявка #{row.id} подтверждена. Продлено ферм: {confirmed_items}."
            )
            if row.subscriber and row.subscriber.chat_id:
                await bot.send_message(
                    chat_id=int(row.subscriber.chat_id),
                    text=(
                        f"✅ Batch-заявка #{row.id} подтверждена. "
                        f"Продлено ферм: {confirmed_items}."
                    ),
                )
        await callback.answer("Подтверждено")

    @router.callback_query(F.data.startswith("admin_batch_reject:"))
    async def on_admin_batch_reject(callback: CallbackQuery) -> None:
        batch_id = int(callback.data.split(":", 1)[1])
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
                await callback.message.answer(f"Не удалось отклонить batch-заявку: {exc}")
                await callback.answer("Ошибка")
                return

            await callback.message.answer(f"Batch-заявка #{row.id} отклонена.")
            if row.subscriber and row.subscriber.chat_id:
                await bot.send_message(
                    chat_id=int(row.subscriber.chat_id),
                    text=(
                        f"❌ Batch-заявка #{row.id} отклонена: {row.rejection_reason}. "
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
    """Уведомляет админов о новой batch-заявке."""

    row = RenewalBatchRequest.query.get(batch_id)
    if not row:
        return False

    if not admin_chat_ids:
        logger.warning("Список admin_chat_ids пуст, batch-заявка %s не будет отправлена в Telegram.", batch_id)
        return False

    selected_items = [item for item in row.items if item.selected_for_renewal]
    skipped_items = [item for item in row.items if not item.selected_for_renewal]
    selected_lines = [f"• {item.account_name_snapshot} (ID: {item.account_id})" for item in selected_items]
    skipped_lines = [f"• {item.account_name_snapshot} (ID: {item.account_id})" for item in skipped_items]
    selected_text = "\n".join(selected_lines) if selected_lines else "—"
    skipped_text = "\n".join(skipped_lines) if skipped_lines else "—"

    text = (
        f"🧾 Новая batch-заявка #{row.id}\n"
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
                "Не удалось отправить batch-заявку %s админу %s: %s",
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
        candidates = collect_notification_candidates(cfg.reminder_days)
        grouped: dict[tuple[int, str], list[NotificationCandidate]] = defaultdict(list)
        for candidate in candidates:
            key = (candidate.user.id, candidate.subscriber.chat_id)
            grouped[key].append(candidate)

        for group_candidates in grouped.values():
            group_candidates.sort(key=lambda item: (item.due_on, item.account.id))
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
