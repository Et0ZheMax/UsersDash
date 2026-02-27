"""Aiogram-бот для уведомлений об аренде и заявок на продление."""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from datetime import datetime

from aiogram import Bot, Dispatcher, F, Router
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from flask import Flask

from UsersDash.config import Config
from UsersDash.models import Account, RenewalRequest, TelegramSubscriber, User, db
from UsersDash.services.rental_bot import (
    RentalBotError,
    TokenValidationError,
    admin_dashboard_snapshot,
    bind_telegram_chat,
    collect_notification_candidates,
    confirm_renewal_request,
    create_renewal_request,
    get_bot_settings,
    log_notification_result,
    reject_renewal_request,
    render_reminder_text,
    unresolved_requests,
)


class PaymentFSM(StatesGroup):
    """FSM для приёма данных об оплате от клиента."""

    waiting_amount = State()
    waiting_method = State()
    waiting_comment = State()


@dataclass(slots=True)
class RuntimeConfig:
    """Runtime-конфиг Telegram-бота."""

    token: str
    admin_chat_ids: set[int]
    reminder_days: list[int]


logger = logging.getLogger(__name__)


def build_runtime_config() -> RuntimeConfig:
    """Собирает настройки запуска из ENV."""

    token = (os.environ.get("TELEGRAM_BOT_TOKEN") or "").strip()
    if not token:
        token = Config.TELEGRAM_BOT_TOKEN
    if not token:
        raise RuntimeError("Не задан TELEGRAM_BOT_TOKEN")

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


def build_user_keyboard(account_id: int, admin_contact: str | None) -> InlineKeyboardMarkup:
    """Кнопки для пользовательского уведомления."""

    rows = [
        [InlineKeyboardButton(text="Продлить", callback_data=f"renew:{account_id}")],
        [InlineKeyboardButton(text="Я уже оплатил", callback_data=f"paid:{account_id}")],
    ]
    if admin_contact:
        rows.append([InlineKeyboardButton(text="Связаться с администратором", url=admin_contact)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_admin_keyboard(request_id: int) -> InlineKeyboardMarkup:
    """Кнопки подтверждения заявки для админа."""

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"admin_confirm:{request_id}")],
            [InlineKeyboardButton(text="❌ Отклонить", callback_data=f"admin_reject:{request_id}")],
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
            settings = get_bot_settings()
            account = Account.query.get(account_id)
            if not account:
                await callback.message.answer("Аренда не найдена.")
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
        await state.set_data({"account_id": account_id})
        await state.set_state(PaymentFSM.waiting_amount)
        await callback.message.answer("Введите сумму оплаты в рублях (только число).")
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

            request_row = create_renewal_request(
                user_id=profile.user_id,
                account_id=account_id,
                subscriber_id=profile.id,
                amount_rub=int(data.get("amount_rub", 0)) or None,
                payment_method=str(data.get("payment_method") or "") or None,
                comment=(message.text or "").strip() or None,
                receipt_file_id=None,
            )
            sent_to_admin = await notify_admins_about_request(bot, cfg.admin_chat_ids, request_row.id)
            if not cfg.admin_chat_ids:
                logger.warning("Список TELEGRAM_ADMIN_CHAT_IDS пуст. Заявка %s сохранена без Telegram-уведомления админам.", request_row.id)
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

    @router.message(F.photo)
    async def on_photo(message: Message, state: FSMContext) -> None:
        current_state = await state.get_state()
        if current_state != PaymentFSM.waiting_comment.state:
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

            await callback.message.answer(f"Заявка #{row.id} подтверждена. Аренда продлена до {row.confirmed_paid_until:%d.%m.%Y}.")
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
        row.last_admin_reminder_at = datetime.utcnow()
        db.session.commit()

    return delivered


async def run_notifications_job(app: Flask, bot: Bot, cfg: RuntimeConfig) -> None:
    """Фоновая задача отправки напоминаний."""

    with app.app_context():
        settings = get_bot_settings()
        candidates = collect_notification_candidates(cfg.reminder_days)
        for candidate in candidates:
            text = render_reminder_text(settings, candidate.account, candidate.days_left)
            keyboard = build_user_keyboard(candidate.account.id, settings.admin_contact)
            try:
                msg = await bot.send_message(
                    chat_id=int(candidate.subscriber.chat_id),
                    text=text,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=keyboard,
                )
                log_notification_result(
                    account_id=candidate.account.id,
                    user_id=candidate.user.id,
                    subscriber_id=candidate.subscriber.id,
                    due_on=candidate.due_on,
                    days_left=candidate.days_left,
                    status="delivered",
                    message_id=str(msg.message_id),
                    payload={"chat_id": candidate.subscriber.chat_id},
                )
            except Exception as exc:  # pragma: no cover
                log_notification_result(
                    account_id=candidate.account.id,
                    user_id=candidate.user.id,
                    subscriber_id=candidate.subscriber.id,
                    due_on=candidate.due_on,
                    days_left=candidate.days_left,
                    status="failed",
                    error_text=str(exc),
                )

        for row in unresolved_requests(limit=20):
            age_hours = int((datetime.utcnow() - row.created_at).total_seconds() / 3600) if row.created_at else 0
            if age_hours < settings.pending_admin_reminder_hours:
                continue

            if row.last_admin_reminder_at:
                remind_delta = datetime.utcnow() - row.last_admin_reminder_at
                if remind_delta.total_seconds() < settings.pending_admin_reminder_hours * 3600:
                    continue

            await notify_admins_about_request(bot, cfg.admin_chat_ids, row.id)


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
