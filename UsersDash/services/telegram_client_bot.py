"""Сервис для Telegram-бота, который обслуживает базу клиентов."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable, Mapping

import requests
from flask import current_app

from UsersDash.models import Account, ActionLog, TelegramSubscriber, User, db
from UsersDash.services.info_message import get_global_info_message_text
from UsersDash.telegram_settings import load_telegram_settings

DEFAULT_TIMEOUT = 15


@dataclass(frozen=True)
class TelegramBotConfig:
    """Настройки Telegram-бота."""

    token: str
    admin_chat_ids: set[str]
    bind_code: str
    reminder_days: int
    reminder_hour: int
    poll_timeout: int = 20


@dataclass(frozen=True)
class TelegramUpdate:
    """Упрощённая обёртка над входящим сообщением Telegram."""

    update_id: int
    chat_id: str
    username: str
    first_name: str
    last_name: str
    text: str


def load_bot_config(config: Mapping[str, object] | None = None) -> TelegramBotConfig:
    """Готовит настройки бота из Flask-конфига и env."""

    config = config or {}
    token = str(config.get("TELEGRAM_BOT_TOKEN") or "").strip()
    if not token:
        token, _ = load_telegram_settings()
    if not token:
        raise RuntimeError("Не задан TELEGRAM_BOT_TOKEN для Telegram-бота.")

    admin_chat_ids = _parse_chat_ids(config.get("TELEGRAM_ADMIN_CHAT_IDS"))
    bind_code = str(config.get("TELEGRAM_BIND_CODE") or "").strip()
    reminder_days = _safe_int(config.get("TELEGRAM_REMINDER_DAYS"), default=3)
    reminder_hour = _safe_int(config.get("TELEGRAM_REMINDER_HOUR"), default=10)

    return TelegramBotConfig(
        token=token,
        admin_chat_ids=admin_chat_ids,
        bind_code=bind_code,
        reminder_days=reminder_days,
        reminder_hour=reminder_hour,
    )


def _parse_chat_ids(raw_value: object) -> set[str]:
    if isinstance(raw_value, str):
        return {cid.strip() for cid in raw_value.split(",") if cid.strip()}
    if isinstance(raw_value, (list, tuple)):
        return {str(cid).strip() for cid in raw_value if str(cid).strip()}
    return set()


def _safe_int(raw_value: object, default: int) -> int:
    try:
        return int(raw_value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def fetch_updates(token: str, offset: int | None, timeout: int) -> list[TelegramUpdate]:
    """Получает апдейты через long polling."""

    params = {"timeout": timeout}
    if offset is not None:
        params["offset"] = offset

    response = requests.get(
        f"https://api.telegram.org/bot{token}/getUpdates",
        params=params,
        timeout=timeout + 5,
    )
    response.raise_for_status()
    payload = response.json()
    if not payload.get("ok"):
        return []

    updates: list[TelegramUpdate] = []
    for item in payload.get("result", []):
        message = item.get("message") or item.get("edited_message")
        if not message:
            continue
        text = (message.get("text") or "").strip()
        if not text:
            continue
        chat = message.get("chat") or {}
        user = message.get("from") or {}
        updates.append(
            TelegramUpdate(
                update_id=int(item.get("update_id")),
                chat_id=str(chat.get("id")),
                username=str(user.get("username") or "").strip(),
                first_name=str(user.get("first_name") or "").strip(),
                last_name=str(user.get("last_name") or "").strip(),
                text=text,
            )
        )

    return updates


def send_message(token: str, chat_id: str, text: str) -> bool:
    """Отправляет сообщение в Telegram."""

    if not text:
        return False

    response = requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        data={"chat_id": chat_id, "text": text},
        timeout=DEFAULT_TIMEOUT,
    )
    if not response.ok:
        _log_bot(
            f"Не удалось отправить сообщение в Telegram (chat_id={chat_id}): "
            f"{response.status_code} {response.text}"
        )
        return False
    return True


def normalize_command(text: str) -> tuple[str, list[str]]:
    """Парсит команду вида /cmd arg1 arg2."""

    parts = text.strip().split()
    if not parts:
        return "", []
    command = parts[0].split("@", 1)[0].lower()
    return command, parts[1:]


def build_help_text(is_admin: bool) -> str:
    """Формирует справку по доступным командам."""

    lines = [
        "Доступные команды:",
        "/start — краткая справка",
        "/bind <логин> <код> — привязка к аккаунту UsersDash",
        "/my — мои фермы и ближайшие оплаты",
        "/info — последнее информационное сообщение",
        "/unsubscribe — отключить рассылки",
        "/subscribe — включить рассылки",
        "/settimezone <таймзона> — указать часовой пояс (например, Europe/Moscow)",
    ]

    if is_admin:
        lines.extend(
            [
                "",
                "Админ-команды:",
                "/payments [дней] — список оплат за период",
                "/remind — отправить напоминания сейчас",
                "/broadcast <текст> — рассылка всем клиентам",
                "/client <логин> — карточка клиента",
            ]
        )

    return "\n".join(lines)


def is_admin(chat_id: str, config: TelegramBotConfig) -> bool:
    return chat_id in config.admin_chat_ids


def handle_update(app, config: TelegramBotConfig, update: TelegramUpdate) -> None:
    """Обрабатывает входящее сообщение."""

    command, args = normalize_command(update.text)
    if not command.startswith("/"):
        return

    with app.app_context():
        subscriber = TelegramSubscriber.query.filter_by(chat_id=update.chat_id).first()
        _touch_subscriber(subscriber)

        if command in {"/start", "/help"}:
            send_message(config.token, update.chat_id, build_help_text(is_admin(update.chat_id, config)))
            return

        if command == "/bind":
            _handle_bind(config, update, args)
            return

        if command == "/my":
            _handle_my_accounts(config, update, subscriber)
            return

        if command == "/info":
            _handle_info_message(config, update)
            return

        if command == "/unsubscribe":
            _set_broadcast_preference(config, update, subscriber, allow=False)
            return

        if command == "/subscribe":
            _set_broadcast_preference(config, update, subscriber, allow=True)
            return

        if command == "/settimezone":
            _handle_set_timezone(config, update, subscriber, args)
            return

        if command == "/payments":
            _handle_payments_admin(config, update, args)
            return

        if command == "/remind":
            _handle_remind_admin(config, update)
            return

        if command == "/broadcast":
            _handle_broadcast_admin(config, update, args)
            return

        if command == "/client":
            _handle_client_admin(config, update, args)
            return

        send_message(config.token, update.chat_id, "Команда не распознана. Используйте /help.")


def _touch_subscriber(subscriber: TelegramSubscriber | None) -> None:
    if not subscriber:
        return
    subscriber.last_interaction_at = datetime.utcnow()
    db.session.commit()


def _handle_bind(config: TelegramBotConfig, update: TelegramUpdate, args: list[str]) -> None:
    if len(args) < 1:
        send_message(
            config.token,
            update.chat_id,
            "Укажите логин: /bind <логин> <код>.",
        )
        return

    if config.bind_code:
        if len(args) < 2 or args[1] != config.bind_code:
            send_message(
                config.token,
                update.chat_id,
                "Неверный код привязки. Обратитесь к администратору.",
            )
            return

    username = args[0]
    user = User.query.filter_by(username=username).first()
    if not user:
        send_message(config.token, update.chat_id, "Клиент не найден. Проверьте логин.")
        return

    existing_by_chat = TelegramSubscriber.query.filter_by(chat_id=update.chat_id).first()
    if existing_by_chat and existing_by_chat.user_id != user.id:
        send_message(
            config.token,
            update.chat_id,
            "Этот чат уже привязан к другому клиенту. Свяжитесь с администратором.",
        )
        return

    existing_by_user = TelegramSubscriber.query.filter_by(user_id=user.id).first()
    subscriber = existing_by_chat or existing_by_user

    if not subscriber:
        subscriber = TelegramSubscriber(user_id=user.id, chat_id=update.chat_id)
        db.session.add(subscriber)
    else:
        subscriber.user_id = user.id
        subscriber.chat_id = update.chat_id

    subscriber.username = update.username or subscriber.username
    subscriber.first_name = update.first_name or subscriber.first_name
    subscriber.last_name = update.last_name or subscriber.last_name
    subscriber.is_active = True
    subscriber.last_interaction_at = datetime.utcnow()
    db.session.commit()

    send_message(
        config.token,
        update.chat_id,
        "Привязка выполнена. Теперь вы будете получать уведомления.",
    )


def _require_binding(config: TelegramBotConfig, update: TelegramUpdate, subscriber: TelegramSubscriber | None) -> bool:
    if subscriber and subscriber.is_active:
        return True
    send_message(
        config.token,
        update.chat_id,
        "Чат не привязан. Используйте /bind <логин> <код> для привязки.",
    )
    return False


def _handle_my_accounts(
    config: TelegramBotConfig,
    update: TelegramUpdate,
    subscriber: TelegramSubscriber | None,
) -> None:
    if not _require_binding(config, update, subscriber):
        return

    user = User.query.get(subscriber.user_id)
    if not user:
        send_message(config.token, update.chat_id, "Клиент не найден в базе.")
        return

    message = _build_accounts_summary(user)
    send_message(config.token, update.chat_id, message)


def _handle_info_message(config: TelegramBotConfig, update: TelegramUpdate) -> None:
    text = get_global_info_message_text()
    if not text:
        send_message(config.token, update.chat_id, "Пока нет актуального сообщения.")
        return
    send_message(config.token, update.chat_id, text)


def _set_broadcast_preference(
    config: TelegramBotConfig,
    update: TelegramUpdate,
    subscriber: TelegramSubscriber | None,
    allow: bool,
) -> None:
    if not _require_binding(config, update, subscriber):
        return

    subscriber.allow_broadcasts = allow
    subscriber.updated_at = datetime.utcnow()
    db.session.commit()
    status = "включены" if allow else "отключены"
    send_message(config.token, update.chat_id, f"Рассылки {status}.")


def _handle_set_timezone(
    config: TelegramBotConfig,
    update: TelegramUpdate,
    subscriber: TelegramSubscriber | None,
    args: list[str],
) -> None:
    if not _require_binding(config, update, subscriber):
        return

    if not args:
        send_message(config.token, update.chat_id, "Укажите таймзону, например Europe/Moscow.")
        return

    timezone = args[0]
    subscriber.timezone = timezone
    subscriber.updated_at = datetime.utcnow()
    db.session.commit()
    send_message(config.token, update.chat_id, f"Таймзона сохранена: {timezone}.")


def _handle_payments_admin(config: TelegramBotConfig, update: TelegramUpdate, args: list[str]) -> None:
    if not is_admin(update.chat_id, config):
        send_message(config.token, update.chat_id, "Команда доступна только администратору.")
        return

    days = _safe_int(args[0], config.reminder_days) if args else config.reminder_days
    message = _build_payments_overview(days)
    send_message(config.token, update.chat_id, message)


def _handle_remind_admin(config: TelegramBotConfig, update: TelegramUpdate) -> None:
    if not is_admin(update.chat_id, config):
        send_message(config.token, update.chat_id, "Команда доступна только администратору.")
        return

    count = send_payment_reminders(config, datetime.utcnow())
    send_message(config.token, update.chat_id, f"Отправлено напоминаний: {count}.")


def _handle_broadcast_admin(
    config: TelegramBotConfig,
    update: TelegramUpdate,
    args: list[str],
) -> None:
    if not is_admin(update.chat_id, config):
        send_message(config.token, update.chat_id, "Команда доступна только администратору.")
        return

    text = " ".join(args).strip()
    if not text:
        send_message(config.token, update.chat_id, "Укажите текст рассылки.")
        return

    count = send_broadcast(config, text)
    send_message(config.token, update.chat_id, f"Рассылка отправлена: {count} сообщений.")


def _handle_client_admin(config: TelegramBotConfig, update: TelegramUpdate, args: list[str]) -> None:
    if not is_admin(update.chat_id, config):
        send_message(config.token, update.chat_id, "Команда доступна только администратору.")
        return

    if not args:
        send_message(config.token, update.chat_id, "Укажите логин клиента: /client <логин>.")
        return

    user = User.query.filter_by(username=args[0]).first()
    if not user:
        send_message(config.token, update.chat_id, "Клиент не найден.")
        return

    message = _build_accounts_summary(user, admin_view=True)
    send_message(config.token, update.chat_id, message)


def _build_accounts_summary(user: User, admin_view: bool = False) -> str:
    accounts = user.accounts.order_by(Account.name.asc()).all()
    if not accounts:
        return "У клиента нет активных ферм."

    header = f"Клиент: {user.username}" if admin_view else "Ваши фермы:"
    lines = [header]
    for account in accounts:
        payment_line = _format_payment_line(account)
        blocked = " (блокировка за оплату)" if account.blocked_for_payment else ""
        lines.append(f"- {account.name}{blocked}: {payment_line}")
    return "\n".join(lines)


def _build_payments_overview(days: int) -> str:
    now = datetime.utcnow()
    deadline = now + timedelta(days=days)

    accounts = (
        Account.query.filter(Account.next_payment_at.isnot(None))
        .filter(Account.next_payment_at <= deadline)
        .order_by(Account.next_payment_at.asc())
        .all()
    )

    if not accounts:
        return "Нет ближайших оплат."

    lines = [f"Оплаты в ближайшие {days} дн.:"]
    for account in accounts:
        owner = account.owner.username if account.owner else "-"
        payment = _format_payment_line(account)
        lines.append(f"- {owner} / {account.name}: {payment}")
    return "\n".join(lines)


def _format_payment_line(account: Account) -> str:
    if not account.next_payment_at:
        return "дата оплаты не задана"
    date_value = account.next_payment_at.strftime("%d.%m.%Y")
    amount_part = f"{account.next_payment_amount} ₽" if account.next_payment_amount else "сумма не задана"
    tariff_part = f", тариф {account.next_payment_tariff}" if account.next_payment_tariff else ""
    return f"{date_value}, {amount_part}{tariff_part}"


def _log_action(user_id: int, account_id: int | None, action_type: str, payload: str) -> None:
    entry = ActionLog(
        user_id=user_id,
        account_id=account_id,
        action_type=action_type,
        payload_json=payload,
        created_at=datetime.utcnow(),
    )
    db.session.add(entry)
    db.session.commit()


def _log_bot(message: str) -> None:
    if current_app:
        current_app.logger.info("[telegram_bot] %s", message)
    else:
        print(f"[telegram_bot] {message}")


def send_payment_reminders(config: TelegramBotConfig, now: datetime) -> int:
    """Отправляет напоминания по оплатам и возвращает количество сообщений."""

    deadline = now + timedelta(days=config.reminder_days)
    accounts = (
        Account.query.filter(Account.next_payment_at.isnot(None))
        .filter(Account.next_payment_at <= deadline)
        .filter(Account.is_active.is_(True))
        .order_by(Account.next_payment_at.asc())
        .all()
    )
    if not accounts:
        return 0

    account_ids = [account.id for account in accounts]
    already_sent = _collect_sent_reminders(account_ids, now)

    grouped: dict[int, list[Account]] = {}
    for account in accounts:
        if account.id in already_sent:
            continue
        grouped.setdefault(account.owner_id, []).append(account)

    if not grouped:
        return 0

    subscribers = (
        TelegramSubscriber.query.filter(TelegramSubscriber.user_id.in_(grouped.keys()))
        .filter(TelegramSubscriber.is_active.is_(True))
        .all()
    )

    sent_count = 0
    for subscriber in subscribers:
        user_accounts = grouped.get(subscriber.user_id, [])
        if not user_accounts:
            continue
        message = _build_payment_reminder_message(user_accounts)
        if send_message(config.token, subscriber.chat_id, message):
            sent_count += 1
            for account in user_accounts:
                _log_action(
                    user_id=account.owner_id,
                    account_id=account.id,
                    action_type="telegram_payment_reminder",
                    payload=f"reminder_date={now.date().isoformat()}",
                )

    return sent_count


def _collect_sent_reminders(account_ids: Iterable[int], now: datetime) -> set[int]:
    start_day = datetime(now.year, now.month, now.day)
    entries = (
        ActionLog.query.filter(ActionLog.account_id.in_(account_ids))
        .filter(ActionLog.action_type == "telegram_payment_reminder")
        .filter(ActionLog.created_at >= start_day)
        .all()
    )
    return {entry.account_id for entry in entries if entry.account_id}


def _build_payment_reminder_message(accounts: list[Account]) -> str:
    lines = ["Напоминание об оплате:"]
    for account in accounts:
        lines.append(f"- {account.name}: {_format_payment_line(account)}")
    lines.append("Если оплатили, сообщите администратору.")
    return "\n".join(lines)


def send_broadcast(config: TelegramBotConfig, text: str) -> int:
    """Отправляет информационное сообщение всем подписчикам."""

    subscribers = (
        TelegramSubscriber.query.filter(TelegramSubscriber.is_active.is_(True))
        .filter(TelegramSubscriber.allow_broadcasts.is_(True))
        .all()
    )

    sent_count = 0
    for subscriber in subscribers:
        if send_message(config.token, subscriber.chat_id, text):
            sent_count += 1
            _log_action(
                user_id=subscriber.user_id,
                account_id=None,
                action_type="telegram_broadcast",
                payload=text[:250],
            )
    return sent_count
