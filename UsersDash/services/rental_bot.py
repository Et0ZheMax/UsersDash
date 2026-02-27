"""Сервисы Telegram-бота продления аренды Viking Rise."""

from __future__ import annotations

import hashlib
import json
import secrets
import uuid
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Iterable

from flask import current_app
from sqlalchemy import and_

from UsersDash.models import (
    Account,
    RenewalAdminAction,
    RenewalRequest,
    RentalNotificationLog,
    TelegramBotSettings,
    TelegramLinkToken,
    TelegramSubscriber,
    User,
    db,
)


PENDING_STATUSES = {"payment_pending_confirmation", "payment_data_collecting"}


@dataclass(slots=True)
class NotificationCandidate:
    """Кандидат для отправки напоминания по аренде."""

    account: Account
    user: User
    subscriber: TelegramSubscriber
    days_left: int
    due_on: date


class RentalBotError(Exception):
    """Базовая ошибка домена Telegram-бота аренды."""


class TokenValidationError(RentalBotError):
    """Ошибка привязки Telegram-чата по токену."""


def utcnow() -> datetime:
    """Возвращает текущее время в UTC с timezone."""

    return datetime.now(tz=UTC)


def default_template(days_left: int) -> str:
    """Возвращает дефолтный шаблон уведомления для количества дней до оплаты."""

    if days_left == 3:
        return (
            "⏳ До окончания аренды *{account_name}* осталось 3 дня (до {due_date}).\n"
            "Чтобы бот не остановился, продлите аренду заранее."
        )
    if days_left == 1:
        return (
            "⚠️ Напоминаем: аренда *{account_name}* заканчивается завтра ({due_date}).\n"
            "Продлите сейчас, чтобы избежать простоя."
        )
    if days_left <= 0:
        return (
            "🚨 Срок аренды *{account_name}* истёк {due_date}.\n"
            "Оплатите продление, чтобы вернуть сервис в активный режим."
        )
    return (
        "ℹ️ До окончания аренды *{account_name}* осталось {days_left} дн. (до {due_date})."
    )


def get_bot_settings() -> TelegramBotSettings:
    """Возвращает singleton-настройку Telegram-бота."""

    settings = TelegramBotSettings.query.filter_by(singleton_key="default").first()
    if settings:
        return settings

    settings = TelegramBotSettings(
        singleton_key="default",
        renewal_price_rub=0,
        renew_duration_days=30,
        payment_instructions="Уточните реквизиты у администратора.",
        template_reminder_3d=default_template(3),
        template_reminder_1d=default_template(1),
        template_reminder_0d=default_template(0),
        template_expired=default_template(-1),
    )
    db.session.add(settings)
    db.session.commit()
    return settings


def generate_link_token(user_id: int, created_by_user_id: int | None, ttl_hours: int = 24) -> str:
    """Создаёт одноразовый токен для deep-link привязки Telegram."""

    raw = secrets.token_urlsafe(24)
    token_hash = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    token = TelegramLinkToken(
        user_id=user_id,
        token_hash=token_hash,
        expires_at=utcnow() + timedelta(hours=ttl_hours),
        created_by_user_id=created_by_user_id,
    )
    db.session.add(token)
    db.session.commit()
    return raw


def bind_telegram_chat(
    *,
    raw_token: str,
    chat_id: str,
    username: str | None,
    first_name: str | None,
    last_name: str | None,
) -> TelegramSubscriber:
    """Привязывает Telegram чат к клиенту по одноразовому токену."""

    token_hash = hashlib.sha256(raw_token.encode("utf-8")).hexdigest()
    token = TelegramLinkToken.query.filter_by(token_hash=token_hash).first()
    if not token:
        raise TokenValidationError("Токен привязки не найден.")

    now = utcnow()
    if token.consumed_at:
        raise TokenValidationError("Токен уже использован.")
    if token.expires_at < now:
        raise TokenValidationError("Срок действия токена истёк.")

    another_owner = TelegramSubscriber.query.filter(
        and_(TelegramSubscriber.chat_id == chat_id, TelegramSubscriber.user_id != token.user_id)
    ).first()
    if another_owner:
        raise TokenValidationError("Этот чат уже привязан к другому клиенту.")

    profile = TelegramSubscriber.query.filter_by(user_id=token.user_id).first()
    if not profile:
        profile = TelegramSubscriber(user_id=token.user_id, chat_id=chat_id)
        db.session.add(profile)

    profile.chat_id = chat_id
    profile.username = (username or "").strip() or None
    profile.first_name = (first_name or "").strip() or None
    profile.last_name = (last_name or "").strip() or None
    profile.is_active = True
    profile.last_interaction_at = datetime.utcnow()
    token.consumed_at = now
    db.session.commit()
    return profile


def collect_notification_candidates(reminder_days: Iterable[int]) -> list[NotificationCandidate]:
    """Подбирает активные аккаунты, подходящие под окна уведомлений."""

    days_set = {int(day) for day in reminder_days}
    if not days_set:
        return []

    now = datetime.utcnow()
    candidates: list[NotificationCandidate] = []

    query = (
        Account.query.join(User, User.id == Account.owner_id)
        .outerjoin(TelegramSubscriber, TelegramSubscriber.user_id == User.id)
        .filter(Account.is_active.is_(True))
        .filter(Account.next_payment_at.isnot(None))
    )

    for account, user, subscriber in query.with_entities(Account, User, TelegramSubscriber).all():
        if not subscriber or not subscriber.is_active:
            continue
        due = account.next_payment_at.date()
        days_left = (due - now.date()).days
        if days_left not in days_set:
            continue
        if has_notification_log(account.id, due, days_left):
            continue
        candidates.append(
            NotificationCandidate(
                account=account,
                user=user,
                subscriber=subscriber,
                days_left=days_left,
                due_on=due,
            )
        )

    return candidates


def has_notification_log(account_id: int, due_on: date, days_left: int) -> bool:
    """Проверяет, отправлялось ли напоминание на конкретный этап."""

    stage = notification_stage(days_left)
    return (
        RentalNotificationLog.query.filter_by(account_id=account_id, due_on=due_on, stage=stage)
        .filter(RentalNotificationLog.status.in_(["sent", "delivered"]))
        .first()
        is not None
    )


def notification_stage(days_left: int) -> str:
    """Маппинг количества дней до оплаты в код этапа уведомления."""

    if days_left <= -1:
        return "expired_plus"
    if days_left == 0:
        return "on_expiry"
    return f"before_{days_left}d"


def render_reminder_text(settings: TelegramBotSettings, account: Account, days_left: int) -> str:
    """Собирает текст уведомления с учётом настраиваемых шаблонов."""

    mapping = {
        3: settings.template_reminder_3d,
        1: settings.template_reminder_1d,
        0: settings.template_reminder_0d,
    }
    if days_left < 0:
        template = settings.template_expired or default_template(days_left)
    else:
        template = mapping.get(days_left) or default_template(days_left)

    return template.format(
        account_name=account.name,
        due_date=account.next_payment_at.strftime("%d.%m.%Y") if account.next_payment_at else "—",
        days_left=days_left,
        amount=account.next_payment_amount or settings.renewal_price_rub,
    )


def create_renewal_request(
    *,
    user_id: int,
    account_id: int,
    subscriber_id: int | None,
    amount_rub: int | None,
    payment_method: str | None,
    comment: str | None,
    receipt_file_id: str | None,
) -> RenewalRequest:
    """Создаёт заявку на подтверждение оплаты и защищает от дублей."""

    existing = (
        RenewalRequest.query.filter_by(user_id=user_id, account_id=account_id)
        .filter(RenewalRequest.status.in_(PENDING_STATUSES))
        .order_by(RenewalRequest.created_at.desc())
        .first()
    )
    if existing:
        return existing

    account = Account.query.get(account_id)
    if not account:
        raise RentalBotError("Аккаунт не найден.")

    previous_paid_until = account.next_payment_at
    base_date = previous_paid_until if previous_paid_until and previous_paid_until > datetime.utcnow() else datetime.utcnow()
    settings = get_bot_settings()
    expected_days = max(1, settings.renew_duration_days)

    request_row = RenewalRequest(
        request_uid=str(uuid.uuid4()),
        user_id=user_id,
        account_id=account_id,
        subscriber_id=subscriber_id,
        amount_rub=amount_rub,
        payment_method=payment_method,
        comment=comment,
        receipt_file_id=receipt_file_id,
        status="payment_pending_confirmation",
        expected_days=expected_days,
        previous_paid_until=previous_paid_until,
        requested_paid_until=base_date + timedelta(days=expected_days),
    )
    db.session.add(request_row)
    db.session.commit()
    return request_row


def confirm_renewal_request(request_id: int, admin_user_id: int) -> RenewalRequest:
    """Подтверждает заявку и продлевает аренду идемпотентно."""

    request_row = RenewalRequest.query.get(request_id)
    if not request_row:
        raise RentalBotError("Заявка не найдена.")

    if request_row.status == "payment_confirmed":
        return request_row
    if request_row.status in {"rejected", "cancelled"}:
        raise RentalBotError("Нельзя подтвердить отклонённую или отменённую заявку.")

    account = Account.query.get(request_row.account_id)
    if not account:
        raise RentalBotError("Аккаунт заявки не найден.")

    now = datetime.utcnow()
    base = account.next_payment_at if account.next_payment_at and account.next_payment_at > now else now
    account.next_payment_at = base + timedelta(days=request_row.expected_days)
    if request_row.amount_rub:
        account.next_payment_amount = request_row.amount_rub

    request_row.status = "payment_confirmed"
    request_row.confirmed_by_user_id = admin_user_id
    request_row.confirmed_at = now
    request_row.confirmed_paid_until = account.next_payment_at

    action = RenewalAdminAction(
        renewal_request_id=request_row.id,
        actor_user_id=admin_user_id,
        action_type="confirm",
        details_json=json.dumps(
            {
                "new_paid_until": account.next_payment_at.isoformat() if account.next_payment_at else None,
                "amount_rub": request_row.amount_rub,
            },
            ensure_ascii=False,
        ),
    )
    db.session.add(action)
    db.session.commit()
    return request_row


def reject_renewal_request(request_id: int, admin_user_id: int, reason: str | None) -> RenewalRequest:
    """Отклоняет заявку клиента на продление."""

    request_row = RenewalRequest.query.get(request_id)
    if not request_row:
        raise RentalBotError("Заявка не найдена.")
    if request_row.status == "payment_confirmed":
        raise RentalBotError("Оплата уже подтверждена и не может быть отклонена.")

    request_row.status = "rejected"
    request_row.rejected_by_user_id = admin_user_id
    request_row.rejected_at = datetime.utcnow()
    request_row.rejection_reason = reason or "Причина не указана"

    db.session.add(
        RenewalAdminAction(
            renewal_request_id=request_row.id,
            actor_user_id=admin_user_id,
            action_type="reject",
            details_json=json.dumps({"reason": request_row.rejection_reason}, ensure_ascii=False),
        )
    )
    db.session.commit()
    return request_row


def log_notification_result(
    *,
    account_id: int,
    user_id: int,
    subscriber_id: int | None,
    due_on: date,
    days_left: int,
    status: str,
    message_id: str | None = None,
    error_text: str | None = None,
    payload: dict | None = None,
) -> None:
    """Пишет запись в журнал уведомлений с идемпотентным upsert по ключу этапа."""

    stage = notification_stage(days_left)
    record = RentalNotificationLog.query.filter_by(
        account_id=account_id,
        stage=stage,
        due_on=due_on,
    ).first()

    if not record:
        record = RentalNotificationLog(
            account_id=account_id,
            user_id=user_id,
            subscriber_id=subscriber_id,
            stage=stage,
            due_on=due_on,
        )
        db.session.add(record)

    record.user_id = user_id
    record.subscriber_id = subscriber_id
    record.status = status
    record.message_id = message_id
    record.error_text = error_text
    record.payload_json = json.dumps(payload or {}, ensure_ascii=False)
    db.session.commit()


def unresolved_requests(limit: int = 50) -> list[RenewalRequest]:
    """Возвращает список ожидающих подтверждения заявок."""

    return (
        RenewalRequest.query.filter(RenewalRequest.status.in_(PENDING_STATUSES))
        .order_by(RenewalRequest.created_at.asc())
        .limit(limit)
        .all()
    )


def admin_dashboard_snapshot() -> dict[str, int]:
    """Сводные счётчики для страницы настроек Telegram-бота."""

    linked = TelegramSubscriber.query.filter_by(is_active=True).count()
    without_link = (
        User.query.filter_by(role="client")
        .outerjoin(TelegramSubscriber, TelegramSubscriber.user_id == User.id)
        .filter(TelegramSubscriber.id.is_(None))
        .count()
    )
    pending = RenewalRequest.query.filter(RenewalRequest.status.in_(PENDING_STATUSES)).count()

    return {
        "linked_clients": linked,
        "unlinked_clients": without_link,
        "pending_requests": pending,
    }


def app_logger_info(message: str) -> None:
    """Унифицированный логгер для сервиса."""

    current_app.logger.info("[rental-bot] %s", message)
