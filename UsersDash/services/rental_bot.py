"""Сервисы Telegram-бота продления аренды Viking Rise."""

from __future__ import annotations

import hashlib
import json
import secrets
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Iterable

from flask import current_app
from sqlalchemy import and_, distinct
from sqlalchemy.orm import joinedload

from UsersDash.models import (
    Account,
    FarmData,
    RenewalBatchAdminAction,
    RenewalBatchItem,
    RenewalBatchRequest,
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
BATCH_PENDING_STATUSES = {
    "payment_pending_confirmation",
    "payment_data_collecting",
    "pending_manual_review",
}
BATCH_ACTIVE_STATUSES = {
    "draft",
    "payment_data_collecting",
    "payment_pending_confirmation",
    "pending_manual_review",
}
ADMIN_SINGLE_PENDING_STATUSES = (
    "payment_pending_confirmation",
    "payment_data_collecting",
    "needs_info",
)
ADMIN_MULTI_PENDING_STATUS_CANDIDATES = (
    "submitted",
    "pending",
    "payment_pending_confirmation",
    "payment_data_collecting",
    "pending_manual_review",
    "needs_info",
)


@dataclass(slots=True)
class NotificationCandidate:
    """Кандидат для отправки напоминания по аренде."""

    account: Account
    user: User
    subscriber: TelegramSubscriber
    telegram_tag: str | None
    days_left: int
    due_on: date


class RentalBotError(Exception):
    """Базовая ошибка домена Telegram-бота аренды."""


class TokenValidationError(RentalBotError):
    """Ошибка привязки Telegram-чата по токену."""


class BatchValidationError(RentalBotError):
    """Ошибка операций с batch-заявками."""


def utcnow() -> datetime:
    """Возвращает текущее время в UTC в naive-формате."""

    return datetime.utcnow()


def to_utc_naive(value: datetime | None) -> datetime | None:
    """Нормализует дату ко внутреннему стандарту naive UTC."""

    if value is None:
        return None
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


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
        payment_details_text="Реквизиты уточните у администратора.",
        payment_instruction_text="После оплаты нажмите «Я оплатил(а)» и приложите чек.",
        reminder_days="3,1,0,-1",
        reminders_enabled=True,
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
        profile = TelegramSubscriber.query.filter_by(user_id=token.user_id).first()
        if profile and profile.chat_id == chat_id:
            profile.last_interaction_at = now
            db.session.commit()
            return profile
        raise TokenValidationError("Токен уже использован.")
    token_expires_at = to_utc_naive(token.expires_at)
    if token_expires_at is None or token_expires_at < now:
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
    profile.last_interaction_at = now
    token.consumed_at = now
    db.session.commit()
    return profile


def collect_notification_candidates(reminder_days: Iterable[int]) -> list[NotificationCandidate]:
    """Подбирает активные аккаунты, подходящие под окна уведомлений."""

    days_set = {int(day) for day in reminder_days}
    if not days_set:
        return []

    now = utcnow()
    candidates: list[NotificationCandidate] = []

    query = (
        Account.query.join(User, User.id == Account.owner_id)
        .outerjoin(TelegramSubscriber, TelegramSubscriber.user_id == User.id)
        .outerjoin(FarmData, FarmData.account_id == Account.id)
        .filter(Account.is_active.is_(True))
        .filter(Account.next_payment_at.isnot(None))
    )

    for account, user, subscriber, farm_data in query.with_entities(
        Account,
        User,
        TelegramSubscriber,
        FarmData,
    ).all():
        telegram_tag = (farm_data.telegram_tag or "").strip() if farm_data and farm_data.telegram_tag else None
        if not subscriber or not subscriber.is_active or not subscriber.chat_id:
            app_logger_info(
                f"Пропуск уведомления account_id={account.id}, user_id={user.id}: "
                f"нет активной Telegram-привязки (telegram_tag={telegram_tag or '—'})."
            )
            continue
        due_at = to_utc_naive(account.next_payment_at)
        if due_at is None:
            continue
        due = due_at.date()
        days_left = (due - now.date()).days
        if days_left not in days_set:
            continue
        if has_notification_log(account.id, user.id, due, days_left):
            continue
        candidates.append(
            NotificationCandidate(
                account=account,
                user=user,
                subscriber=subscriber,
                telegram_tag=telegram_tag,
                days_left=days_left,
                due_on=due,
            )
        )

    return candidates


def has_notification_log(account_id: int, user_id: int, due_on: date, days_left: int) -> bool:
    """Проверяет, отправлялось ли напоминание на конкретный этап."""

    stage = notification_stage(days_left)
    return (
        RentalNotificationLog.query.filter_by(account_id=account_id, user_id=user_id, due_on=due_on, stage=stage)
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
    request_type: str = "payment",
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

    previous_paid_until = to_utc_naive(account.next_payment_at)
    now = utcnow()
    base_date = previous_paid_until if previous_paid_until and previous_paid_until > now else now
    settings = get_bot_settings()
    expected_days = max(1, settings.renew_duration_days)

    if account.owner_id != user_id:
        raise RentalBotError("Аккаунт не принадлежит выбранному клиенту.")

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
        request_type=request_type or "payment",
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

    now = utcnow()
    current_paid_until = to_utc_naive(account.next_payment_at)
    base = current_paid_until if current_paid_until and current_paid_until > now else now
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
    request_row.rejected_at = utcnow()
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
        user_id=user_id,
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


def create_notification_batch(
    *,
    user_id: int,
    subscriber_id: int,
    candidates: list[NotificationCandidate],
) -> RenewalBatchRequest:
    """Создаёт draft batch-заявку из набора уведомлённых ферм."""

    if not candidates:
        raise BatchValidationError("Нельзя создать batch без ферм.")

    candidate_ids = {item.account.id for item in candidates}
    active_batch = (
        RenewalBatchRequest.query.filter_by(user_id=user_id, subscriber_id=subscriber_id)
        .filter(RenewalBatchRequest.status.in_(BATCH_ACTIVE_STATUSES))
        .order_by(RenewalBatchRequest.created_at.desc())
        .first()
    )
    if active_batch:
        existing_items = RenewalBatchItem.query.filter_by(batch_request_id=active_batch.id).all()
        existing_ids = {item.account_id for item in existing_items}
        if existing_ids == candidate_ids or active_batch.status in {
            "payment_pending_confirmation",
            "pending_manual_review",
        }:
            return active_batch

        stale_items = [item for item in existing_items if item.account_id not in candidate_ids]
        for item in stale_items:
            db.session.delete(item)

        for candidate in candidates:
            item = RenewalBatchItem.query.filter_by(
                batch_request_id=active_batch.id,
                account_id=candidate.account.id,
            ).first()
            if not item:
                db.session.add(
                    RenewalBatchItem(
                        batch_request_id=active_batch.id,
                        account_id=candidate.account.id,
                        account_name_snapshot=candidate.account.name,
                        amount_rub_snapshot=candidate.account.next_payment_amount,
                        tariff_snapshot=candidate.account.next_payment_tariff,
                        due_at_snapshot=to_utc_naive(candidate.account.next_payment_at),
                        is_active_snapshot=candidate.account.is_active,
                        blocked_snapshot=candidate.account.blocked_for_payment,
                    )
                )
                continue

            item.account_name_snapshot = candidate.account.name
            item.amount_rub_snapshot = candidate.account.next_payment_amount
            item.tariff_snapshot = candidate.account.next_payment_tariff
            item.due_at_snapshot = to_utc_naive(candidate.account.next_payment_at)
            item.is_active_snapshot = candidate.account.is_active
            item.blocked_snapshot = candidate.account.blocked_for_payment

        active_batch.total_amount_rub = sum((item.account.next_payment_amount or 0) for item in candidates)
        db.session.commit()
        return active_batch

    batch = RenewalBatchRequest(
        batch_uid=str(uuid.uuid4()),
        user_id=user_id,
        subscriber_id=subscriber_id,
        status="draft",
        mode=None,
        total_amount_rub=sum((item.account.next_payment_amount or 0) for item in candidates),
    )
    db.session.add(batch)
    db.session.flush()

    for candidate in candidates:
        db.session.add(
            RenewalBatchItem(
                batch_request_id=batch.id,
                account_id=candidate.account.id,
                account_name_snapshot=candidate.account.name,
                amount_rub_snapshot=candidate.account.next_payment_amount,
                tariff_snapshot=candidate.account.next_payment_tariff,
                due_at_snapshot=to_utc_naive(candidate.account.next_payment_at),
                is_active_snapshot=candidate.account.is_active,
                blocked_snapshot=candidate.account.blocked_for_payment,
                selected_for_renewal=False,
                result_status="pending",
            )
        )

    db.session.commit()
    return batch


def get_batch_for_user(batch_id: int, user_id: int) -> RenewalBatchRequest:
    """Возвращает batch-заявку клиента или поднимает доменную ошибку."""

    batch = RenewalBatchRequest.query.get(batch_id)
    if not batch or batch.user_id != user_id:
        raise BatchValidationError("Платёжная сессия не найдена или недоступна.")
    return batch


def mark_batch_mode(batch: RenewalBatchRequest, mode: str) -> RenewalBatchRequest:
    """Фиксирует режим обработки batch-заявки."""

    if mode not in {"full", "partial", "manual_change"}:
        raise BatchValidationError("Некорректный режим batch-заявки.")
    if batch.status not in {"draft", "payment_data_collecting"}:
        raise BatchValidationError("Эта batch-заявка уже закрыта.")

    batch.mode = mode
    batch.status = "payment_data_collecting"
    db.session.commit()
    return batch


def ensure_batch_editable(batch: RenewalBatchRequest) -> None:
    """Проверяет, что batch доступен для клиентского редактирования."""

    if batch.status not in {"draft", "payment_data_collecting"}:
        raise BatchValidationError("Эта batch-заявка уже отправлена и недоступна для редактирования.")


def set_batch_selected_accounts(
    *,
    batch: RenewalBatchRequest,
    selected_account_ids: set[int],
) -> None:
    """Обновляет выбранные фермы внутри batch-заявки."""

    batch_items = RenewalBatchItem.query.filter_by(batch_request_id=batch.id).all()
    allowed_ids = {item.account_id for item in batch_items}
    unknown_ids = selected_account_ids - allowed_ids
    if unknown_ids:
        raise BatchValidationError("В batch переданы недопустимые фермы.")

    for item in batch_items:
        item.selected_for_renewal = item.account_id in selected_account_ids

    db.session.commit()


def submit_batch_request(
    *,
    batch: RenewalBatchRequest,
    amount_rub: int | None,
    payment_method: str | None,
    comment: str | None,
    receipt_file_id: str | None,
) -> RenewalBatchRequest:
    """Переводит batch в режим ожидания подтверждения админом."""

    if batch.status in {"payment_pending_confirmation", "pending_manual_review", "payment_confirmed"}:
        return batch
    if batch.status not in {"draft", "payment_data_collecting"}:
        raise BatchValidationError("Batch уже закрыт и не может быть отправлен повторно.")
    if batch.mode not in {"full", "partial", "manual_change"}:
        raise BatchValidationError("Сначала выберите сценарий оплаты.")

    if batch.mode == "full":
        set_batch_selected_accounts(
            batch=batch,
            selected_account_ids={item.account_id for item in batch.items},
        )

    items = RenewalBatchItem.query.filter_by(batch_request_id=batch.id).all()
    selected_items = [item for item in items if item.selected_for_renewal]
    selected_count = len(selected_items)
    if batch.mode in {"full", "partial"} and selected_count == 0:
        raise BatchValidationError("Выберите хотя бы одну ферму для продления.")

    if batch.mode == "manual_change":
        batch.total_amount_rub = amount_rub
    elif batch.mode == "full":
        batch.total_amount_rub = sum(item.amount_rub_snapshot or 0 for item in items)
    else:
        batch.total_amount_rub = sum(item.amount_rub_snapshot or 0 for item in selected_items)
    batch.payment_method = payment_method
    batch.comment = comment
    batch.receipt_file_id = receipt_file_id
    batch.status = "pending_manual_review" if batch.mode == "manual_change" else "payment_pending_confirmation"
    db.session.commit()
    return batch


def confirm_batch_request(batch_id: int, admin_user_id: int) -> RenewalBatchRequest:
    """Подтверждает batch-заявку и продлевает выбранные фермы."""

    batch = RenewalBatchRequest.query.get(batch_id)
    if not batch:
        raise BatchValidationError("Batch-заявка не найдена.")
    if batch.status == "payment_confirmed":
        return batch
    if batch.status in {"rejected", "cancelled"}:
        raise BatchValidationError("Нельзя подтвердить отклонённую или отменённую batch-заявку.")

    if batch.mode == "manual_change":
        batch.status = "payment_confirmed"
        batch.confirmed_by_user_id = admin_user_id
        batch.confirmed_at = utcnow()
        for item in RenewalBatchItem.query.filter_by(batch_request_id=batch.id).all():
            if item.result_status == "pending":
                item.result_status = "skipped"
        db.session.add(
            RenewalBatchAdminAction(
                batch_request_id=batch.id,
                actor_user_id=admin_user_id,
                action_type="confirm_manual_change",
                details_json=json.dumps({"mode": batch.mode}, ensure_ascii=False),
            )
        )
        db.session.commit()
        return batch

    settings = get_bot_settings()
    now = utcnow()
    expected_days = max(1, settings.renew_duration_days)

    for item in RenewalBatchItem.query.filter_by(batch_request_id=batch.id).all():
        if not item.selected_for_renewal:
            item.result_status = "skipped"
            continue

        account = Account.query.get(item.account_id)
        if not account or account.owner_id != batch.user_id:
            item.result_status = "rejected"
            continue

        current_paid_until = to_utc_naive(account.next_payment_at)
        base = current_paid_until if current_paid_until and current_paid_until > now else now
        account.next_payment_at = base + timedelta(days=expected_days)
        if item.amount_rub_snapshot:
            account.next_payment_amount = item.amount_rub_snapshot
        item.result_status = "confirmed"

    batch.status = "payment_confirmed"
    batch.confirmed_by_user_id = admin_user_id
    batch.confirmed_at = now

    db.session.add(
        RenewalBatchAdminAction(
            batch_request_id=batch.id,
            actor_user_id=admin_user_id,
            action_type="confirm",
            details_json=json.dumps(
                {
                    "mode": batch.mode,
                    "selected_accounts": [
                        item.account_id
                        for item in RenewalBatchItem.query.filter_by(batch_request_id=batch.id).all()
                        if item.selected_for_renewal
                    ],
                },
                ensure_ascii=False,
            ),
        )
    )
    db.session.commit()
    return batch


def reject_batch_request(batch_id: int, admin_user_id: int, reason: str | None) -> RenewalBatchRequest:
    """Отклоняет batch-заявку клиента."""

    batch = RenewalBatchRequest.query.get(batch_id)
    if not batch:
        raise BatchValidationError("Batch-заявка не найдена.")
    if batch.status == "payment_confirmed":
        raise BatchValidationError("Batch уже подтверждён и не может быть отклонён.")

    batch.status = "rejected"
    batch.rejected_by_user_id = admin_user_id
    batch.rejected_at = utcnow()
    batch.rejection_reason = reason or "Причина не указана"

    for item in RenewalBatchItem.query.filter_by(batch_request_id=batch.id).all():
        if item.result_status == "pending":
            item.result_status = "rejected"

    db.session.add(
        RenewalBatchAdminAction(
            batch_request_id=batch.id,
            actor_user_id=admin_user_id,
            action_type="reject",
            details_json=json.dumps({"reason": batch.rejection_reason}, ensure_ascii=False),
        )
    )
    db.session.commit()
    return batch


def unresolved_batch_requests(limit: int = 50) -> list[RenewalBatchRequest]:
    """Возвращает заявки по нескольким фермам, ожидающие действий администратора."""

    overview = get_admin_pending_overview(limit_single=0, limit_multi=limit)
    return overview["multi"]


def get_multi_pending_statuses() -> list[str]:
    """Определяет pending-статусы заявок по нескольким фермам на основе реальных данных."""

    actual_statuses = {
        (row[0] or "").strip()
        for row in db.session.query(distinct(RenewalBatchRequest.status)).all()
        if row and row[0]
    }
    if not actual_statuses:
        return list(BATCH_PENDING_STATUSES)

    selected = [
        status for status in ADMIN_MULTI_PENDING_STATUS_CANDIDATES if status in actual_statuses
    ]
    if selected:
        return selected

    fallback = [status for status in BATCH_PENDING_STATUSES if status in actual_statuses]
    return fallback or list(BATCH_PENDING_STATUSES)


def get_admin_pending_overview(limit_single: int = 50, limit_multi: int = 50) -> dict[str, object]:
    """Возвращает общую очередь подтверждений как для сайта, так и для Telegram-бота."""

    single_query = (
        RenewalRequest.query.options(
            joinedload(RenewalRequest.account),
            joinedload(RenewalRequest.subscriber),
        )
        .filter(RenewalRequest.status.in_(ADMIN_SINGLE_PENDING_STATUSES))
        .outerjoin(Account, Account.id == RenewalRequest.account_id)
        .order_by(
            Account.next_payment_at.is_(None),
            Account.next_payment_at.asc(),
            RenewalRequest.created_at.desc(),
        )
    )
    if limit_single > 0:
        single = single_query.limit(limit_single).all()
    else:
        single = []

    multi_statuses = get_multi_pending_statuses()
    multi_query = (
        RenewalBatchRequest.query.options(
            joinedload(RenewalBatchRequest.subscriber),
            joinedload(RenewalBatchRequest.items).joinedload(RenewalBatchItem.account),
        )
        .filter(RenewalBatchRequest.status.in_(multi_statuses))
        .order_by(RenewalBatchRequest.created_at.desc())
    )
    if limit_multi > 0:
        multi = multi_query.limit(limit_multi).all()
    else:
        multi = []

    single_count = RenewalRequest.query.filter(
        RenewalRequest.status.in_(ADMIN_SINGLE_PENDING_STATUSES)
    ).count()
    multi_count = RenewalBatchRequest.query.filter(
        RenewalBatchRequest.status.in_(multi_statuses)
    ).count()
    return {
        "single": single,
        "multi": multi,
        "counts": {
            "single": single_count,
            "multi": multi_count,
            "total": single_count + multi_count,
            "single_statuses": list(ADMIN_SINGLE_PENDING_STATUSES),
            "multi_statuses": multi_statuses,
        },
    }


def unresolved_requests(limit: int = 50) -> list[RenewalRequest]:
    """Возвращает список ожидающих подтверждения заявок."""

    overview = get_admin_pending_overview(limit_single=limit, limit_multi=0)
    return overview["single"]


def admin_dashboard_snapshot() -> dict[str, int]:
    """Сводные счётчики для страницы настроек Telegram-бота."""

    linked = TelegramSubscriber.query.filter_by(is_active=True).count()
    without_link = (
        User.query.filter_by(role="client")
        .outerjoin(TelegramSubscriber, TelegramSubscriber.user_id == User.id)
        .filter(TelegramSubscriber.id.is_(None))
        .count()
    )
    overview = get_admin_pending_overview(limit_single=0, limit_multi=0)
    pending = int(overview["counts"]["total"])

    return {
        "linked_clients": linked,
        "unlinked_clients": without_link,
        "pending_requests": pending,
    }


def app_logger_info(message: str) -> None:
    """Унифицированный логгер для сервиса."""

    current_app.logger.info("[rental-bot] %s", message)
