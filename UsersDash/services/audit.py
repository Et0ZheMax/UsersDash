"""
Аудит действий на странице настроек.

- log_settings_action: нормализует и сохраняет событие в БД
- settings_audit_context: контекстный менеджер для безопасной записи
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Any, Iterable, Mapping

from flask import current_app, request

from UsersDash.models import Account, SettingsAuditLog, User, db


SENSITIVE_KEYS = {"password", "token", "secret", "api_key", "access_token"}
AUDIT_BUFFER: list[SettingsAuditLog] = []


def _mask_value(value: Any, force_mask: bool = False) -> Any:
    if value is None:
        return None

    if force_mask:
        return "***"

    if isinstance(value, str):
        return value

    if isinstance(value, Mapping):
        return {k: _mask_value(v, force_mask=k.lower() in SENSITIVE_KEYS) for k, v in value.items()}

    if isinstance(value, Iterable) and not isinstance(value, (bytes, bytearray, str)):
        return [_mask_value(v) for v in value]

    return value


def _serialize(value: Any) -> str | None:
    if value is None:
        return None

    masked = _mask_value(value)
    try:
        if isinstance(masked, str):
            return masked
        return json.dumps(masked, ensure_ascii=False, default=str)
    except Exception:
        return str(masked)


def _get_ip_from_request() -> str | None:
    if not request:
        return None
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.remote_addr


def _get_user_agent() -> str | None:
    if not request:
        return None
    ua = request.headers.get("User-Agent")
    if ua and len(ua) > 255:
        return ua[:252] + "..."
    return ua


def _buffer_enabled() -> bool:
    return bool(current_app and current_app.config.get("AUDIT_BUFFER_ENABLED"))


def _flush_buffer_if_needed(force: bool = False):
    threshold = current_app.config.get("AUDIT_BUFFER_THRESHOLD", 20) if current_app else 20
    if not force and len(AUDIT_BUFFER) < threshold:
        return

    if not AUDIT_BUFFER:
        return

    db.session.bulk_save_objects(list(AUDIT_BUFFER))
    AUDIT_BUFFER.clear()
    db.session.commit()


def _maybe_trigger_alert(user_id: int | None, field_name: str | None, action_type: str, event_time: datetime):
    if not current_app:
        return

    window_minutes = current_app.config.get("AUDIT_ALERT_WINDOW_MINUTES", 10)
    threshold = current_app.config.get("AUDIT_ALERT_THRESHOLD", 20)
    critical_fields = set(current_app.config.get("AUDIT_CRITICAL_FIELDS", ["password", "api_token", "token"]))

    if not user_id:
        return

    window_start = event_time - timedelta(minutes=window_minutes)
    q = SettingsAuditLog.query.filter(
        SettingsAuditLog.user_id == user_id,
        SettingsAuditLog.created_at >= window_start,
    )

    if field_name and field_name.lower() in {f.lower() for f in critical_fields}:
        q = q.filter(SettingsAuditLog.field_name == field_name)

    recent_count = q.count()
    if recent_count >= threshold:
        current_app.logger.warning(
            "[audit] threshold exceeded: user=%s field=%s action=%s count=%s",
            user_id,
            field_name,
            action_type,
            recent_count,
        )


def log_settings_action(
    user: User | None,
    actor: User | None,
    action: str,
    context: dict[str, Any] | None = None,
) -> SettingsAuditLog:
    ctx = context or {}
    account: Account | None = ctx.get("account")
    account_id = ctx.get("account_id") or (account.id if account else None)

    log_entry = SettingsAuditLog(
        user_id=user.id if user else None,
        actor_id=actor.id if actor else None,
        account_id=account_id,
        action_type=action,
        field_name=ctx.get("field") or ctx.get("field_name"),
        old_value=_serialize(ctx.get("old_value")),
        new_value=_serialize(ctx.get("new_value")),
        extra_json=_serialize({k: v for k, v in ctx.items() if k not in {"field", "field_name", "old_value", "new_value", "account", "account_id"}}),
        ip_address=ctx.get("ip") or _get_ip_from_request(),
        user_agent=ctx.get("user_agent") or _get_user_agent(),
    )

    if _buffer_enabled():
        AUDIT_BUFFER.append(log_entry)
        _flush_buffer_if_needed()
    else:
        db.session.add(log_entry)
        db.session.commit()

    _maybe_trigger_alert(log_entry.user_id, log_entry.field_name, action, log_entry.created_at)
    return log_entry


@contextmanager
def settings_audit_context(
    user: User | None,
    actor: User | None,
    action: str,
    base_context: dict[str, Any] | None = None,
):
    context: dict[str, Any] = dict(base_context or {})
    try:
        yield context
    finally:
        log_settings_action(user, actor, action, context)


def flush_audit_buffer():
    _flush_buffer_if_needed(force=True)
