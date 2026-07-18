"""Сбор и чтение централизованных логов ферм."""

from __future__ import annotations

import hashlib
from datetime import date, datetime
from typing import Any

from sqlalchemy.orm import joinedload

from UsersDash.models import Account, FarmLogEntry, db
from UsersDash.services.remote_api import fetch_account_logs_view


def _parse_event_time(value: Any) -> datetime | None:
    """Преобразует время из API логов в naive UTC/локальный datetime для SQLite-фильтров."""

    raw = str(value or "").strip()
    if not raw:
        return None

    normalized = raw.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None

    if parsed.tzinfo is not None:
        parsed = parsed.astimezone().replace(tzinfo=None)
    return parsed


def _build_event_hash(account_id: int, remote_acc_id: str, item: dict[str, Any]) -> str:
    """Создаёт стабильный ключ, чтобы повторный сбор не дублировал одинаковые строки."""

    parts = [
        str(account_id),
        remote_acc_id,
        str(item.get("time") or ""),
        str(item.get("group") or ""),
        str(item.get("event_text") or item.get("raw_text") or ""),
    ]
    return hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()


def save_account_logs(account: Account, payload: dict[str, Any]) -> int:
    """Сохраняет новые строки логов одной фермы и возвращает количество добавленных записей."""

    items = payload.get("items") if isinstance(payload.get("items"), list) else []
    remote_acc_id = str(payload.get("acc_id") or account.internal_id or account.name or "").strip()
    added = 0

    for item in items:
        if not isinstance(item, dict):
            continue

        event_text = str(item.get("event_text") or item.get("raw_text") or "").strip()
        if not event_text:
            continue

        event_hash = _build_event_hash(account.id, remote_acc_id, item)
        if FarmLogEntry.query.filter_by(event_hash=event_hash).first():
            continue

        event_time = _parse_event_time(item.get("time"))
        db.session.add(FarmLogEntry(
            account_id=account.id,
            server_id=account.server_id,
            owner_id=account.owner_id,
            remote_acc_id=remote_acc_id or None,
            event_time=event_time,
            event_date=event_time.date() if event_time else None,
            group=str(item.get("group") or "system")[:32],
            group_label=str(item.get("group_label") or "Система")[:64],
            event_text=event_text,
            raw_text=str(item.get("raw_text") or "").strip() or None,
            event_hash=event_hash,
        ))
        added += 1

    if added:
        db.session.commit()
    return added


def sync_account_logs(account: Account, *, limit: int = 300, include_debug: bool = False) -> tuple[int, str]:
    """Загружает логи фермы с RSS-сервера и сохраняет новые строки в общей БД."""

    payload, error = fetch_account_logs_view(account, limit=limit, include_debug=include_debug)
    if error:
        db.session.rollback()
        return 0, error
    return save_account_logs(account, payload or {}), ""


def query_logs(*, account_id: int | None, server_id: int | None, day: date | None, limit: int = 500):
    """Возвращает сохранённые логи с фильтрами по ферме, серверу и дате."""

    query = FarmLogEntry.query.options(
        joinedload(FarmLogEntry.account),
        joinedload(FarmLogEntry.server),
        joinedload(FarmLogEntry.owner),
    )
    if account_id:
        query = query.filter(FarmLogEntry.account_id == account_id)
    if server_id:
        query = query.filter(FarmLogEntry.server_id == server_id)
    if day:
        query = query.filter(FarmLogEntry.event_date == day)

    return (
        query
        .order_by(FarmLogEntry.event_time.desc().nullslast(), FarmLogEntry.collected_at.desc())
        .limit(limit)
        .all()
    )
