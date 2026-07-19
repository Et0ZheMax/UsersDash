"""Сохранение, дедупликация и чтение централизованных логов ферм."""

from __future__ import annotations

import hashlib
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable

from sqlalchemy import and_, or_
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import joinedload

from UsersDash.models import Account, FarmLogEntry, Server, db


def _utcnow() -> datetime:
    """Возвращает UTC как naive datetime для совместимости с текущей SQLite-схемой."""

    return datetime.now(timezone.utc).replace(tzinfo=None)


def query_farm_log_filter_servers() -> list[Server]:
    """Возвращает только активные серверы для фильтра логов."""

    return Server.query.filter(Server.is_active.is_(True)).order_by(Server.name.asc()).all()


def query_farm_log_filter_accounts(server_id: int | None = None) -> list[Account]:
    """Возвращает оплаченные активные фермы, при необходимости — одного сервера."""

    query = (
        Account.query.options(joinedload(Account.server))
        .join(Account.server)
        .filter(
            Account.is_active.is_(True),
            Account.blocked_for_payment.is_(False),
            Server.is_active.is_(True),
        )
    )
    if server_id:
        query = query.filter(Account.server_id == server_id)
    return query.order_by(Account.name.asc()).all()


def _format_timezone_offset(value: datetime) -> str | None:
    """Преобразует UTC offset datetime в строку `+03:00`."""

    offset = value.utcoffset()
    if offset is None:
        return None
    total_minutes = int(offset.total_seconds() // 60)
    sign = "+" if total_minutes >= 0 else "-"
    hours, minutes = divmod(abs(total_minutes), 60)
    return f"{sign}{hours:02d}:{minutes:02d}"


def _parse_event_timestamp(value: Any) -> tuple[datetime | None, date | None, str | None]:
    """Возвращает UTC-naive время, локальную дату источника и его UTC offset."""

    raw = str(value or "").strip()
    if not raw:
        return None, None, None

    normalized = raw.replace("Z", "+00:00")
    parsed = None
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        for fmt in (
            "%Y-%m-%d %H:%M:%S.%f %z",
            "%Y-%m-%d %H:%M:%S %z",
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S",
        ):
            try:
                parsed = datetime.strptime(raw, fmt)
                break
            except ValueError:
                continue

    if parsed is None:
        return None, None, None

    source_date = parsed.date()
    source_timezone = _format_timezone_offset(parsed)
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed, source_date, source_timezone


def _parse_event_time(value: Any) -> datetime | None:
    """Совместимый helper: преобразует полный timestamp события в UTC-naive datetime."""

    parsed, _, _ = _parse_event_timestamp(value)
    return parsed


def _build_event_hash(account: Account, remote_acc_id: str, item: dict[str, Any]) -> str:
    """Создаёт ключ события на основе устойчивого source_id либо legacy-полей."""

    source_id = str(item.get("source_id") or "").strip()
    if source_id:
        parts = [str(account.server_id or ""), source_id]
    else:
        parts = [
            str(account.id),
            remote_acc_id,
            str(item.get("event_at") or item.get("time") or ""),
            str(item.get("group") or ""),
            str(item.get("event_text") or item.get("raw_text") or ""),
        ]
    return hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()


def _build_legacy_event_hash(account: Account, remote_acc_id: str, item: dict[str, Any]) -> str:
    """Воссоздаёт event_hash старой схемы для бесшовного upgrade записей."""

    parts = [
        str(account.id),
        remote_acc_id,
        str(item.get("time") or ""),
        str(item.get("group") or ""),
        str(item.get("event_text") or item.get("raw_text") or ""),
    ]
    return hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()


def _build_log_row(account: Account, item: dict[str, Any]) -> dict[str, Any] | None:
    """Валидирует одно событие RSS и превращает его в mapping для bulk insert."""

    event_text = str(item.get("event_text") or item.get("raw_text") or "").strip()
    if not event_text:
        return None

    remote_acc_id = str(
        item.get("acc_id") or account.internal_id or account.name or ""
    ).strip()
    event_at_source = str(item.get("event_at") or item.get("time") or "").strip()
    event_time, event_date, source_timezone = _parse_event_timestamp(event_at_source)
    source_id = str(item.get("source_id") or "").strip() or None

    try:
        source_cursor = int(item.get("source_cursor"))
    except (TypeError, ValueError):
        source_cursor = None

    level = str(item.get("level") or "info").strip().lower()[:16] or "info"
    parser_version_raw = item.get("parser_version") or 1
    try:
        parser_version = max(1, int(parser_version_raw))
    except (TypeError, ValueError):
        parser_version = 1

    return {
        "account_id": account.id,
        "server_id": account.server_id,
        "owner_id": account.owner_id,
        "remote_acc_id": remote_acc_id or None,
        "source_id": source_id,
        "source_cursor": source_cursor,
        "event_time": event_time,
        "event_date": event_date,
        "event_at_source": event_at_source or None,
        "source_timezone": source_timezone,
        "level": level,
        "group": str(item.get("group") or "system")[:32],
        "group_label": str(item.get("group_label") or "Система")[:64],
        "event_code": str(item.get("event_code") or "system_message")[:64],
        "event_text": event_text,
        "raw_text": str(item.get("raw_text") or "").strip() or None,
        "parser_version": parser_version,
        "event_hash": _build_event_hash(account, remote_acc_id, item),
        "collected_at": _utcnow(),
    }


def save_log_items(mapped_items: Iterable[tuple[Account, dict[str, Any]]]) -> int:
    """Пакетно сохраняет события и атомарно игнорирует уже известные source_id/hash."""

    prepared_rows: list[tuple[dict[str, Any], str]] = []
    for account, item in mapped_items:
        if not isinstance(item, dict):
            continue
        row = _build_log_row(account, item)
        if row:
            legacy_hash = _build_legacy_event_hash(account, row["remote_acc_id"] or "", item)
            prepared_rows.append((row, legacy_hash))

    if not prepared_rows:
        return 0

    # Старая версия хранила время без даты и не имела source_id. При первом v2-сборе
    # обогащаем такую строку на месте, а не создаём рядом дубль.
    by_legacy_hash: dict[str, list[int]] = {}
    for index, (_, legacy_hash) in enumerate(prepared_rows):
        by_legacy_hash.setdefault(legacy_hash, []).append(index)

    legacy_entries: list[FarmLogEntry] = []
    legacy_hashes = list(by_legacy_hash)
    for start in range(0, len(legacy_hashes), 500):
        legacy_entries.extend(
            FarmLogEntry.query.filter(
                FarmLogEntry.source_id.is_(None),
                FarmLogEntry.event_hash.in_(legacy_hashes[start : start + 500]),
            ).all()
        )

    source_ids_by_server: dict[int | None, set[str]] = {}
    for row, _ in prepared_rows:
        if row["source_id"]:
            source_ids_by_server.setdefault(row["server_id"], set()).add(row["source_id"])
    existing_source_keys: set[tuple[int | None, str]] = set()
    for server_id, server_source_ids in source_ids_by_server.items():
        source_ids = list(server_source_ids)
        for start in range(0, len(source_ids), 500):
            existing_rows = (
                db.session.query(FarmLogEntry.source_id)
                .filter(
                    FarmLogEntry.server_id == server_id,
                    FarmLogEntry.source_id.in_(source_ids[start : start + 500]),
                )
                .all()
            )
            existing_source_keys.update(
                (server_id, source_id)
                for source_id, in existing_rows
                if source_id
            )

    consumed_indices: set[int] = set()
    upgraded_count = 0
    for entry in legacy_entries:
        candidate_indices = by_legacy_hash.get(entry.event_hash) or []
        candidate_index = next(
            (index for index in candidate_indices if index not in consumed_indices),
            None,
        )
        if candidate_index is None:
            continue
        row = prepared_rows[candidate_index][0]
        if (row["server_id"], row["source_id"]) in existing_source_keys:
            db.session.delete(entry)
            consumed_indices.add(candidate_index)
            continue
        for column_name, value in row.items():
            setattr(entry, column_name, value)
        consumed_indices.add(candidate_index)
        upgraded_count += 1

    rows = [
        row
        for index, (row, _) in enumerate(prepared_rows)
        if index not in consumed_indices
    ]
    if consumed_indices:
        db.session.flush()
    if not rows:
        db.session.commit()
        return upgraded_count

    dialect_name = db.session.bind.dialect.name if db.session.bind else ""
    if dialect_name != "sqlite":
        # Проект штатно использует SQLite. Для другого backend сохраняем корректность,
        # а не SQLite-специфичный upsert.
        added = 0
        for row in rows:
            exists = FarmLogEntry.query.filter_by(event_hash=row["event_hash"]).first()
            if exists:
                continue
            db.session.add(FarmLogEntry(**row))
            added += 1
        db.session.commit()
        return upgraded_count + added

    statement = sqlite_insert(FarmLogEntry).values(rows).on_conflict_do_nothing()
    try:
        result = db.session.execute(statement)
        db.session.commit()
        return upgraded_count + max(0, int(result.rowcount or 0))
    except Exception:
        db.session.rollback()
        raise


def save_account_logs(account: Account, payload: dict[str, Any]) -> int:
    """Сохраняет legacy/v2 payload одной фермы через общий пакетный upsert."""

    items = payload.get("items") if isinstance(payload.get("items"), list) else []
    remote_acc_id = str(payload.get("acc_id") or account.internal_id or account.name or "").strip()
    normalized_items = []
    for item in items:
        if not isinstance(item, dict):
            continue
        normalized = dict(item)
        normalized.setdefault("acc_id", remote_acc_id)
        normalized_items.append((account, normalized))
    return save_log_items(normalized_items)


def query_logs_page(
    *,
    account_id: int | None,
    server_id: int | None,
    day: date | None,
    level: str | None = None,
    event_code: str | None = None,
    search: str | None = None,
    before_id: int | None = None,
    limit: int = 200,
) -> tuple[list[FarmLogEntry], int | None]:
    """Возвращает хронологическую страницу с keyset по event_time/id."""

    safe_limit = max(1, min(int(limit or 200), 500))
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
    if level:
        query = query.filter(FarmLogEntry.level == level)
    if event_code:
        query = query.filter(FarmLogEntry.event_code == event_code)
    if search:
        pattern = f"%{search.strip()}%"
        query = query.filter(
            or_(
                FarmLogEntry.event_text.ilike(pattern),
                FarmLogEntry.raw_text.ilike(pattern),
                FarmLogEntry.remote_acc_id.ilike(pattern),
            )
        )
    if before_id:
        anchor = db.session.get(FarmLogEntry, before_id)
        if anchor and anchor.event_time:
            query = query.filter(
                or_(
                    FarmLogEntry.event_time < anchor.event_time,
                    and_(
                        FarmLogEntry.event_time == anchor.event_time,
                        FarmLogEntry.id < anchor.id,
                    ),
                    FarmLogEntry.event_time.is_(None),
                )
            )
        elif anchor:
            query = query.filter(
                FarmLogEntry.event_time.is_(None),
                FarmLogEntry.id < anchor.id,
            )
        else:
            query = query.filter(FarmLogEntry.id < before_id)

    rows = (
        query.order_by(FarmLogEntry.event_time.desc().nullslast(), FarmLogEntry.id.desc())
        .limit(safe_limit + 1)
        .all()
    )
    has_more = len(rows) > safe_limit
    page = rows[:safe_limit]
    next_before_id = page[-1].id if has_more and page else None
    return page, next_before_id


def query_logs(
    *, account_id: int | None, server_id: int | None, day: date | None, limit: int = 500
) -> list[FarmLogEntry]:
    """Совместимый wrapper старого API чтения без следующего cursor."""

    rows, _ = query_logs_page(
        account_id=account_id,
        server_id=server_id,
        day=day,
        limit=limit,
    )
    return rows


def _serialize_log(log_entry: FarmLogEntry) -> dict[str, Any]:
    """Преобразует сохранённую запись в контракт модалки логов."""

    event_at = log_entry.event_at_source
    if not event_at and log_entry.event_time:
        event_at = log_entry.event_time.replace(tzinfo=timezone.utc).isoformat()
    return {
        "time": event_at or "",
        "event_at": event_at or "",
        "level": log_entry.level or "info",
        "group": log_entry.group or "system",
        "group_label": log_entry.group_label or "Система",
        "event_code": log_entry.event_code or "system_message",
        "event_text": log_entry.event_text,
        "raw_text": log_entry.raw_text or "",
        "source_id": log_entry.source_id,
        "source_cursor": log_entry.source_cursor,
    }


def _build_summary(items: list[dict[str, Any]]) -> dict[str, Any]:
    """Строит сводку для UI из уже сохранённых структурированных событий."""

    warnings = 0
    errors = 0
    finished = False
    reached_max_marches = False
    for item in items:
        level = str(item.get("level") or "").lower()
        code = str(item.get("event_code") or "").lower()
        text = str(item.get("event_text") or item.get("raw_text") or "").lower()
        warnings += int(level == "warning" or code in {"reached_max_marches", "update_game_required"})
        errors += int(
            level == "error"
            or any(token in text for token in ("error", "exception", "failed", "traceback"))
        )
        finished = finished or code in {"gathervip_finished", "finished"} or "finished" in text
        reached_max_marches = (
            reached_max_marches
            or code == "reached_max_marches"
            or "reached maximum of marches" in text
        )

    if errors:
        scenario_state = "error"
    elif warnings:
        scenario_state = "warning"
    elif finished:
        scenario_state = "finished"
    elif items:
        scenario_state = "running"
    else:
        scenario_state = "idle"

    last_item = items[-1] if items else {}
    return {
        "total": len(items),
        "warnings": warnings,
        "errors": errors,
        "finished": finished,
        "reached_max_marches": reached_max_marches,
        "last_event_time": last_item.get("event_at") or last_item.get("time") or "",
        "last_event_text": last_item.get("event_text") or "",
        "scenario_state": scenario_state,
    }


def build_account_logs_payload(
    account: Account, *, limit: int = 150, include_debug: bool = False
) -> dict[str, Any]:
    """Читает модалку из центральной БД без сетевых запросов и побочных записей в GET."""

    query = FarmLogEntry.query.filter(FarmLogEntry.account_id == account.id)
    if not include_debug:
        query = query.filter(FarmLogEntry.level != "debug")
    rows = (
        query.order_by(FarmLogEntry.event_time.desc().nullslast(), FarmLogEntry.id.desc())
        .limit(max(1, min(limit, 300)))
        .all()
    )
    items = [_serialize_log(row) for row in reversed(rows)]
    return {
        "ok": True,
        "account_id": account.id,
        "acc_id": account.internal_id or account.name,
        "account_name": account.name,
        "owner_name": account.owner.username if account.owner else None,
        "server_name": account.server.name if account.server else None,
        "items": items,
        "summary": _build_summary(items),
    }


def delete_expired_logs(retention_days: int) -> int:
    """Удаляет события старше retention, включая legacy-строки без event_time."""

    safe_days = max(1, int(retention_days or 90))
    cutoff = _utcnow() - timedelta(days=safe_days)
    query = FarmLogEntry.query.filter(
        or_(
            FarmLogEntry.event_time < cutoff,
            and_(FarmLogEntry.event_time.is_(None), FarmLogEntry.collected_at < cutoff),
        )
    )
    deleted = query.delete(synchronize_session=False)
    db.session.commit()
    return int(deleted or 0)
