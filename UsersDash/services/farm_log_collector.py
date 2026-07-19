"""Фоновый cursor-сбор логов с RSSv7-серверов."""

from __future__ import annotations

import hashlib
import json
import threading
import traceback
from datetime import date, datetime, timedelta, timezone
from typing import Iterable

from flask import Flask

from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from UsersDash.models import Account, FarmLogPendingEvent, FarmLogSyncState, Server, db
from UsersDash.services.farm_logs import (
    _parse_event_time,
    delete_expired_logs,
    save_log_items,
)
from UsersDash.services.remote_api import fetch_server_logs_v2


_COLLECTOR_THREAD: threading.Thread | None = None
_COLLECTOR_EVENT = threading.Event()
_COLLECTOR_QUEUE_LOCK = threading.Lock()
_REQUESTED_SERVER_IDS: set[int] = set()
_SERVER_LOCKS: dict[int, threading.Lock] = {}
_SERVER_LOCKS_GUARD = threading.Lock()
_LAST_CLEANUP_DATE: date | None = None


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _server_lock(server_id: int) -> threading.Lock:
    """Возвращает per-server lock, исключающий два сбора одного cursor в процессе."""

    with _SERVER_LOCKS_GUARD:
        return _SERVER_LOCKS.setdefault(server_id, threading.Lock())


def _sync_state(server_id: int) -> FarmLogSyncState:
    state = db.session.get(FarmLogSyncState, server_id)
    if state is None:
        state = FarmLogSyncState(server_id=server_id, cursor=0, status="idle")
        db.session.add(state)
        db.session.commit()
    return state


def _build_account_index(server_id: int) -> tuple[dict[str, Account], dict[str, Account]]:
    accounts = Account.query.filter(Account.server_id == server_id).all()
    by_remote_id: dict[str, Account] = {}
    accounts_by_name: dict[str, list[Account]] = {}
    for account in accounts:
        internal_id = str(account.internal_id or "").strip()
        name = str(account.name or "").strip()
        if internal_id:
            by_remote_id[internal_id] = account
        if name:
            accounts_by_name.setdefault(name.casefold(), []).append(account)
    # Неоднозначное имя без remote acc_id нельзя безопасно привязать к владельцу.
    by_name = {
        normalized_name: candidates[0]
        for normalized_name, candidates in accounts_by_name.items()
        if len(candidates) == 1
    }
    return by_remote_id, by_name


def _resolve_account(
    item: dict,
    by_remote_id: dict[str, Account],
    by_name: dict[str, Account],
) -> Account | None:
    remote_acc_id = str(item.get("acc_id") or "").strip()
    account_name = str(item.get("account_name") or "").strip()
    return by_remote_id.get(remote_acc_id) or (
        by_name.get(account_name.casefold()) if account_name else None
    )


def _save_pending_events(server_id: int, items: list[dict]) -> None:
    """Сохраняет несопоставленные события, чтобы повторить mapping после обновления Account."""

    if not items:
        return
    now = _utcnow()
    rows = []
    for item in items:
        payload_json = json.dumps(item, ensure_ascii=False, separators=(",", ":"))
        source_id = str(item.get("source_id") or "").strip()
        if not source_id:
            source_id = "pending:" + hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
        try:
            source_cursor = int(item.get("source_cursor"))
        except (TypeError, ValueError):
            source_cursor = None
        rows.append(
            {
                "server_id": server_id,
                "source_id": source_id,
                "source_cursor": source_cursor,
                "remote_acc_id": str(item.get("acc_id") or "")[:128] or None,
                "account_name": str(item.get("account_name") or "")[:128] or None,
                "payload_json": payload_json,
                "first_seen_at": now,
                "last_seen_at": now,
            }
        )

    statement = sqlite_insert(FarmLogPendingEvent).values(rows)
    statement = statement.on_conflict_do_update(
        index_elements=["server_id", "source_id"],
        set_={
            "source_cursor": statement.excluded.source_cursor,
            "remote_acc_id": statement.excluded.remote_acc_id,
            "account_name": statement.excluded.account_name,
            "payload_json": statement.excluded.payload_json,
            "last_seen_at": statement.excluded.last_seen_at,
        },
    )
    db.session.execute(statement)
    db.session.commit()


def _replay_pending_events(
    server_id: int,
    by_remote_id: dict[str, Account],
    by_name: dict[str, Account],
) -> tuple[int, int]:
    pending_rows = (
        FarmLogPendingEvent.query.filter(FarmLogPendingEvent.server_id == server_id)
        .order_by(FarmLogPendingEvent.source_cursor.asc(), FarmLogPendingEvent.id.asc())
        .limit(2000)
        .all()
    )
    mapped_items: list[tuple[Account, dict]] = []
    resolved_ids: list[int] = []
    for pending in pending_rows:
        try:
            item = json.loads(pending.payload_json)
        except (TypeError, ValueError):
            continue
        if not isinstance(item, dict):
            continue
        account = _resolve_account(item, by_remote_id, by_name)
        if account is None:
            continue
        mapped_items.append((account, item))
        resolved_ids.append(pending.id)

    added = save_log_items(mapped_items)
    if resolved_ids:
        FarmLogPendingEvent.query.filter(FarmLogPendingEvent.id.in_(resolved_ids)).delete(
            synchronize_session=False
        )
        db.session.commit()
    return added, len(resolved_ids)


def collect_server_logs(server_id: int) -> dict[str, int | str]:
    """Собирает все доступные cursor-страницы одного сервера и обновляет checkpoint."""

    with _server_lock(server_id):
        server = db.session.get(Server, server_id)
        if server is None:
            return {"added": 0, "skipped": 0, "error": "server not found"}

        state = _sync_state(server_id)
        state.status = "running"
        state.last_started_at = _utcnow()
        state.last_error = None
        db.session.commit()

        cursor = int(state.cursor or 0)
        total_added = 0
        total_skipped = 0
        last_event_at = state.last_event_at
        first_page = True
        page_count = 0

        try:
            by_remote_id, by_name = _build_account_index(server_id)
            replay_added, _ = _replay_pending_events(server_id, by_remote_id, by_name)
            total_added += replay_added
            while True:
                payload, error = fetch_server_logs_v2(
                    server,
                    after_id=cursor,
                    limit=1000,
                    include_debug=False,
                    refresh=first_page,
                )
                first_page = False
                if error or payload is None:
                    raise RuntimeError(error or "Пустой ответ потока логов")

                if payload.get("reset_required"):
                    if cursor == 0:
                        raise RuntimeError("RSS-сервер повторно запросил сброс нулевого cursor")
                    cursor = 0
                    state.cursor = 0
                    db.session.commit()
                    first_page = False
                    continue

                mapped_items: list[tuple[Account, dict]] = []
                pending_items: list[dict] = []
                for item in payload.get("items", []):
                    if not isinstance(item, dict):
                        total_skipped += 1
                        continue
                    account = _resolve_account(item, by_remote_id, by_name)
                    if account is None:
                        total_skipped += 1
                        pending_items.append(item)
                        continue
                    mapped_items.append((account, item))
                    event_at = _parse_event_time(item.get("event_at"))
                    if event_at and (last_event_at is None or event_at > last_event_at):
                        last_event_at = event_at

                total_added += save_log_items(mapped_items)
                _save_pending_events(server_id, pending_items)
                next_cursor = int(payload.get("next_cursor") or 0)
                if next_cursor < cursor:
                    raise RuntimeError(
                        f"RSS-сервер вернул cursor назад: {cursor} -> {next_cursor}"
                    )
                if next_cursor == cursor and payload.get("has_more"):
                    raise RuntimeError(f"RSS-сервер не продвинул cursor {cursor}")

                cursor = next_cursor
                db.session.refresh(state)
                state.cursor = max(int(state.cursor or 0), cursor)
                cursor = int(state.cursor)
                state.last_event_at = last_event_at
                db.session.commit()
                page_count += 1

                if not payload.get("has_more"):
                    break
                if page_count >= 100:
                    raise RuntimeError("Превышен лимит 100 cursor-страниц за один сбор")

            state.status = "warning" if total_skipped else "success"
            state.last_success_at = _utcnow()
            state.last_error = (
                f"Не сопоставлено событий: {total_skipped}" if total_skipped else None
            )
            state.collected_total = int(state.collected_total or 0) + total_added
            state.skipped_total = int(state.skipped_total or 0) + total_skipped
            db.session.commit()
            return {"added": total_added, "skipped": total_skipped, "error": ""}
        except Exception as exc:
            db.session.rollback()
            state = _sync_state(server_id)
            state.status = "error"
            state.last_error = str(exc)
            db.session.commit()
            return {"added": total_added, "skipped": total_skipped, "error": str(exc)}


def _consume_requested_server_ids() -> set[int]:
    with _COLLECTOR_QUEUE_LOCK:
        server_ids = set(_REQUESTED_SERVER_IDS)
        _REQUESTED_SERVER_IDS.clear()
    return server_ids


def _active_server_ids() -> list[int]:
    return [row.id for row in Server.query.filter(Server.is_active.is_(True)).all()]


def _run_retention_if_due(retention_days: int) -> None:
    global _LAST_CLEANUP_DATE

    today = datetime.now(timezone.utc).date()
    if _LAST_CLEANUP_DATE == today:
        return
    pending_cutoff = _utcnow() - timedelta(days=max(1, retention_days))
    FarmLogPendingEvent.query.filter(
        FarmLogPendingEvent.first_seen_at < pending_cutoff
    ).delete(synchronize_session=False)
    db.session.commit()
    delete_expired_logs(retention_days)
    _LAST_CLEANUP_DATE = today


def _collector_worker(app: Flask) -> None:
    interval = max(15, int(app.config.get("FARM_LOG_SYNC_INTERVAL_SECONDS", 60)))
    retention_days = max(1, int(app.config.get("FARM_LOG_RETENTION_DAYS", 90)))
    first_run = True

    while True:
        if not first_run:
            _COLLECTOR_EVENT.wait(timeout=interval)
        first_run = False
        _COLLECTOR_EVENT.clear()

        with app.app_context():
            try:
                requested_ids = _consume_requested_server_ids()
                server_ids: Iterable[int] = requested_ids or _active_server_ids()
                for server_id in server_ids:
                    result = collect_server_logs(int(server_id))
                    if result.get("error"):
                        print(f"[farm-logs] Сервер {server_id}: {result['error']}")
                _run_retention_if_due(retention_days)
            except Exception as exc:
                db.session.rollback()
                print(f"[farm-logs] Ошибка фонового цикла: {exc}")
                traceback.print_exc()
            finally:
                db.session.remove()


def start_farm_log_collector(app: Flask) -> threading.Thread:
    """Идемпотентно запускает периодический daemon-поток сбора."""

    global _COLLECTOR_THREAD
    with _COLLECTOR_QUEUE_LOCK:
        if _COLLECTOR_THREAD and _COLLECTOR_THREAD.is_alive():
            return _COLLECTOR_THREAD
        thread = threading.Thread(
            target=_collector_worker,
            args=(app,),
            daemon=True,
            name="usersdash-farm-log-collector",
        )
        thread.start()
        _COLLECTOR_THREAD = thread
        return thread


def queue_farm_log_sync(app: Flask, server_ids: Iterable[int]) -> int:
    """Ставит серверы в очередь и гарантирует наличие фонового worker."""

    normalized_ids = {int(server_id) for server_id in server_ids if int(server_id) > 0}
    if not normalized_ids:
        return 0
    with _COLLECTOR_QUEUE_LOCK:
        _REQUESTED_SERVER_IDS.update(normalized_ids)
    _COLLECTOR_EVENT.set()
    start_farm_log_collector(app)
    return len(normalized_ids)
