from __future__ import annotations

import threading
import time
from datetime import datetime

from sqlalchemy.orm import joinedload

from UsersDash.models import Account, FarmData, FarmMenuSyncJob, db
from UsersDash.services.remote_api import update_account_menu_data


_worker_lock = threading.Lock()
_worker_thread: threading.Thread | None = None
_wake_event = threading.Event()


def enqueue_menu_sync(account_ids: list[int]) -> list[dict[str, int | str]]:
    """Создаёт/обновляет устойчивые задания и немедленно возвращает их статусы."""

    jobs: list[FarmMenuSyncJob] = []
    for account_id in dict.fromkeys(account_ids):
        job = FarmMenuSyncJob.query.filter_by(account_id=account_id).first()
        if job is None:
            job = FarmMenuSyncJob(account_id=account_id, version=1)
            db.session.add(job)
        else:
            job.version = int(job.version or 0) + 1
        job.status = "pending"
        job.attempts = 0
        job.error = None
        job.finished_at = None
        jobs.append(job)
    db.session.commit()
    _wake_event.set()
    return [serialize_job(job) for job in jobs]


def serialize_job(job: FarmMenuSyncJob) -> dict[str, int | str | None]:
    return {
        "account_id": job.account_id,
        "version": job.version,
        "status": job.status,
        "attempts": job.attempts,
        "error": job.error,
    }


def get_menu_sync_status(account_ids: list[int]) -> list[dict[str, int | str | None]]:
    if not account_ids:
        return []
    jobs = FarmMenuSyncJob.query.filter(FarmMenuSyncJob.account_id.in_(account_ids)).all()
    return [serialize_job(job) for job in jobs]


def _process_one(app) -> bool:
    with app.app_context():
        job = (
            FarmMenuSyncJob.query.filter_by(status="pending")
            .order_by(FarmMenuSyncJob.updated_at.asc(), FarmMenuSyncJob.id.asc())
            .first()
        )
        if job is None:
            return False

        claimed_version = job.version
        job.status = "running"
        job.attempts = int(job.attempts or 0) + 1
        db.session.commit()

        account = (
            Account.query.options(joinedload(Account.server))
            .filter_by(id=job.account_id)
            .first()
        )
        farm_data = FarmData.query.filter_by(account_id=job.account_id).first()
        if account is None or farm_data is None:
            ok, message = False, "аккаунт или данные фермы не найдены"
        else:
            ok, message = update_account_menu_data(
                account,
                email=farm_data.email,
                password=farm_data.password,
                igg_id=farm_data.igg_id,
            )

        db.session.expire_all()
        current = FarmMenuSyncJob.query.filter_by(id=job.id).first()
        if current is None:
            return True
        if current.version != claimed_version:
            # Во время отправки пользователь успел отредактировать строку ещё раз.
            # Новая версия уже pending и должна быть отправлена отдельно.
            if current.status == "running":
                current.status = "pending"
            db.session.commit()
            return True

        if ok:
            current.status = "succeeded"
            current.error = None
            current.finished_at = datetime.utcnow()
        elif current.attempts < 3:
            current.status = "pending"
            current.error = message
        else:
            current.status = "failed"
            current.error = message
            current.finished_at = datetime.utcnow()
        db.session.commit()
        return True


def start_menu_sync_worker(app) -> threading.Thread:
    """Запускает один worker на процесс и восстанавливает задания после рестарта."""

    global _worker_thread
    with _worker_lock:
        if _worker_thread is not None and _worker_thread.is_alive():
            return _worker_thread

        with app.app_context():
            FarmMenuSyncJob.query.filter_by(status="running").update(
                {"status": "pending"}, synchronize_session=False
            )
            db.session.commit()

        def worker() -> None:
            while True:
                try:
                    processed = _process_one(app)
                except Exception:
                    app.logger.exception("Ошибка фоновой синхронизации MenuData")
                    processed = False
                if processed:
                    time.sleep(0.15)
                    continue
                _wake_event.wait(timeout=2.0)
                _wake_event.clear()

        _worker_thread = threading.Thread(
            target=worker,
            daemon=True,
            name="usersdash-menu-sync",
        )
        _worker_thread.start()
        return _worker_thread
