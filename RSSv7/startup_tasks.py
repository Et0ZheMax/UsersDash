"""Запуск необязательных стартовых операций RSSv7 в фоновом потоке."""

from __future__ import annotations

import threading
from collections.abc import Callable, Iterable

StartupTask = tuple[str, Callable[[], object]]


class RefreshTask:
    """Объединяет одновременные запросы обновления в один фоновый проход."""

    def __init__(self, task: Callable[[], object]) -> None:
        self._task = task
        self._lock = threading.Lock()
        self._job: dict | None = None
        self._generation = 0

    def status(self) -> dict:
        """Возвращает состояние без ожидания обработки или блокировки БД."""
        with self._lock:
            job = self._job
            return {
                "running": bool(job and not job["done"].is_set()),
                "error": str(job["error"]) if job and job["error"] else None,
                "generation": self._generation,
            }

    def request(self, *, wait: bool = False) -> dict:
        """Запускает проход либо присоединяется к текущему; ожидание опционально."""
        with self._lock:
            job = self._job
            if job is None or job["done"].is_set():
                job = {"done": threading.Event(), "error": None}
                self._job = job
                self._generation += 1
                threading.Thread(
                    target=self._run, args=(job,), name="rssv7-log-refresh", daemon=True,
                ).start()
        if wait:
            job["done"].wait()
            if job["error"] is not None:
                raise job["error"]
        return self.status()

    def _run(self, job: dict) -> None:
        try:
            self._task()
        except Exception as exc:
            with self._lock:
                job["error"] = exc
            print(f"[LOG-REFRESH] error: {exc}", flush=True)
        finally:
            job["done"].set()


def run_background_tasks(
    tasks: Iterable[StartupTask],
    logger: Callable[[str], object] = print,
) -> None:
    """Последовательно выполняет фоновые задачи, не прерывая цепочку при ошибке."""

    for name, task in tasks:
        try:
            logger(f"[BACKGROUND-STARTUP] start: {name}")
            task()
            logger(f"[BACKGROUND-STARTUP] done: {name}")
        except Exception as exc:
            logger(f"[BACKGROUND-STARTUP] error in {name}: {exc}")


def start_background_tasks(
    tasks: Iterable[StartupTask],
    logger: Callable[[str], object] = print,
) -> threading.Thread:
    """Немедленно возвращает управление, выполняя переданные задачи в daemon-потоке."""

    task_list = list(tasks)
    thread = threading.Thread(
        target=run_background_tasks,
        args=(task_list, logger),
        name="rssv7-background-startup",
        daemon=True,
    )
    thread.start()
    return thread

