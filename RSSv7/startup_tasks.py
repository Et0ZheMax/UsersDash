"""Запуск необязательных стартовых операций RSSv7 в фоновом потоке."""

from __future__ import annotations

import threading
from collections.abc import Callable, Iterable

StartupTask = tuple[str, Callable[[], object]]


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

