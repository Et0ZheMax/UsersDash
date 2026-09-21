"""Тесты неблокирующего запуска тяжёлых стартовых задач RSSv7."""

import threading
import unittest

from startup_tasks import run_background_tasks, start_background_tasks


class StartupTasksTests(unittest.TestCase):
    def test_start_returns_before_slow_task_finishes(self) -> None:
        started = threading.Event()
        release = threading.Event()
        finished = threading.Event()

        def slow_task() -> None:
            started.set()
            release.wait(timeout=5)
            finished.set()

        thread = start_background_tasks([("slow", slow_task)], logger=lambda _: None)

        self.assertTrue(started.wait(timeout=1))
        self.assertFalse(finished.is_set())
        self.assertTrue(thread.daemon)
        self.assertEqual(thread.name, "rssv7-background-startup")

        release.set()
        thread.join(timeout=1)
        self.assertTrue(finished.is_set())

    def test_failure_does_not_skip_following_tasks(self) -> None:
        calls: list[str] = []
        messages: list[str] = []

        def broken() -> None:
            calls.append("broken")
            raise RuntimeError("boom")

        def healthy() -> None:
            calls.append("healthy")

        run_background_tasks(
            [("broken", broken), ("healthy", healthy)],
            logger=messages.append,
        )

        self.assertEqual(calls, ["broken", "healthy"])
        self.assertTrue(any("error in broken" in message for message in messages))
        self.assertTrue(any("done: healthy" in message for message in messages))


if __name__ == "__main__":
    unittest.main()

