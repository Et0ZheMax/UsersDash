"""Тесты неблокирующего запуска тяжёлых стартовых задач RSSv7."""

import threading
import unittest

from startup_tasks import RefreshTask, run_background_tasks, start_background_tasks, start_periodic_refresh


class StartupTasksTests(unittest.TestCase):
    def test_periodic_refresh_runs_without_http_requests(self) -> None:
        stop = threading.Event()
        calls = []

        def parse() -> None:
            calls.append(1)
            if len(calls) == 2:
                stop.set()

        refresh = RefreshTask(parse)
        thread = start_periodic_refresh(refresh, interval_seconds=0.01, stop_event=stop)
        thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertEqual(len(calls), 2)
        self.assertFalse(refresh.status()["running"])

    def test_periodic_refresh_retries_after_failure(self) -> None:
        stop = threading.Event()
        calls = []

        def parse() -> None:
            calls.append(1)
            if len(calls) == 1:
                raise RuntimeError("temporary failure")
            stop.set()

        refresh = RefreshTask(parse)
        thread = start_periodic_refresh(refresh, interval_seconds=0.01, stop_event=stop)
        thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertEqual(len(calls), 2)
        self.assertIsNone(refresh.status()["error"])

    def test_refresh_requests_share_one_running_task(self) -> None:
        started = threading.Event()
        release = threading.Event()
        calls = []

        def slow() -> None:
            calls.append(1)
            started.set()
            release.wait(5)

        refresh = RefreshTask(slow)
        refresh.request()
        self.assertTrue(started.wait(1))
        for _ in range(10):
            self.assertTrue(refresh.request()["running"])
        waiter = threading.Thread(target=lambda: refresh.request(wait=True))
        waiter.start()
        self.assertEqual(refresh.status()["generation"], 1)
        self.assertEqual(calls, [1])
        release.set()
        waiter.join(1)
        self.assertFalse(waiter.is_alive())
        self.assertFalse(refresh.status()["running"])

    def test_refresh_failure_is_reported_and_can_be_retried(self) -> None:
        calls = []

        def task() -> None:
            calls.append(1)
            if len(calls) == 1:
                raise RuntimeError("failure")

        refresh = RefreshTask(task)
        with self.assertRaisesRegex(RuntimeError, "failure"):
            refresh.request(wait=True)
        self.assertEqual(refresh.status()["error"], "failure")
        self.assertIsNone(refresh.request(wait=True)["error"])
        self.assertEqual(len(calls), 2)

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

