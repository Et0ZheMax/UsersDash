import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

from flask import Flask
from requests import RequestException
from sqlalchemy import text

from UsersDash.models import (
    Account,
    FarmLogEntry,
    FarmLogPendingEvent,
    FarmLogSyncState,
    Server,
    User,
    db,
)
from UsersDash.services.farm_log_collector import (
    _build_account_index,
    _resolve_account,
    collect_server_logs,
)
from UsersDash.services.farm_logs import (
    _build_legacy_event_hash,
    _parse_event_timestamp,
    build_account_logs_payload,
    query_logs_page,
    save_log_items,
)
from UsersDash.services.farm_logs_migration import ensure_farm_logs_schema
from UsersDash.services import remote_api


class FarmLogsTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.app = Flask(__name__)
        self.app.config.update(
            TESTING=True,
            SQLALCHEMY_DATABASE_URI=f"sqlite:///{Path(self.tmp.name) / 'farm-logs.db'}",
            SQLALCHEMY_TRACK_MODIFICATIONS=False,
        )
        db.init_app(self.app)
        self.ctx = self.app.app_context()
        self.ctx.push()
        db.create_all()

        self.owner = User(username="farm-owner", password_hash="x", role="client")
        self.server = Server(
            name="RSS-208",
            host="127.0.0.1:5001",
            api_base_url="http://127.0.0.1:5001/api",
            is_active=True,
        )
        db.session.add_all([self.owner, self.server])
        db.session.flush()
        self.account = Account(
            name="Farm A",
            internal_id="acc-a",
            owner_id=self.owner.id,
            server_id=self.server.id,
        )
        db.session.add(self.account)
        db.session.commit()

    def tearDown(self):
        db.session.remove()
        db.drop_all()
        db.engine.dispose()
        self.ctx.pop()
        self.tmp.cleanup()

    @staticmethod
    def _event(source_id: str, event_at: str, source_cursor: int) -> dict:
        return {
            "acc_id": "acc-a",
            "source_id": source_id,
            "source_cursor": source_cursor,
            "event_at": event_at,
            "level": "info",
            "group": "march",
            "group_label": "Марш",
            "event_code": "send_troops",
            "event_text": "Отряд отправлен",
            "raw_text": "raw log line",
            "parser_version": 2,
        }

    def test_timestamp_keeps_source_date_and_normalizes_utc(self):
        event_time, event_date, source_timezone = _parse_event_timestamp(
            "2026-07-19T12:34:56.123+03:00"
        )

        self.assertEqual(event_time.isoformat(), "2026-07-19T09:34:56.123000")
        self.assertEqual(event_date.isoformat(), "2026-07-19")
        self.assertEqual(source_timezone, "+03:00")

    def test_source_id_deduplicates_retries_but_not_different_days(self):
        first = self._event("source-1", "2026-07-19T12:00:00+03:00", 1)
        second_day = self._event("source-2", "2026-07-20T12:00:00+03:00", 2)

        self.assertEqual(save_log_items([(self.account, first)]), 1)
        self.assertEqual(save_log_items([(self.account, first)]), 0)
        self.assertEqual(save_log_items([(self.account, second_day)]), 1)
        self.assertEqual(FarmLogEntry.query.count(), 2)

        day_rows, _ = query_logs_page(
            account_id=self.account.id,
            server_id=None,
            day=_parse_event_timestamp(first["event_at"])[1],
            limit=20,
        )
        self.assertEqual([row.source_id for row in day_rows], ["source-1"])

    def test_first_v2_sync_upgrades_matching_legacy_row_without_duplicate(self):
        event = self._event("source-upgrade", "2026-07-19T12:00:00+03:00", 10)
        event["time"] = "12:00:00"
        legacy_hash = _build_legacy_event_hash(self.account, "acc-a", event)
        db.session.add(
            FarmLogEntry(
                account_id=self.account.id,
                server_id=self.server.id,
                owner_id=self.owner.id,
                remote_acc_id="acc-a",
                group=event["group"],
                group_label=event["group_label"],
                event_text=event["event_text"],
                raw_text=event["event_text"],
                event_hash=legacy_hash,
            )
        )
        db.session.commit()

        self.assertEqual(save_log_items([(self.account, event)]), 1)
        self.assertEqual(save_log_items([(self.account, event)]), 0)
        self.assertEqual(FarmLogEntry.query.count(), 1)
        upgraded = FarmLogEntry.query.one()
        self.assertEqual(upgraded.source_id, "source-upgrade")
        self.assertEqual(upgraded.event_date.isoformat(), "2026-07-19")

    def test_duplicate_account_name_is_not_used_as_ambiguous_fallback(self):
        second_owner = User(username="second-owner", password_hash="x", role="client")
        db.session.add(second_owner)
        db.session.flush()
        db.session.add(
            Account(
                name=self.account.name,
                internal_id="acc-b",
                owner_id=second_owner.id,
                server_id=self.server.id,
            )
        )
        db.session.commit()

        by_remote_id, by_name = _build_account_index(self.server.id)
        resolved = _resolve_account(
            {"acc_id": "unknown-id", "account_name": self.account.name},
            by_remote_id,
            by_name,
        )
        exact = _resolve_account(
            {"acc_id": "acc-a", "account_name": self.account.name},
            by_remote_id,
            by_name,
        )

        self.assertIsNone(resolved)
        self.assertEqual(exact.id, self.account.id)

    def test_keyset_pagination_and_modal_contract(self):
        events = [
            self._event("source-2", "2026-07-19T12:00:02+03:00", 2),
            self._event("source-1", "2026-07-19T12:00:01+03:00", 1),
            self._event("source-3", "2026-07-19T12:00:03+03:00", 3),
        ]
        self.assertEqual(save_log_items([(self.account, event) for event in events]), 3)

        first_page, next_cursor = query_logs_page(
            account_id=self.account.id,
            server_id=None,
            day=None,
            limit=2,
        )
        self.assertEqual(len(first_page), 2)
        self.assertEqual([row.source_id for row in first_page], ["source-3", "source-2"])
        self.assertIsNotNone(next_cursor)
        second_page, final_cursor = query_logs_page(
            account_id=self.account.id,
            server_id=None,
            day=None,
            before_id=next_cursor,
            limit=2,
        )
        self.assertEqual(len(second_page), 1)
        self.assertEqual(second_page[0].source_id, "source-1")
        self.assertIsNone(final_cursor)

        payload = build_account_logs_payload(self.account, limit=10)
        self.assertEqual(payload["summary"]["total"], 3)
        self.assertTrue(payload["items"][-1]["event_at"].endswith("+03:00"))

    @patch("UsersDash.services.farm_log_collector.fetch_server_logs_v2")
    def test_collector_advances_checkpoint_and_is_idempotent(self, fetch_mock):
        event = self._event("stream-source-7", "2026-07-19T13:00:00+03:00", 7)

        def fake_fetch(server, *, after_id, **kwargs):
            if after_id == 0:
                return {
                    "ok": True,
                    "items": [event],
                    "next_cursor": 7,
                    "max_id": 7,
                    "has_more": False,
                    "reset_required": False,
                }, ""
            return {
                "ok": True,
                "items": [],
                "next_cursor": after_id,
                "max_id": after_id,
                "has_more": False,
                "reset_required": False,
            }, ""

        fetch_mock.side_effect = fake_fetch
        first = collect_server_logs(self.server.id)
        second = collect_server_logs(self.server.id)

        self.assertEqual(first["added"], 1)
        self.assertEqual(second["added"], 0)
        self.assertEqual(db.session.get(FarmLogSyncState, self.server.id).cursor, 7)
        self.assertEqual(FarmLogEntry.query.count(), 1)

    @patch("UsersDash.services.farm_log_collector.fetch_server_logs_v2")
    def test_unmatched_event_is_replayed_after_account_mapping_appears(self, fetch_mock):
        event = self._event("pending-source-8", "2026-07-19T14:00:00+03:00", 8)
        event["acc_id"] = "new-remote-id"
        event["account_name"] = "New Farm"

        def fake_fetch(server, *, after_id, **kwargs):
            items = [event] if after_id == 0 else []
            next_cursor = 8 if after_id == 0 else after_id
            return {
                "ok": True,
                "items": items,
                "next_cursor": next_cursor,
                "max_id": next_cursor,
                "has_more": False,
                "reset_required": False,
            }, ""

        fetch_mock.side_effect = fake_fetch
        first = collect_server_logs(self.server.id)
        self.assertEqual(first["skipped"], 1)
        self.assertEqual(FarmLogPendingEvent.query.count(), 1)
        self.assertEqual(FarmLogEntry.query.count(), 0)

        db.session.add(
            Account(
                name="New Farm",
                internal_id="new-remote-id",
                owner_id=self.owner.id,
                server_id=self.server.id,
            )
        )
        db.session.commit()
        second = collect_server_logs(self.server.id)

        self.assertEqual(second["added"], 1)
        self.assertEqual(FarmLogPendingEvent.query.count(), 0)
        self.assertEqual(FarmLogEntry.query.one().remote_acc_id, "new-remote-id")


class FarmLogsMigrationTestCase(unittest.TestCase):
    def test_legacy_table_gets_v2_columns_and_indexes(self):
        app = Flask("farm-log-migration-test")
        app.config.update(
            SQLALCHEMY_DATABASE_URI="sqlite://",
            SQLALCHEMY_TRACK_MODIFICATIONS=False,
        )
        db.init_app(app)
        with app.app_context():
            with db.engine.begin() as connection:
                connection.execute(text("""
                CREATE TABLE farm_log_entries (
                    id INTEGER PRIMARY KEY,
                    account_id INTEGER NOT NULL,
                    server_id INTEGER,
                    owner_id INTEGER,
                    remote_acc_id VARCHAR(128),
                    event_time DATETIME,
                    event_date DATE,
                    "group" VARCHAR(32),
                    group_label VARCHAR(64),
                    event_text TEXT NOT NULL,
                    raw_text TEXT,
                    event_hash VARCHAR(64) NOT NULL,
                    collected_at DATETIME NOT NULL
                )
                """))

            try:
                ensure_farm_logs_schema()
                columns = {
                    row[1] for row in db.session.execute(text("PRAGMA table_info(farm_log_entries)"))
                }
                indexes = {
                    row[1] for row in db.session.execute(text("PRAGMA index_list(farm_log_entries)"))
                }
            finally:
                db.session.remove()
                db.engine.dispose()

        self.assertIn("source_id", columns)
        self.assertIn("event_at_source", columns)
        self.assertIn("level", columns)
        self.assertIn("parser_version", columns)
        self.assertIn("uq_farm_log_server_source", indexes)


class RemoteLogRefreshTestCase(unittest.TestCase):
    @patch("UsersDash.services.remote_api.requests.post")
    def test_failed_refresh_is_retried_but_success_is_throttled(self, post_mock):
        server = SimpleNamespace(id=987654, name="RSS-test")
        successful_response = SimpleNamespace(ok=True)
        post_mock.side_effect = [RequestException("offline"), successful_response]
        remote_api._REMOTE_REFRESH_TS.pop(server.id, None)

        first_error = remote_api._refresh_remote_logs_cache(
            server,
            base="http://127.0.0.1/api",
        )
        second_error = remote_api._refresh_remote_logs_cache(
            server,
            base="http://127.0.0.1/api",
        )
        third_error = remote_api._refresh_remote_logs_cache(
            server,
            base="http://127.0.0.1/api",
        )

        self.assertIn("offline", first_error)
        self.assertEqual(second_error, "")
        self.assertEqual(third_error, "")
        self.assertEqual(post_mock.call_count, 2)


if __name__ == "__main__":
    unittest.main()
