import unittest
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from UsersDash import app as app_module
from UsersDash.config import Config
from UsersDash.models import Account, FarmLogEntry, Server, User, db


class ClientFarmLogsRouteTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        Config.DATA_DIR = Path(self.tmp.name)
        Config.SQLALCHEMY_DATABASE_URI = f"sqlite:///{Path(self.tmp.name) / 'client-logs.db'}"

        self.app = app_module.create_app(enable_background_workers=False)
        self.app.config.update(TESTING=True)
        self.ctx = self.app.app_context()
        self.ctx.push()

        self.owner = User(username="logs-owner", password_hash="hash", role="client")
        self.other_owner = User(username="logs-other", password_hash="hash", role="client")
        self.server = Server(name="LogsServer", host="127.0.0.1", is_active=True)
        db.session.add_all([self.owner, self.other_owner, self.server])
        db.session.flush()
        self.account = Account(
            name="Client Farm",
            internal_id="client-farm-id",
            owner_id=self.owner.id,
            server_id=self.server.id,
        )
        self.other_account = Account(
            name="Other Farm",
            internal_id="other-farm-id",
            owner_id=self.other_owner.id,
            server_id=self.server.id,
        )
        db.session.add_all([self.account, self.other_account])
        db.session.flush()
        db.session.add(
            FarmLogEntry(
                account_id=self.account.id,
                server_id=self.server.id,
                owner_id=self.owner.id,
                remote_acc_id="client-farm-id",
                source_id="client-route-source",
                source_cursor=1,
                event_time=datetime(2026, 7, 19, 9, 0, 0),
                event_at_source="2026-07-19T12:00:00+03:00",
                level="info",
                group="march",
                group_label="Марш",
                event_code="send_troops",
                event_text="Отряд отправлен",
                raw_text="секретная техническая строка",
                parser_version=2,
                event_hash="client-route-hash",
            )
        )
        db.session.commit()

    def tearDown(self):
        db.session.remove()
        db.drop_all()
        db.engine.dispose()
        self.ctx.pop()
        self.tmp.cleanup()

    def _login(self, client, user: User) -> None:
        with client.session_transaction() as session:
            session["_user_id"] = str(user.id)
            session["_fresh"] = True

    def test_owner_receives_sanitized_feed(self):
        client = self.app.test_client()
        self._login(client, self.owner)

        response = client.get(f"/account/{self.account.id}/logs")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["items"][0]["title"], "Отряд отправлен на сбор")
        self.assertNotIn("raw_text", payload["items"][0])
        self.assertNotIn("секретная", response.get_data(as_text=True))

    def test_owner_cannot_read_another_users_farm(self):
        client = self.app.test_client()
        self._login(client, self.owner)

        response = client.get(f"/account/{self.other_account.id}/logs")

        self.assertEqual(response.status_code, 404)

    def test_account_cycle_events_are_translated_and_level_menu_warning_is_hidden(self):
        events = (
            (2, "Preparing Account"),
            (3, "Gather: Cannot find Level Menu"),
            (4, "Recruit: Porter already in progress"),
            (5, "Account Done"),
        )
        for cursor, raw_text in events:
            db.session.add(
                FarmLogEntry(
                    account_id=self.account.id,
                    server_id=self.server.id,
                    owner_id=self.owner.id,
                    remote_acc_id="client-farm-id",
                    source_id=f"client-cycle-source-{cursor}",
                    source_cursor=cursor,
                    event_time=datetime(2026, 7, 19, 9, cursor, 0),
                    event_at_source=f"2026-07-19T12:0{cursor}:00+03:00",
                    level="info",
                    group="system",
                    group_label="Система",
                    event_code="system_message",
                    event_text=raw_text,
                    raw_text=raw_text,
                    parser_version=2,
                    event_hash=f"client-cycle-hash-{cursor}",
                )
            )
        db.session.commit()

        client = self.app.test_client()
        self._login(client, self.owner)
        payload = client.get(f"/account/{self.account.id}/logs").get_json()
        items = payload["items"]

        self.assertEqual(items[0]["title"], "Сценарий завершен")
        self.assertEqual(items[0]["tone"], "complete")
        self.assertTrue(any(item["title"] == "Грузчики уже обучаются" for item in items))
        self.assertTrue(any(item["title"] == "Бот вошел в аккаунт" for item in items))
        self.assertFalse(any(item["title"] == "Не удалось выполнить действие" for item in items))


if __name__ == "__main__":
    unittest.main()
