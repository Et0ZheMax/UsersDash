import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from UsersDash import app as app_module
from UsersDash.config import Config
from UsersDash.models import Account, Server, User, db


class AdminDashboardActiveServersTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        Config.DATA_DIR = Path(self.tmp.name)
        Config.SQLALCHEMY_DATABASE_URI = f"sqlite:///{Path(self.tmp.name) / 'test.db'}"

        self.app = app_module.create_app(enable_background_workers=False)
        self.app.config.update(TESTING=True)
        self.ctx = self.app.app_context()
        self.ctx.push()

        self.admin = User(username="admin_active_servers", password_hash="hash", role="admin")
        self.owner = User(username="client_active_servers", password_hash="hash", role="client")
        self.active_server = Server(name="ActiveServer", host="127.0.0.1", is_active=True)
        self.inactive_server = Server(name="InactiveServer", host="127.0.0.2", is_active=False)
        db.session.add_all([self.admin, self.owner, self.active_server, self.inactive_server])
        db.session.commit()

        self.visible_account = Account(
            name="VisibleFarm",
            owner_id=self.owner.id,
            server_id=self.active_server.id,
            is_active=True,
        )
        self.hidden_account = Account(
            name="HiddenInactiveServerFarm",
            owner_id=self.owner.id,
            server_id=self.inactive_server.id,
            is_active=True,
        )
        db.session.add_all([self.visible_account, self.hidden_account])
        db.session.commit()

    def tearDown(self):
        db.session.remove()
        self.ctx.pop()
        self.tmp.cleanup()

    def _login_admin(self, client):
        with client.session_transaction() as sess:
            sess["_user_id"] = str(self.admin.id)
            sess["_fresh"] = True

    def test_dashboard_hides_active_accounts_from_inactive_servers(self):
        client = self.app.test_client()
        self._login_admin(client)

        with patch("UsersDash.admin_views.fetch_resources_for_accounts", return_value={}):
            resp = client.get("/admin/dashboard")

        self.assertEqual(resp.status_code, 200)
        self.assertIn(b"VisibleFarm", resp.data)
        self.assertNotIn(b"HiddenInactiveServerFarm", resp.data)

    def test_resources_api_fetches_only_accounts_from_active_servers(self):
        client = self.app.test_client()
        self._login_admin(client)

        captured_names = []

        def fake_fetch(accounts, *, force_refresh=False):
            captured_names.extend(acc.name for acc in accounts)
            return {self.visible_account.id: {"brief": "OK", "today_gain": None, "last_updated_fmt": None}}

        with patch("UsersDash.admin_views.fetch_resources_for_accounts", side_effect=fake_fetch):
            resp = client.get("/admin/api/account-resources")

        self.assertEqual(resp.status_code, 200)
        payload = resp.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(captured_names, ["VisibleFarm"])
        self.assertEqual([item["account_id"] for item in payload["items"]], [self.visible_account.id])


if __name__ == "__main__":
    unittest.main()
