import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from UsersDash import app as app_module
from UsersDash.config import Config
from UsersDash.models import Account, FarmData, Server, User, db


class FarmDataPullApplyTests(unittest.TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        Config.DATA_DIR = Path(self.tmp.name)
        Config.SQLALCHEMY_DATABASE_URI = f"sqlite:///{Path(self.tmp.name) / 'test.db'}"
        self.app = app_module.create_app(enable_background_workers=False)
        self.app.config.update(TESTING=True)
        self.ctx = self.app.app_context()
        self.ctx.push()

        self.admin = User(username="admin_pull", password_hash="hash", role="admin")
        self.server = Server(name="pull-test", host="127.0.0.1", is_active=True)
        db.session.add_all([self.admin, self.server])
        db.session.commit()

    def tearDown(self):
        db.session.remove()
        db.engine.dispose()
        self.ctx.pop()
        self.tmp.cleanup()

    def _login_admin(self, client):
        with client.session_transaction() as session:
            session["_user_id"] = str(self.admin.id)
            session["_fresh"] = True

    def test_import_creates_farm_without_synchronous_full_backup(self):
        client = self.app.test_client()
        self._login_admin(client)
        payload = {
            "items": [
                {
                    "server_id": self.server.id,
                    "farm_name": "NewFarm1",
                    "internal_id": "new-farm-id",
                    "is_new": True,
                    "email": "new@example.test",
                    "login": "new-login",
                }
            ]
        }

        with patch("UsersDash.admin_views.daily_backup_exists", return_value=True), patch(
            "UsersDash.admin_views.backup_database"
        ) as full_backup:
            response = client.post("/admin/farm-data/pull-apply", json=payload)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["updated"], 1)
        full_backup.assert_not_called()
        account = Account.query.filter_by(internal_id="new-farm-id").one()
        farm_data = FarmData.query.filter_by(account_id=account.id).one()
        self.assertEqual(account.server_id, self.server.id)
        self.assertEqual(farm_data.email, "new@example.test")
        self.assertEqual(farm_data.login, "new-login")


if __name__ == "__main__":
    unittest.main()
