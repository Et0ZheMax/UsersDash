import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from UsersDash import app as app_module
from UsersDash.config import Config
from UsersDash.models import Account, Server, User, db
from UsersDash.services import remote_api


class RemoteActiveSyncTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        Config.DATA_DIR = Path(self.tmp.name)
        Config.SQLALCHEMY_DATABASE_URI = f"sqlite:///{Path(self.tmp.name) / 'test.db'}"

        self.app = app_module.create_app(enable_background_workers=False)
        self.app.config.update(TESTING=True)
        self.ctx = self.app.app_context()
        self.ctx.push()

        self.owner = User(username="client_remote_active", password_hash="hash", role="client")
        self.server = Server(name="RemoteActiveServer", host="127.0.0.1", is_active=True)
        db.session.add_all([self.owner, self.server])
        db.session.commit()

    def tearDown(self):
        db.session.remove()
        self.ctx.pop()
        self.tmp.cleanup()

    def test_sync_accounts_active_from_remote_disables_active_false_profile(self):
        account = Account(
            name="DisabledFarm",
            internal_id="remote-disabled-id",
            owner_id=self.owner.id,
            server_id=self.server.id,
            is_active=True,
        )
        db.session.add(account)
        db.session.commit()

        with patch.object(
            remote_api,
            "fetch_rssv7_manage_accounts",
            return_value=([{"Id": "remote-disabled-id", "Name": "DisabledFarm", "Active": False}], ""),
        ):
            err = remote_api.sync_accounts_active_from_remote(self.server, [account])

        self.assertEqual(err, "")
        self.assertFalse(account.is_active)

    def test_fetch_resources_does_not_reactivate_locally_disabled_account(self):
        account = Account(
            name="LocallyDisabledFarm",
            internal_id="remote-resource-id",
            owner_id=self.owner.id,
            server_id=self.server.id,
            is_active=False,
        )
        db.session.add(account)
        db.session.commit()

        resource_payload = {
            "remote-resource-id": {
                "id": "remote-resource-id",
                "nickname": "LocallyDisabledFarm",
                "food_view": "1",
                "wood_view": "2",
                "stone_view": "3",
                "gold_view": "4",
            }
        }
        with patch.object(remote_api, "fetch_resources_for_server", return_value=resource_payload):
            result = remote_api.fetch_resources_for_accounts([account])

        self.assertIn(account.id, result)
        self.assertFalse(account.is_active)


if __name__ == "__main__":
    unittest.main()
