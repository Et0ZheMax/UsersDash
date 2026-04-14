import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from UsersDash import app as app_module
from UsersDash.config import Config
from UsersDash.models import Account, Server, User, db


class ApiInstanceIdUpdateSuggestionsTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        Config.DATA_DIR = Path(self.tmp.name)
        Config.SQLALCHEMY_DATABASE_URI = f"sqlite:///{Path(self.tmp.name) / 'test.db'}"

        self.app = app_module.create_app()
        self.app.config.update(TESTING=True)
        self.ctx = self.app.app_context()
        self.ctx.push()

        self.user = User(username="client_one", password_hash="hash", role="client")
        self.server = Server(name="S1", host="http://localhost", api_token="token-1")
        db.session.add_all([self.user, self.server])
        db.session.commit()

        self.account = Account(
            name="Farm Alpha",
            owner_id=self.user.id,
            server_id=self.server.id,
            internal_id="old-123",
        )
        db.session.add(self.account)
        db.session.commit()

    def tearDown(self):
        db.session.remove()
        self.ctx.pop()
        self.tmp.cleanup()

    def _save_url(self) -> str:
        return f"/api/farms/v1/save?server={self.server.name}&token={self.server.api_token}"

    def test_suggests_instance_id_update_when_name_matches(self):
        client = self.app.test_client()
        resp = client.post(
            self._save_url(),
            json={
                "items": [
                    {
                        "name": "Farm Alpha",
                        "internal_id": "new-999",
                    }
                ]
            },
        )
        self.assertEqual(resp.status_code, 200)
        payload = resp.get_json()
        self.assertTrue(payload["ok"])

        suggestions = payload.get("instance_id_updates") or []
        self.assertEqual(len(suggestions), 1)
        self.assertFalse(suggestions[0]["applied"])
        self.assertEqual(suggestions[0]["old_internal_id"], "old-123")
        self.assertEqual(suggestions[0]["new_internal_id"], "new-999")

        db.session.refresh(self.account)
        self.assertEqual(self.account.internal_id, "old-123")

    def test_applies_instance_id_update_by_flag(self):
        client = self.app.test_client()
        resp = client.post(
            self._save_url(),
            json={
                "apply_instance_id_updates": True,
                "items": [
                    {
                        "name": "Farm Alpha",
                        "internal_id": "new-999",
                    }
                ],
            },
        )
        self.assertEqual(resp.status_code, 200)
        payload = resp.get_json()
        self.assertTrue(payload["ok"])

        suggestions = payload.get("instance_id_updates") or []
        self.assertEqual(len(suggestions), 1)
        self.assertTrue(suggestions[0]["applied"])

        db.session.refresh(self.account)
        self.assertEqual(self.account.internal_id, "new-999")


if __name__ == "__main__":
    unittest.main()
