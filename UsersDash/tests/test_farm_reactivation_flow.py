import unittest
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from UsersDash import app as app_module
from UsersDash.config import Config
from UsersDash.models import Account, FarmData, Server, User, db


class FarmReactivationFlowTests(unittest.TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        Config.DATA_DIR = Path(self.tmp.name)
        Config.SQLALCHEMY_DATABASE_URI = f"sqlite:///{Path(self.tmp.name) / 'test.db'}"
        self.app = app_module.create_app(enable_background_workers=False)
        self.app.config.update(TESTING=True)
        self.ctx = self.app.app_context()
        self.ctx.push()

        self.admin = User(username="admin_reactivation", password_hash="hash", role="admin")
        self.owner = User(username="client_reactivation", password_hash="hash", role="client")
        self.server = Server(
            name="208-test",
            host="http://127.0.0.1:5001",
            api_token="server-token",
            is_active=True,
        )
        db.session.add_all([self.admin, self.owner, self.server])
        db.session.commit()
        self.account = Account(
            name="ANGEL",
            internal_id="expected-id",
            owner_id=self.owner.id,
            server_id=self.server.id,
            is_active=False,
            blocked_for_payment=True,
            next_payment_at=datetime(2026, 8, 1),
        )
        db.session.add(self.account)
        db.session.commit()
        db.session.add(
            FarmData(
                account_id=self.account.id,
                user_id=self.owner.id,
                farm_name=self.account.name,
                email="angel@example.test",
                password="secret",
                igg_id="777",
            )
        )
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

    def test_mark_paid_prepares_missing_remote_farm(self):
        client = self.app.test_client()
        self._login_admin(client)
        with patch(
            "UsersDash.admin_views.update_account_active",
            return_value=(False, "HTTP 404: acc not found"),
        ), patch(
            "UsersDash.admin_views.prepare_account_reactivation",
            return_value=(True, "OK", {"source": "archive"}),
        ):
            response = client.post(f"/admin/payments/{self.account.id}/mark-paid")

        self.assertEqual(response.status_code, 202)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertTrue(payload["reactivation_prepared"])
        self.assertEqual(payload["source"], "archive")
        db.session.refresh(self.account)
        self.assertFalse(self.account.is_active)
        self.assertTrue(self.account.blocked_for_payment)
        self.assertEqual(self.account.next_payment_at.date().isoformat(), "2026-08-31")

    def test_server_callback_completes_exact_account(self):
        client = self.app.test_client()
        response = client.post(
            "/api/farms/v1/reactivation/complete",
            query_string={"server": self.server.name, "token": self.server.api_token},
            json={"internal_id": "expected-id", "name": "ANGEL", "instance_id": 42},
        )

        self.assertEqual(response.status_code, 200)
        db.session.refresh(self.account)
        self.assertTrue(self.account.is_active)
        self.assertFalse(self.account.blocked_for_payment)

    def test_server_callback_rejects_same_name_with_wrong_id(self):
        client = self.app.test_client()
        response = client.post(
            "/api/farms/v1/reactivation/complete",
            query_string={"server": self.server.name, "token": self.server.api_token},
            json={"internal_id": "wrong-id", "name": "ANGEL", "instance_id": 42},
        )

        self.assertEqual(response.status_code, 404)
        db.session.refresh(self.account)
        self.assertFalse(self.account.is_active)
        self.assertTrue(self.account.blocked_for_payment)


if __name__ == "__main__":
    unittest.main()
