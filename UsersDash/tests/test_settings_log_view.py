import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from UsersDash import app as app_module
from UsersDash.config import Config
from UsersDash.models import SettingsAuditLog, User, db
from UsersDash.services.audit import log_settings_action


class SettingsLogViewTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        Config.DATA_DIR = Path(self.tmp.name)
        Config.SQLALCHEMY_DATABASE_URI = f"sqlite:///{Path(self.tmp.name) / 'test.db'}"

        self.app = app_module.create_app()
        self.app.config.update(TESTING=True)
        self.ctx = self.app.app_context()
        self.ctx.push()

        self.admin = User(username="admin_view", password_hash="hash", role="admin")
        self.user = User(username="client_view", password_hash="hash", role="client")
        db.session.add_all([self.admin, self.user])
        db.session.commit()

        log_settings_action(self.user, self.admin, "settings_page_opened", {"field": "page", "new_value": "view"})
        log_settings_action(
            self.user,
            self.admin,
            "step_update",
            {"field": "step:1", "old_value": "old", "new_value": "new"},
        )

    def tearDown(self):
        db.session.remove()
        self.ctx.pop()
        self.tmp.cleanup()

    def test_filter_by_action_and_export(self):
        client = self.app.test_client()
        with client.session_transaction() as sess:
            sess["_user_id"] = str(self.admin.id)
            sess["_fresh"] = True

        resp = client.get("/admin/settings-log?action=step_update")
        self.assertEqual(resp.status_code, 200)
        self.assertIn(b"step_update", resp.data)
        self.assertNotIn(b"settings_page_opened", resp.data)

        export_resp = client.get("/admin/settings-log?action=step_update&format=json")
        self.assertEqual(export_resp.status_code, 200)
        self.assertEqual(export_resp.mimetype, "application/json")

    def test_diff_endpoint(self):
        client = self.app.test_client()
        with client.session_transaction() as sess:
            sess["_user_id"] = str(self.admin.id)
            sess["_fresh"] = True

        entry = SettingsAuditLog.query.filter_by(action_type="step_update").first()
        resp = client.get(f"/admin/settings-log/{entry.id}/diff")
        self.assertEqual(resp.status_code, 200)
        diff_payload = resp.get_json()["diff"]
        self.assertTrue(diff_payload)
        self.assertIn("new", diff_payload)


if __name__ == "__main__":
    unittest.main()
