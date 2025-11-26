import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from UsersDash import app as app_module
from UsersDash.config import Config
from UsersDash.models import Account, SettingsAuditLog, User, db
from UsersDash.services.audit import log_settings_action


class AuditServiceTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        Config.DATA_DIR = Path(self.tmp.name)
        Config.SQLALCHEMY_DATABASE_URI = f"sqlite:///{Path(self.tmp.name) / 'test.db'}"

        self.app = app_module.create_app()
        self.app.config.update(TESTING=True)
        self.ctx = self.app.app_context()
        self.ctx.push()

        self.user = User(username="client", password_hash="x", role="client")
        self.admin = User(username="admin2", password_hash="y", role="admin")
        db.session.add_all([self.user, self.admin])
        db.session.flush()
        self.account = Account(name="Farm1", server_id=1, owner_id=self.user.id)
        db.session.add(self.account)
        db.session.commit()

    def tearDown(self):
        db.session.remove()
        self.ctx.pop()
        self.tmp.cleanup()

    def test_log_masks_sensitive_fields(self):
        log_settings_action(
            self.user,
            self.admin,
            "step_update",
            {
                "account": self.account,
                "field": "step:0",
                "old_value": {"password": "secret", "visible": "keep"},
                "new_value": {"token": "abcd", "visible": "ok"},
            },
        )

        entry = SettingsAuditLog.query.first()
        self.assertIsNotNone(entry)
        self.assertIn("***", entry.old_value)
        self.assertIn("***", entry.new_value)
        self.assertIn("keep", entry.old_value)
        self.assertIn("ok", entry.new_value)


if __name__ == "__main__":
    unittest.main()
