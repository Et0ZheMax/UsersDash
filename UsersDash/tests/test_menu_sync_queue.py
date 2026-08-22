import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from flask import Flask

from UsersDash.models import Account, FarmData, FarmMenuSyncJob, Server, User, db
from UsersDash.services import menu_sync_queue


class MenuSyncQueueTestCase(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.app = Flask(__name__)
        db_path = Path(self.temp_dir.name) / "queue.db"
        self.app.config.update(
            SQLALCHEMY_DATABASE_URI=f"sqlite:///{db_path.as_posix()}",
            SQLALCHEMY_TRACK_MODIFICATIONS=False,
        )
        db.init_app(self.app)
        with self.app.app_context():
            db.create_all()
            owner = User(username="owner", password_hash="hash", role="client")
            server = Server(name="F99", host="f99")
            db.session.add_all([owner, server])
            db.session.flush()
            account = Account(
                name="German1",
                internal_id="remote-id",
                owner_id=owner.id,
                server_id=server.id,
            )
            db.session.add(account)
            db.session.flush()
            db.session.add(
                FarmData(
                    account_id=account.id,
                    user_id=owner.id,
                    farm_name=account.name,
                    email="mail@example.com",
                    password="pass",
                    igg_id="123",
                )
            )
            db.session.commit()
            self.account_id = account.id

    def tearDown(self):
        with self.app.app_context():
            db.session.remove()
            db.drop_all()
            db.engine.dispose()
        self.temp_dir.cleanup()

    def test_job_is_persisted_and_confirmed(self):
        with self.app.app_context():
            jobs = menu_sync_queue.enqueue_menu_sync([self.account_id])
            self.assertEqual(jobs[0]["status"], "pending")

        with patch.object(
            menu_sync_queue, "update_account_menu_data", return_value=(True, "OK")
        ):
            self.assertTrue(menu_sync_queue._process_one(self.app))

        with self.app.app_context():
            job = FarmMenuSyncJob.query.filter_by(account_id=self.account_id).one()
            self.assertEqual(job.status, "succeeded")
            self.assertIsNone(job.error)

    def test_job_exposes_error_after_three_failed_attempts(self):
        with self.app.app_context():
            menu_sync_queue.enqueue_menu_sync([self.account_id])

        with patch.object(
            menu_sync_queue,
            "update_account_menu_data",
            return_value=(False, "profile unavailable"),
        ):
            for _ in range(3):
                self.assertTrue(menu_sync_queue._process_one(self.app))

        with self.app.app_context():
            job = FarmMenuSyncJob.query.filter_by(account_id=self.account_id).one()
            self.assertEqual(job.status, "failed")
            self.assertEqual(job.attempts, 3)
            self.assertEqual(job.error, "profile unavailable")


if __name__ == "__main__":
    unittest.main()
