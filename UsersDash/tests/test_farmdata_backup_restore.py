import unittest

from UsersDash.services.farmdata_backup_restore import (
    FarmDataBackupRow,
    build_farmdata_restore_candidates,
)


class FarmDataBackupRestoreCandidateTests(unittest.TestCase):
    def test_builds_changes_for_visible_and_hidden_farm_fields(self):
        main_rows = [
            FarmDataBackupRow(
                row_id=1,
                account_id=10,
                user_id=2,
                owner_name="client",
                farm_name="Farm A",
                values={
                    "email": None,
                    "login": None,
                    "password": "old-pass",
                    "igg_id": None,
                    "server": None,
                    "telegram_tag": None,
                    "next_payment_at": None,
                    "next_payment_amount": None,
                    "next_payment_tariff": None,
                },
            )
        ]
        backup_rows = [
            FarmDataBackupRow(
                row_id=99,
                account_id=10,
                user_id=2,
                owner_name="client",
                farm_name="Farm A",
                values={
                    "email": "mail@example.com",
                    "login": "login-a",
                    "password": "backup-pass",
                    "igg_id": "123",
                    "server": "K55",
                    "telegram_tag": "@client",
                    "next_payment_at": "2026-05-10",
                    "next_payment_amount": 1200,
                    "next_payment_tariff": 1000,
                },
            )
        ]

        candidates = build_farmdata_restore_candidates(main_rows, backup_rows)

        self.assertEqual(len(candidates), 1)
        fields = {change["field"] for change in candidates[0].changes}
        self.assertEqual(
            fields,
            {
                "email",
                "login",
                "password",
                "igg_id",
                "server",
                "telegram_tag",
                "next_payment_at",
                "next_payment_amount",
                "next_payment_tariff",
            },
        )

    def test_skips_account_payment_fields_without_account_id(self):
        main_rows = [
            FarmDataBackupRow(
                row_id=1,
                account_id=None,
                user_id=2,
                owner_name="client",
                farm_name="Farm A",
                values={
                    "email": None,
                    "login": None,
                    "password": None,
                    "igg_id": None,
                    "server": None,
                    "telegram_tag": None,
                    "next_payment_at": None,
                    "next_payment_amount": None,
                    "next_payment_tariff": None,
                },
            )
        ]
        backup_rows = [
            FarmDataBackupRow(
                row_id=99,
                account_id=None,
                user_id=2,
                owner_name="client",
                farm_name="farm a",
                values={
                    "email": "mail@example.com",
                    "login": None,
                    "password": None,
                    "igg_id": None,
                    "server": None,
                    "telegram_tag": None,
                    "next_payment_at": "2026-05-10",
                    "next_payment_amount": 1200,
                    "next_payment_tariff": 1000,
                },
            )
        ]

        candidates = build_farmdata_restore_candidates(main_rows, backup_rows)

        self.assertEqual(len(candidates), 1)
        fields = {change["field"] for change in candidates[0].changes}
        self.assertEqual(fields, {"email"})


if __name__ == "__main__":
    unittest.main()
