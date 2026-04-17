import unittest

from UsersDash.tools.farmdata_restore_gui import FarmDataRow, build_restore_candidates, build_status_text


class BuildRestoreCandidatesTests(unittest.TestCase):
    def test_matches_by_account_id_when_ids_same(self):
        main_rows = [
            FarmDataRow(
                id=10,
                account_id=100,
                user_id=1,
                farm_name="Farm A",
                email=None,
                login=None,
                password=None,
                next_payment_at=None,
                next_payment_amount=None,
            )
        ]
        backup_rows = [
            FarmDataRow(
                id=99,
                account_id=100,
                user_id=1,
                farm_name="Farm A",
                email=None,
                login="login-a",
                password="pass-a",
                next_payment_at=None,
                next_payment_amount=None,
            )
        ]

        candidates = build_restore_candidates(main_rows, backup_rows)

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].row_id, 10)
        self.assertEqual(candidates[0].backup_login, "login-a")
        self.assertEqual(candidates[0].backup_password, "pass-a")

    def test_falls_back_to_user_and_farm_when_account_id_changed(self):
        main_rows = [
            FarmDataRow(
                id=15,
                account_id=555,
                user_id=3,
                farm_name=" Alpha Farm ",
                email=None,
                login=None,
                password="",
                next_payment_at=None,
                next_payment_amount=None,
            )
        ]
        backup_rows = [
            FarmDataRow(
                id=77,
                account_id=321,
                user_id=3,
                farm_name="alpha farm",
                email=None,
                login="restored-login",
                password="restored-pass",
                next_payment_at=None,
                next_payment_amount=None,
            )
        ]

        candidates = build_restore_candidates(main_rows, backup_rows)

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].row_id, 15)
        self.assertTrue(candidates[0].can_restore_login)
        self.assertTrue(candidates[0].can_restore_password)

    def test_restores_email_and_payment_fields(self):
        main_rows = [
            FarmDataRow(
                id=20,
                account_id=1001,
                user_id=7,
                farm_name="Farm Z",
                email=None,
                login="main-login",
                password="main-pass",
                next_payment_at=None,
                next_payment_amount=None,
            )
        ]
        backup_rows = [
            FarmDataRow(
                id=22,
                account_id=1001,
                user_id=7,
                farm_name="Farm Z",
                email="mail@example.com",
                login="main-login",
                password="main-pass",
                next_payment_at="2026-04-20 00:00:00",
                next_payment_amount=1200,
            )
        ]

        candidates = build_restore_candidates(main_rows, backup_rows)

        self.assertEqual(len(candidates), 1)
        self.assertTrue(candidates[0].can_restore_email)
        self.assertTrue(candidates[0].can_restore_next_payment_at)
        self.assertTrue(candidates[0].can_restore_next_payment_amount)

    def test_does_not_restore_payment_without_account_id(self):
        main_rows = [
            FarmDataRow(
                id=30,
                account_id=None,
                user_id=8,
                farm_name="Farm Q",
                email=None,
                login=None,
                password=None,
                next_payment_at=None,
                next_payment_amount=None,
            )
        ]
        backup_rows = [
            FarmDataRow(
                id=31,
                account_id=None,
                user_id=8,
                farm_name="Farm Q",
                email="q@example.com",
                login=None,
                password=None,
                next_payment_at="2026-05-01 00:00:00",
                next_payment_amount=500,
            )
        ]

        candidates = build_restore_candidates(main_rows, backup_rows)

        self.assertEqual(len(candidates), 1)
        self.assertTrue(candidates[0].can_restore_email)
        self.assertFalse(candidates[0].can_restore_next_payment_at)
        self.assertFalse(candidates[0].can_restore_next_payment_amount)


class BuildStatusTextTests(unittest.TestCase):
    def test_shows_filter_hint_when_candidates_hidden(self):
        status = build_status_text(
            main_rows_count=100,
            backup_rows_count=100,
            candidates_count=12,
            visible_count=0,
            only_empty_enabled=True,
            search_text="",
        )

        self.assertIn("Кандидаты: 12", status)
        self.assertIn("Показано: 0", status)
        self.assertIn("включён фильтр только пустых полей", status)
        self.assertIn("ничего не видно из-за фильтров", status)

    def test_shows_no_matches_hint(self):
        status = build_status_text(
            main_rows_count=50,
            backup_rows_count=48,
            candidates_count=0,
            visible_count=0,
            only_empty_enabled=False,
            search_text="farm-1",
        )

        self.assertIn("поиск: «farm-1»", status)
        self.assertIn("совпадений для восстановления не найдено", status)


if __name__ == "__main__":
    unittest.main()
