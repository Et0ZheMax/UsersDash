import unittest

from UsersDash.tools.farmdata_restore_gui import FarmDataRow, build_restore_candidates


class BuildRestoreCandidatesTests(unittest.TestCase):
    def test_matches_by_account_id_when_ids_same(self):
        main_rows = [
            FarmDataRow(
                id=10,
                account_id=100,
                user_id=1,
                farm_name="Farm A",
                login=None,
                password=None,
            )
        ]
        backup_rows = [
            FarmDataRow(
                id=99,
                account_id=100,
                user_id=1,
                farm_name="Farm A",
                login="login-a",
                password="pass-a",
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
                login=None,
                password="",
            )
        ]
        backup_rows = [
            FarmDataRow(
                id=77,
                account_id=321,
                user_id=3,
                farm_name="alpha farm",
                login="restored-login",
                password="restored-pass",
            )
        ]

        candidates = build_restore_candidates(main_rows, backup_rows)

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].row_id, 15)
        self.assertTrue(candidates[0].can_restore_login)
        self.assertTrue(candidates[0].can_restore_password)


if __name__ == "__main__":
    unittest.main()
