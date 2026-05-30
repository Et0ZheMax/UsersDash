import unittest
from types import SimpleNamespace
from unittest.mock import patch

from UsersDash.services import remote_api
from UsersDash.scripts import sync_menu_data


class RemoteApiMenuDataSyncTestCase(unittest.TestCase):
    def test_update_account_menu_data_matches_bot_config_shape(self):
        account = SimpleNamespace(id=10, name="Airat", server=SimpleNamespace(name="S1"))
        settings = {
            "Data": [{"ScriptId": "vikingbot.base.gathervip"}],
            "MenuData": {
                "ScriptId": "appmenu",
                "OrderId": 1,
                "Config": {
                    "Email": "old@example.com",
                    "Password": "old-pass",
                },
                "Id": 1,
                "IsActive": True,
                "IsCopy": False,
                "ScheduleData": {
                    "Active": False,
                    "Last": "0001-01-01T00:00:00",
                    "Daily": False,
                    "Hourly": False,
                    "Weekly": False,
                },
                "ScheduleRules": [],
            },
        }

        captured_payloads = []

        def fake_update(_account, payload):
            captured_payloads.append(payload)
            return True, "OK"

        with patch.object(remote_api, "fetch_account_settings", return_value=settings), patch.object(
            remote_api,
            "update_account_settings_full",
            side_effect=fake_update,
        ):
            ok, msg = remote_api.update_account_menu_data(
                account,
                email="agalyaetdinov@yandex.ru",
                password="Parol2024!",
                igg_id="1961805684",
            )

        self.assertTrue(ok)
        self.assertEqual(msg, "OK")
        self.assertEqual(len(captured_payloads), 1)

        payload = captured_payloads[0]
        self.assertIs(payload["Data"], settings["Data"])
        self.assertIsInstance(payload["MenuData"], dict)
        self.assertEqual(payload["MenuData"]["ScriptId"], "appmenu")
        self.assertEqual(
            payload["MenuData"]["Config"],
            {
                "Email": "agalyaetdinov@yandex.ru",
                "Password": "Parol2024!",
                "Custom": "1961805684",
                "Slot": "igg",
            },
        )

    def test_update_account_menu_data_clears_stale_custom_value(self):
        account = SimpleNamespace(id=10, name="Airat", server=SimpleNamespace(name="S1"))
        settings = {
            "Data": [{"ScriptId": "vikingbot.base.gathervip"}],
            "MenuData": {
                "ScriptId": "appmenu",
                "Config": {
                    "Email": "old@example.com",
                    "Password": "old-pass",
                    "Custom": "1961805684",
                    "Slot": "igg",
                },
            },
        }

        captured_payloads = []

        def fake_update(_account, payload):
            captured_payloads.append(payload)
            return True, "OK"

        with patch.object(remote_api, "fetch_account_settings", return_value=settings), patch.object(
            remote_api,
            "update_account_settings_full",
            side_effect=fake_update,
        ):
            ok, _ = remote_api.update_account_menu_data(
                account,
                email="new@example.com",
                password="new-pass",
                igg_id=None,
            )

        self.assertTrue(ok)
        self.assertEqual(captured_payloads[0]["MenuData"]["Config"]["Custom"], "")
        self.assertEqual(captured_payloads[0]["MenuData"]["Config"]["Slot"], "igg")

    def test_update_account_menu_data_sets_slot_when_igg_id_is_empty(self):
        account = SimpleNamespace(id=10, name="Airat", server=SimpleNamespace(name="S1"))
        settings = {
            "Data": [{"ScriptId": "vikingbot.base.gathervip"}],
            "MenuData": {
                "ScriptId": "appmenu",
                "Config": {
                    "Email": "old@example.com",
                    "Password": "old-pass",
                    "Custom": "",
                },
            },
        }
        captured_payloads = []

        def fake_update(_account, payload):
            captured_payloads.append(payload)
            return True, "OK"

        with patch.object(remote_api, "fetch_account_settings", return_value=settings), patch.object(
            remote_api,
            "update_account_settings_full",
            side_effect=fake_update,
        ):
            ok, _ = remote_api.update_account_menu_data(
                account,
                email="Turist_sso@mail.ru",
                password="D6543210",
                igg_id=None,
            )

        self.assertTrue(ok)
        self.assertEqual(
            captured_payloads[0]["MenuData"]["Config"],
            {
                "Email": "Turist_sso@mail.ru",
                "Password": "D6543210",
                "Custom": "",
                "Slot": "igg",
            },
        )

    def test_local_menu_config_always_contains_custom_and_slot_keys(self):
        farm_data = SimpleNamespace(email="mail@example.com", password="pass", igg_id=None)

        self.assertEqual(
            sync_menu_data.build_menu_config(farm_data),
            {
                "Email": "mail@example.com",
                "Password": "pass",
                "Custom": "",
                "Slot": "igg",
            },
        )


if __name__ == "__main__":
    unittest.main()
