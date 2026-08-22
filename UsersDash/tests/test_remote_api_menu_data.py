import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from UsersDash.services import remote_api
from UsersDash.scripts import sync_menu_data


class RemoteApiMenuDataSyncTestCase(unittest.TestCase):
    def test_profile_update_rejects_missing_remote_account(self):
        account = SimpleNamespace(
            id=10,
            name="German1",
            internal_id="missing-id",
            server=SimpleNamespace(name="F99"),
        )
        response = Mock(status_code=200)
        response.json.return_value = {
            "status": "error",
            "error": "account not found",
            "missing": ["missing-id"],
        }

        with patch.object(remote_api, "_get_effective_api_base", return_value="http://f99/api"), patch.object(
            remote_api, "fetch_resources_for_server", return_value={}
        ), patch.object(remote_api.requests, "patch", return_value=response):
            ok, msg = remote_api.update_account_profile_menu_data(
                account,
                email="mail@example.com",
                password="password",
                igg_id="1960188204",
            )

        self.assertFalse(ok)
        self.assertEqual(msg, "account not found")

    def test_profile_update_accepts_confirmed_remote_write(self):
        account = SimpleNamespace(
            id=10,
            name="German1",
            internal_id="remote-id",
            server=SimpleNamespace(name="F99"),
        )
        response = Mock(status_code=200)
        response.json.return_value = {"status": "ok", "updated": 1, "missing": []}

        with patch.object(remote_api, "_get_effective_api_base", return_value="http://f99/api"), patch.object(
            remote_api, "fetch_resources_for_server", return_value={}
        ), patch.object(remote_api.requests, "patch", return_value=response):
            ok, msg = remote_api.update_account_profile_menu_data(
                account,
                email="mail@example.com",
                password="password",
                igg_id="1960188204",
            )

        self.assertTrue(ok)
        self.assertEqual(msg, "OK")

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

        with patch.object(
            remote_api, "update_account_profile_menu_data", return_value=(False, "legacy endpoint")
        ), patch.object(remote_api, "fetch_account_settings", return_value=settings), patch.object(
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

        with patch.object(
            remote_api, "update_account_profile_menu_data", return_value=(False, "legacy endpoint")
        ), patch.object(remote_api, "fetch_account_settings", return_value=settings), patch.object(
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

        with patch.object(
            remote_api, "update_account_profile_menu_data", return_value=(False, "legacy endpoint")
        ), patch.object(remote_api, "fetch_account_settings", return_value=settings), patch.object(
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

    def test_update_account_menu_data_uses_fast_patch_without_fetching_settings(self):
        account = SimpleNamespace(id=10, name="German1", server=SimpleNamespace(name="F99"))

        with patch.object(
            remote_api, "update_account_profile_menu_data", return_value=(True, "OK")
        ) as profile_update, patch.object(
            remote_api, "fetch_account_settings"
        ) as fetch_settings, patch.object(
            remote_api, "update_account_settings_full"
        ) as full_update:
            ok, msg = remote_api.update_account_menu_data(
                account,
                email="mail@example.com",
                password="pass",
                igg_id="1960188204",
            )

        self.assertTrue(ok)
        self.assertEqual(msg, "OK")
        profile_update.assert_called_once()
        fetch_settings.assert_not_called()
        full_update.assert_not_called()

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
