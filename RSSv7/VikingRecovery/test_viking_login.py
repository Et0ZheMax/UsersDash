"""Модульные тесты безопасного выбора эмулятора VikingLogin."""

from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import Mock

from viking_login import LoginEngine, build_login_targets
from viking_recovery import Farm, Instance


class LoginTargetTests(unittest.TestCase):
    def test_matches_by_instance_id_even_when_names_differ(self) -> None:
        farm = Farm("Akhm2", "mail", "password", "203689", "igg", True, "farm-id", 5)
        targets = build_login_targets([Instance(5, "Akhm2Login-key")], [farm])

        self.assertEqual(len(targets), 1)
        self.assertIs(targets[0].farm, farm)
        self.assertTrue(targets[0].ready)

    def test_excludes_clean_template_id_zero(self) -> None:
        targets = build_login_targets([Instance(0, "clean"), Instance(5, "Akhm2")], [])

        self.assertEqual([target.instance.index for target in targets], [5])

    def test_offers_each_farm_when_instance_is_shared(self) -> None:
        farms = [
            Farm("One", "mail", "password", "1", "igg", True, "one", 5),
            Farm("Two", "mail", "password", "2", "igg", True, "two", 5),
        ]
        targets = build_login_targets([Instance(5, "Shared")], farms)

        self.assertEqual([target.farm.name for target in targets if target.farm], ["One", "Two"])
        self.assertTrue(all(target.ready for target in targets))

    def test_login_uses_existing_instance_without_rename_or_profile_update(self) -> None:
        farm = Farm("Akhm2", "mail", "password", "203689", "igg", True, "farm-id", 5)
        instance = Instance(5, "Akhm2")
        engine = object.__new__(LoginEngine)
        engine.logger = Mock()
        engine.new_index = None
        engine.validate_login_environment = Mock()
        engine.list_instances = Mock(return_value=[instance])
        engine._validate_instance_storage = Mock()
        engine.launch_and_wait = Mock()
        engine.open_login = Mock()
        engine.submit_credentials = Mock()
        engine.select_game_id = Mock()
        engine.wait_for_game = Mock()
        engine.ldconsole = Path("ldconsole.exe")

        result = engine.login(farm, 5)

        self.assertIs(result, instance)
        self.assertEqual(engine.new_index, 5)
        engine._validate_instance_storage.assert_called_once_with(5)
        engine.submit_credentials.assert_called_once_with(farm)
        engine.select_game_id.assert_called_once_with(farm)


if __name__ == "__main__":
    unittest.main()
