"""Проверка выбора IGG, сохранности профиля и восстановления сервера после ошибки."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from viking_recovery import OcrWord, RecoveryError
from viking_tools import NewFarm, Provisioner, choose_game_row, install_profile_record, read_json


class SelectionTests(unittest.TestCase):
    def test_empty_selects_first_visible_and_prefix_is_not_substring(self):
        words = [OcrWord('9999912345', 100, 127, 200, 135), OcrWord('1234567890', 100, 77, 200, 85)]
        self.assertEqual(choose_game_row(words, ''), (300, 81, '1234567890'))
        self.assertEqual(choose_game_row(words, '99999'), (300, 131, '9999912345'))
        with self.assertRaises(RecoveryError):
            choose_game_row(words, '91234')

    def test_duplicate_prefix_stops_instead_of_logging_into_wrong_farm(self):
        words = [OcrWord('1234567890', 100, 77, 200, 85), OcrWord('1234599999', 100, 127, 200, 135)]
        with self.assertRaises(RecoveryError):
            choose_game_row(words, '12345')

    def test_other_numbers_in_row_do_not_pollute_igg_id(self):
        words = [OcrWord('1234567890', 100, 77, 200, 85),
                 OcrWord('2026/10/04', 300, 77, 400, 85), OcrWord('25', 440, 77, 450, 85)]
        self.assertEqual(choose_game_row(words, ''), (300, 81, '1234567890'))


class ProvisionTests(unittest.TestCase):
    def test_profile_creation_preserves_other_records_and_full_igg_id(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'profile.json'
            original = dict(Id='existing', Name='Old', AppId='vikingbot', InstanceId=7, Active=True,
                            Data='[]', MenuData=json.dumps({'Config': {'Email': 'old', 'Password': 'old'}}))
            path.write_text(json.dumps([original]), encoding='utf-8')
            engine = Mock(profile_path=path)
            item = NewFarm('New', 'new@example.test', 'pw', '12345', 500)
            steps = [{'ScriptId': 'vikingbot.base.gathervip', 'IsActive': True}]
            install_profile_record(engine, item, steps, 8, '1234567890', True)
            saved = read_json(path)
            self.assertEqual(saved[0], original)
            self.assertEqual(saved[1]['InstanceId'], 8)
            self.assertEqual(json.loads(saved[1]['MenuData'])['Config']['Custom'], '1234567890')
            self.assertEqual(len(list(path.parent.glob('*.before_tools_*'))), 1)

    def test_failed_authorization_restores_maintenance_and_keeps_clone_checkpoint(self):
        with tempfile.TemporaryDirectory() as folder:
            config = Mock(state_dir=Path(folder))
            config.template.return_value = [{'ScriptId': 'gather', 'IsActive': True}]
            engine = Mock()
            engine.clone_template.return_value = 8
            engine.open_login.side_effect = RecoveryError('IGG недоступен')
            provisioner = Provisioner(engine, config)
            provisioner.preflight = Mock()
            provisioner.maintenance = Mock()
            provisioner.maintenance.state_path.exists.return_value = True
            item = NewFarm('New', 'new@example.test', 'pw')
            with patch('viking_tools.install_profile_record'):
                with self.assertRaisesRegex(RecoveryError, 'IGG недоступен'):
                    provisioner.create([item])
            provisioner.maintenance.resume.assert_called_once()
            saved = read_json(provisioner.journal_path)
            self.assertEqual(saved['items'][0]['index'], 8)
            self.assertEqual(saved['items'][0]['stage'], 'cloned')
            self.assertNotIn('password', json.dumps(saved))
            config.dash.complete.assert_not_called()

    def test_retry_notification_does_not_clone_or_pause_server(self):
        from viking_tools import atomic_json

        with tempfile.TemporaryDirectory() as folder:
            config = Mock(state_dir=Path(folder))
            engine = Mock()
            provisioner = Provisioner(engine, config)
            provisioner.maintenance = Mock()
            item = NewFarm('New', 'new@example.test', 'pw')
            atomic_json(provisioner.journal_path, {'status': 'running', 'items': [
                dict(operation_id=item.operation_id, stage='completed', index=8, igg_id='1234567890'),
            ]})
            provisioner.create([item], resume=True)
            provisioner.maintenance.pause.assert_not_called()
            engine.clone_template.assert_not_called()
            config.dash.call.assert_called_once_with('/notify', {'internal_ids': [item.operation_id]})
            self.assertEqual(read_json(provisioner.journal_path)['status'], 'completed')


if __name__ == '__main__':
    unittest.main()
