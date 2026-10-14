import unittest
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch, AsyncMock
from types import SimpleNamespace
import emulator_health as health


def sample(ready=None, failed=None, windows=None):
    return {'ready': ready or [], 'failed': failed or {}, 'windows': windows or [],
            'headless': ready or [], 'gn_running': True, 'errors': []}


class HealthTests(unittest.TestCase):
    def test_mass_restart_loop_is_reported_despite_booted_android(self):
        value = sample(ready=[1, 2, 3], windows=[1, 2, 3])
        value['restart_loop'] = True
        _, send, signature, _ = health.evaluate(value, {}, 1000)
        self.assertTrue(send)
        self.assertEqual(signature, 'restart-loop')

    def test_restart_log_requires_multiple_accounts_and_absent_progress(self):
        now = 1791447300
        lines = []
        for offset in (290, 250, 210, 170, 130, 90, 50):
            stamp = datetime.fromtimestamp(now - offset, timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            lines.append(f'{stamp}.000 +00:00 [INF] INFO|{offset % 3:016x}|Launch: We will now restart the instance')
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / 'bot20261008.txt'
            log.write_text('\n'.join(lines), encoding='utf-8')
            self.assertTrue(health.restart_loop(directory, now))
            stamp = datetime.fromtimestamp(now - 5, timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            with log.open('a', encoding='utf-8') as stream:
                stream.write(f'\n{stamp}.000 +00:00 [INF] INFO|ffffffffffffffff|getMap: Found world')
            self.assertTrue(health.restart_loop(directory, now))
            with log.open('a', encoding='utf-8') as stream:
                stream.write(f'\n{stamp}.000 +00:00 [INF] getMap: Found world')
            self.assertFalse(health.restart_loop(directory, now))

    def test_fresh_window_not_alerted_but_persistent_missing_vm_is(self):
        value = sample(failed={'9': 'missing VM'}, windows=[9])
        first, send, _, _ = health.evaluate(value, {}, 1000)
        self.assertFalse(send)
        state, send, signature, _ = health.evaluate(value, first, 1180)
        self.assertTrue(send)
        self.assertEqual(signature, 'all')

    def test_all_outage_survives_instance_rotation(self):
        first, _, _, _ = health.evaluate(sample(failed={'1': 'offline'}, windows=[1]), {}, 1000)
        _, send, signature, _ = health.evaluate(sample(failed={'2': 'offline'}, windows=[2]), first, 1180)
        self.assertTrue(send)
        self.assertEqual(signature, 'all')

    def test_partial_failure_and_recovery(self):
        value = sample(ready=[1], failed={'9': 'missing VM'}, windows=[1, 9])
        first, _, _, _ = health.evaluate(value, {}, 1000)
        state, send, signature, _ = health.evaluate(value, first, 1180)
        self.assertTrue(send)
        self.assertEqual(signature, '9')
        state.update(last_signature='9', last_sent=1180)
        _, send, _, _ = health.evaluate(value, state, 1240)
        self.assertFalse(send)
        healed, send, _, _ = health.evaluate(sample(ready=[1, 9], windows=[1, 9]), state, 1240)
        self.assertFalse(send)
        self.assertEqual(healed['failed_since'], {})
        self.assertIsNone(healed['outage_since'])

    def test_stopped_farm_is_not_an_outage(self):
        value = sample()
        value['gn_running'] = False
        _, send, _, _ = health.evaluate(value, {}, 1000)
        self.assertFalse(send)

    def test_snapshot_requires_boot_complete_and_ignores_other_virtualbox(self):
        processes = [SimpleNamespace(info={'name': 'dnplayer.exe', 'cmdline': ['index=9|'],
                                          'exe': r'C:\LDPlayer\dnplayer.exe', 'create_time': 0}),
                     SimpleNamespace(info={'name': 'Ld9BoxHeadless.exe', 'cmdline': ['--comment', 'leidian9']}),
                     SimpleNamespace(info={'name': 'VBoxHeadless.exe', 'cmdline': ['--comment', 'homeos']})]
        with patch.object(health.watchdog.psutil, 'process_iter', return_value=processes), \
             patch.object(health.watchdog, 'is_process_running', return_value=True), \
             patch.object(health.subprocess, 'run', return_value=SimpleNamespace(returncode=0, stdout='0\n')):
            result = health.snapshot()
        self.assertEqual(result['ready'], [])
        self.assertEqual(result['headless'], [9])
        self.assertIn('9', result['failed'])


class DeliveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_failure_does_not_claim_delivery(self):
        bot = AsyncMock()
        bot.__aenter__.return_value = bot
        bot.send_message.side_effect = RuntimeError('secret must not be printed')
        cfg = SimpleNamespace(chat_id='1', thread_id=None)
        with patch.object(health.watchdog, 'build_bot', return_value=bot):
            with self.assertRaisesRegex(RuntimeError, '^Telegram delivery failed$'):
                await health.send_verified(cfg, 'test')


if __name__ == '__main__':
    unittest.main()
