import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import emulator_health as h

class NameTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);(self.root/'profiles').mkdir()
        (self.root/'config.json').write_text(json.dumps({'SelectedProfile':'LIVE'}),encoding='utf-8-sig')
    def profile(self,rows,name='LIVE'):
        (self.root/'profiles'/f'{name}.json').write_text(json.dumps(rows),encoding='utf8')
    def test_names_come_from_current_profile_and_refresh_after_remap(self):
        self.profile([dict(InstanceId=52,Name='Oo2')])
        self.assertEqual(h.account_names(self.root),{'52':'Oo2'})
        self.profile([dict(InstanceId='52',Name='New Account')])
        self.assertEqual(h.account_names(self.root),{'52':'New Account'})
    def test_missing_or_corrupt_profile_keeps_monitor_available(self):
        self.assertEqual(h.account_names(self.root),{})
        (self.root/'profiles'/'LIVE.json').write_text('broken',encoding='utf8')
        self.assertEqual(h.account_names(self.root),{})
    def test_ambiguous_binding_is_not_guessed(self):
        self.profile([dict(InstanceId=52,Name='Oo2'),dict(InstanceId=52,Name='Other')])
        self.assertEqual(h.account_names(self.root),{})
    def test_names_are_single_line_and_bounded(self):
        self.profile([dict(InstanceId=52,Name='  Oo2\n  farm  ')])
        self.assertEqual(h.account_names(self.root),{'52':'Oo2 farm'})
    def test_alert_contains_account_and_diagnostic_index(self):
        s=dict(windows=[52],headless=[52],ready=[],failed={'52':'offline'},account_names={'52':'Oo2'})
        self.assertIn('• Oo2 (LDPlayer 52): offline',h.alert_text(s,confirmed=['52']))
        s['account_names']={}
        self.assertIn('• LDPlayer 52: offline',h.alert_text(s,confirmed=['52']))
    def test_server_label_is_not_tied_to_f99(self):
        s=dict(windows=[52],headless=[52],ready=[],failed={'52':'offline'},account_names={'52':'Oo2'})
        with patch.dict(h.os.environ,{'GNLDCHECK_SERVER_LABEL':'208'}):
            self.assertTrue(h.alert_text(s,confirmed=['52']).startswith('208 🚨'))

if __name__=='__main__':unittest.main()
