import json
import unittest


from UsersDash.client_views import _extract_steps_and_menu


class ExtractStepsAndMenuTests(unittest.TestCase):
    def test_parses_root_json_string(self):
        payload = json.dumps({"Data": [{"ScriptId": "foo"}]})

        steps, menu, debug = _extract_steps_and_menu(payload, return_debug=True)

        self.assertEqual(len(steps), 1)
        self.assertEqual(steps[0].get("ScriptId"), "foo")
        self.assertEqual(menu, {})
        self.assertEqual(debug["steps_count"], 1)

    def test_parses_data_string_inside_dict(self):
        payload = {"Data": json.dumps([{ "ScriptId": "bar", "Config": {"Name": "Test"}}])}

        steps, menu, debug = _extract_steps_and_menu(payload, return_debug=True)

        self.assertEqual(len(steps), 1)
        self.assertEqual(steps[0].get("ScriptId"), "bar")
        self.assertEqual(steps[0].get("Config", {}).get("Name"), "Test")
        self.assertEqual(menu, {})
        self.assertEqual(debug["steps_count"], 1)


if __name__ == "__main__":
    unittest.main()
