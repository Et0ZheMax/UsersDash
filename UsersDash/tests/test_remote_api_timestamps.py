import unittest

from UsersDash.services import remote_api


class RemoteApiTimestampFormattingTestCase(unittest.TestCase):
    def test_last_updated_hides_epoch_placeholder(self):
        self.assertIsNone(remote_api._fmt_last_updated("1970-01-01T00:00:00"))
        self.assertIsNone(remote_api._fmt_last_updated("1970-01-01T00:00:00Z"))

    def test_last_updated_supports_z_suffix(self):
        self.assertEqual(
            remote_api._fmt_last_updated("2026-04-09T17:31:00Z"),
            "20:31 09.04.2026",
        )

    def test_generated_at_formats_iso_and_z(self):
        self.assertEqual(
            remote_api._fmt_generated_at("2026-04-09T17:31:00+00:00"),
            "09.04 20:31",
        )
        self.assertEqual(
            remote_api._fmt_generated_at("2026-04-09T17:31:00Z"),
            "09.04 20:31",
        )


if __name__ == "__main__":
    unittest.main()
