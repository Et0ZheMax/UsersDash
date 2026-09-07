"""Тесты безопасной настройки обратной связи RSSv7 с UsersDash."""

from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from configure_usersdash_link import configure_link


class ConfigureUsersDashLinkTests(unittest.TestCase):
    def test_saves_server_name_url_and_matching_token(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config_path = root / "config.json"
            database_path = root / "app.db"
            config_path.write_text(
                json.dumps({"SERVER_NAME": "", "existing": True}),
                encoding="utf-8",
            )
            connection = sqlite3.connect(database_path)
            try:
                connection.execute("CREATE TABLE servers (name TEXT, api_token TEXT)")
                connection.executemany(
                    "INSERT INTO servers (name, api_token) VALUES (?, ?)",
                    [("207", "wrong-token"), ("208", "right-token")],
                )
                connection.commit()
            finally:
                connection.close()

            configure_link(config_path, database_path, "208", "http://127.0.0.1:5555/")

            configured = json.loads(config_path.read_text(encoding="utf-8"))
            self.assertEqual(configured["SERVER_NAME"], "208")
            self.assertEqual(configured["USERSDASH_API_URL"], "http://127.0.0.1:5555")
            self.assertEqual(configured["USERSDASH_API_TOKEN"], "right-token")
            self.assertTrue(configured["existing"])


if __name__ == "__main__":
    unittest.main()
