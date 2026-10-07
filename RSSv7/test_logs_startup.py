"""Проверки миграции и освобождения БД без запуска Windows-приложения."""

import ast
from contextlib import closing
from datetime import datetime
from pathlib import Path
import re
import sqlite3
import tempfile
import unittest
import uuid
import os


class LogsStartupTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.statements = []
        self.connections = []

        def open_db(path):
            connection = sqlite3.connect(path)
            connection.row_factory = sqlite3.Row
            connection.set_trace_callback(self.statements.append)
            self.connections.append(connection)
            return connection

        self.env = {
            "open_db": open_db,
            "LOGS_DB": str(root / "logs.db"),
            "RESOURCES_DB": str(root / "resources.db"),
            "LOGS_DIR": str(root),
            "sqlite3": sqlite3, "os": os, "uuid": uuid,
            "LOG_PATTERN": re.compile(r"(?!)"),
            "_DT_RE": re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} [+-]\d{2}:\d{2})"),
        }
        tree = ast.parse(Path(__file__).with_name("RssCounterWebV7.py").read_text(encoding="utf-8"))
        functions = [node for node in tree.body if isinstance(node, ast.FunctionDef)
                     and node.name in {"init_logs_db", "do_resources_update"}]
        exec(compile(ast.Module(body=functions, type_ignores=[]), "rssv7", "exec"), self.env)

    def test_repeat_startup_does_not_scan_or_rewrite_logs(self) -> None:
        self.env["init_logs_db"]()
        with closing(sqlite3.connect(self.env["LOGS_DB"])) as conn, conn:
            conn.execute("INSERT INTO cached_logs(acc_id, dt, raw_line, source_id) VALUES('a','d','r','s')")
        self.statements.clear()
        self.env["init_logs_db"]()
        sql = "\n".join(self.statements).upper()
        self.assertNotIn("DELETE FROM", sql)
        self.assertNotIn("UPDATE CACHED_LOGS", sql)
        self.assertNotIn("SELECT ID, ACC_ID", sql)
        with closing(sqlite3.connect(self.env["LOGS_DB"])) as conn, conn:
            self.assertEqual(conn.execute("SELECT source_id FROM cached_logs").fetchall(), [("s",)])

    def test_legacy_schema_is_migrated_once(self) -> None:
        with closing(sqlite3.connect(self.env["LOGS_DB"])) as conn, conn:
            conn.execute("CREATE TABLE cached_logs(id INTEGER PRIMARY KEY,acc_id TEXT,nickname TEXT,dt TEXT,raw_line TEXT)")
            conn.executemany("INSERT INTO cached_logs VALUES(?, 'a', 'n', 'd', 'r')", [(1,), (2,)])
        self.env["init_logs_db"]()
        with closing(sqlite3.connect(self.env["LOGS_DB"])) as conn, conn:
            rows = conn.execute("SELECT id,source_id FROM cached_logs").fetchall()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0][0], 1)
            self.assertTrue(rows[0][1].startswith("legacy:"))

    def test_failed_parse_closes_connections_and_keeps_offset(self) -> None:
        self.env["init_logs_db"]()
        with closing(sqlite3.connect(self.env["LOGS_DB"])) as conn, conn:
            conn.execute("CREATE TRIGGER fail_insert BEFORE INSERT ON cached_logs BEGIN SELECT RAISE(ABORT, 'failure'); END")
        root = Path(self.temp.name)
        today = datetime.now().strftime("%Y%m%d")
        (root / f"bot{today}.txt").write_text("2026-10-07 12:00:00.000 +03:00 |acc| event\n", encoding="utf-8")
        self.connections.clear()
        with self.assertRaisesRegex(sqlite3.IntegrityError, "failure"):
            self.env["do_resources_update"]({"acc": "farm"})
        for connection in self.connections:
            with self.assertRaises(sqlite3.ProgrammingError):
                connection.execute("SELECT 1")
        with closing(sqlite3.connect(self.env["LOGS_DB"])) as conn, conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM files_offset").fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM cached_logs").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
