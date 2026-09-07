"""Настраивает защищённую обратную связь RSSv7 с локальным UsersDash."""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import tempfile
from pathlib import Path
from typing import Sequence


def configure_link(config_path: Path, database_path: Path, server_name: str, url: str) -> None:
    """Берёт серверный токен из UsersDash DB и атомарно сохраняет его в RSSv7 config."""

    connection = sqlite3.connect(f"file:{database_path}?mode=ro", uri=True, timeout=30)
    try:
        row = connection.execute(
            "SELECT api_token FROM servers WHERE name = ?",
            (server_name,),
        ).fetchone()
    finally:
        connection.close()
    token = str(row[0] if row else "").strip()
    if not token:
        raise RuntimeError(f"Для сервера {server_name} в UsersDash не задан api_token")

    config = json.loads(config_path.read_text(encoding="utf-8-sig"))
    if not isinstance(config, dict):
        raise RuntimeError("RSSv7 config должен содержать JSON-объект")
    config["SERVER_NAME"] = server_name
    config["USERSDASH_API_URL"] = url.rstrip("/")
    config["USERSDASH_API_TOKEN"] = token

    fd, temp_name = tempfile.mkstemp(
        prefix=f".{config_path.name}.",
        suffix=".tmp",
        dir=str(config_path.parent),
        text=True,
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(config, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, config_path)
    except Exception:
        try:
            os.unlink(temp_name)
        except OSError:
            pass
        raise


def main(argv: Sequence[str] | None = None) -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--server", required=True)
    parser.add_argument("--url", required=True)
    args = parser.parse_args(argv)
    configure_link(args.config, args.database, args.server, args.url)
    print(f"Связь RSSv7 -> UsersDash настроена для сервера {args.server}; токен не выводится.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
