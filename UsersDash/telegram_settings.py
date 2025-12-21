"""Загрузка локальных настроек Telegram для отправки уведомлений."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

SETTINGS_PATH = Path(__file__).resolve().parent / "telegram_settings.json"


def load_telegram_settings(path: Path = SETTINGS_PATH) -> tuple[str | None, list[str]]:
    """
    Возвращает токен и список chat_id для Telegram.

    Настройки читаются из JSON-файла вида:
    {
      "token": "123456:ABCDEF",
      "chat_ids": ["123", "456"]
    }
    Если файл отсутствует или не валиден, возвращаются пустые значения.
    """

    token: str | None = None
    chat_ids: list[str] = []

    if not path.exists():
        return token, chat_ids

    try:
        raw_data: dict[str, object] = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # pragma: no cover - защитный вывод
        print(f"[telegram_settings] Не удалось прочитать {path}: {exc}")
        return token, chat_ids

    token = str(raw_data.get("token") or "").strip() or None

    raw_chat_ids: Iterable[str] | str | None = raw_data.get("chat_ids")  # type: ignore[assignment]
    if isinstance(raw_chat_ids, str):
        chat_ids = [cid.strip() for cid in raw_chat_ids.split(",") if cid.strip()]
    elif isinstance(raw_chat_ids, (list, tuple)):
        chat_ids = [str(cid).strip() for cid in raw_chat_ids if str(cid).strip()]

    return token, chat_ids
