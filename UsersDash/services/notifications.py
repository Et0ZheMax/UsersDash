"""Уведомления в Telegram/Slack через уже заведённые вебхуки."""

from __future__ import annotations

import os
from typing import Iterable, Sequence

import requests
from flask import current_app

from UsersDash.telegram_settings import load_telegram_settings

DEFAULT_TIMEOUT = 10


def _iter_slack_hooks(config: dict) -> Iterable[str]:
    raw_hooks: Sequence[str] | str | None = config.get("SLACK_WEBHOOK_URLS") or os.environ.get(
        "SLACK_WEBHOOK_URLS"
    )
    if isinstance(raw_hooks, str):
        return [hook.strip() for hook in raw_hooks.split(",") if hook.strip()]
    if isinstance(raw_hooks, (list, tuple)):
        return [str(hook).strip() for hook in raw_hooks if str(hook).strip()]
    return []


def _iter_telegram_chats(config: dict) -> Iterable[tuple[str, str]]:
    token = config.get("TELEGRAM_BOT_TOKEN") or os.environ.get("TELEGRAM_BOT_TOKEN")
    raw_chat_ids: Sequence[str] | str | None = config.get("TELEGRAM_CHAT_IDS") or os.environ.get(
        "TELEGRAM_CHAT_IDS"
    )

    if not token or not raw_chat_ids:
        file_token, file_chat_ids = load_telegram_settings()
        token = token or file_token
        raw_chat_ids = raw_chat_ids or file_chat_ids

    if not token or not raw_chat_ids:
        return []

    if isinstance(raw_chat_ids, str):
        chat_ids = [cid.strip() for cid in raw_chat_ids.split(",") if cid.strip()]
    elif isinstance(raw_chat_ids, (list, tuple)):
        chat_ids = [str(cid).strip() for cid in raw_chat_ids if str(cid).strip()]
    else:
        chat_ids = []

    return [(token, chat_id) for chat_id in chat_ids]


def _log_fallback(message: str) -> None:
    if current_app:
        current_app.logger.info("[notify] %s", message)
    else:
        print(f"[notify] {message}")


def send_notification(message: str) -> None:
    """
    Отправляет уведомление в доступные каналы.

    Поддерживаются:
    - Slack вебхуки (SLACK_WEBHOOK_URLS в конфиге или env);
    - Telegram бот (TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_IDS в конфиге, env или файле
      telegram_settings.json).
    Ошибки при отправке логируются и не прерывают выполнение.
    """

    if not message:
        return

    config = current_app.config if current_app else {}
    delivered = False

    for webhook in _iter_slack_hooks(config):
        try:
            resp = requests.post(webhook, json={"text": message}, timeout=DEFAULT_TIMEOUT)
            delivered = delivered or resp.ok
        except Exception as exc:  # pragma: no cover - защита от падений
            _log_fallback(f"Не удалось отправить Slack-уведомление: {exc}")

    for token, chat_id in _iter_telegram_chats(config):
        try:
            resp = requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                data={"chat_id": chat_id, "text": message},
                timeout=DEFAULT_TIMEOUT,
            )
            delivered = delivered or resp.ok
            if not resp.ok:
                _log_fallback(
                    f"Не удалось отправить Telegram-уведомление в чат {chat_id}: "
                    f"{resp.status_code} {resp.text}"
                )
        except Exception as exc:  # pragma: no cover - защита от падений
            _log_fallback(f"Не удалось отправить Telegram-уведомление: {exc}")

    if not delivered:
        _log_fallback(message)
