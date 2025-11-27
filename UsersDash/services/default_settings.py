"""
Утилиты для работы с дефолтными схемами manage по тарифам.
"""
from __future__ import annotations

"""Утилиты для работы с дефолтными схемами manage по тарифам."""

import copy
import json
from functools import lru_cache
from pathlib import Path

from UsersDash.models import Account
from UsersDash.services.remote_api import (
    fetch_account_settings,
    update_account_step_settings,
)
from UsersDash.services.tariffs import get_tariff_name_by_price

# Маппинг цены тарифа -> файл с дефолтной конфигурацией
DEFAULT_CONFIG_FILES: dict[int, str] = {
    500: "OnlyFarm_defaults.json",
    1000: "Extended_defaults.json",
    1400: "Premium_defaults.json",
}


def _normalize_price(price: int | str | None) -> int | None:
    if price is None:
        return None
    if isinstance(price, int):
        return price
    if isinstance(price, str) and price.isdigit():
        return int(price)
    return None


def _configs_root() -> Path:
    return Path(__file__).resolve().parent.parent / "bot_farm_configs"


@lru_cache(maxsize=None)
def _load_defaults_from_file(path: Path) -> list[dict]:
    if not path.exists():
        return []

    raw_text = path.read_text().strip()
    if raw_text.endswith(","):
        raw_text = raw_text[:-1]

    try:
        payload = json.loads("{" + raw_text + "}")
    except Exception:
        return []

    data_section: list[dict] | str | dict = payload.get("Data") or payload
    if isinstance(data_section, str):
        try:
            parsed = json.loads(data_section)
        except Exception:
            return []
        return parsed if isinstance(parsed, list) else []

    if isinstance(data_section, list):
        return data_section

    return []


def get_default_steps_for_tariff(price: int | str | None) -> list[dict]:
    normalized = _normalize_price(price)
    if normalized is None:
        return []

    filename = DEFAULT_CONFIG_FILES.get(normalized)
    if not filename:
        return []

    path = _configs_root() / filename
    defaults = _load_defaults_from_file(path)
    return copy.deepcopy(defaults)


def has_defaults_for_tariff(price: int | str | None) -> bool:
    normalized = _normalize_price(price)
    if normalized is None:
        return False
    filename = DEFAULT_CONFIG_FILES.get(normalized)
    if not filename:
        return False
    path = _configs_root() / filename
    return path.exists() and bool(_load_defaults_from_file(path))


def apply_defaults_for_account(account: Account, *, tariff_price: int | str | None = None) -> tuple[bool, str]:
    price = tariff_price if tariff_price is not None else getattr(account, "next_payment_amount", None)
    defaults = get_default_steps_for_tariff(price)
    if not defaults:
        tariff_name = get_tariff_name_by_price(price) or "неизвестный тариф"
        return False, f"Не найдена схема по умолчанию для тарифа '{tariff_name}'"

    raw_settings = fetch_account_settings(account)
    existing_steps = None
    if isinstance(raw_settings, dict):
        data_section = raw_settings.get("Data") or raw_settings.get("MenuData")
        if isinstance(data_section, list):
            existing_steps = len(data_section)
    if existing_steps == 0:
        return False, "Для аккаунта нет доступных шагов manage"

    applied = 0
    skipped = 0
    for idx, step in enumerate(defaults):
        if existing_steps is not None and idx >= existing_steps:
            skipped += 1
            continue

        payload: dict = {}
        config = step.get("Config")
        if isinstance(config, dict):
            payload["Config"] = config
        if "IsActive" in step:
            payload["IsActive"] = bool(step.get("IsActive"))
        if isinstance(step.get("ScheduleRules"), list):
            payload["ScheduleRules"] = step.get("ScheduleRules") or []

        if not payload:
            continue

        ok, msg = update_account_step_settings(account, idx, payload)
        if not ok:
            return False, msg or "Не удалось обновить шаг"

        applied += 1

    if applied == 0:
        return False, "Не удалось применить настройки — нет подходящих шагов"

    if skipped:
        return True, f"Применено {applied} шагов, пропущено {skipped}"

    return True, "OK"
