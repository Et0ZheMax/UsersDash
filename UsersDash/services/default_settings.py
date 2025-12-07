"""
Утилиты для работы с дефолтными схемами manage по тарифам.
"""
from __future__ import annotations

"""Утилиты для работы с дефолтными схемами manage по тарифам."""

import copy
import json
from functools import lru_cache
from pathlib import Path
from typing import Any

from UsersDash.models import Account
from UsersDash.services.remote_api import (
    apply_template_for_account,
    fetch_account_settings,
    fetch_template_schema,
    update_account_step_settings,
)
from UsersDash.services.tariffs import get_tariff_name_by_price

# Маппинг цены тарифа -> файл с дефолтной конфигурацией
DEFAULT_CONFIG_FILES: dict[int, str] = {
    500: "OnlyFarm_defaults.json",
    1000: "Extended_defaults.json",
    1400: "Premium_defaults.json",
}

TARIFF_TEMPLATE_ALIASES: dict[int, list[str]] = {
    500: ["500", "OnlyFarm"],
    1000: ["1000", "Extended"],
    1400: ["1400", "Premium"],
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


def _template_candidates(price: int | str | None) -> list[str]:
    normalized = _normalize_price(price)
    candidates: list[str] = []
    if normalized is None:
        return candidates

    aliases = TARIFF_TEMPLATE_ALIASES.get(normalized, [])
    candidates.extend(aliases)
    for alias in aliases:
        candidates.append(f"{normalized}{alias}")
        candidates.append(f"{alias}{normalized}")

    filename = DEFAULT_CONFIG_FILES.get(normalized)
    if filename:
        stem = Path(filename).stem
        if stem:
            candidates.append(stem)
            if stem.endswith("_defaults"):
                candidates.append(stem.replace("_defaults", ""))

    tariff_name = get_tariff_name_by_price(normalized)
    if tariff_name:
        candidates.append(tariff_name)
        candidates.append(tariff_name.replace(" ", ""))

    unique: list[str] = []
    seen = set()
    for name in candidates:
        key = name.strip()
        if not key:
            continue
        if key in seen:
            continue
        seen.add(key)
        unique.append(key)
    return unique


def _extract_schema_scripts(schema_payload: Any) -> dict[str, Any]:
    if not isinstance(schema_payload, dict):
        return {}

    scripts: Any = None

    nested = schema_payload.get("schema") if isinstance(schema_payload, dict) else None
    if isinstance(nested, dict):
        scripts = nested.get("Scripts") or nested

    if scripts is None:
        scripts = schema_payload.get("Scripts") if isinstance(schema_payload, dict) else None

    if scripts is None:
        scripts = schema_payload

    return scripts if isinstance(scripts, dict) else {}


def _default_value_by_spec(spec: dict[str, Any]) -> Any:
    stype = spec.get("type")
    if stype == "select":
        return {
            "value": spec.get("default", {}).get("value", ""),
            "options": spec.get("options", []) or [],
        }
    if stype == "bool":
        return False
    if stype == "number":
        return 0
    return ""


def _inflate_steps_with_schema(steps: list[dict], schema_scripts: dict[str, Any]) -> list[dict]:
    inflated: list[dict] = []
    for step in steps:
        copy_step = copy.deepcopy(step)
        script_id = copy_step.get("ScriptId")
        if not script_id or script_id not in schema_scripts:
            inflated.append(copy_step)
            continue

        cfg = copy_step.setdefault("Config", {})
        fields_spec = schema_scripts[script_id].get("fields") if isinstance(schema_scripts[script_id], dict) else None
        if not isinstance(fields_spec, dict):
            inflated.append(copy_step)
            continue

        for key, spec in fields_spec.items():
            if key not in cfg:
                cfg[key] = _default_value_by_spec(spec if isinstance(spec, dict) else {})
            elif isinstance(cfg[key], dict) and isinstance(spec, dict) and spec.get("type") == "select":
                cfg[key].setdefault("options", list(spec.get("options") or []))

        inflated.append(copy_step)

    return inflated


def _schema_scripts_for_account(account: Account) -> dict[str, Any]:
    server = getattr(account, "server", None)
    if not server:
        return {}

    schema_payload, _ = fetch_template_schema(server)
    return _extract_schema_scripts(schema_payload)


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
    file_exists = False
    if filename:
        path = _configs_root() / filename
        file_exists = path.exists() and bool(_load_defaults_from_file(path))

    # Даже если локального файла нет, можем опираться на шаблоны на сервере
    # (см. _template_candidates).
    has_templates = bool(_template_candidates(normalized))

    return file_exists or has_templates


def apply_defaults_for_account(account: Account, *, tariff_price: int | str | None = None) -> tuple[bool, str]:
    price = tariff_price if tariff_price is not None else getattr(account, "next_payment_amount", None)

    template_attempts: list[str] = []
    for template_name in _template_candidates(price):
        ok, msg = apply_template_for_account(account, template_name)
        if ok:
            label = template_name
            if msg and msg != "OK":
                return True, msg
            return True, f"Применён шаблон '{label}'"
        template_attempts.append(f"{template_name}: {msg}")

    defaults = get_default_steps_for_tariff(price)
    if not defaults:
        tariff_name = get_tariff_name_by_price(price) or "неизвестный тариф"
        extra = f"; попытки шаблонов: {', '.join(template_attempts)}" if template_attempts else ""
        return False, f"Не найдена схема по умолчанию для тарифа '{tariff_name}'{extra}"

    schema_scripts = _schema_scripts_for_account(account)
    if schema_scripts:
        defaults = _inflate_steps_with_schema(defaults, schema_scripts)

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

    fallback_note = (
        f" (шаблоны: {', '.join(template_attempts)})"
        if template_attempts
        else ""
    )

    return True, f"OK{fallback_note}".strip()
