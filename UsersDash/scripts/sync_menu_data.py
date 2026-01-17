"""Скрипт синхронизации MenuData из данных фермы.

Поддерживает два режима:
- обновление локальных JSON в bot_farm_configs;
- отправка данных на серверы через API.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from UsersDash.app import create_app  # noqa: E402
from UsersDash.models import Account, FarmData  # noqa: E402
from UsersDash.services.remote_api import update_account_menu_data  # noqa: E402


def parse_args() -> argparse.Namespace:
    """Читает параметры командной строки."""
    parser = argparse.ArgumentParser(
        description=(
            "Синхронизирует MenuData из таблиц Account/FarmData и обновляет "
            "локальные конфиги или серверы."
        ),
    )
    parser.add_argument(
        "--mode",
        choices=("local", "server", "both"),
        default="local",
        help="Режим работы: local, server или both (по умолчанию local).",
    )
    parser.add_argument(
        "--configs-dir",
        default=str(Path("UsersDash") / "bot_farm_configs"),
        help=(
            "Путь к каталогу с bot_farm_configs (по умолчанию UsersDash/bot_farm_configs)."
        ),
    )
    return parser.parse_args()


def build_menu_config(fd: FarmData) -> dict[str, str]:
    """Готовит словарь Config для MenuData на основе FarmData."""
    menu_config = {
        "Email": fd.email or "",
        "Password": fd.password or "",
    }
    if fd.igg_id is not None:
        menu_config["Custom"] = fd.igg_id or ""
        if fd.igg_id and "Slot" not in menu_config:
            menu_config["Slot"] = "igg"
    return menu_config


def load_json(path: Path) -> Any:
    """Загружает JSON-файл и возвращает структуру данных."""
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        logging.warning("[local] пропускаем %s: JSON не разобран (%s)", path, exc)
        return None


def dump_json(path: Path, payload: Any) -> None:
    """Записывает структуру данных обратно в JSON-файл."""
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def update_menu_data_payload(menu_data_raw: str, menu_config: dict[str, str]) -> str | None:
    """Обновляет MenuData.Config внутри строкового JSON и возвращает новую строку."""
    try:
        menu_data = json.loads(menu_data_raw)
    except json.JSONDecodeError:
        return None

    if not isinstance(menu_data, dict):
        return None

    current_config = menu_data.get("Config")
    if not isinstance(current_config, dict):
        current_config = {}

    current_config.update(menu_config)
    if menu_config.get("Custom") and "Slot" not in current_config:
        current_config["Slot"] = "igg"

    menu_data["Config"] = current_config
    return json.dumps(menu_data, ensure_ascii=False)


def update_local_configs(
    configs_dir: Path,
    accounts_map: dict[tuple[int, str], dict[str, str]],
) -> tuple[int, int, int]:
    """Обновляет MenuData в локальных конфигурациях.

    Возвращает кортеж (updated, skipped_no_match, skipped_invalid).
    """
    updated = 0
    skipped_invalid = 0
    found_names: set[str] = set()

    name_to_config = {
        name: cfg for (_, name), cfg in accounts_map.items()
    }

    for path in sorted(configs_dir.glob("*.json")):
        payload = load_json(path)
        if payload is None:
            skipped_invalid += 1
            continue

        if isinstance(payload, list):
            touched = False
            for entry in payload:
                if not isinstance(entry, dict):
                    continue
                entry_name = entry.get("Name")
                if not entry_name or entry_name not in name_to_config:
                    continue
                found_names.add(entry_name)
                menu_data_raw = entry.get("MenuData")
                if not isinstance(menu_data_raw, str):
                    skipped_invalid += 1
                    continue

                updated_menu_data = update_menu_data_payload(
                    menu_data_raw,
                    name_to_config[entry_name],
                )
                if updated_menu_data is None:
                    skipped_invalid += 1
                    continue

                entry["MenuData"] = updated_menu_data
                updated += 1
                touched = True

            if touched:
                dump_json(path, payload)
        else:
            skipped_invalid += 1

    skipped_no_match = len(name_to_config.keys() - found_names)
    return updated, skipped_no_match, skipped_invalid


def sync_server(accounts: list[Account], data_map: dict[tuple[int, str], FarmData]) -> tuple[int, int]:
    """Отправляет MenuData на сервера для аккаунтов из списка."""
    updated = 0
    skipped = 0

    for acc in accounts:
        fd = data_map.get((acc.owner_id, acc.name))
        if not fd:
            skipped += 1
            continue
        if not (fd.email or fd.password or fd.igg_id):
            skipped += 1
            continue

        ok, _ = update_account_menu_data(
            acc,
            email=fd.email,
            password=fd.password,
            igg_id=fd.igg_id,
        )
        if ok:
            updated += 1
        else:
            skipped += 1

    return updated, skipped


def main() -> None:
    """Точка входа CLI."""
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = parse_args()

    app = create_app()
    configs_dir = Path(args.configs_dir)

    with app.app_context():
        accounts = Account.query.all()
        farm_data = FarmData.query.all()

    farm_data_map = {(fd.user_id, fd.farm_name): fd for fd in farm_data}
    accounts_map: dict[tuple[int, str], dict[str, str]] = {}
    skipped_no_data = 0

    for acc in accounts:
        fd = farm_data_map.get((acc.owner_id, acc.name))
        if not fd:
            skipped_no_data += 1
            continue
        if not (fd.email or fd.password or fd.igg_id):
            skipped_no_data += 1
            continue
        accounts_map[(acc.owner_id, acc.name)] = build_menu_config(fd)

    logging.info("Всего аккаунтов: %s", len(accounts))
    logging.info("Готово к синхронизации: %s", len(accounts_map))
    logging.info("Пропущено (нет данных): %s", skipped_no_data)

    if args.mode in ("local", "both"):
        if not configs_dir.exists():
            logging.warning("[local] каталог %s не найден", configs_dir)
        else:
            updated, skipped_no_match, skipped_invalid = update_local_configs(
                configs_dir,
                accounts_map,
            )
            logging.info("[local] обновлено записей: %s", updated)
            logging.info("[local] пропущено (нет соответствия): %s", skipped_no_match)
            logging.info("[local] пропущено (ошибки формата): %s", skipped_invalid)

    if args.mode in ("server", "both"):
        with app.app_context():
            updated, skipped = sync_server(accounts, farm_data_map)
        logging.info("[server] обновлено аккаунтов: %s", updated)
        logging.info("[server] пропущено аккаунтов: %s", skipped)


if __name__ == "__main__":
    main()
