"""Безопасное восстановление удалённой записи фермы в профиле GnBots."""

from __future__ import annotations

import copy
import json
import os
import shutil
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


MARKER_KEY = "UsersDashReactivation"


class ReactivationError(RuntimeError):
    """Понятная оператору ошибка подготовки возобновления."""


@dataclass(frozen=True)
class ArchiveCandidate:
    """Запись фермы, найденная в историческом профиле GnBots."""

    record: dict[str, Any]
    path: Path
    backup_at: datetime


def _load_profile(path: Path) -> list[dict[str, Any]]:
    try:
        data = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ReactivationError(f"Не удалось прочитать профиль {path}: {exc}") from exc
    if not isinstance(data, list):
        raise ReactivationError(f"Профиль {path} должен содержать список ферм")
    return [item for item in data if isinstance(item, dict)]


def _backup_timestamp(path: Path) -> datetime:
    for parent in (path.parent, *path.parents):
        try:
            return datetime.strptime(parent.name, "%d__%m__%Y").replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    try:
        return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    except OSError:
        return datetime.min.replace(tzinfo=timezone.utc)


def find_archive_candidate(
    backup_root: Path,
    *,
    account_id: str,
    farm_name: str,
) -> ArchiveCandidate:
    """Находит самый свежий архив строго по ID, не путая одноимённые фермы."""

    exact_id: list[ArchiveCandidate] = []
    same_name_ids: set[str] = set()
    same_name: list[ArchiveCandidate] = []
    if not backup_root.is_dir():
        raise ReactivationError(f"Каталог архивов профиля не найден: {backup_root}")

    for path in backup_root.rglob("*.json"):
        try:
            records = _load_profile(path)
        except ReactivationError:
            continue
        backup_at = _backup_timestamp(path)
        for record in records:
            record_id = str(record.get("Id") or "").strip()
            record_name = str(record.get("Name") or "").strip()
            candidate = ArchiveCandidate(copy.deepcopy(record), path, backup_at)
            if record_id == account_id:
                exact_id.append(candidate)
            if record_name.casefold() == farm_name.casefold():
                same_name.append(candidate)
                if record_id:
                    same_name_ids.add(record_id)

    if exact_id:
        candidate = max(exact_id, key=lambda item: (item.backup_at, str(item.path)))
        archived_name = str(candidate.record.get("Name") or "").strip()
        if archived_name and archived_name.casefold() != farm_name.casefold():
            raise ReactivationError(
                f"ID {account_id} в архиве принадлежит ферме {archived_name}, а не {farm_name}"
            )
        return candidate

    if same_name:
        known_ids = ", ".join(sorted(same_name_ids)) or "без ID"
        raise ReactivationError(
            f"В архиве найдена ферма {farm_name}, но её ID ({known_ids}) не совпадает с {account_id}"
        )

    raise ReactivationError(f"Ферма {farm_name} с ID {account_id} не найдена в архивах GnBots")


def _decode_menu(value: Any) -> tuple[dict[str, Any], bool]:
    was_string = isinstance(value, str)
    if was_string:
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ReactivationError(f"В архиве повреждено MenuData: {exc}") from exc
    if not isinstance(value, dict):
        raise ReactivationError("В архиве MenuData имеет неожиданный формат")
    return value, was_string


def _encode_menu(value: dict[str, Any], was_string: bool) -> dict[str, Any] | str:
    if was_string:
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return value


def _write_profile_atomic(path: Path, records: list[dict[str, Any]]) -> None:
    fd, temp_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=str(path.parent),
        text=True,
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(records, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, path)
    except Exception:
        try:
            os.unlink(temp_name)
        except OSError:
            pass
        raise


def _save_rollback_copy(profile_path: Path, rollback_root: Path) -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    target_dir = rollback_root / stamp
    target_dir.mkdir(parents=True, exist_ok=False)
    target = target_dir / profile_path.name
    shutil.copy2(profile_path, target)
    return target


def prepare_reactivation(
    profile_path: Path,
    backup_root: Path,
    rollback_root: Path,
    *,
    account_id: str,
    farm_name: str,
    email: str,
    password: str,
    igg_id: str,
) -> dict[str, Any]:
    """Возвращает запись в профиль неактивной и помечает ожидание Viking Recovery."""

    account_id = str(account_id or "").strip()
    farm_name = str(farm_name or "").strip()
    email = str(email or "").strip()
    password = str(password or "")
    igg_id = str(igg_id or "").strip()
    if not account_id or not farm_name:
        raise ReactivationError("Для возобновления обязательны ID и имя фермы")
    if not email or not password or not igg_id:
        raise ReactivationError("В UsersDash должны быть заполнены Email, Password и IGG ID")

    records = _load_profile(profile_path)
    by_id = [item for item in records if str(item.get("Id") or "").strip() == account_id]
    if len(by_id) > 1:
        raise ReactivationError(f"В активном профиле найдено несколько записей с ID {account_id}")

    same_name_other = [
        item
        for item in records
        if str(item.get("Name") or "").strip().casefold() == farm_name.casefold()
        and str(item.get("Id") or "").strip() != account_id
    ]
    if same_name_other:
        other_ids = ", ".join(str(item.get("Id") or "без ID") for item in same_name_other)
        raise ReactivationError(
            f"В активном профиле имя {farm_name} занято другой записью ({other_ids})"
        )

    if by_id:
        record = by_id[0]
        source = "current"
        source_path = profile_path
    else:
        candidate = find_archive_candidate(
            backup_root,
            account_id=account_id,
            farm_name=farm_name,
        )
        record = copy.deepcopy(candidate.record)
        records.append(record)
        source = "archive"
        source_path = candidate.path

    menu, was_string = _decode_menu(record.get("MenuData", {}))
    config = menu.setdefault("Config", {})
    if not isinstance(config, dict):
        raise ReactivationError("В архиве MenuData.Config имеет неожиданный формат")
    config.update(
        {
            "Email": email,
            "Password": password,
            "Custom": igg_id,
            "Slot": "igg",
        }
    )

    requested_at = datetime.now(timezone.utc).isoformat()
    record["Id"] = account_id
    record["Name"] = farm_name
    record["MenuData"] = _encode_menu(menu, was_string)
    record["InstanceId"] = -1
    record["Active"] = False
    record[MARKER_KEY] = {
        "status": "prepared",
        "requested_at": requested_at,
        "account_id": account_id,
    }

    rollback_path = _save_rollback_copy(profile_path, rollback_root)
    try:
        _write_profile_atomic(profile_path, records)
    except Exception as exc:
        raise ReactivationError(f"Не удалось записать подготовленный профиль: {exc}") from exc

    return {
        "account_id": account_id,
        "farm_name": farm_name,
        "source": source,
        "source_path": str(source_path),
        "rollback_path": str(rollback_path),
        "requested_at": requested_at,
    }


def iter_pending_ready(records: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]]:
    """Возвращает подготовленные записи, которым Viking Recovery уже назначил LD ID."""

    for record in records:
        marker = record.get(MARKER_KEY)
        if not isinstance(marker, dict) or marker.get("status") != "prepared":
            continue
        try:
            instance_id = int(record.get("InstanceId"))
        except (TypeError, ValueError):
            continue
        if instance_id >= 0:
            yield record


def mark_emulator_ready(record: dict[str, Any]) -> None:
    """Помечает проверенную связку фермы и LDPlayer готовой к UsersDash."""

    marker = record.get(MARKER_KEY)
    if not isinstance(marker, dict):
        raise ReactivationError("У фермы нет маркера возобновления")
    marker["status"] = "emulator_ready"
    marker["instance_id"] = int(record.get("InstanceId"))
    marker["emulator_ready_at"] = datetime.now(timezone.utc).isoformat()
    record["Active"] = True


def mark_usersdash_completed(record: dict[str, Any]) -> None:
    """Фиксирует подтверждение UsersDash, сохраняя понятную историю в профиле."""

    marker = record.get(MARKER_KEY)
    if not isinstance(marker, dict):
        raise ReactivationError("У фермы нет маркера возобновления")
    marker["status"] = "completed"
    marker["completed_at"] = datetime.now(timezone.utc).isoformat()


def write_profile_atomic(path: Path, records: list[dict[str, Any]]) -> None:
    """Публичная обёртка атомарной записи для интеграции RSSv7."""

    _write_profile_atomic(path, records)

