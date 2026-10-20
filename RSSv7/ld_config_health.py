"""Живая диагностика всех VM LDPlayer и восстановление только проверенных конфигов.

Остановленная VM сама по себе не является неисправной. Исправление конфигурации
не доказывает загрузку Android или работу игрового аккаунта.
"""

from __future__ import annotations

from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import uuid
import time
import threading
from typing import Callable

from config_recovery import ConfigRecoveryError, restore_config, device_identity, vm_generation


class RepairBlocked(ConfigRecoveryError):
    """Нет достаточных доказательств для безопасного восстановления."""


_SIZE_CACHE: dict[str, tuple[float, dict]] = {}
_SIZE_LOCK = threading.Lock()


def disk_usage(folder: Path) -> dict:
    """Считает занятое файлами место, учитывая sparse/compressed VMDK в Windows.

    Read-only обход корневого junction разрешён; вложенные ссылки не обходятся.
    Ошибка чтения не выдаётся за нулевой размер. Кеш ограничен двумя минутами.
    """
    key = str(folder.resolve())
    with _SIZE_LOCK:
        cached = _SIZE_CACHE.get(key)
        if cached and time.monotonic() - cached[0] < 120:
            return dict(cached[1])
    logical, allocated, errors = 0, 0, []
    try:
        if not folder.is_dir():
            raise OSError("Нет каталога VM")
        if os.name == "nt":
            import ctypes
            from ctypes import wintypes
            get_size = ctypes.WinDLL('kernel32', use_last_error=True).GetCompressedFileSizeW
            get_size.argtypes = [wintypes.LPCWSTR, ctypes.POINTER(wintypes.DWORD)]
            get_size.restype = wintypes.DWORD
        for root, dirs, files in os.walk(folder, onerror=lambda exc: errors.append(str(exc))):
            dirs[:] = [name for name in dirs if not (Path(root) / name).is_symlink()
                       and not (Path(root) / name).is_junction()]
            for name in files:
                path = Path(root) / name
                try:
                    if path.is_symlink():
                        continue
                    stat = path.stat()
                    logical += stat.st_size
                    if os.name == "nt":
                        high = wintypes.DWORD()
                        ctypes.set_last_error(0)
                        low = get_size(str(path), ctypes.byref(high))
                        if low == 0xFFFFFFFF and ctypes.get_last_error():
                            raise ctypes.WinError(ctypes.get_last_error())
                        allocated += (high.value << 32) | low
                    else:
                        allocated += getattr(stat, 'st_blocks', stat.st_size // 512) * 512
                except OSError as exc:
                    errors.append(str(exc))
    except OSError as exc:
        errors.append(str(exc))
    result = {'disk_bytes': None if errors else allocated, 'logical_bytes': logical,
              'disk_checked_at': datetime.now(timezone.utc).isoformat(), 'disk_errors': errors}
    with _SIZE_LOCK:
        _SIZE_CACHE[key] = (time.monotonic(), result)
    return dict(result)


def read_json(path: Path):
    """Читает JSON, включая распространённый в Windows BOM."""
    return json.loads(path.read_text(encoding="utf-8-sig"))


def config_problems(config: object) -> list[str]:
    """Проверяет содержимое, а не только существование/хеш файла."""
    if not isinstance(config, dict):
        return ["config_not_object"]
    problems = []
    name = str(config.get("statusSettings.playerName") or "").strip()
    if not name or re.fullmatch(r"ldplayer(?:\s*[-_]?\s*\d+)?", name, re.I):
        problems.append("player_name_lost")
    resolution = config.get("advancedSettings.resolution")
    # Старые штатные config могут использовать заводское разрешение без этого поля.
    if "advancedSettings.resolution" in config and (not isinstance(resolution, dict) or any(
        type(resolution.get(key)) is not int or resolution[key] <= 0 for key in ("width", "height")
    )):
        problems.append("resolution_invalid")
    return problems


def instance_number(value: object) -> int:
    """Нормализует индекс, не позволяя передать путь или булево значение."""
    if isinstance(value, bool) or not re.fullmatch(r"\d{1,4}", str(value)):
        raise RepairBlocked("Некорректный индекс LDPlayer")
    return int(value)


def load_profile(path: Path) -> list[dict]:
    """Читает все записи, включая выключенные, без fallback на старый кеш."""
    data = read_json(path)
    if not isinstance(data, list) or any(not isinstance(row, dict) for row in data):
        raise RepairBlocked("Некорректный JSON профиля")
    return data


def _json_field(value: object) -> object:
    """Разбирает строковые Data/MenuData без изменения профиля."""
    if isinstance(value, str):
        try:
            return json.loads(value)
        except ValueError:
            return None
    return value


def _setting(value: object) -> object:
    """Извлекает value из схемы параметров GnBots."""
    return value.get('value') if isinstance(value, dict) else value


def character_slot(account: dict) -> int | None:
    """Возвращает явно включённый выбор персонажа; login/IGG ID наружу не передаются."""
    steps = _json_field(account.get('Data'))
    slots = set()
    if not isinstance(steps, list):
        return None
    for step in steps:
        if (not isinstance(step, dict) or step.get('ScriptId') != 'vikingbot.base.accountswitch'
                or step.get('IsActive') is not True):
            continue
        config = step.get('Config')
        if not isinstance(config, dict):
            continue
        if (_setting(config.get('mode')) != 'Player'
                or _setting(config.get('skip', 0)) not in (0, False, '0', None)):
            continue
        value = _setting(config.get('account'))
        if not isinstance(value, bool) and str(value) in {str(n) for n in range(1, 8)}:
            slots.add(int(value))
    return next(iter(slots)) if len(slots) == 1 else None


def _login_identity(account: dict) -> tuple[str, str] | None:
    """Внутренний ключ IGG-входа. Не возвращать его в API/журналах."""
    menu = _json_field(account.get('MenuData'))
    config = menu.get('Config') if isinstance(menu, dict) else None
    if not isinstance(config, dict) or _setting(config.get('Slot')) != 'igg':
        return None
    email = _setting(config.get('Email'))
    if isinstance(email, str) and email.strip():
        return email.strip().casefold(), str(_setting(config.get('Custom')) or '').strip()
    return None


def _shared_login(account: dict) -> tuple[str, str] | None:
    """Для вывода об общей VM дополнительно нужен явный выбор персонажа."""
    return _login_identity(account) if character_slot(account) is not None else None


def _binding_accounts(profiles: list[dict], vms: Path) -> tuple[dict, dict]:
    """Собирает несколько персонажей в одну VM, включая исчезнувший старый индекс.

    Для вывода об устаревшей ссылке требуются общий IGG-вход, разные явно выбранные
    персонажи, ровно один существующий каталог VM среди привязок этой группы и полное
    отсутствие файлов/каталога старого индекса. При неоднозначности индексы сохраняются.
    Профиль, пароль и выбор персонажа не меняются; email в публичный результат не попадает.
    """
    indices = [instance_number(account.get('InstanceId')) for account in profiles]
    def absent(path: Path) -> bool:
        try:
            path.lstat()
        except FileNotFoundError:
            return True
        except OSError:
            return False
        return False
    groups: dict[tuple[str, str], list[int]] = {}
    for position, account in enumerate(profiles):
        key = _shared_login(account)
        if key:
            groups.setdefault(key, []).append(position)
    resolved = list(indices)
    aliases = {}
    for positions in groups.values():
        characters = [character_slot(profiles[position]) for position in positions]
        if len(positions) < 2 or len(set(characters)) != len(characters):
            continue
        declared = {indices[position] for position in positions}
        try:
            existing = {index for index in declared if (vms / f'leidian{index}').is_dir()}
            if len(existing) != 1:
                continue
            target = next(iter(existing))
            # Общий рабочий индекс не должен одновременно принадлежать другому игровому входу.
            key = _shared_login(profiles[positions[0]])
            if any(index == target and _shared_login(profiles[position]) != key
                   for position, index in enumerate(indices)):
                continue
            for position in positions:
                old = indices[position]
                paths = (vms / f'leidian{old}', vms / 'config' / f'leidian{old}.config')
                if old != target and all(absent(path) for path in paths):
                    resolved[position] = target
                    aliases[str(old)] = target
        except OSError:
            # Непроверенный или отключённый junction не считается удалённой VM.
            continue
    accounts = {}
    for position, account in enumerate(profiles):
        index = resolved[position]
        accounts.setdefault(index, []).append({
            'id': str(account.get('Id') or ''), 'name': str(account.get('Name') or ''),
            'active': bool(account.get('Active')), 'character': character_slot(account),
            'configured_instance_id': indices[position],
            'binding_source': 'shared_character_vm' if index != indices[position] else 'profile',
        })
    return accounts, aliases


def target_is_disabled(index: int, profile: Path, gn_config: Path, vms: Path | None = None) -> bool:
    """Разрешает открытый GUI только для выключенной VM в реально выбранном профиле."""
    try:
        selected = read_json(gn_config).get('SelectedProfile')
        if not isinstance(selected, str) or Path(selected).name != selected:
            return False
        if Path(selected).suffix.lower() != '.json':
            selected += '.json'
        if selected.casefold() != profile.name.casefold():
            return False
        if vms is None:
            # Без фактической папки VM нельзя доказать отсутствие активных связанных персонажей.
            return False
        accounts, _ = _binding_accounts(load_profile(profile), vms)
        rows = accounts.get(index, [])
        return bool(rows) and all(row['active'] is False for row in rows)
    except (OSError, ValueError, AttributeError, ConfigRecoveryError):
        return False


def inventory(vms: Path, profile: Path) -> dict:
    """Объединяет индексы из профиля, config и каталогов VM; ошибки не скрывает."""
    errors, accounts, aliases, profiles = [], {}, {}, []
    try:
        profiles = load_profile(profile)
        ids = [str(account.get('Id')) for account in profiles if account.get('Id')]
        if len(ids) != len(set(ids)):
            raise RepairBlocked('В профиле повторяется постоянный ID аккаунта')
        accounts, aliases = _binding_accounts(profiles, vms)
    except (OSError, ValueError, ConfigRecoveryError) as exc:
        errors.append(f"profile: {exc}")
    indices = set(accounts)
    try:
        config_paths = list((vms / "config").iterdir())
        vm_paths = list(vms.iterdir())
        for path in config_paths + vm_paths:
            match = re.fullmatch(r"leidian(\d+)(?:\.config)?", path.name, re.I)
            if match:
                indices.add(int(match[1]))
    except OSError as exc:
        errors.append(f"inventory: {exc}")
    rows = []
    for index in sorted(indices):
        config = vms / "config" / f"leidian{index}.config"
        issues, name = [], ""
        try:
            data = read_json(config)
            issues = config_problems(data)
            if isinstance(data, dict):
                name = str(data.get("statusSettings.playerName") or "")
        except FileNotFoundError:
            issues = ["config_missing"]
        except (OSError, ValueError) as exc:
            issues = ["config_unreadable"]
        linked = accounts.get(index, [])
        if not (vms / f"leidian{index}").is_dir():
            issues.append("vm_directory_missing")
        rows.append({
            "instance_id": index, "filename": config.name, "player_name": name,
            "accounts": linked, "active": any(a["active"] for a in linked),
            "orphan": not linked, "issues": issues,
            "config_status": "damaged" if issues else "valid",
            "android_status": "not_checked",
            "usage_class": "unlinked" if not linked else (
                "active" if any(a["active"] for a in linked) else "disabled"),
        })
    by_id = {str(account.get('Id') or ''): account for account in profiles}
    for row in rows:
        linked = row['accounts']
        binding_issues = []
        expected = {account['name'] for account in linked}
        name = row['player_name']
        known_name = bool(name and 'player_name_lost' not in row['issues'])
        if linked and known_name and not _name_matches(name, expected):
            binding_issues.append('account_binding_mismatch')
        if len(linked) > 1:
            keys = [_shared_login(by_id.get(account['id'], {})) for account in linked]
            slots = [account['character'] for account in linked]
            characters_valid = (None not in keys and len(set(keys)) == 1
                                and None not in slots and len(set(slots)) == len(slots))
            # GnBots также умеет менять IGG-вход на одной явно указанной VM.
            # Это не основание переносить на неё другие номера автоматически.
            logins = [_login_identity(by_id.get(account['id'], {})) for account in linked]
            login_switch_valid = (all(slot is None for slot in slots)
                and all(key and key[1] for key in logins) and len(set(logins)) == len(logins))
            if not characters_valid and not login_switch_valid:
                binding_issues.append('unproven_shared_binding')
        if any(account['binding_source'] == 'shared_character_vm' for account in linked):
            binding_issues.append('stale_profile_binding')
        row['binding_issues'] = binding_issues
        row['binding_status'] = 'conflict' if binding_issues else (
            'unlinked' if not linked else 'matched' if known_name else 'unknown')
        row['candidate_instance_ids'] = [other['instance_id'] for other in rows
            if linked and other['player_name'] and _name_matches(other['player_name'], expected)
            and other['instance_id'] != row['instance_id']] if binding_issues else []
        row['issues'].extend(binding_issues)
        if binding_issues:
            row['config_status'] = 'damaged'
    return {"checked_at": datetime.now(timezone.utc).isoformat(), "ok": not errors,
            "errors": errors, "instances": rows, "binding_aliases": aliases,
            'binding_conflicts': [row['instance_id'] for row in rows if row['binding_issues']],
            "crashed": [row["filename"] for row in rows if row["issues"]]}


def console_rows(console: Path) -> dict[int, dict]:
    """Не принимает ошибку list2 за отсутствие работающих VM."""
    result = subprocess.run([str(console), "list2"], capture_output=True, timeout=15, check=False)
    if result.returncode:
        raise RepairBlocked(f"ldconsole list2: код {result.returncode}")
    rows = {}
    for line in result.stdout.decode("utf-8", errors="replace").splitlines():
        if not line.strip():
            continue
        parts = line.strip().split(",")
        try:
            # Имя может содержать запятую; последние 8 полей имеют фиксированный формат.
            if len(parts) < 10:
                raise ValueError("короткая строка")
            tail = [int(value) for value in parts[-8:]]
            index = instance_number(parts[0])
            rows[index] = {"running": any(value > 0 for value in tail[:5]),
                           "title": ",".join(parts[1:-8]),
                           "pids": [value for value in tail[3:5] if value > 0]}
        except (ValueError, ConfigRecoveryError) as exc:
            raise RepairBlocked("Не удалось разобрать ответ list2") from exc
    if not rows:
        raise RepairBlocked("ldconsole list2 вернул пустой список")
    return rows


def runtime_inventory(report: dict, console: Path) -> dict:
    """Добавляет runtime; остановленная VM остаётся stopped, а не crashed."""
    try:
        live = console_rows(console)
        for row in report["instances"]:
            item = live.get(row["instance_id"])
            row["runtime_status"] = "unknown" if item is None else (
                "running" if item["running"] else "stopped")
            row["pids"] = item['pids'] if item else []
            if item is None:
                row["issues"].append("instance_not_registered")
                row["config_status"] = "damaged"
        def boot_status(row: dict) -> None:
            if row.get("runtime_status") != "running":
                return
            try:
                probe = subprocess.run(
                    [str(console), "adb", "--index", str(row["instance_id"]),
                     "--command", "shell getprop sys.boot_completed"],
                    capture_output=True, timeout=8, check=False,
                )
                row["android_status"] = "ready" if probe.returncode == 0 and (
                    probe.stdout.decode(errors="replace").strip() == "1") else "booting_or_unavailable"
            except (OSError, subprocess.SubprocessError):
                row["android_status"] = "unknown"
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(boot_status, report["instances"]))
        report["crashed"] = [row["filename"] for row in report["instances"] if row["issues"]]
    except (OSError, subprocess.SubprocessError, ConfigRecoveryError) as exc:
        report["errors"].append(f"runtime: {exc}")
        report["ok"] = False
    return report


def _name_matches(name: str, expected: set[str]) -> bool:
    """Разрешает общий эмулятор Filosof3/4 без подстрочного совпадения Airat/Airat2."""
    normalized = name.strip().casefold()
    expanded = {normalized}
    parts = normalized.split("/")
    prefix = re.sub(r"\d+$", "", parts[0])
    expanded.update(prefix + part if part.isdigit() else part for part in parts)
    return bool(expanded & {value.strip().casefold() for value in expected})


def select_source(index: int, expected: set[str], snapshot_root: Path, daily_root: Path,
                  override: Path | None = None, *, vms: Path | None = None,
                  current: dict | None = None, account_ids: set[str] | None = None) -> tuple[Path, bytes]:
    """Выбирает семантически целый снимок нужного владельца, затем сверяет manifest.

    Непривязанный индекс без имени владельца не восстанавливается по истории:
    один индекс LDPlayer мог принадлежать разным людям в разные месяцы.
    """
    if not expected or not any(name.strip() for name in expected):
        raise RepairBlocked("Нет привязки к аккаунту: сначала установите владельца VM")
    filename = f"leidian{index}.config"
    candidates = []
    if override is not None:
        candidates = [(override / filename, None)]
    else:
        for folder in sorted(snapshot_root.iterdir(), reverse=True) if snapshot_root.is_dir() else []:
            if folder.is_dir() and not folder.name.startswith("."):
                try:
                    entry = read_json(folder / "manifest.json").get("files", {}).get(filename)
                    if isinstance(entry, dict):
                        candidates.append((folder / filename, entry))
                except (OSError, ValueError, AttributeError):
                    continue
        dated = []
        for folder in daily_root.iterdir() if daily_root.is_dir() else []:
            try:
                stamp = datetime.strptime(folder.name, "%d__%m__%Y")
                if folder.is_dir():
                    dated.append((stamp, folder))
            except ValueError:
                continue
        candidates.extend((folder / filename, None) for _, folder in sorted(dated, reverse=True))
    for path, entry in candidates:
        try:
            raw = path.read_bytes()
            if entry and (len(raw) != entry.get("size") or
                          hashlib.sha256(raw).hexdigest() != entry.get("sha256")):
                continue
            config = json.loads(raw.decode("utf-8-sig"))
            if config_problems(config) or not _name_matches(config["statusSettings.playerName"], expected):
                continue
            if vms is not None:
                if entry and 'account_ids' in entry:
                    saved_ids = entry['account_ids']
                    if (not isinstance(saved_ids, list) or any(not isinstance(key, str) for key in saved_ids)
                            or set(saved_ids) != (account_ids or set())):
                        continue
                saved_generation = entry.get('vm_generation') if entry else None
                live_generation = vm_generation(vms, index)
                if saved_generation:
                    if saved_generation != live_generation:
                        continue
                elif not device_identity(current) or device_identity(current) != device_identity(config):
                    continue
            return path, raw
        except (OSError, ValueError, KeyError):
            continue
    raise RepairBlocked(f"Нет бэкапа {filename} с подтверждённым владельцем и идентичностью VM: "
                        f"{', '.join(sorted(expected))}")


@contextmanager
def repair_lock(root: Path):
    """Общий OS lock для HTTP и отдельных процессов, освобождается при аварии."""
    root.mkdir(parents=True, exist_ok=True)
    with (root / "config-repair.lock").open("a+b") as stream:
        stream.seek(0)
        stream.write(b"0")
        stream.flush()
        stream.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise RepairBlocked("Уже выполняется другое восстановление") from exc
        try:
            yield
        finally:
            stream.seek(0)
            if os.name == "nt":
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(stream, fcntl.LOCK_UN)


def repair_instances(indices: list, *, vms: Path, profile: Path, snapshot_root: Path,
                     daily_root: Path, rollback_root: Path, assert_stopped: Callable[[int], None],
                     override: Path | None = None, dry_run: bool = False) -> dict:
    """Проверяет план до записи; каждый config имеет откат и результат read-back.

    Никогда не копирует/удаляет диски VM, не меняет Active, не запускает GnBots.
    Ошибка одного элемента не превращает пакет в успешный результат.
    """
    if not isinstance(indices, list) or not indices or len(indices) > 200:
        raise RepairBlocked("Нужен непустой список индексов (до 200)")
    indices = list(dict.fromkeys(instance_number(value) for value in indices))
    results = []
    with repair_lock(rollback_root):
        profile_bytes = profile.read_bytes()
        report = inventory(vms, profile)
        if not report["ok"]:
            raise RepairBlocked("; ".join(report["errors"]))
        rows = {row["instance_id"]: row for row in report["instances"]}
        conflicts = [row for row in report['instances'] if row['active'] and row.get('binding_issues')]
        for index in indices:
            item = {"instance_id": index, "ok": False, "android_status": "not_checked"}
            try:
                row = rows.get(index)
                if row is None:
                    shared = report.get('binding_aliases', {}).get(str(index))
                    if shared is not None:
                        raise RepairBlocked(f"Этот персонаж использует общий LDPlayer {shared}; выберите его")
                    raise RepairBlocked("Индекс не найден в инвентаризации")
                relevant_conflicts = conflicts + ([row] if row['binding_issues'] and row not in conflicts else [])
                if relevant_conflicts:
                    details = '; '.join(f"{', '.join(a['name'] for a in conflict['accounts'])} → "
                        f"№{conflict['instance_id']} ({conflict['player_name'] or 'без имени'})"
                        for conflict in relevant_conflicts)
                    raise RepairBlocked('Сначала проверьте имена VM и привязки профиля GnBots: ' + details)
                if not row["issues"]:
                    item.update(ok=True, status="unchanged", message="Config уже целый; запись не нужна")
                    results.append(item)
                    continue
                if "vm_directory_missing" in row["issues"]:
                    raise RepairBlocked("Каталог VM отсутствует: восстановление config не вернёт диски")
                destination = vms / "config" / f"leidian{index}.config"
                before = destination.read_bytes() if destination.exists() else None
                try:
                    current_config = json.loads(before.decode('utf-8-sig')) if before is not None else {}
                except (ValueError, UnicodeError):
                    current_config = {}
                source, source_raw = select_source(index, {a["name"] for a in row["accounts"]},
                    snapshot_root, daily_root, override, vms=vms, current=current_config,
                    account_ids={a['id'] for a in row['accounts']})
                restored = json.loads(source_raw.decode('utf-8-sig'))
                # Восстанавливаем настройки, сохраняя действующие аппаратные/сетевые свойства VM.
                if isinstance(current_config, dict):
                    for key, value in current_config.items():
                        if key.startswith(('propertySettings.', 'networkSettings.')):
                            restored[key] = value
                raw = (json.dumps(restored, ensure_ascii=False, indent=2) + '\n').encode('utf-8')
                item.update(source=str(source), sha256=hashlib.sha256(raw).hexdigest(),
                            source_sha256=hashlib.sha256(source_raw).hexdigest(), identity_preserved=True)
                assert_stopped(index)
                if profile.read_bytes() != profile_bytes:
                    raise RepairBlocked("Профиль изменился во время подготовки; повторите диагностику")
                if dry_run:
                    item.update(ok=True, status="planned", message="Источник и остановка проверены; запись не выполнялась")
                else:
                    # Зафиксированные байты источника исключают гонку с заменой резервной копии.
                    with tempfile.TemporaryDirectory(prefix=".repair-", dir=rollback_root) as stage:
                        staged = Path(stage) / destination.name
                        staged.write_bytes(raw)
                        assert_stopped(index)
                        if profile.read_bytes() != profile_bytes:
                            raise RepairBlocked("Профиль изменился перед записью; восстановление отменено")
                        current = destination.read_bytes() if destination.exists() else None
                        if current != before:
                            raise RepairBlocked("Рабочий config изменился; восстановление отменено")
                        rollback = restore_config(staged, destination, rollback_root)
                        item["rollback"] = str(rollback) if rollback else None
                        if destination.read_bytes() != raw or config_problems(read_json(destination)):
                            raise RepairBlocked("Проверка записанного config не прошла; сохранён откат")
                        post = inventory(vms, profile)
                        if not post['ok'] or any(r['active'] and r['binding_issues'] for r in post['instances']):
                            raise RepairBlocked('После записи обнаружена ошибка привязок; запуск бота запрещён')
                    item.update(ok=True, status="config_restored",
                                message="Config восстановлен и прочитан повторно. Android и игра ещё не проверены")
            except (OSError, ValueError, ConfigRecoveryError, subprocess.SubprocessError) as exc:
                item.update(status="blocked", message=str(exc))
            results.append(item)
        journal = rollback_root / f"repair-{datetime.now():%Y%m%d-%H%M%S}-{uuid.uuid4().hex}.json"
        journal.write_text(json.dumps({"dry_run": dry_run, "results": results},
                                      ensure_ascii=False, indent=2), encoding="utf-8")
    logs = [f"{'OK' if row['ok'] else 'BLOCKED'} LDPlayer {row['instance_id']}: {row['message']}"
            + (f"; source={row['source']}" if row.get("source") else "")
            + (f"; rollback={row['rollback']}" if row.get("rollback") else "") for row in results]
    return {"ok": all(row["ok"] for row in results), "results": results, "logs": logs,
            "verification": "config_only; android_and_game_not_checked"}
