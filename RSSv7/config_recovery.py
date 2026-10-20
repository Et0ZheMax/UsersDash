"""Проверенные неизменяемые снимки и атомарное восстановление config LDPlayer."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
import time
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path


class ConfigRecoveryError(RuntimeError):
    """Снимок или восстановление конфигурации нельзя безопасно завершить."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _is_candidate(path: Path) -> bool:
    return path.is_file() and path.name.lower().startswith("leidian") and path.suffix.lower() == ".config"


def device_identity(config: object) -> str | None:
    """Отпечаток устройства, без публикации IMEI/Android ID/MAC."""
    keys = ('propertySettings.phoneIMEI', 'propertySettings.phoneAndroidId', 'propertySettings.macAddress')
    if not isinstance(config, dict) or not all(isinstance(config.get(key), str) and config[key].strip() for key in keys):
        return None
    return hashlib.sha256(json.dumps({key: config[key] for key in keys}, sort_keys=True).encode()).hexdigest()


def vm_generation(vms: Path, index: int) -> dict | None:
    """Поколение диска: один номер или index-derived UUID не доказывают одну и ту же VM."""
    folder = Path(vms) / f'leidian{index}'
    try:
        disk = (folder / 'data.vmdk').stat()
        tree = ET.parse(folder / 'leidian.vbox')
        machine = next(node for node in tree.iter() if node.tag.rsplit('}', 1)[-1] == 'Machine')
        if not disk.st_ino or not machine.get('uuid'):
            return None
        return {'device': str(disk.st_dev), 'file_id': str(disk.st_ino), 'machine_uuid': machine.get('uuid')}
    except (OSError, ET.ParseError, StopIteration):
        return None


def create_snapshot(source: Path, snapshot_root: Path, *, now: datetime | None = None,
                    vms: Path | None = None, profile: Path | None = None) -> Path:
    """Создаёт полный проверенный снимок без перезаписи прошлых снимков.

    Снимок публикуется только после копирования и сверки SHA-256. Незаконченные
    каталоги имеют префикс точки и никогда не участвуют в восстановлении.
    """

    source, snapshot_root = Path(source), Path(snapshot_root)
    if not source.is_dir():
        raise ConfigRecoveryError(f"Папка config не найдена: {source}")

    stamp = (now or datetime.now()).strftime("%Y-%m-%d_%H-%M-%S_%f")
    snapshot_root.mkdir(parents=True, exist_ok=True)
    staging = snapshot_root / f".{stamp}.{uuid.uuid4().hex}.new"
    published = snapshot_root / stamp
    vm_root = Path(vms) if vms is not None else (source.parent if source.name.lower() == 'config' else None)
    generations = {path.stem[7:]: vm_generation(vm_root, int(path.stem[7:]))
                   for path in source.glob('leidian*.config') if vm_root is not None and path.stem[7:].isdigit()}
    try:
        profile_bytes = Path(profile).read_bytes() if profile is not None else None
        owners = {}
        if profile_bytes is not None:
            records = json.loads(profile_bytes.decode('utf-8-sig'))
            if not isinstance(records, list):
                raise ValueError('Корень профиля должен быть массивом')
            for account in records:
                if isinstance(account, dict) and account.get('Id') and str(account.get('InstanceId', '')).isdigit():
                    owners.setdefault(str(account['InstanceId']), []).append(str(account['Id']))
    except (OSError, ValueError, UnicodeError) as exc:
        raise ConfigRecoveryError(f'Не удалось проверить владельцев снимка: {exc}') from exc
    try:
        shutil.copytree(source, staging)
        files = sorted(path for path in staging.rglob("*") if _is_candidate(path))
        if not files:
            raise ConfigRecoveryError("В исходной папке нет leidian*.config")
        manifest = {
            "created_at": (now or datetime.now()).isoformat(),
            "files": {
                path.relative_to(staging).as_posix(): {"size": path.stat().st_size, "sha256": _sha256(path)}
                for path in files
            },
        }
        for path in files:
            number = path.stem[7:]
            if vm_root is not None and number.isdigit():
                generation = vm_generation(vm_root, int(number))
                if generation != generations.get(number):
                    raise ConfigRecoveryError('Поколение VM изменилось во время снимка')
                entry = manifest['files'][path.relative_to(staging).as_posix()]
                entry['vm_generation'] = generation
                if profile_bytes is not None:
                    entry['account_ids'] = sorted(owners.get(number, []))
        if profile_bytes is not None and Path(profile).read_bytes() != profile_bytes:
            raise ConfigRecoveryError('Профиль изменился во время снимка')
        (staging / "manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        # На Windows Defender/индексатор может кратко удерживать только что
        # созданный каталог. Снимок остаётся скрытым до успешной публикации.
        for attempt in range(6):
            try:
                staging.rename(published)
                break
            except OSError as exc:
                if (getattr(exc, "winerror", None) not in (5, 32, 33)
                        or published.exists() or attempt == 5):
                    raise
                time.sleep(0.05 * (2 ** attempt))
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return published


def _read_manifest(snapshot: Path) -> dict:
    try:
        return json.loads((snapshot / "manifest.json").read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ConfigRecoveryError(f"Некорректный снимок {snapshot}: {exc}") from exc


def find_latest_config(snapshot_root: Path, filename: str, *, required_text: str = "") -> Path:
    """Возвращает новейший целый снимок с требуемым конфигом и маркером аккаунта."""

    snapshot_root = Path(snapshot_root)
    marker = required_text.encode("utf-8") if required_text else b""
    for snapshot in sorted(snapshot_root.iterdir(), reverse=True) if snapshot_root.is_dir() else []:
        if not snapshot.is_dir() or snapshot.name.startswith("."):
            continue
        try:
            entry = _read_manifest(snapshot).get("files", {}).get(filename)
            candidate = snapshot / filename
            if not entry or not _is_candidate(candidate) or candidate.stat().st_size != entry.get("size"):
                continue
            if _sha256(candidate) != entry.get("sha256"):
                continue
            if marker and marker not in candidate.read_bytes():
                continue
            return candidate
        except (ConfigRecoveryError, OSError):
            continue
    raise ConfigRecoveryError(f"Нет проверенного снимка для {filename}")


def restore_config(source: Path, destination: Path, rollback_root: Path) -> Path | None:
    """Атомарно восстанавливает файл, сохраняя прежнюю версию и сверяя SHA-256."""

    source, destination, rollback_root = Path(source), Path(destination), Path(rollback_root)
    if not _is_candidate(source):
        raise ConfigRecoveryError(f"Некорректный источник восстановления: {source}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    rollback_root.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        rollback = rollback_root / f"{destination.name}.{datetime.now():%Y%m%d_%H%M%S_%f}.before-fix"
        shutil.copy2(destination, rollback)
        if _sha256(destination) != _sha256(rollback):
            raise ConfigRecoveryError("Резервная копия перед восстановлением не прошла проверку SHA-256")
    else:
        rollback = None

    fd, temp_name = tempfile.mkstemp(prefix=f".{destination.name}.", suffix=".new", dir=destination.parent)
    os.close(fd)
    temporary = Path(temp_name)
    try:
        shutil.copy2(source, temporary)
        if _sha256(source) != _sha256(temporary):
            raise ConfigRecoveryError("Контрольная сумма временного файла не совпала")
        with temporary.open("r+b") as durable:
            os.fsync(durable.fileno())
        os.replace(temporary, destination)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    return rollback
