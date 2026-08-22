"""Автоматическое восстановление фермы Viking Rise в LDPlayer.

Модуль читает учётные данные напрямую из профиля GnBots, клонирует чистый
LDPlayer и проводит авторизацию через ADB. Секреты не записываются в журнал
и передаются дочернему процессу только через stdin.
"""

from __future__ import annotations

import argparse
import base64
import json
import logging
import msvcrt
import os
import queue
import re
import secrets
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterable, Sequence


LDPLAYER_DIR = Path(r"C:\LDPlayer\LDPlayer9")
GNBOTS_DIR = Path(r"C:\Program Files\GnBots")
PROFILE_PATH = GNBOTS_DIR / "profiles" / "LDPplayer.json"
TEMPLATE_INDEX = 0
PACKAGE = "com.igg.android.vikingriseglobal"
ACTIVITY = f"{PACKAGE}/com.gpc.sdk.unity.GPCSDKMainActivity"
EXPECTED_SIZE = (640, 480)
MIN_FREE_BYTES = 8 * 1024**3
RESOURCE_RESERVE_BYTES = 3 * 1024**3
DEFAULT_TIMEOUT = 30 * 60
CREATE_NO_WINDOW = getattr(subprocess, "CREATE_NO_WINDOW", 0)


class RecoveryError(RuntimeError):
    """Понятная пользователю ошибка восстановления."""


class CancelledError(RecoveryError):
    """Операция отменена пользователем в безопасной точке."""


@dataclass(frozen=True)
class Farm:
    """Запись фермы из профиля GnBots."""

    name: str
    email: str
    password: str
    custom: str
    slot: str
    active: bool
    record_id: str = ""
    instance_id: int | None = None

    @property
    def ready(self) -> bool:
        return bool(self.email and self.password and self.slot.lower() == "igg")


@dataclass(frozen=True)
class Instance:
    """Краткая запись LDPlayer из вывода list2."""

    index: int
    name: str


@dataclass(frozen=True)
class OcrWord:
    """Слово и его координаты, полученные через Tesseract GnBots."""

    text: str
    x1: int
    y1: int
    x2: int
    y2: int


def _nested_json(value: object) -> object:
    if isinstance(value, str):
        return json.loads(value)
    return value


def resolve_profile_path(gnbots_dir: Path = GNBOTS_DIR) -> Path:
    """Найти активный профиль по `SelectedProfile` в конфигурации GnBots."""

    config_path = gnbots_dir / "config.json"
    try:
        config = json.loads(config_path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RecoveryError(f"Не удалось прочитать конфигурацию GnBots: {exc}") from exc
    selected = str(config.get("SelectedProfile", "")).strip()
    if not selected or Path(selected).name != selected:
        raise RecoveryError("В GnBots не указан корректный SelectedProfile")
    profile = gnbots_dir / "profiles" / f"{selected}.json"
    if not profile.is_file():
        raise RecoveryError(f"Активный профиль GnBots не найден: {profile}")
    return profile


def load_farms(path: Path | None = None) -> list[Farm]:
    """Загрузить фермы, не сохраняя отдельную базу логинов и паролей."""

    path = path or resolve_profile_path()
    try:
        records = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RecoveryError(f"Не удалось прочитать профиль GnBots: {exc}") from exc
    if not isinstance(records, list):
        raise RecoveryError("Профиль GnBots имеет неожиданный формат: ожидался список")

    farms: list[Farm] = []
    for record in records:
        if not isinstance(record, dict):
            continue
        try:
            menu = _nested_json(record.get("MenuData", {}))
        except json.JSONDecodeError:
            continue
        if not isinstance(menu, dict) or menu.get("ScriptId") != "appmenu":
            continue
        config = menu.get("Config", {})
        if not isinstance(config, dict):
            continue
        farms.append(
            Farm(
                name=str(record.get("Name", "")).strip(),
                email=str(config.get("Email", "")).strip(),
                password=str(config.get("Password", "")),
                custom=str(config.get("Custom", "")).strip(),
                slot=str(config.get("Slot", "")).strip(),
                active=bool(record.get("Active", False)),
                record_id=str(record.get("Id", "")).strip(),
                instance_id=(
                    int(record["InstanceId"])
                    if str(record.get("InstanceId", "")).strip().isdigit()
                    else None
                ),
            )
        )
    return sorted((farm for farm in farms if farm.name), key=lambda item: item.name.casefold())


def choose_backup_name(target: str, existing_names: Iterable[str], now: datetime | None = None) -> str:
    """Вернуть свободное имя для сохранения старого эмулятора."""

    names = set(existing_names)
    preferred = f"{target}_OLD"
    if preferred not in names:
        return preferred
    stamp = (now or datetime.now()).strftime("%Y%m%d_%H%M%S")
    candidate = f"{preferred}_{stamp}"
    suffix = 2
    while candidate in names:
        candidate = f"{preferred}_{stamp}_{suffix}"
        suffix += 1
    return candidate


def update_profile_instance(profile_path: Path, farm: Farm, new_index: int) -> int | None:
    """Атомарно заменить InstanceId выбранной фермы и подтвердить запись."""

    try:
        raw = profile_path.read_text(encoding="utf-8-sig")
        records = json.loads(raw)
    except (OSError, json.JSONDecodeError) as exc:
        raise RecoveryError(f"Не удалось прочитать профиль GnBots для обновления ID: {exc}") from exc
    if not isinstance(records, list):
        raise RecoveryError("Профиль GnBots имеет неожиданный формат: ожидался список")

    matches = []
    for record in records:
        if not isinstance(record, dict):
            continue
        if farm.record_id and str(record.get("Id", "")).strip() == farm.record_id:
            matches = [record]
            break
        if not farm.record_id and str(record.get("Name", "")).strip() == farm.name:
            matches.append(record)
    if not matches:
        raise RecoveryError(f"Ферма {farm.name} больше не найдена в активном профиле GnBots")
    if len(matches) != 1:
        raise RecoveryError(f"В профиле найдено несколько записей фермы {farm.name}")

    record = matches[0]
    old_raw = record.get("InstanceId")
    old_index = int(old_raw) if str(old_raw).strip().isdigit() else None
    record["InstanceId"] = int(new_index)

    backup_path = profile_path.with_name(profile_path.name + ".before_viking_recovery")
    fd, temp_name = tempfile.mkstemp(
        prefix=f".{profile_path.name}.",
        suffix=".tmp",
        dir=str(profile_path.parent),
        text=True,
    )
    try:
        shutil.copy2(profile_path, backup_path)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(records, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, profile_path)
    except Exception as exc:
        try:
            os.unlink(temp_name)
        except OSError:
            pass
        raise RecoveryError(f"Не удалось записать новый LDPlayer ID в профиль GnBots: {exc}") from exc

    try:
        saved = json.loads(profile_path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RecoveryError(f"Не удалось проверить профиль GnBots после записи: {exc}") from exc
    saved_record = next(
        (
            item
            for item in saved
            if isinstance(item, dict)
            and (
                (farm.record_id and str(item.get("Id", "")).strip() == farm.record_id)
                or (not farm.record_id and str(item.get("Name", "")).strip() == farm.name)
            )
        ),
        None,
    )
    if saved_record is None or str(saved_record.get("InstanceId")) != str(int(new_index)):
        raise RecoveryError("Профиль GnBots не подтвердил новый LDPlayer ID")
    return old_index


def parse_wm_size(output: str) -> tuple[int, int] | None:
    """Извлечь итоговое разрешение Android из `wm size`."""

    matches = re.findall(r"(\d+)x(\d+)", output)
    return tuple(map(int, matches[-1])) if matches else None


def required_free_space(template: Path) -> int:
    """Оценить безопасный запас: логический размер образа плюс ресурсы игры."""

    try:
        template_size = sum(path.stat().st_size for path in template.rglob("*") if path.is_file())
    except OSError as exc:
        raise RecoveryError(f"Не удалось вычислить размер чистого образа: {exc}") from exc
    return max(MIN_FREE_BYTES, template_size + RESOURCE_RESERVE_BYTES)


def is_resource_prompt(text: str) -> bool:
    """Распознать стилизованный заголовок RESOURCE DOWNLOAD с допустимыми OCR-ошибками."""

    normalized = text.casefold()
    return bool(re.search(r"\bdownload\b", normalized)) and any(
        marker in normalized for marker in ("resource", "resour", "resolr", "required")
    )


def select_igg_row(words: Sequence[OcrWord], custom: str) -> tuple[int, int]:
    """Найти строку нужного IGG ID по OCR и вернуть безопасную точку клика."""

    target = "".join(re.findall(r"\d", custom))
    visible = [word for word in words if 50 <= word.y1 <= 420]
    rows: list[list[OcrWord]] = []
    for word in sorted(visible, key=lambda item: (item.y1, item.x1)):
        for row in rows:
            baseline = sum(item.y1 for item in row) / len(row)
            if abs(word.y1 - baseline) <= 9:
                row.append(word)
                break
        else:
            rows.append([word])

    candidates: list[tuple[str, int]] = []
    for row in rows:
        digits = "".join(
            "".join(re.findall(r"\d", word.text)) for word in sorted(row, key=lambda item: item.x1)
        )
        if len(digits) >= 6:
            y = round(sum((word.y1 + word.y2) / 2 for word in row) / len(row))
            candidates.append((digits, y))

    if target:
        matches = [(digits, y) for digits, y in candidates if target in digits or digits in target]
        if len(matches) == 1:
            return 300, matches[0][1]
        if not matches:
            raise RecoveryError(f"На экране выбора не найден IGG ID из поля Custom ({custom})")
        raise RecoveryError(f"Поле Custom ({custom}) соответствует нескольким строкам IGG ID")

    unique_rows = {(digits, y) for digits, y in candidates}
    if len(unique_rows) == 1:
        return 300, next(iter(unique_rows))[1]
    raise RecoveryError("В аккаунте несколько IGG ID, но поле Custom в профиле не заполнено")


def parse_ocr_payload(payload: str) -> list[OcrWord]:
    """Разобрать безопасный JSON OCR, поддерживая и прежний формат helper-а."""

    try:
        data = json.loads(payload.strip() or "[]")
    except json.JSONDecodeError as exc:
        raise RecoveryError("OCR вернул некорректный результат") from exc
    if isinstance(data, dict):
        data = [data]
    if not isinstance(data, list):
        raise RecoveryError("OCR вернул результат неожиданного типа")
    words: list[OcrWord] = []
    try:
        for item in data:
            if "TextBase64" in item:
                text = base64.b64decode(str(item["TextBase64"]), validate=True).decode("utf-8")
            else:
                text = str(item["Text"])
            words.append(
                OcrWord(text, int(item["X1"]), int(item["Y1"]), int(item["X2"]), int(item["Y2"]))
            )
    except (KeyError, TypeError, ValueError, UnicodeDecodeError) as exc:
        raise RecoveryError("OCR вернул некорректные поля") from exc
    return words


def classify_login_screen(text: str) -> str:
    """Определить безопасное действие на экране запуска Viking Rise."""

    normalized = text.casefold()
    if "help" in normalized:
        return "confirm_expired"
    provider_markers = ("igg account", "1gg account", "facebook", "google account", "guest")
    if any(marker in normalized for marker in provider_markers):
        return "choose_provider"
    return "wait"


class ProcessRunner:
    """Запуск LDPlayer/ADB без shell и без вывода секретов."""

    def __init__(self, logger: logging.Logger):
        self.logger = logger

    def run(
        self,
        command: Sequence[str | os.PathLike[str]],
        *,
        timeout: float = 120,
        input_text: str | None = None,
        cwd: Path | None = None,
        check: bool = True,
        sensitive: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        args = [str(item) for item in command]
        shown = [Path(args[0]).name, *("<скрыто>" if sensitive else item for item in args[1:])]
        self.logger.debug("Команда: %s", " ".join(shown))
        try:
            result = subprocess.run(
                args,
                input=input_text,
                text=True,
                encoding="utf-8",
                errors="replace",
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=timeout,
                cwd=str(cwd) if cwd else None,
                creationflags=CREATE_NO_WINDOW,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise RecoveryError(f"Не удалось выполнить {Path(args[0]).name}: {exc}") from exc
        if check and result.returncode != 0:
            detail = (result.stderr or result.stdout).strip()
            if sensitive:
                detail = "секретный ввод завершился с ошибкой"
            raise RecoveryError(f"{Path(args[0]).name} вернул код {result.returncode}: {detail}")
        return result


class RecoveryEngine:
    """Пошаговый контроллер восстановления одной фермы."""

    def __init__(
        self,
        logger: logging.Logger,
        status: Callable[[str], None] | None = None,
        cancel_event: threading.Event | None = None,
        ldplayer_dir: Path = LDPLAYER_DIR,
        gnbots_dir: Path = GNBOTS_DIR,
    ):
        self.logger = logger
        self.status = status or (lambda message: None)
        self.cancel_event = cancel_event or threading.Event()
        self.ldplayer_dir = ldplayer_dir
        self.gnbots_dir = gnbots_dir
        self.profile_path = resolve_profile_path(gnbots_dir)
        self.ldconsole = ldplayer_dir / "ldconsole.exe"
        self.adb = ldplayer_dir / "adb.exe"
        self.runner = ProcessRunner(logger)
        self.serial = ""
        self.new_index: int | None = None
        self.work_dir = Path(tempfile.gettempdir()) / "VikingRecovery"
        self.work_dir.mkdir(parents=True, exist_ok=True)

    def _step(self, message: str) -> None:
        self._check_cancel()
        self.logger.info(message)
        self.status(message)

    def _check_cancel(self) -> None:
        if self.cancel_event.is_set():
            raise CancelledError("Операция отменена пользователем")

    def validate(self) -> None:
        missing = [path for path in (self.ldconsole, self.adb, self.profile_path) if not path.exists()]
        if missing:
            raise RecoveryError("Не найдены обязательные файлы: " + ", ".join(map(str, missing)))
        template = self.ldplayer_dir / "vms" / f"leidian{TEMPLATE_INDEX}"
        if not template.is_dir():
            raise RecoveryError(f"Чистый образ ID {TEMPLATE_INDEX} не найден: {template}")
        try:
            resolved_template = template.resolve(strict=True)
        except OSError as exc:
            raise RecoveryError(f"Ссылка чистого образа ID {TEMPLATE_INDEX} повреждена: {exc}") from exc
        self._validate_vmdk_uuids(resolved_template)
        self.logger.info("Чистый образ: %s -> %s", template, resolved_template)
        free = shutil.disk_usage(self.ldplayer_dir).free
        required = required_free_space(resolved_template)
        if free < required:
            raise RecoveryError(
                f"На диске C свободно только {free / 1024**3:.1f} ГБ; для клона и ресурсов требуется "
                f"не менее {required / 1024**3:.1f} ГБ"
            )
        ocr = self.gnbots_dir / "Tesseract.dll"
        trained = self.gnbots_dir / "tessdata" / "eng.traineddata"
        if not ocr.exists() or not trained.exists():
            raise RecoveryError("Не найдены OCR-компоненты GnBots")

    def list_instances(self) -> list[Instance]:
        output = self.runner.run([self.ldconsole, "list2"], timeout=30).stdout
        instances: list[Instance] = []
        for line in output.splitlines():
            parts = line.strip().split(",", 2)
            if len(parts) >= 2 and parts[0].isdigit():
                instances.append(Instance(int(parts[0]), parts[1]))
        return instances

    def _adb(self, *args: str, timeout: float = 120, check: bool = True) -> str:
        if not self.serial:
            raise RecoveryError("ADB serial ещё не определён")
        return self.runner.run(
            [self.adb, "-s", self.serial, *args], timeout=timeout, check=check
        ).stdout

    def _shell(self, *args: str, timeout: float = 120, check: bool = True) -> str:
        return self._adb("shell", *args, timeout=timeout, check=check)

    def _wait_until(self, predicate: Callable[[], bool], timeout: float, description: str) -> None:
        deadline = time.monotonic() + timeout
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            self._check_cancel()
            try:
                if predicate():
                    return
            except RecoveryError as exc:
                last_error = exc
            time.sleep(2)
        suffix = f": {last_error}" if last_error else ""
        raise RecoveryError(f"Истекло время ожидания: {description}{suffix}")

    def clone_template(self, farm: Farm) -> int:
        instances_before = self.list_instances()
        recovery_prefix = f"RECOVERY_{farm.name}_"
        reusable = [item for item in instances_before if item.name.startswith(recovery_prefix)]
        if len(reusable) > 1:
            raise RecoveryError(
                f"Найдено несколько незавершённых клонов {farm.name}; требуется ручной выбор или очистка"
            )
        if reusable:
            self.new_index = reusable[0].index
            self._repair_missing_vbox(self.new_index)
            self._validate_instance_storage(self.new_index)
            self._step(f"Повторное использование проверенного клона ID {self.new_index}")
            return self.new_index

        self._step(f"Клонирование чистого образа ID {TEMPLATE_INDEX}")
        before = {item.index for item in instances_before}
        temporary_name = f"RECOVERY_{farm.name}_{datetime.now():%Y%m%d_%H%M%S}"
        result = self.runner.run(
            [self.ldconsole, "copy", "--name", temporary_name, "--from", str(TEMPLATE_INDEX)],
            timeout=20 * 60,
            check=False,
        )
        after = self.list_instances()
        created = [item for item in after if item.index not in before]
        if len(created) != 1:
            free = shutil.disk_usage(self.ldplayer_dir).free
            detail = (result.stderr or result.stdout).strip()
            suffix = f"; {detail}" if detail else ""
            raise RecoveryError(
                f"LDPlayer не создал ровно один новый экземпляр; код {result.returncode}; "
                f"свободно {free / 1024**3:.2f} ГБ{suffix}"
            )
        self.new_index = created[0].index
        self._repair_missing_vbox(self.new_index)
        self._validate_instance_storage(self.new_index)
        self.logger.info("Создан ID %s с временным именем %s", self.new_index, temporary_name)
        return self.new_index

    def _repair_missing_vbox(self, index: int) -> None:
        """Достроить метаданные клона, которые LDPlayer теряет у junction-образа.

        На F99 ``ldconsole copy`` успешно копирует VMDK и создаёт запись в
        глобальном config, но не записывает ``leidian.vbox``. Ремонт допустим
        только для ещё не запускавшегося RECOVERY-клона и повторяет точную
        схему UUID штатных клонов LDPlayer.
        """

        vm_path = self.ldplayer_dir / "vms" / f"leidian{index}"
        if list(vm_path.glob("*.vbox")):
            return
        instances = {item.index: item.name for item in self.list_instances()}
        if not instances.get(index, "").startswith("RECOVERY_"):
            raise RecoveryError(f"Отказано в ремонте .vbox для неслужебного ID {index}")
        required_disks = {"data.vmdk", "sdcard.vmdk", "system.vmdk"}
        present_disks = {item.name.casefold() for item in vm_path.glob("*.vmdk")}
        if not required_disks.issubset(present_disks):
            missing = ", ".join(sorted(required_disks - present_disks))
            raise RecoveryError(f"Неполный клон ID {index}: отсутствуют {missing}")

        template_path = (self.ldplayer_dir / "vms" / f"leidian{TEMPLATE_INDEX}").resolve(
            strict=True
        )
        template_vbox = list(template_path.glob("*.vbox"))
        if len(template_vbox) != 1:
            raise RecoveryError("Нельзя восстановить .vbox: у чистого образа нет единственного шаблона")
        text = template_vbox[0].read_text(encoding="utf-8", errors="strict")
        machine_match = re.search(r'<Machine\s+uuid="\{([^}]+)\}"\s+name="([^"]+)"', text)
        disk_match = re.search(
            r'<HardDisk\s+uuid="\{([^}]+)\}"\s+location="data\.vmdk"', text,
            flags=re.IGNORECASE,
        )
        if not machine_match or not disk_match:
            raise RecoveryError("Нельзя восстановить .vbox: структура шаблона LDPlayer неизвестна")

        suffix = f"{index:012x}"
        machine_uuid = f"20160302-aaaa-aaaa-0eee-{suffix}"
        disk_uuid = f"20160302-cccc-cccc-0eee-{suffix}"
        source_machine_uuid = machine_match.group(1)
        source_disk_uuid = disk_match.group(1)
        text = text.replace(f"{{{source_machine_uuid}}}", f"{{{machine_uuid}}}")
        text = text.replace(f"{{{source_disk_uuid}}}", f"{{{disk_uuid}}}")
        text = text.replace(f'name="{machine_match.group(2)}"', f'name="leidian{index}"', 1)
        text, mac_count = re.subn(
            r'MACAddress="[0-9A-Fa-f]{12}"',
            f'MACAddress="00DB{secrets.token_hex(4).upper()}"',
            text,
            count=1,
        )
        if mac_count != 1:
            raise RecoveryError("Нельзя восстановить .vbox: MACAddress не найден в шаблоне")

        data_vmdk = vm_path / "data.vmdk"
        with data_vmdk.open("r+b") as stream:
            head = stream.read(256 * 1024)
            pattern = re.compile(rb'(ddb\.uuid\.image\s*=\s*")([^"]+)(")', re.IGNORECASE)
            uuid_match = pattern.search(head)
            if not uuid_match:
                raise RecoveryError("Нельзя восстановить .vbox: UUID не найден в data.vmdk")
            current_uuid = uuid_match.group(2).decode("ascii")
            allowed = {
                re.sub(r"[^0-9a-f]", "", source_disk_uuid.casefold()),
                re.sub(r"[^0-9a-f]", "", disk_uuid.casefold()),
            }
            if re.sub(r"[^0-9a-f]", "", current_uuid.casefold()) not in allowed:
                raise RecoveryError("Нельзя восстановить .vbox: data.vmdk принадлежит другому экземпляру")
            replacement = disk_uuid.encode("ascii")
            if len(replacement) != len(uuid_match.group(2)):
                raise RecoveryError("Нельзя восстановить .vbox: неожиданная длина UUID data.vmdk")
            repaired_head = head[: uuid_match.start(2)] + replacement + head[uuid_match.end(2) :]
            stream.seek(0)
            stream.write(repaired_head)
            stream.flush()
            os.fsync(stream.fileno())

        target = vm_path / "leidian.vbox"
        temporary = vm_path / "leidian.vbox.recovery.tmp"
        temporary.write_text(text, encoding="utf-8", newline="\n")
        os.replace(temporary, target)
        (vm_path / "player_life").touch(exist_ok=True)
        self.logger.warning("Автоматически восстановлен отсутствующий .vbox для ID %s", index)

    def _validate_instance_storage(self, index: int) -> Path:
        """Проверить путь и UUID конкретного клона до его запуска."""

        vm_path = self.ldplayer_dir / "vms" / f"leidian{index}"
        if not vm_path.is_dir():
            raise RecoveryError(f"Каталог нового экземпляра не найден: {vm_path}")
        try:
            resolved_vm = vm_path.resolve(strict=True)
        except OSError as exc:
            raise RecoveryError(f"Ссылка нового экземпляра повреждена: {exc}") from exc
        self._validate_vmdk_uuids(resolved_vm)
        self.logger.info("Хранилище нового ID %s: %s -> %s", index, vm_path, resolved_vm)
        return resolved_vm

    def _validate_vmdk_uuids(self, vm_path: Path) -> None:
        """Проверить соответствие UUID дисков в VBox и VMDK до первого запуска."""

        vbox_files = list(vm_path.glob("*.vbox"))
        if len(vbox_files) != 1:
            raise RecoveryError(f"В {vm_path} ожидался один файл .vbox")
        try:
            vbox_text = vbox_files[0].read_text(encoding="utf-8", errors="replace")
        except OSError as exc:
            raise RecoveryError(f"Не удалось прочитать {vbox_files[0]}: {exc}") from exc
        expected: dict[str, str] = {}
        for uuid, location in re.findall(
            r'<HardDisk\s+uuid="\{([^}]+)\}"\s+location="([^"]+\.vmdk)"',
            vbox_text,
            flags=re.IGNORECASE,
        ):
            expected[Path(location).name.casefold()] = re.sub(r"[^0-9a-f]", "", uuid.casefold())
        if not expected:
            raise RecoveryError("В .vbox не найдены зарегистрированные VMDK")
        for filename, expected_uuid in expected.items():
            disk = vm_path / filename
            if not disk.is_file():
                raise RecoveryError(f"Не найден диск {disk}")
            try:
                descriptor = disk.open("rb").read(256 * 1024).decode("latin-1", errors="ignore")
            except OSError as exc:
                raise RecoveryError(f"Не удалось прочитать заголовок {disk}: {exc}") from exc
            match = re.search(r'ddb\.uuid\.image\s*=\s*"([^"]+)"', descriptor, flags=re.IGNORECASE)
            if not match:
                raise RecoveryError(f"В {disk.name} не найден ddb.uuid.image")
            actual_uuid = re.sub(r"[^0-9a-f]", "", match.group(1).casefold())
            if actual_uuid != expected_uuid:
                raise RecoveryError(
                    f"UUID диска {disk.name} не совпадает с .vbox; запуск остановлен во избежание InvalidMedium"
                )

    def launch_and_wait(self) -> None:
        if self.new_index is None:
            raise RecoveryError("Новый экземпляр ещё не создан")
        self._step(f"Запуск LDPlayer ID {self.new_index}")
        self.serial = f"emulator-{5554 + self.new_index * 2}"
        self.runner.run([self.ldconsole, "launch", "--index", str(self.new_index)], timeout=60)

        def booted() -> bool:
            state = self.runner.run(
                [self.adb, "-s", self.serial, "get-state"], timeout=10, check=False
            )
            if state.returncode != 0 or "device" not in state.stdout:
                return False
            return self._shell("getprop", "sys.boot_completed", timeout=10, check=False).strip() == "1"

        self._wait_until(booted, 6 * 60, "загрузка Android")
        size = parse_wm_size(self._shell("wm", "size"))
        if size != EXPECTED_SIZE:
            raise RecoveryError(f"Ожидалось разрешение 640x480, получено {size}")

    def _screenshot(self, label: str) -> Path:
        remote = f"/sdcard/{label}.png"
        local = self.work_dir / f"{label}_{self.new_index}.png"
        self._shell("screencap", "-p", remote, timeout=30)
        self._adb("pull", remote, str(local), timeout=60)
        return local

    def _ocr(self, label: str = "screen") -> list[OcrWord]:
        image = self._screenshot(label)
        helper = Path(__file__).with_name("ocr_words.ps1")
        powershell = Path(os.environ.get("WINDIR", r"C:\Windows")) / (
            r"SysWOW64\WindowsPowerShell\v1.0\powershell.exe"
        )
        result = self.runner.run(
            [
                powershell,
                "-NoProfile",
                "-NonInteractive",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                helper,
                "-ImagePath",
                image,
            ],
            timeout=60,
        )
        return parse_ocr_payload(result.stdout)

    @staticmethod
    def _ocr_text(words: Sequence[OcrWord]) -> str:
        return " ".join(word.text for word in words).casefold()

    def _wait_screen(self, markers: Sequence[str], timeout: float, description: str) -> list[OcrWord]:
        found: list[OcrWord] = []

        def matches() -> bool:
            nonlocal found
            found = self._ocr("wait")
            text = self._ocr_text(found)
            return any(marker.casefold() in text for marker in markers)

        self._wait_until(matches, timeout, description)
        return found

    def _tap(self, x: int, y: int) -> None:
        self._shell("input", "tap", str(x), str(y), timeout=15)

    def _current_focus(self) -> str:
        return self._shell("dumpsys", "window", "windows", timeout=30, check=False)

    def _send_private_text(self, value: str) -> None:
        payload = base64.b64encode(value.encode("utf-8")).decode("ascii")
        script = f'v=$(echo {payload} | base64 -d); input text "$v"\n'
        self.runner.run(
            [self.adb, "-s", self.serial, "shell"],
            input_text=script,
            timeout=30,
            sensitive=True,
        )
        payload = ""
        script = ""

    def open_login(self) -> None:
        self._step("Запуск Viking Rise и ожидание экрана авторизации")
        self._shell("am", "force-stop", PACKAGE)
        self._shell("am", "start", "-W", "-n", ACTIVITY, timeout=60)
        deadline = time.monotonic() + 12 * 60
        help_confirmed = False
        while time.monotonic() < deadline:
            self._check_cancel()
            if "GPCPassportWebViewActivity" in self._current_focus():
                break
            try:
                words = self._ocr("login")
            except RecoveryError as exc:
                self.logger.warning("Временная ошибка OCR при запуске игры: %s; повтор через 5 секунд", exc)
                time.sleep(5)
                continue
            action = classify_login_screen(self._ocr_text(words))
            if action == "confirm_expired":
                self._tap(320, 305)
                help_confirmed = True
                time.sleep(3)
                continue
            if action == "choose_provider" or help_confirmed:
                # После подтверждения истёкшей сессии здесь находится IGG Account.
                self._tap(320, 405)
                help_confirmed = False
                time.sleep(5)
                continue
            # На загрузочном экране ничего не нажимаем: ждём ресурсов и следующего кадра.
            time.sleep(5)
        else:
            raise RecoveryError("Не удалось открыть форму IGG Account")
        self._wait_screen(["igg account login", "1gg account login"], 60, "форма IGG Account Login")

    def submit_credentials(self, farm: Farm) -> None:
        self._step(f"Ввод учётных данных фермы {farm.name}")
        self._tap(300, 61)
        self._send_private_text(farm.email)
        self._tap(300, 104)
        self._send_private_text(farm.password)
        self._tap(270, 147)
        words = self._wait_screen(
            ["confirm login", "incorrect", "invalid", "error"],
            90,
            "подтверждение IGG-аккаунта",
        )
        text = self._ocr_text(words)
        if "confirm login" not in text:
            raise RecoveryError("IGG отклонил вход или показал ошибку")
        self._tap(320, 163)

    def select_game_id(self, farm: Farm) -> None:
        self._step("Выбор нужного игрового IGG ID")
        words = self._wait_screen(["select igg id", "select 166 1d"], 90, "список IGG ID")
        x, y = select_igg_row(words, farm.custom)
        self._adb("logcat", "-c", timeout=30)
        self._tap(x, y)

    def wait_for_game(self, timeout: float = DEFAULT_TIMEOUT) -> None:
        self._step("Ожидание загрузки ресурсов и входа на игровую карту")
        deadline = time.monotonic() + timeout
        next_ocr = 0.0
        resource_confirmed = False
        while time.monotonic() < deadline:
            self._check_cancel()
            if not self._shell("pidof", PACKAGE, timeout=10, check=False).strip():
                raise RecoveryError("Viking Rise завершился во время загрузки")
            log = self._adb("logcat", "-d", "-t", "2500", timeout=30, check=False)
            if "MainUIFunctionBtnLayout UpdateVisible" in log or "TopResourcesView:OnEnable()" in log:
                self.logger.info("Игровая карта загружена")
                return
            now = time.monotonic()
            if now >= next_ocr:
                words = self._ocr("loading")
                text = self._ocr_text(words)
                if is_resource_prompt(text):
                    self._tap(393, 307)
                    resource_confirmed = True
                    self.status("Загрузка обязательных ресурсов; это может занять несколько минут")
                next_ocr = now + 20
            time.sleep(5)
        detail = " после подтверждения загрузки ресурсов" if resource_confirmed else ""
        raise RecoveryError(f"Игра не вышла на карту за {timeout // 60} минут{detail}")

    def rename_transaction(self, farm_name: str, keep_old: bool = True) -> tuple[str | None, int]:
        if self.new_index is None:
            raise RecoveryError("Новый экземпляр ещё не создан")
        self._step("Безопасное переименование эмуляторов")
        instances = self.list_instances()
        existing = sorted(
            (item for item in instances if item.name == farm_name and item.index != self.new_index),
            key=lambda item: item.index,
        )
        if existing and not keep_old:
            raise RecoveryError("Удаление старого эмулятора запрещено этой версией автоматизации")
        used_names = {item.name for item in instances}
        renamed_old: list[tuple[Instance, str]] = []
        try:
            for old in existing:
                backup_name = choose_backup_name(farm_name, used_names)
                self.runner.run(
                    [self.ldconsole, "rename", "--index", str(old.index), "--title", backup_name],
                    timeout=60,
                )
                renamed_old.append((old, backup_name))
                used_names.add(backup_name)
            self.runner.run(
                [self.ldconsole, "rename", "--index", str(self.new_index), "--title", farm_name], timeout=60
            )
        except Exception:
            for old, backup_name in reversed(renamed_old):
                self.runner.run(
                    [self.ldconsole, "rename", "--index", str(old.index), "--title", farm_name],
                    timeout=60,
                    check=False,
                )
            raise
        current = {item.index: item.name for item in self.list_instances()}
        if current.get(self.new_index) != farm_name:
            raise RecoveryError("Не удалось подтвердить новое имя эмулятора")
        for old, backup_name in renamed_old:
            if current.get(old.index) != backup_name:
                raise RecoveryError("Не удалось подтвердить имя сохранённого старого эмулятора")
        backup_summary = ", ".join(name for _, name in renamed_old) or None
        return backup_summary, self.new_index

    def recover(self, farm: Farm) -> tuple[str | None, int]:
        """Выполнить полный цикл восстановления выбранной фермы."""

        if not farm.ready:
            raise RecoveryError("Для фермы нужны непустые Email/Password и Slot=igg")
        self.validate()
        self.clone_template(farm)
        self.launch_and_wait()
        self.open_login()
        self.submit_credentials(farm)
        self.select_game_id(farm)
        self.wait_for_game()
        backup_name, new_index = self.rename_transaction(farm.name)
        self._step("Обновление LDPlayer ID в профиле GnBots")
        old_index = update_profile_instance(self.profile_path, farm, new_index)
        self.logger.info(
            "Профиль GnBots обновлён: ферма %s, LDPlayer ID %s -> %s",
            farm.name,
            old_index if old_index is not None else "—",
            new_index,
        )
        return backup_name, new_index


class SingleRunLock:
    """Межпроцессная блокировка, не позволяющая запустить два клонирования."""

    def __init__(self, path: Path):
        self.path = path
        self.handle = None

    def __enter__(self) -> "SingleRunLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.path.open("a+b")
        self.handle.seek(0, os.SEEK_END)
        if self.handle.tell() == 0:
            self.handle.write(b"0")
            self.handle.flush()
        self.handle.seek(0)
        try:
            msvcrt.locking(self.handle.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError as exc:
            self.handle.close()
            raise RecoveryError("Уже выполняется другое восстановление") from exc
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        if self.handle:
            self.handle.seek(0)
            msvcrt.locking(self.handle.fileno(), msvcrt.LK_UNLCK, 1)
            self.handle.close()


def configure_logging(log_path: Path | None = None) -> logging.Logger:
    """Создать журнал без секретных данных."""

    path = log_path or Path(__file__).with_name("logs") / "viking_recovery.log"
    path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("viking_recovery")
    logger.setLevel(logging.DEBUG)
    if not logger.handlers:
        handler = logging.FileHandler(path, encoding="utf-8")
        handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        logger.addHandler(handler)
    return logger


def find_farm(name: str, farms: Sequence[Farm]) -> Farm:
    matches = [farm for farm in farms if farm.name.casefold() == name.casefold()]
    if len(matches) != 1:
        raise RecoveryError(f"Ферма {name!r} не найдена однозначно")
    return matches[0]


def main(argv: Sequence[str] | None = None) -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--list", action="store_true", help="показать доступные фермы без секретов")
    parser.add_argument("--check", action="store_true", help="проверить окружение без изменений")
    parser.add_argument("--farm", help="запустить восстановление указанной фермы")
    parser.add_argument("--dry-run", action="store_true", help="проверить выбранную ферму без изменений")
    args = parser.parse_args(argv)
    logger = configure_logging()
    try:
        farms = load_farms()
        if args.list:
            for farm in farms:
                print(f"{farm.name}\tIGG={farm.custom or '-'}\tготова={'да' if farm.ready else 'нет'}")
            return 0
        engine = RecoveryEngine(logger, status=print)
        if args.check:
            engine.validate()
            print(f"Проверка пройдена. Ферм в профиле: {len(farms)}")
            return 0
        if args.farm:
            farm = find_farm(args.farm, farms)
            if args.dry_run:
                if not farm.ready:
                    raise RecoveryError("Для фермы нужны непустые Email/Password и Slot=igg")
                engine.validate()
                print(f"Ферма {farm.name} готова к восстановлению; Slot={farm.slot}; IGG={farm.custom or '-'}")
                return 0
            with SingleRunLock(Path(tempfile.gettempdir()) / "VikingRecovery" / "run.lock"):
                backup, index = engine.recover(farm)
            print(
                f"Готово: {farm.name}, новый ID {index} записан в профиль GnBots, "
                f"старый экземпляр: {backup or 'не найден'}"
            )
            return 0
        parser.print_help()
        return 0
    except RecoveryError as exc:
        logger.exception("Восстановление завершилось ошибкой: %s", exc)
        print(f"Ошибка: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
