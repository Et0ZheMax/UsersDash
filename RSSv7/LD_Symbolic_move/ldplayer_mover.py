#!/usr/bin/env python3
"""Safely move selected LDPlayer virtual machines to another Windows drive.

The original VM path is replaced with a directory junction (default) or a true
symbolic link, so LDPlayer keeps using its usual path.
"""

from __future__ import annotations

import argparse
import csv
import ctypes
import json
import os
import re
import shutil
import subprocess
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable


DEFAULT_SOURCE = Path(r"C:\LDPlayer\LDPlayer9\vms")
DEFAULT_DESTINATION = Path(r"V:\vms")
VM_NAME_RE = re.compile(r"^leidian(\d+)$", re.IGNORECASE)
REPARSE_POINT_ATTRIBUTE = 0x400
LDPLAYER_PROCESSES = {
    "dnmultiplayer.exe",
    "dnplayer.exe",
    "ld9boxheadless.exe",
    "ldplayer.exe",
    "ldconsole.exe",
    "ldmultiplayer.exe",
    "ldmultiplayerex.exe",
    "ldvboxheadless.exe",
}


@dataclass
class VM:
    name: str
    index: int
    source: Path
    destination: Path
    is_link: bool
    destination_exists: bool
    bytes_used: int = 0
    logical_bytes: int = 0
    files: int = 0
    scan_error: str | None = None

    @property
    def movable(self) -> bool:
        return not self.is_link and not self.destination_exists and not self.scan_error

    @property
    def recommended(self) -> bool:
        return self.movable and self.index % 2 == 0


class MoveError(RuntimeError):
    pass


def enable_ansi() -> None:
    """Enable ANSI escape processing on supported Windows consoles."""
    if os.name != "nt":
        return
    kernel32 = ctypes.windll.kernel32
    handle = kernel32.GetStdHandle(-11)
    mode = ctypes.c_uint()
    if kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
        kernel32.SetConsoleMode(handle, mode.value | 0x0004)


def is_reparse_point(path: Path) -> bool:
    if os.name != "nt":
        return path.is_symlink()
    attrs = ctypes.windll.kernel32.GetFileAttributesW(str(path))
    # ctypes returns a signed c_int by default, so INVALID_FILE_ATTRIBUTES can
    # be either -1 or 0xFFFFFFFF depending on the Python/Windows combination.
    return attrs not in (-1, 0xFFFFFFFF) and bool(attrs & REPARSE_POINT_ATTRIBUTE)


def human_size(value: int) -> str:
    units = ("Б", "КБ", "МБ", "ГБ", "ТБ")
    amount = float(value)
    for unit in units:
        if amount < 1024 or unit == units[-1]:
            return f"{amount:.1f} {unit}" if unit != "Б" else f"{int(amount)} {unit}"
        amount /= 1024
    return f"{amount:.1f} ТБ"


def allocated_file_size(path: Path, logical_size: int) -> int:
    """Return allocated bytes where the platform exposes them.

    LDPlayer virtual disks can be sparse, so logical file length may greatly
    exceed the space that will actually be freed on C:.
    """
    if os.name == "nt":
        high = ctypes.c_ulong(0)
        function = ctypes.windll.kernel32.GetCompressedFileSizeW
        function.restype = ctypes.c_uint32
        low = function(str(path), ctypes.byref(high))
        if low != 0xFFFFFFFF:
            return (high.value << 32) | low
        return logical_size
    stat = path.stat()
    return getattr(stat, "st_blocks", 0) * 512 or logical_size


def tree_stats(root: Path) -> tuple[int, int, int]:
    allocated = 0
    logical = 0
    count = 0
    for current, dirs, files in os.walk(root, followlinks=False):
        current_path = Path(current)
        dirs[:] = [name for name in dirs if not is_reparse_point(current_path / name)]
        for name in files:
            path = current_path / name
            if path.is_symlink():
                continue
            try:
                logical_size = path.stat().st_size
                logical += logical_size
                allocated += allocated_file_size(path, logical_size)
                count += 1
            except FileNotFoundError:
                # A transient file will be caught by the verification before move.
                continue
    return allocated, logical, count


def tree_manifest(root: Path) -> dict[str, int]:
    result: dict[str, int] = {}
    for current, dirs, files in os.walk(root, followlinks=False):
        current_path = Path(current)
        for name in dirs:
            path = current_path / name
            if is_reparse_point(path):
                raise MoveError(f"Внутри ВМ найдена ссылка на папку: {path}")
            relative = str(path.relative_to(root)).casefold() + os.sep
            result[relative] = -1
        for name in files:
            path = current_path / name
            if path.is_symlink():
                raise MoveError(f"Внутри ВМ найдена файловая ссылка: {path}")
            relative = str(path.relative_to(root)).casefold()
            result[relative] = path.stat().st_size
    return result


def scan_vms(source_root: Path, destination_root: Path) -> list[VM]:
    found: list[VM] = []
    for entry in source_root.iterdir():
        match = VM_NAME_RE.fullmatch(entry.name)
        if not match:
            continue
        linked = is_reparse_point(entry)
        if not linked and not entry.is_dir():
            continue
        target = destination_root / entry.name
        found.append(
            VM(
                name=entry.name,
                index=int(match.group(1)),
                source=entry,
                destination=target,
                is_link=linked,
                destination_exists=target.exists() or is_reparse_point(target),
            )
        )

    local = [vm for vm in found if not vm.is_link]
    with ThreadPoolExecutor(max_workers=min(4, max(1, len(local)))) as pool:
        futures = {pool.submit(tree_stats, vm.source): vm for vm in local}
        for future in as_completed(futures):
            vm = futures[future]
            try:
                vm.bytes_used, vm.logical_bytes, vm.files = future.result()
            except (OSError, PermissionError) as exc:
                vm.scan_error = str(exc)
    return sorted(found, key=lambda vm: vm.index)


def status_text(vm: VM) -> str:
    if vm.is_link:
        return "уже перенесён (ссылка)"
    if vm.scan_error:
        return "ошибка чтения"
    if vm.destination_exists:
        return "конфликт: папка уже есть на резервном диске"
    return "РЕКОМЕНДУЕТСЯ" if vm.recommended else "можно перенести"


def print_table(vms: list[VM]) -> list[VM]:
    candidates = [vm for vm in vms if vm.movable]
    number_by_name = {vm.name: pos for pos, vm in enumerate(candidates, 1)}
    print("\nНайденные эмуляторы:")
    print(f"{'№':>3}  {'Имя':<14} {'Размер':>11}  Состояние")
    print("-" * 73)
    for vm in vms:
        number = str(number_by_name[vm.name]) if vm.movable else "—"
        size = human_size(vm.bytes_used) if not vm.is_link and not vm.scan_error else "—"
        print(f"{number:>3}  {vm.name:<14} {size:>11}  {status_text(vm)}")
    return candidates


def parse_selection(value: str, count: int) -> set[int]:
    selected: set[int] = set()
    normalized = value.strip().replace(" ", "")
    if not normalized:
        return selected
    for part in normalized.split(","):
        if not part:
            raise ValueError("пустой элемент списка")
        if "-" in part:
            pieces = part.split("-", 1)
            if len(pieces) != 2 or not all(piece.isdigit() for piece in pieces):
                raise ValueError(f"неверный диапазон: {part}")
            start, end = map(int, pieces)
            if start > end:
                raise ValueError(f"диапазон задан наоборот: {part}")
            selected.update(range(start, end + 1))
        elif part.isdigit():
            selected.add(int(part))
        else:
            raise ValueError(f"неверный номер: {part}")
    invalid = sorted(number for number in selected if number < 1 or number > count)
    if invalid:
        raise ValueError(f"нет номеров: {', '.join(map(str, invalid))}")
    return selected


def choose_vms(candidates: list[VM]) -> list[VM]:
    recommended_numbers = {
        pos for pos, vm in enumerate(candidates, 1) if vm.recommended
    }
    while True:
        print("\nВыбор: R — рекомендуемые, A — все доступные, Q — выход")
        print("Или номера через запятую/диапазон, например: 2,4-7")
        answer = input("Что переносим? [R]: ").strip()
        command = answer.casefold()
        if command in {"q", "й", "quit", "выход"}:
            return []
        if not answer or command in {"r", "к"}:
            numbers = recommended_numbers
        elif command in {"a", "ф", "all", "все"}:
            numbers = set(range(1, len(candidates) + 1))
        else:
            try:
                numbers = parse_selection(answer, len(candidates))
            except ValueError as exc:
                print(f"Ошибка выбора: {exc}")
                continue
        if not numbers:
            print("В этой группе нет доступных эмуляторов.")
            continue
        return [vm for pos, vm in enumerate(candidates, 1) if pos in numbers]


def running_ldplayer_processes() -> list[str]:
    if os.name != "nt":
        return []
    try:
        output = subprocess.run(
            ["tasklist", "/FO", "CSV", "/NH"],
            check=True,
            capture_output=True,
            text=True,
            errors="replace",
        ).stdout
    except (OSError, subprocess.CalledProcessError):
        return []
    found = set()
    for row in csv.reader(output.splitlines()):
        if row and row[0].casefold() in LDPLAYER_PROCESSES:
            found.add(row[0])
    return sorted(found, key=str.casefold)


def validate_roots(source: Path, destination: Path) -> None:
    if not source.exists() or not source.is_dir():
        raise MoveError(f"Исходная папка не найдена: {source}")
    if source.drive.casefold() == destination.drive.casefold():
        raise MoveError("Исходная и резервная папки должны быть на разных дисках.")
    drive_root = Path(destination.anchor)
    if not drive_root.exists():
        raise MoveError(f"Резервный диск недоступен: {drive_root}")


def write_journal(path: Path, payload: dict[str, str]) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    temporary.replace(path)


def remove_tree(path: Path) -> None:
    def make_writable(function: Callable[..., object], target: str, _: object) -> None:
        os.chmod(target, 0o700)
        function(target)

    if path.exists():
        # onerror keeps the utility compatible with Python 3.10/3.11, which
        # are still common on Windows servers (onexc was added in 3.12).
        shutil.rmtree(path, onerror=make_writable)


def create_directory_link(link: Path, target: Path, link_type: str) -> None:
    if link_type == "symlink":
        os.symlink(target, link, target_is_directory=True)
        return
    result = subprocess.run(
        ["cmd", "/d", "/c", "mklink", "/J", str(link), str(target)],
        capture_output=True,
        text=True,
        errors="replace",
    )
    if result.returncode != 0:
        message = (result.stderr or result.stdout).strip()
        raise MoveError(f"не удалось создать junction: {message}")


def copy_with_progress(
    source: Path,
    staging: Path,
    total: int,
    progress_callback: Callable[[int, int], None] | None = None,
) -> None:
    copied = 0
    last_update = 0.0

    def copy_file(src: str, dst: str, *, follow_symlinks: bool = True) -> str:
        nonlocal copied, last_update
        result = shutil.copy2(src, dst, follow_symlinks=follow_symlinks)
        copied += os.path.getsize(src)
        now = time.monotonic()
        if now - last_update >= 0.4 or copied >= total:
            if progress_callback:
                progress_callback(copied, total)
            else:
                percent = 100.0 if total == 0 else min(100.0, copied * 100 / total)
                print(
                    f"\r    Копирование: {percent:6.1f}% "
                    f"({human_size(copied)} / {human_size(total)})",
                    end="",
                    flush=True,
                )
            last_update = now
        return result

    try:
        shutil.copytree(source, staging, copy_function=copy_file, symlinks=True)
    finally:
        if progress_callback:
            progress_callback(copied, total)
        else:
            print()


def move_one(
    vm: VM,
    destination_root: Path,
    link_type: str,
    progress_callback: Callable[[int, int], None] | None = None,
    status_callback: Callable[[str], None] | None = None,
) -> str | None:
    token = uuid.uuid4().hex[:10]
    staging = destination_root / f".{vm.name}.copying-{token}"
    backup = vm.source.parent / f".{vm.name}.original-{token}"
    journal = vm.source.parent / f".ldplayer-mover-{vm.name}.json"
    payload = {
        "vm": vm.name,
        "source": str(vm.source),
        "destination": str(vm.destination),
        "staging": str(staging),
        "backup": str(backup),
        "stage": "starting",
    }

    switched = False
    destination_created = False
    try:
        if is_reparse_point(vm.source) or not vm.source.is_dir():
            raise MoveError("исходная папка изменилась после сканирования")
        if vm.destination.exists() or is_reparse_point(vm.destination):
            raise MoveError("целевая папка уже существует")

        if status_callback:
            status_callback("Подготовка списка файлов")
        before = tree_manifest(vm.source)
        total = sum(size for size in before.values() if size >= 0)
        payload["stage"] = "copying"
        write_journal(journal, payload)
        if status_callback:
            status_callback("Копирование")
        copy_with_progress(vm.source, staging, total, progress_callback)

        if status_callback:
            status_callback("Проверка копии")
        else:
            print("    Проверка копии...", end="", flush=True)
        after_source = tree_manifest(vm.source)
        after_copy = tree_manifest(staging)
        if before != after_source:
            raise MoveError("файлы ВМ изменились во время копирования; перенос отменён")
        if before != after_copy:
            raise MoveError("копия не прошла проверку по именам и размерам")
        if not status_callback:
            print(" готово")

        payload["stage"] = "renaming_copy"
        write_journal(journal, payload)
        staging.rename(vm.destination)
        destination_created = True
        payload["stage"] = "copied"
        write_journal(journal, payload)

        vm.source.rename(backup)
        payload["stage"] = "source_renamed"
        write_journal(journal, payload)

        if status_callback:
            status_callback("Создание ссылки")
        create_directory_link(vm.source, vm.destination, link_type)
        switched = True
        payload["stage"] = "link_created"
        write_journal(journal, payload)
    except Exception as exc:
        # Once the verified destination is linked, it is the safe canonical
        # copy. Never roll back to an original that cleanup may have partly
        # deleted; the next launch can finish that cleanup from the journal.
        if switched:
            return (
                "перенос завершён, но старую копию не удалось полностью удалить; "
                "очистка будет повторена при следующем запуске"
            )
        # Restore the original path whenever the switch was already started.
        try:
            if is_reparse_point(vm.source):
                os.rmdir(vm.source)
            if backup.exists() and not vm.source.exists():
                backup.rename(vm.source)
            if staging.exists():
                remove_tree(staging)
            if (
                destination_created
                and vm.destination.exists()
                and vm.source.exists()
                and not is_reparse_point(vm.source)
            ):
                remove_tree(vm.destination)
            journal.unlink(missing_ok=True)
        except Exception as rollback_exc:
            raise MoveError(
                f"{exc}; также не завершён автоматический откат: {rollback_exc}. "
                f"Не удаляйте вручную {backup} и {vm.destination}."
            ) from exc
        if isinstance(exc, MoveError):
            raise
        raise MoveError(str(exc)) from exc

    try:
        if status_callback:
            status_callback("Очистка исходного диска")
        remove_tree(backup)
        journal.unlink(missing_ok=True)
    except Exception:
        return (
            "перенос завершён, но старую копию не удалось полностью удалить; "
            "очистка будет повторена при следующем запуске"
        )
    return None


def recover_journals(
    source_root: Path,
    destination_root: Path,
    report_callback: Callable[[str], None] | None = None,
) -> None:
    journals = list(source_root.glob(".ldplayer-mover-*.json"))
    if not journals:
        return
    report = report_callback or print
    report("Обнаружена незавершённая предыдущая операция. Выполняю безопасный откат.")
    for journal in journals:
        try:
            data = json.loads(journal.read_text(encoding="utf-8"))
            source = Path(data["source"])
            destination = Path(data["destination"])
            staging = Path(data["staging"])
            backup = Path(data["backup"])
            vm_name = str(data["vm"])
            stage = str(data.get("stage", ""))
            expected_source = source_root / vm_name
            expected_destination = destination_root / vm_name
            if (
                not VM_NAME_RE.fullmatch(vm_name)
                or source != expected_source
                or destination != expected_destination
                or staging.parent != destination_root
                or backup.parent != source_root
                or not staging.name.startswith(f".{vm_name}.copying-")
                or not backup.name.startswith(f".{vm_name}.original-")
            ):
                raise MoveError("журнал содержит пути вне разрешённых папок")
            destination_owned = stage in {
                "renaming_copy",
                "copied",
                "source_renamed",
                "link_created",
            }
            if is_reparse_point(source) and backup.exists():
                # The link is valid only if its target survived.
                if destination.exists():
                    remove_tree(backup)
                    journal.unlink(missing_ok=True)
                    report(f"{data['vm']}: перенос был завершён, удалён остаток оригинала.")
                    continue
                os.rmdir(source)
            if is_reparse_point(source) and destination.exists() and not backup.exists():
                journal.unlink(missing_ok=True)
                report(f"{data['vm']}: перенос уже был успешно завершён.")
                continue
            if backup.exists() and not source.exists():
                backup.rename(source)
            if staging.exists():
                remove_tree(staging)
            if (
                destination_owned
                and destination.exists()
                and source.exists()
                and not is_reparse_point(source)
            ):
                remove_tree(destination)
            journal.unlink(missing_ok=True)
            report(f"{data['vm']}: исходное состояние восстановлено.")
        except Exception as exc:
            raise MoveError(f"Не удалось восстановить операцию из {journal}: {exc}") from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Перенос ВМ LDPlayer на другой диск с сохранением исходного пути."
    )
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE, help="папка vms LDPlayer")
    parser.add_argument(
        "--destination", type=Path, default=DEFAULT_DESTINATION, help="резервная папка vms"
    )
    parser.add_argument(
        "--link-type",
        choices=("junction", "symlink"),
        default="junction",
        help="тип ссылки; junction обычно не требует прав администратора",
    )
    parser.add_argument("--scan-only", action="store_true", help="только показать состояние")
    parser.add_argument("--yes", action="store_true", help="не спрашивать финальное подтверждение")
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    enable_ansi()
    args = build_parser().parse_args(argv)
    source = args.source.absolute()
    destination = args.destination.absolute()
    print("LDPlayer VM Mover")
    print(f"Источник:  {source}")
    print(f"Назначение: {destination}")
    print(f"Ссылка:     {args.link_type}")

    try:
        validate_roots(source, destination)
        recover_journals(source, destination)
        vms = scan_vms(source, destination)
    except MoveError as exc:
        print(f"\nОШИБКА: {exc}", file=sys.stderr)
        return 1
    except PermissionError as exc:
        print(f"\nНЕТ ДОСТУПА: {exc}", file=sys.stderr)
        return 1

    if not vms:
        print("\nПапки вида leidianN не найдены.")
        return 0
    candidates = print_table(vms)
    if args.scan_only:
        return 0
    if not candidates:
        print("\nНет доступных для переноса эмуляторов.")
        return 0

    selected = choose_vms(candidates)
    if not selected:
        print("Перенос отменён.")
        return 0
    total = sum(vm.bytes_used for vm in selected)
    free = shutil.disk_usage(destination.anchor).free
    # Windows copy preserves sparse-file allocation on NTFS. If a target
    # filesystem behaves differently, disk-full still fails before switching
    # the original path and the temporary copy is removed.
    reserve = max(2 * 1024**3, int(total * 0.05))
    print("\nБудут перенесены: " + ", ".join(vm.name for vm in selected))
    print(f"Освободится на диске C: примерно {human_size(total)}")
    print(f"Свободно на {destination.drive}: {human_size(free)}")
    if total + reserve > free:
        print(
            f"ОШИБКА: нужно примерно {human_size(total + reserve)} "
            "с учётом запаса. Выберите меньше эмуляторов.",
            file=sys.stderr,
        )
        return 1

    running = running_ldplayer_processes()
    if running:
        print("\nОШИБКА: сначала полностью закройте LDPlayer:", ", ".join(running))
        return 1
    if not args.yes:
        confirmation = input("\nДля запуска переноса введите ДА: ").strip().casefold()
        if confirmation not in {"да", "yes"}:
            print("Перенос отменён.")
            return 0

    try:
        destination.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        print(f"ОШИБКА: не удалось создать папку назначения: {exc}", file=sys.stderr)
        return 1
    succeeded: list[tuple[VM, str | None]] = []
    failed: list[tuple[VM, str]] = []
    for position, vm in enumerate(selected, 1):
        print(f"\n[{position}/{len(selected)}] {vm.name} ({human_size(vm.bytes_used)})")
        try:
            running = running_ldplayer_processes()
            if running:
                raise MoveError(
                    "LDPlayer был запущен во время операции: " + ", ".join(running)
                )
            warning = move_one(vm, destination, args.link_type)
            succeeded.append((vm, warning))
            print("    Готово: оригинальный путь теперь ведёт на резервный диск.")
            if warning:
                print(f"    ПРЕДУПРЕЖДЕНИЕ: {warning}")
        except MoveError as exc:
            failed.append((vm, str(exc)))
            print(f"    ОШИБКА: {exc}", file=sys.stderr)

    print("\nИтог:")
    if succeeded:
        cleaned = [vm for vm, warning in succeeded if warning is None]
        pending = [vm for vm, warning in succeeded if warning is not None]
        print(f"  Перенесено: {', '.join(vm.name for vm, _ in succeeded)}")
        if cleaned:
            freed = sum(vm.bytes_used for vm in cleaned)
            print(f"  Освобождено на C: примерно {human_size(freed)}")
        if pending:
            print(
                "  Ожидают очистки старой копии: "
                + ", ".join(vm.name for vm in pending)
            )
    if failed:
        print("  Не перенесены:")
        for vm, error in failed:
            print(f"    {vm.name}: {error}")
    return 1 if failed else 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nОперация прервана пользователем.")
        raise SystemExit(130)
