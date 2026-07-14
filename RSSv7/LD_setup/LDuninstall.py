from __future__ import annotations

import argparse
import copy
import ctypes
from ctypes import wintypes
from datetime import datetime
import json
import logging
import ntpath
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import traceback
from typing import Any, Iterable


# =============================================================================
# БАЗОВЫЕ ПУТИ И ГЛОБАЛЬНОЕ СОСТОЯНИЕ
# =============================================================================

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "ldplayer_reinstall_config.json"
STATE_PATH = BASE_DIR / "ldplayer_reinstall_state.json"
RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")

LOGGER = logging.getLogger("ldplayer_reinstall")
RUN_DIR: Path | None = None
MUTEX_HANDLE: int | None = None

CONFIG: dict[str, Any] = {}
PATHS: dict[str, Path] = {}
TEMPLATE_PATHS: dict[str, Path] = {}

# path.png больше не нужен:
# путь установки читается непосредственно из текстового поля через Ctrl+C.
REQUIRED_TEMPLATE_KEYS = (
    "delete",
    "delete_done",
    "window",
    "settings",
    "install",
    "install_alt",
    "done",
)

# Импортируем GUI-библиотеки безопасно.
# Если зависимость отсутствует, health-check остановит работу до удаления LDPlayer.
PYAUTOGUI_ERROR: Exception | None = None
CV2_ERROR: Exception | None = None

try:
    import pyautogui
    import pyscreeze
except Exception as exc:
    pyautogui = None  # type: ignore[assignment]
    pyscreeze = None  # type: ignore[assignment]
    PYAUTOGUI_ERROR = exc

try:
    import cv2
except Exception as exc:
    cv2 = None  # type: ignore[assignment]
    CV2_ERROR = exc


# =============================================================================
# КОНФИГУРАЦИЯ ПО УМОЛЧАНИЮ
# =============================================================================

DEFAULT_CONFIG: dict[str, Any] = {
    "paths": {
        "uninstaller": r"C:\LDPlayer\LDPlayer9\dnuninst.exe",
        "installer": r"C:\LDPlayer.exe",
        "target_dir": r"C:\LDPlayer\LDPlayer9",

        # Если оставить пустым, используется target_dir\vms.
        "vms_dir": "",

        # Фиксированная временная точка резервирования.
        "backup_vms": r"C:\LD_backup\vms",

        # Относительные пути считаются от папки скрипта.
        "templates_dir": ".",
        "logs_dir": "logs",
    },

    # Процессы LDPlayer, которые нужно остановить перед работой с vms.
    "processes": [
        "dnplayer.exe",
        "ld.exe",
        "ldconsole.exe",
        "dnmultiplayer.exe",
        "LdVBoxHeadless.exe",
        "Ld9BoxHeadless.exe",
        "ldadb.exe",
        "dnuninst.exe",
        "ldinst.exe",
    ],

    # Настройки PNG-шаблонов.
    # Слишком низкий confidence не применяется из-за риска ложных кликов.
    "templates": {
        "delete": {
            "file": "del.png",
            "timeout": 20,
            "confidence": [0.82, 0.77, 0.72],
            "grayscale_fallback": True,
        },
        "delete_done": {
            "file": "del_done.png",
            "timeout": 20,
            "confidence": [0.82, 0.77, 0.72],
            "grayscale_fallback": True,
        },
        "window": {
            "file": "window.png",
            "timeout": 40,
            "confidence": [0.82, 0.77, 0.72],
            "grayscale_fallback": True,
        },
        "settings": {
            "file": "settings.png",
            "timeout": 20,
            "confidence": [0.84, 0.79, 0.74],
            "grayscale_fallback": True,
        },
        "install": {
            "file": "install_button.png",
            "timeout": 20,
            "confidence": [0.84, 0.79, 0.74],
            "grayscale_fallback": True,
        },
        "install_alt": {
            "file": "install_button2.png",
            "timeout": 20,
            "confidence": [0.84, 0.79, 0.74],
            "grayscale_fallback": True,
        },
        "done": {
            "file": "done.png",
            "timeout": 300,
            "confidence": [0.82, 0.77, 0.72],
            "grayscale_fallback": True,
        },
    },

    "ui": {
        "pyautogui_pause": 0.45,
        "failsafe": True,
        "search_interval": 0.40,

        # Насколько расширять поиск вокруг window.png.
        "window_region_padding": 120,

        # Слепое нажатие Enter при отсутствии settings.png выключено.
        "allow_enter_if_settings_missing": False,
    },

    # Скрипт сохраняет весь путь, предложенный установщиком,
    # и при необходимости меняет только букву диска.
    "install_path": {
        # Важно: здесь должна быть латинская C.
        "required_drive": "C",

        # Старое относительное положение оставлено для совместимости.
        # Оно используется только как один из запасных вариантов.
        "field_relative": [0.40, 0.90],

        # Основные варианты положения поля относительно полного окна.
        # Скрипт перебирает их и принимает только значение,
        # похожее на настоящий Windows-путь вида C:\\folder.
        "field_relative_candidates": [
            [0.25, 0.84],
            [0.40, 0.84],
            [0.55, 0.84],
            [0.25, 0.86],
            [0.40, 0.86],
            [0.25, 0.82],
            [0.40, 0.82]
        ],

        # Дополнительный, более устойчивый способ:
        # поле LDPlayer обычно находится примерно в 75–95 пикселях
        # от нижней границы окна независимо от его высоты.
        "field_bottom_offsets": [78, 84, 90],

        # Горизонтальные точки внутри длинного поля пути.
        # Значения специально не доходят до кнопки «Обзор...».
        "field_x_candidates": [0.20, 0.35, 0.50, 0.65],

        # Автоматические попытки чтения, замены и повторной проверки.
        "auto_attempts": 3,

        # Задержка операций с буфером обмена.
        "clipboard_delay": 0.25,

        # Максимум повторных ручных проверок.
        "manual_max_checks": 10,

        # Если поле невозможно прочитать автоматически, разрешить пользователю
        # визуально подтвердить, что путь уже начинается с латинского C:.
        "manual_allow_confirm_without_read": True,

        # Нажать Tab после вставки, чтобы установщик применил поле.
        "commit_with_tab": True,

        # Вернуть текстовый буфер обмена пользователя после проверки.
        "restore_clipboard": True,
    },

    "timeouts": {
        "process_stop": 20,
        "uninstaller_exit": 120,
        "after_delete_click": 3,
        "after_delete_done_click": 1,
        "after_installer_start": 2,
        "after_settings_click": 1,
        "after_install_click": 1,
        "verify_installation": 60,
    },

    "safety": {
        # Если найденная vms пуста, скрипт не продолжит работу.
        "require_nonempty_vms": True,

        # Для обычной папки vms по умолчанию запрещён перенос между дисками.
        # Для junction это ограничение не применяется.
        "allow_cross_volume_backup": False,

        # Запас свободного места для междискового копирования.
        "minimum_free_space_factor": 1.20,

        # Автоматически восстановить незавершённый прошлый запуск.
        "auto_recover_previous_state": True,

        # Не устанавливать поверх существующей установки без деинсталлятора.
        "allow_install_over_existing_without_uninstaller": False,
    },

    "verification": {
        # После установки должен появиться хотя бы один файл из списка.
        "expected_files_any": [
            "dnplayer.exe",
            "ldconsole.exe",
            "ld.exe",
        ]
    },

    "debug": {
        "screenshot_before_critical_click": True,
        "screenshot_on_error": True,
        "keep_healthcheck_screenshot": False,
    },
}


# =============================================================================
# ОБЩИЕ ФУНКЦИИ
# =============================================================================


def configure_console() -> None:
    """Включает UTF-8 для консоли, если текущая версия Python это поддерживает."""

    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass


def show_message(title: str, text: str, *, error: bool = False) -> None:
    """Показывает стандартное окно Windows без зависимости от PyAutoGUI."""

    if os.name != "nt":
        print(f"{title}: {text}")
        return

    icon = 0x10 if error else 0x40

    try:
        ctypes.windll.user32.MessageBoxW(None, text, title, icon)
    except Exception:
        print(f"{title}: {text}")


def set_dpi_awareness() -> None:
    """Уменьшает расхождение координат при масштабе Windows 125–150%."""

    if os.name != "nt":
        return

    try:
        ctypes.windll.shcore.SetProcessDpiAwareness(2)
    except Exception:
        try:
            ctypes.windll.user32.SetProcessDPIAware()
        except Exception:
            pass


def is_admin() -> bool:
    """Проверяет, запущен ли процесс от имени администратора."""

    try:
        return bool(ctypes.windll.shell32.IsUserAnAdmin())
    except Exception:
        return False


def elevate_self() -> None:
    """Перезапускает текущий скрипт от имени администратора."""

    if getattr(sys, "frozen", False):
        executable = sys.executable
        arguments = subprocess.list2cmdline(sys.argv[1:])
    else:
        executable = sys.executable
        arguments = subprocess.list2cmdline(
            [str(Path(sys.argv[0]).resolve()), *sys.argv[1:]]
        )

    result = ctypes.windll.shell32.ShellExecuteW(
        None,
        "runas",
        executable,
        arguments,
        str(BASE_DIR),
        1,
    )

    if result <= 32:
        raise PermissionError(
            f"Не удалось получить права администратора: код {result}"
        )


def acquire_mutex() -> None:
    """Запрещает одновременно запускать два экземпляра скрипта."""

    global MUTEX_HANDLE

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)

    kernel32.CreateMutexW.argtypes = [
        wintypes.LPVOID,
        wintypes.BOOL,
        wintypes.LPCWSTR,
    ]
    kernel32.CreateMutexW.restype = wintypes.HANDLE

    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL

    handle = kernel32.CreateMutexW(
        None,
        False,
        r"Local\LDPlayerReinstallV3",
    )

    if not handle:
        raise ctypes.WinError(ctypes.get_last_error())

    # ERROR_ALREADY_EXISTS = 183.
    if ctypes.get_last_error() == 183:
        kernel32.CloseHandle(handle)
        raise RuntimeError("Скрипт уже запущен в другом окне.")

    MUTEX_HANDLE = int(handle)


def release_mutex() -> None:
    """Освобождает блокировку второго экземпляра."""

    global MUTEX_HANDLE

    if not MUTEX_HANDLE:
        return

    try:
        ctypes.windll.kernel32.CloseHandle(wintypes.HANDLE(MUTEX_HANDLE))
    finally:
        MUTEX_HANDLE = None


def atomic_write_json(path: Path, data: dict[str, Any]) -> None:
    """Атомарно записывает JSON через временный файл."""

    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(path.name + ".tmp")

    with temp_path.open("w", encoding="utf-8", newline="\n") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)
        file.write("\n")
        file.flush()
        os.fsync(file.fileno())

    os.replace(temp_path, path)


def merge_defaults(
    defaults: dict[str, Any],
    current: dict[str, Any],
) -> dict[str, Any]:
    """Добавляет отсутствующие настройки, не затирая пользовательские значения."""

    result = copy.deepcopy(current)

    for key, default_value in defaults.items():
        if key not in result:
            result[key] = copy.deepcopy(default_value)
        elif isinstance(default_value, dict) and isinstance(result[key], dict):
            result[key] = merge_defaults(default_value, result[key])

    return result


def load_config() -> tuple[dict[str, Any], bool, bool]:
    """Создаёт или дополняет config.json рядом со скриптом."""

    if not CONFIG_PATH.exists():
        atomic_write_json(CONFIG_PATH, DEFAULT_CONFIG)
        return copy.deepcopy(DEFAULT_CONFIG), True, False

    try:
        with CONFIG_PATH.open("r", encoding="utf-8-sig") as file:
            current = json.load(file)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Ошибка JSON в {CONFIG_PATH.name}: строка {exc.lineno}, "
            f"столбец {exc.colno}: {exc.msg}"
        ) from exc

    if not isinstance(current, dict):
        raise TypeError(f"Корень {CONFIG_PATH.name} должен быть JSON-объектом.")

    merged = merge_defaults(DEFAULT_CONFIG, current)
    updated = merged != current

    if updated:
        atomic_write_json(CONFIG_PATH, merged)

    return merged, False, updated


def resolve_path(value: str) -> Path:
    """Разворачивает переменные окружения и относительные пути."""

    value = os.path.expandvars(os.path.expanduser(value.strip()))
    path = Path(value)

    if not path.is_absolute():
        path = BASE_DIR / path

    return Path(os.path.abspath(path))


def resolve_paths(config: dict[str, Any]) -> dict[str, Path]:
    """Преобразует пути из config.json в абсолютные Path."""

    path_cfg = config["paths"]
    target_dir = resolve_path(path_cfg["target_dir"])
    vms_value = str(path_cfg.get("vms_dir", "")).strip()

    return {
        "uninstaller": resolve_path(path_cfg["uninstaller"]),
        "installer": resolve_path(path_cfg["installer"]),
        "target_dir": target_dir,
        "vms_dir": resolve_path(vms_value) if vms_value else target_dir / "vms",
        "backup_vms": resolve_path(path_cfg["backup_vms"]),
        "templates_dir": resolve_path(path_cfg["templates_dir"]),
        "logs_dir": resolve_path(path_cfg["logs_dir"]),
    }


def setup_logging(logs_dir: Path) -> Path:
    """Создаёт отдельную папку с логом и скриншотами текущего запуска."""

    global RUN_DIR

    RUN_DIR = logs_dir / RUN_ID
    RUN_DIR.mkdir(parents=True, exist_ok=True)

    log_path = RUN_DIR / "run.log"

    LOGGER.setLevel(logging.DEBUG)
    LOGGER.handlers.clear()
    LOGGER.propagate = False

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        "%Y-%m-%d %H:%M:%S",
    )

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    console.setFormatter(formatter)

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    LOGGER.addHandler(console)
    LOGGER.addHandler(file_handler)

    return log_path


def parse_arguments() -> argparse.Namespace:
    """Обрабатывает режим проверки и аварийного восстановления."""

    parser = argparse.ArgumentParser(
        description="Переустановка LDPlayer с безопасным сохранением папки vms."
    )

    parser.add_argument(
        "--health-check-only",
        action="store_true",
        help="Только проверить окружение, ничего не изменяя.",
    )
    parser.add_argument(
        "--recover-only",
        action="store_true",
        help="Только восстановить vms из state.json.",
    )

    return parser.parse_args()


# =============================================================================
# STATE.JSON
# =============================================================================


def load_state() -> dict[str, Any] | None:
    """Читает состояние незавершённой критической операции."""

    if not STATE_PATH.exists():
        return None

    with STATE_PATH.open("r", encoding="utf-8-sig") as file:
        state = json.load(file)

    if not isinstance(state, dict):
        raise TypeError(f"Корень {STATE_PATH.name} должен быть JSON-объектом.")

    return state


def save_state(state: dict[str, Any]) -> None:
    """Сохраняет состояние перед критическим этапом."""

    state["updated_at"] = datetime.now().astimezone().isoformat(timespec="seconds")
    atomic_write_json(STATE_PATH, state)


def update_state(**changes: Any) -> None:
    """Обновляет существующий state.json."""

    state = load_state()

    if state is None:
        raise RuntimeError("state.json отсутствует.")

    state.update(changes)
    save_state(state)


def clear_state() -> None:
    """Удаляет state.json только после успешного восстановления."""

    if STATE_PATH.exists():
        STATE_PATH.unlink()


# =============================================================================
# ПАПКИ, JUNCTION И ПРОВЕРКА ДАННЫХ
# =============================================================================


def lexists(path: Path) -> bool:
    """Возвращает True даже для повреждённой symlink/junction."""

    return os.path.lexists(str(path))


def is_reparse_point(path: Path) -> bool:
    """Проверяет, является ли путь junction или символической ссылкой."""

    if not lexists(path):
        return False

    try:
        if path.is_symlink():
            return True
    except OSError:
        pass

    isjunction = getattr(os.path, "isjunction", None)

    if callable(isjunction):
        try:
            if isjunction(path):
                return True
        except OSError:
            pass

    attributes = ctypes.windll.kernel32.GetFileAttributesW(str(path))

    return attributes != 0xFFFFFFFF and bool(attributes & 0x400)


def powershell(
    command: str,
    *arguments: str,
    timeout: float = 30,
) -> subprocess.CompletedProcess[str]:
    """
    Безопасно запускает PowerShell без shell=True.

    Команда временно записывается в PS1-файл и запускается через -File.
    Благодаря этому переданные после файла значения корректно попадают
    в автоматический массив PowerShell $args.

    Старый вариант с -Command ошибочно присоединял дополнительные значения
    к тексту команды, из-за чего Set-Clipboard и операции с junction
    могли получать лишние позиционные параметры.
    """

    script_dir = RUN_DIR if RUN_DIR is not None else BASE_DIR
    script_dir.mkdir(parents=True, exist_ok=True)

    script_path = (
        script_dir
        / f".ldplayer_ps_{os.getpid()}_{time.time_ns()}.ps1"
    )

    # UTF-8 с BOM нужен для корректной работы Windows PowerShell 5.1
    # с русским текстом внутри временного сценария.
    script_path.write_text(
        command.rstrip() + "\n",
        encoding="utf-8-sig",
    )

    try:
        return subprocess.run(
            [
                "powershell.exe",
                "-NoProfile",
                "-NonInteractive",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                str(script_path),
                *(str(argument) for argument in arguments),
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
            check=False,
        )
    finally:
        try:
            script_path.unlink(missing_ok=True)
        except Exception as exc:
            LOGGER.debug(
                "Не удалось удалить временный PowerShell-файл %s: %s",
                script_path,
                exc,
            )


def resolve_link_target(path: Path) -> Path:
    """Определяет фактическую цель junction или symlink."""

    try:
        target = os.readlink(path)

        for prefix in ("\\\\?\\", "\\??\\"):
            if target.startswith(prefix):
                target = target[len(prefix):]
                break

        target_path = Path(target)

        if not target_path.is_absolute():
            target_path = path.parent / target_path

        return Path(os.path.abspath(target_path))
    except Exception:
        pass

    command = (
        "$ErrorActionPreference='Stop'; "
        "$item=Get-Item -LiteralPath $args[0] -Force; "
        "if($null -eq $item.Target){exit 2}; "
        "[Console]::OutputEncoding=[Text.UTF8Encoding]::new(); "
        "$item.Target"
    )

    result = powershell(command, str(path))
    targets = [line.strip() for line in result.stdout.splitlines() if line.strip()]

    if result.returncode != 0 or not targets:
        raise RuntimeError(
            f"Не удалось определить цель junction {path}: "
            f"{result.stderr.strip() or result.stdout.strip()}"
        )

    return Path(os.path.abspath(targets[0]))


def remove_link(path: Path) -> None:
    """Удаляет только junction/symlink, не затрагивая целевую папку."""

    if not lexists(path):
        return

    if not is_reparse_point(path):
        raise RuntimeError(f"Отказ удаления: {path} не является junction/symlink.")

    errors: list[str] = []

    for remover in (os.unlink, os.rmdir):
        try:
            remover(path)
            break
        except Exception as exc:
            errors.append(str(exc))
    else:
        raise RuntimeError(
            f"Не удалось удалить junction {path}: " + " | ".join(errors)
        )

    if lexists(path):
        raise RuntimeError(f"Junction остался после удаления: {path}")


def create_junction(link: Path, target: Path) -> None:
    """Создаёт junction и затем проверяет его фактическую цель."""

    if lexists(link):
        raise FileExistsError(f"Путь для junction уже существует: {link}")

    if not target.is_dir():
        raise NotADirectoryError(f"Цель junction отсутствует: {target}")

    link.parent.mkdir(parents=True, exist_ok=True)

    command = (
        "$ErrorActionPreference='Stop'; "
        "New-Item -ItemType Junction -Path $args[0] -Target $args[1] | Out-Null"
    )

    result = powershell(command, str(link), str(target))

    if result.returncode != 0:
        raise RuntimeError(
            f"Не удалось создать junction {link} -> {target}: "
            f"{result.stderr.strip() or result.stdout.strip()}"
        )

    actual = resolve_link_target(link)

    if os.path.normcase(str(actual)) != os.path.normcase(str(target)):
        raise RuntimeError(f"Junction указывает на {actual}, ожидалось {target}")


def same_volume(first: Path, second: Path) -> bool:
    """Проверяет, находятся ли два пути на одном диске или UNC-томе."""

    first_drive = os.path.splitdrive(os.path.abspath(first))[0].casefold()
    second_drive = os.path.splitdrive(os.path.abspath(second))[0].casefold()
    return first_drive == second_drive


def is_subpath(candidate: Path, parent: Path) -> bool:
    """Проверяет вложенность одного пути в другой."""

    try:
        candidate_abs = os.path.abspath(candidate)
        parent_abs = os.path.abspath(parent)
        return os.path.commonpath([candidate_abs, parent_abs]) == parent_abs
    except ValueError:
        return False


def build_manifest(root: Path) -> dict[str, int]:
    """Считает файлы, папки и общий размер для проверки копии."""

    if not root.is_dir():
        raise NotADirectoryError(root)

    files_count = 0
    directories_count = 0
    bytes_count = 0

    for current_root, directories, files in os.walk(root, followlinks=False):
        current = Path(current_root)
        directories_count += len(directories)

        # Не заходим во вложенные junction, чтобы случайно не считать внешние данные.
        directories[:] = [
            name for name in directories if not is_reparse_point(current / name)
        ]

        for name in files:
            file_path = current / name
            files_count += 1
            bytes_count += file_path.stat(follow_symlinks=False).st_size

    return {
        "files": files_count,
        "directories": directories_count,
        "bytes": bytes_count,
    }


def manifests_equal(first: dict[str, Any], second: dict[str, Any]) -> bool:
    """Сравнивает два манифеста папок."""

    return all(
        int(first.get(key, -1)) == int(second.get(key, -2))
        for key in ("files", "directories", "bytes")
    )


def manifest_nonempty(manifest: dict[str, Any]) -> bool:
    """Проверяет, содержит ли манифест хотя бы файл или папку."""

    return (
        int(manifest.get("files", 0)) > 0
        or int(manifest.get("directories", 0)) > 0
    )


def inspect_vms(path: Path) -> dict[str, Any]:
    """Определяет тип vms и строит манифест фактических данных."""

    if not lexists(path):
        return {
            "mode": "none",
            "manifest": {"files": 0, "directories": 0, "bytes": 0},
        }

    if is_reparse_point(path):
        target = resolve_link_target(path)

        if not target.is_dir():
            raise FileNotFoundError(f"Цель junction не существует: {target}")

        return {
            "mode": "junction",
            "link_target": str(target),
            "manifest": build_manifest(target),
        }

    if not path.is_dir():
        raise NotADirectoryError(f"vms существует, но это не папка: {path}")

    return {
        "mode": "physical",
        "manifest": build_manifest(path),
    }


def quarantine_path(path: Path, label: str, backup_path: Path) -> Path | None:
    """Не удаляет спорные данные, а переносит их в отдельный карантин."""

    if not lexists(path):
        return None

    quarantine_root = backup_path.parent / "quarantine"
    quarantine_root.mkdir(parents=True, exist_ok=True)

    destination = quarantine_root / f"{RUN_ID}_{label}_{path.name}"
    index = 1

    while lexists(destination):
        destination = quarantine_root / f"{RUN_ID}_{label}_{index}_{path.name}"
        index += 1

    if is_reparse_point(path):
        target = resolve_link_target(path)
        report = destination.with_suffix(".txt")

        report.write_text(
            f"Удалена только ссылка: {path}\nЦель ссылки: {target}\n",
            encoding="utf-8",
        )

        remove_link(path)
        return report

    LOGGER.warning("Переносим данные в карантин: %s -> %s", path, destination)
    shutil.move(str(path), str(destination))

    if lexists(path):
        raise RuntimeError(f"Исходный путь остался после карантина: {path}")

    if not lexists(destination):
        raise RuntimeError(f"Данные не появились в карантине: {destination}")

    return destination


def copy_verified(
    source: Path,
    destination: Path,
    expected: dict[str, Any],
) -> None:
    """Копирует папку и сверяет получившийся манифест."""

    if lexists(destination):
        raise FileExistsError(destination)

    shutil.copytree(
        source,
        destination,
        symlinks=True,
        copy_function=shutil.copy2,
    )

    actual = build_manifest(destination)

    if not manifests_equal(expected, actual):
        raise RuntimeError(
            "Копия не прошла проверку. "
            f"Ожидалось {expected}, получено {actual}."
        )


def remove_physical_directory(path: Path) -> None:
    """Удаляет только обычную папку и никогда не применяет rmtree к junction."""

    if not lexists(path):
        return

    if is_reparse_point(path):
        raise RuntimeError(f"Отказ rmtree для junction: {path}")

    if not path.is_dir():
        raise NotADirectoryError(f"Ожидалась папка: {path}")

    shutil.rmtree(path)

    if lexists(path):
        raise RuntimeError(f"Папка осталась после удаления: {path}")


# =============================================================================
# ОСТАНОВКА ПРОЦЕССОВ
# =============================================================================


def process_running(image_name: str) -> bool:
    """Проверяет наличие процесса через tasklist."""

    result = subprocess.run(
        [
            "tasklist.exe",
            "/FI",
            f"IMAGENAME eq {image_name}",
            "/FO",
            "CSV",
            "/NH",
        ],
        capture_output=True,
        text=True,
        errors="replace",
        timeout=20,
        check=False,
    )

    return (
        result.returncode == 0
        and image_name.casefold() in result.stdout.casefold()
    )


def kill_process(image_name: str) -> None:
    """Завершает процесс вместе с дочерними процессами."""

    result = subprocess.run(
        [
            "taskkill.exe",
            "/F",
            "/T",
            "/IM",
            image_name,
        ],
        capture_output=True,
        text=True,
        errors="replace",
        timeout=20,
        check=False,
    )

    LOGGER.debug(
        "TASKKILL %s, код=%s: %s",
        image_name,
        result.returncode,
        result.stderr.strip() or result.stdout.strip(),
    )


def stop_ldplayer_processes() -> None:
    """Останавливает процессы LDPlayer и проверяет, что они действительно исчезли."""

    names = [str(name) for name in CONFIG["processes"]]
    deadline = time.monotonic() + float(CONFIG["timeouts"]["process_stop"])

    LOGGER.info("Останавливаем процессы LDPlayer...")

    while True:
        running = [name for name in names if process_running(name)]

        if not running:
            LOGGER.info("Процессы LDPlayer остановлены.")
            return

        for name in running:
            kill_process(name)

        if time.monotonic() >= deadline:
            running = [name for name in names if process_running(name)]

            if running:
                raise TimeoutError(
                    "Не удалось остановить процессы: " + ", ".join(running)
                )

            return

        time.sleep(0.6)


# =============================================================================
# PYAUTOGUI И PNG-ШАБЛОНЫ
# =============================================================================


def configure_pyautogui() -> None:
    """Применяет настройки задержек и FailSafe."""

    if pyautogui is None:
        return

    pyautogui.PAUSE = float(CONFIG["ui"]["pyautogui_pause"])
    pyautogui.FAILSAFE = bool(CONFIG["ui"]["failsafe"])

    try:
        pyautogui.useImageNotFoundException()
    except Exception:
        pass


def save_screenshot(label: str) -> Path | None:
    """Сохраняет диагностический скриншот в папку текущего запуска."""

    if pyautogui is None or RUN_DIR is None:
        return None

    safe_label = "".join(
        char if char.isalnum() or char in "-_" else "_" for char in label
    )

    path = RUN_DIR / (
        f"{datetime.now().strftime('%H%M%S_%f')}_{safe_label}.png"
    )

    try:
        pyautogui.screenshot(str(path))
        LOGGER.info("Скриншот: %s", path)
        return path
    except Exception as exc:
        LOGGER.warning("Не удалось сохранить скриншот: %s", exc)
        return None


def resolve_template_file(filename: str) -> Path:
    """Ищет PNG-шаблон без учёта регистра имени файла."""

    candidate = Path(filename)

    if not candidate.is_absolute():
        candidate = PATHS["templates_dir"] / candidate

    if candidate.is_file():
        return candidate

    if candidate.parent.is_dir():
        for item in candidate.parent.iterdir():
            if item.is_file() and item.name.casefold() == candidate.name.casefold():
                return item

    raise FileNotFoundError(f"Не найден PNG-шаблон: {candidate}")


def build_template_paths() -> dict[str, Path]:
    """Кэширует только реально используемые PNG-шаблоны."""

    result: dict[str, Path] = {}

    for key in REQUIRED_TEMPLATE_KEYS:
        if key not in CONFIG["templates"]:
            raise KeyError(f"В config.json отсутствует шаблон: {key}")

        filename = str(CONFIG["templates"][key]["file"])
        result[key] = resolve_template_file(filename)

    return result


def image_not_found_types() -> tuple[type[BaseException], ...]:
    """Собирает типы ImageNotFoundException разных версий библиотек."""

    result: list[type[BaseException]] = []

    for module in (pyautogui, pyscreeze):
        cls = getattr(module, "ImageNotFoundException", None) if module else None

        if isinstance(cls, type) and cls not in result:
            result.append(cls)

    return tuple(result)


def locate_once(
    path: Path,
    confidence: float,
    grayscale: bool,
    region: tuple[int, int, int, int] | None,
) -> Any | None:
    """Выполняет одну попытку поиска PNG на экране."""

    if pyautogui is None:
        raise RuntimeError("PyAutoGUI недоступен.")

    try:
        return pyautogui.locateOnScreen(
            str(path),
            confidence=confidence,
            grayscale=grayscale,
            region=region,
        )
    except Exception as exc:
        not_found_types = image_not_found_types()

        if not_found_types and isinstance(exc, not_found_types):
            return None

        raise


def attempts_for(key: str) -> list[tuple[float, bool]]:
    """Формирует последовательность confidence и grayscale."""

    settings = CONFIG["templates"][key]
    confidences = [float(value) for value in settings["confidence"]]
    attempts = [(value, False) for value in confidences]

    if bool(settings.get("grayscale_fallback", False)):
        attempts.extend((value, True) for value in confidences[-2:])

    return attempts


def locate_template(
    key: str,
    *,
    region: tuple[int, int, int, int] | None = None,
    required: bool = True,
) -> Any | None:
    """Ищет PNG до таймаута и в конце расширяет поиск на весь экран."""

    path = TEMPLATE_PATHS[key]
    timeout = float(CONFIG["templates"][key]["timeout"])
    interval = float(CONFIG["ui"]["search_interval"])

    started = time.monotonic()
    deadline = started + timeout

    LOGGER.info("Ищем %s, таймаут %.1f сек.", path.name, timeout)

    while time.monotonic() < deadline:
        elapsed = time.monotonic() - started

        # Последние 25% времени ищем по всему экрану.
        active_region = region if region and elapsed < timeout * 0.75 else None

        for confidence, grayscale in attempts_for(key):
            location = locate_once(
                path,
                confidence,
                grayscale,
                active_region,
            )

            if location:
                LOGGER.info(
                    "Найден %s: %s; confidence=%.2f; grayscale=%s",
                    path.name,
                    location,
                    confidence,
                    grayscale,
                )
                return location

            if time.monotonic() >= deadline:
                break

        time.sleep(interval)

    save_screenshot(f"timeout_{key}")
    message = f"Шаблон {path.name} не найден за {timeout:.1f} сек."

    if required:
        raise TimeoutError(message)

    LOGGER.warning(message)
    return None


def locate_any(
    keys: Iterable[str],
    *,
    region: tuple[int, int, int, int] | None,
    timeout: float,
) -> tuple[str, Any]:
    """Ищет первый найденный вариант из нескольких PNG-шаблонов."""

    keys = list(keys)
    started = time.monotonic()
    deadline = started + timeout
    interval = float(CONFIG["ui"]["search_interval"])

    while time.monotonic() < deadline:
        elapsed = time.monotonic() - started
        active_region = region if region and elapsed < timeout * 0.75 else None

        for key in keys:
            for confidence, grayscale in attempts_for(key):
                location = locate_once(
                    TEMPLATE_PATHS[key],
                    confidence,
                    grayscale,
                    active_region,
                )

                if location:
                    LOGGER.info(
                        "Найден %s: %s; confidence=%.2f; grayscale=%s",
                        TEMPLATE_PATHS[key].name,
                        location,
                        confidence,
                        grayscale,
                    )
                    return key, location

                if time.monotonic() >= deadline:
                    break

        time.sleep(interval)

    save_screenshot("install_button_timeout")

    raise TimeoutError(
        "Не найдена кнопка установки: "
        + ", ".join(TEMPLATE_PATHS[key].name for key in keys)
    )


def expand_region(
    box: Any,
    padding: int,
) -> tuple[int, int, int, int]:
    """Расширяет найденную область, не выходя за границы экрана."""

    if pyautogui is None:
        raise RuntimeError("PyAutoGUI недоступен.")

    left, top, width, height = map(int, box)
    screen_width, screen_height = pyautogui.size()

    x1 = max(0, left - padding)
    y1 = max(0, top - padding)
    x2 = min(screen_width, left + width + padding)
    y2 = min(screen_height, top + height + padding)

    return (
        x1,
        y1,
        max(1, x2 - x1),
        max(1, y2 - y1),
    )


def click_template(location: Any, label: str) -> None:
    """Сохраняет скриншот и кликает по центру найденной области."""

    if pyautogui is None:
        raise RuntimeError("PyAutoGUI недоступен.")

    if bool(CONFIG["debug"]["screenshot_before_critical_click"]):
        save_screenshot(f"before_click_{label}")

    x, y = pyautogui.center(location)
    LOGGER.info("Клик %s: %s, %s", label, x, y)
    pyautogui.click(x, y)


# =============================================================================
# ЧТЕНИЕ И ИСПРАВЛЕНИЕ ПУТИ УСТАНОВКИ
# =============================================================================


def get_foreground_window_rect() -> tuple[int, int, int, int] | None:
    """Получает координаты полного активного окна Windows."""

    if os.name != "nt":
        return None

    try:
        user32 = ctypes.windll.user32
        hwnd = user32.GetForegroundWindow()

        if not hwnd:
            return None

        rect = wintypes.RECT()

        if not user32.GetWindowRect(hwnd, ctypes.byref(rect)):
            return None

        width = int(rect.right - rect.left)
        height = int(rect.bottom - rect.top)

        if width < 300 or height < 200:
            return None

        return (
            int(rect.left),
            int(rect.top),
            width,
            height,
        )
    except Exception as exc:
        LOGGER.debug("Не удалось получить координаты активного окна: %s", exc)
        return None


def get_installer_window_rect(
    window_location: Any,
) -> tuple[int, int, int, int]:
    """Определяет полный прямоугольник окна установщика."""

    template_left, template_top, template_width, template_height = map(
        int,
        window_location,
    )

    template_center_x = template_left + template_width // 2
    template_center_y = template_top + template_height // 2
    foreground = get_foreground_window_rect()

    if foreground is not None:
        left, top, width, height = foreground
        right = left + width
        bottom = top + height

        # Используем активное окно, только если найденный PNG находится внутри него.
        if (
            left <= template_center_x <= right
            and top <= template_center_y <= bottom
        ):
            LOGGER.info(
                "Полное окно установщика: left=%s, top=%s, width=%s, height=%s",
                left,
                top,
                width,
                height,
            )
            return foreground

    LOGGER.warning(
        "Не удалось подтвердить активное окно. "
        "Для координат используется область window.png."
    )

    return (
        template_left,
        template_top,
        template_width,
        template_height,
    )


def get_install_path_field_points(
    installer_rect: tuple[int, int, int, int],
) -> list[tuple[int, int]]:
    """
    Формирует несколько возможных точек внутри поля пути.

    Основные точки привязаны к нижнему краю окна, потому что поле LDPlayer
    стабильно находится примерно в 78–90 пикселях от нижней границы.
    Дополнительно используются относительные координаты из config.json.
    """

    left, top, width, height = installer_rect
    path_config = CONFIG["install_path"]

    points: list[tuple[int, int]] = []

    def add_point(x: int, y: int) -> None:
        # Не позволяем координате выйти за границы окна.
        x = max(left + 5, min(x, left + width - 5))
        y = max(top + 5, min(y, top + height - 5))

        point = (x, y)

        if point not in points:
            points.append(point)

    # Самый надёжный вариант для текущего интерфейса LDPlayer:
    # несколько высот от нижнего края и несколько X внутри длинного поля.
    for bottom_offset in path_config.get(
        "field_bottom_offsets",
        [78, 84, 90],
    ):
        y = top + height - int(bottom_offset)

        for relative_x in path_config.get(
            "field_x_candidates",
            [0.20, 0.35, 0.50, 0.65],
        ):
            x = int(left + width * float(relative_x))
            add_point(x, y)

    # Дополнительные относительные варианты для других размеров интерфейса.
    relative_candidates = path_config.get(
        "field_relative_candidates",
        [],
    )

    for candidate in relative_candidates:
        if not isinstance(candidate, list) or len(candidate) != 2:
            continue

        x = int(left + width * float(candidate[0]))
        y = int(top + height * float(candidate[1]))
        add_point(x, y)

    # Старое значение остаётся последним запасным вариантом.
    legacy_relative = path_config.get(
        "field_relative",
        [0.40, 0.90],
    )

    if isinstance(legacy_relative, list) and len(legacy_relative) == 2:
        x = int(left + width * float(legacy_relative[0]))
        y = int(top + height * float(legacy_relative[1]))
        add_point(x, y)

    LOGGER.debug(
        "Кандидаты координат поля пути: %s",
        points,
    )

    return points


CF_UNICODETEXT = 13
GMEM_MOVEABLE = 0x0002


def get_clipboard_api() -> tuple[Any, Any]:
    """
    Подготавливает функции WinAPI для работы с Unicode-буфером обмена.

    Буфер читается и записывается напрямую через user32/kernel32,
    поэтому PowerShell, кавычки и передача позиционных аргументов
    больше не участвуют в проверке пути установки.
    """

    if os.name != "nt":
        raise OSError("Буфер обмена WinAPI доступен только в Windows.")

    user32 = ctypes.WinDLL("user32", use_last_error=True)
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)

    user32.OpenClipboard.argtypes = [wintypes.HWND]
    user32.OpenClipboard.restype = wintypes.BOOL

    user32.CloseClipboard.argtypes = []
    user32.CloseClipboard.restype = wintypes.BOOL

    user32.EmptyClipboard.argtypes = []
    user32.EmptyClipboard.restype = wintypes.BOOL

    user32.IsClipboardFormatAvailable.argtypes = [wintypes.UINT]
    user32.IsClipboardFormatAvailable.restype = wintypes.BOOL

    user32.GetClipboardData.argtypes = [wintypes.UINT]
    user32.GetClipboardData.restype = wintypes.HANDLE

    user32.SetClipboardData.argtypes = [
        wintypes.UINT,
        wintypes.HANDLE,
    ]
    user32.SetClipboardData.restype = wintypes.HANDLE

    kernel32.GlobalAlloc.argtypes = [
        wintypes.UINT,
        ctypes.c_size_t,
    ]
    kernel32.GlobalAlloc.restype = wintypes.HANDLE

    kernel32.GlobalLock.argtypes = [wintypes.HANDLE]
    kernel32.GlobalLock.restype = ctypes.c_void_p

    kernel32.GlobalUnlock.argtypes = [wintypes.HANDLE]
    kernel32.GlobalUnlock.restype = wintypes.BOOL

    kernel32.GlobalFree.argtypes = [wintypes.HANDLE]
    kernel32.GlobalFree.restype = wintypes.HANDLE

    return user32, kernel32


def open_clipboard_with_retry(
    user32: Any,
    timeout: float = 3.0,
) -> None:
    """
    Открывает буфер обмена с повторами.

    Иногда установщик или другое приложение удерживает буфер несколько
    миллисекунд. Немедленная ошибка в таком случае была бы ложной.
    """

    deadline = time.monotonic() + timeout
    last_error = 0

    while time.monotonic() < deadline:
        if user32.OpenClipboard(None):
            return

        last_error = ctypes.get_last_error()
        time.sleep(0.05)

    raise OSError(
        last_error,
        "Не удалось открыть буфер обмена Windows "
        f"за {timeout:.1f} сек.",
    )


def get_clipboard_text() -> str:
    """Читает Unicode-текст напрямую из буфера обмена Windows."""

    user32, kernel32 = get_clipboard_api()
    open_clipboard_with_retry(user32)

    try:
        if not user32.IsClipboardFormatAvailable(CF_UNICODETEXT):
            raise RuntimeError(
                "В буфере обмена сейчас нет текстового Unicode-значения."
            )

        memory_handle = user32.GetClipboardData(CF_UNICODETEXT)

        if not memory_handle:
            raise ctypes.WinError(ctypes.get_last_error())

        memory_pointer = kernel32.GlobalLock(memory_handle)

        if not memory_pointer:
            raise ctypes.WinError(ctypes.get_last_error())

        try:
            return ctypes.wstring_at(memory_pointer)
        finally:
            kernel32.GlobalUnlock(memory_handle)

    finally:
        user32.CloseClipboard()


def try_get_clipboard_text() -> str | None:
    """Пытается сохранить старое текстовое содержимое буфера обмена."""

    try:
        return get_clipboard_text()
    except Exception:
        return None


def set_clipboard_text(value: str) -> None:
    """Записывает Unicode-текст напрямую в буфер обмена Windows."""

    user32, kernel32 = get_clipboard_api()

    # create_unicode_buffer автоматически добавляет завершающий NUL.
    text_buffer = ctypes.create_unicode_buffer(str(value))
    buffer_size = ctypes.sizeof(text_buffer)

    memory_handle = kernel32.GlobalAlloc(
        GMEM_MOVEABLE,
        buffer_size,
    )

    if not memory_handle:
        raise ctypes.WinError(ctypes.get_last_error())

    ownership_transferred = False

    try:
        memory_pointer = kernel32.GlobalLock(memory_handle)

        if not memory_pointer:
            raise ctypes.WinError(ctypes.get_last_error())

        try:
            ctypes.memmove(
                memory_pointer,
                ctypes.addressof(text_buffer),
                buffer_size,
            )
        finally:
            kernel32.GlobalUnlock(memory_handle)

        open_clipboard_with_retry(user32)

        try:
            if not user32.EmptyClipboard():
                raise ctypes.WinError(ctypes.get_last_error())

            result = user32.SetClipboardData(
                CF_UNICODETEXT,
                memory_handle,
            )

            if not result:
                raise ctypes.WinError(ctypes.get_last_error())

            # После успешного SetClipboardData память принадлежит Windows.
            ownership_transferred = True

        finally:
            user32.CloseClipboard()

    finally:
        if not ownership_transferred:
            kernel32.GlobalFree(memory_handle)


def clean_install_path_text(value: str) -> str:
    """Очищает скопированный путь от переносов, нулевых символов и кавычек."""

    cleaned = value.replace("\x00", "").strip()

    if len(cleaned) >= 2 and cleaned[0] == '"' and cleaned[-1] == '"':
        cleaned = cleaned[1:-1].strip()

    return cleaned


def get_windows_path_drive(value: str) -> str:
    """Возвращает букву диска без двоеточия или пустую строку."""

    cleaned = clean_install_path_text(value)
    drive, _ = ntpath.splitdrive(cleaned)

    if len(drive) == 2 and drive[1] == ":" and drive[0].isalpha():
        return drive[0].upper()

    return ""


def replace_only_drive_letter(current_path: str, required_drive: str) -> str:
    """Меняет только букву диска, сохраняя остальную часть пути."""

    cleaned = clean_install_path_text(current_path)
    drive, tail = ntpath.splitdrive(cleaned)

    if (
        len(drive) != 2
        or drive[1] != ":"
        or not drive[0].isalpha()
    ):
        raise ValueError(
            "Установщик вернул путь без обычной буквы диска: "
            f"{cleaned!r}"
        )

    return f"{required_drive}:{tail}"


def normalize_windows_path_for_compare(value: str) -> str:
    """Нормализует Windows-путь исключительно для безопасного сравнения."""

    cleaned = clean_install_path_text(value)
    return ntpath.normcase(ntpath.normpath(cleaned))


def copy_install_path_from_field(
    installer_rect: tuple[int, int, int, int],
) -> tuple[str, tuple[int, int]]:
    """
    Перебирает возможные точки поля и возвращает:
    - прочитанный Windows-путь;
    - координату, на которой чтение сработало.

    Значение принимается только тогда, когда оно действительно похоже
    на путь с буквой диска, например C:\\LDPlayer\\LDPlayer9.
    """

    if pyautogui is None:
        raise RuntimeError("PyAutoGUI недоступен.")

    points = get_install_path_field_points(installer_rect)
    clipboard_delay = float(CONFIG["install_path"]["clipboard_delay"])
    errors: list[str] = []

    for attempt_number, (x, y) in enumerate(points, start=1):
        marker = f"__LDPLAYER_PATH_PROBE_{time.time_ns()}__"

        try:
            # Маркер исключает ситуацию, когда Ctrl+C не сработал,
            # а скрипт прочитал старое значение буфера.
            set_clipboard_text(marker)

            pyautogui.click(x, y)
            pyautogui.hotkey("ctrl", "a")
            time.sleep(0.08)
            pyautogui.hotkey("ctrl", "c")
            time.sleep(clipboard_delay)

            copied = clean_install_path_text(get_clipboard_text())

            if not copied or copied == marker:
                errors.append(
                    f"{attempt_number}: точка {x},{y} — поле не скопировалось"
                )
                continue

            drive = get_windows_path_drive(copied)

            # Не принимаем произвольный текст с другого элемента окна.
            if not drive:
                errors.append(
                    f"{attempt_number}: точка {x},{y} — не Windows-путь: {copied!r}"
                )
                continue

            LOGGER.info(
                "Поле пути найдено в точке %s,%s. Значение: %s",
                x,
                y,
                copied,
            )

            return copied, (x, y)

        except Exception as exc:
            errors.append(
                f"{attempt_number}: точка {x},{y} — {exc}"
            )

    LOGGER.debug(
        "Все попытки чтения поля пути: %s",
        " | ".join(errors),
    )

    raise RuntimeError(
        "Поле пути не скопировалось ни в одной из проверенных точек. "
        "Можно исправить путь вручную и подтвердить его визуально."
    )


def write_install_path_to_field(
    field_point: tuple[int, int],
    new_path: str,
) -> None:
    """
    Вставляет путь именно в ту координату,
    на которой поле уже было успешно прочитано.
    """

    if pyautogui is None:
        raise RuntimeError("PyAutoGUI недоступен.")

    x, y = field_point
    set_clipboard_text(new_path)

    pyautogui.click(x, y)
    pyautogui.hotkey("ctrl", "a")
    time.sleep(0.10)
    pyautogui.hotkey("ctrl", "v")
    time.sleep(float(CONFIG["install_path"]["clipboard_delay"]))

    if bool(CONFIG["install_path"]["commit_with_tab"]):
        pyautogui.press("tab")
        time.sleep(0.20)


def show_manual_install_path_dialog(
    required_drive: str,
    detected_path: str,
    error_message: str,
) -> str:
    """
    Показывает окно ручного исправления.

    Возвращает одно из действий:
    - "retry"   — ещё раз попытаться прочитать поле автоматически;
    - "confirm" — пользователь визуально подтверждает латинское C:;
    - "cancel"  — отменить установку.
    """

    result = {"action": "cancel"}

    try:
        import tkinter as tk
        from tkinter import ttk

        root = tk.Tk()
        root.title("Проверка пути LDPlayer")
        root.resizable(False, False)
        root.attributes("-topmost", True)
        root.geometry("640x380+30+80")

        main_frame = ttk.Frame(root, padding=18)
        main_frame.pack(fill="both", expand=True)

        ttk.Label(
            main_frame,
            text="Не удалось автоматически прочитать поле пути.",
            font=("Segoe UI", 12, "bold"),
        ).pack(anchor="w", pady=(0, 12))

        ttk.Label(
            main_frame,
            text=(
                "В открытом окне установки LDPlayer измените только первую "
                f"букву пути на латинскую {required_drive}.\n\n"
                "Пример:\n"
                "D:\\LDPlayer\\LDPlayer9\n"
                f"{required_drive}:\\LDPlayer\\LDPlayer9\n\n"
                "После изменения можно повторить автоматическую проверку.\n"
                "Если в поле уже явно видно латинское C:, нажмите "
                "«Я вижу C: — продолжить»."
            ),
            justify="left",
            wraplength=600,
        ).pack(anchor="w")

        if detected_path:
            ttk.Label(
                main_frame,
                text=f"\nПоследнее прочитанное значение: {detected_path}",
                justify="left",
                wraplength=600,
            ).pack(anchor="w")

        if error_message:
            ttk.Label(
                main_frame,
                text=f"\nПричина: {error_message}",
                justify="left",
                wraplength=600,
            ).pack(anchor="w")

        buttons = ttk.Frame(main_frame)
        buttons.pack(side="bottom", fill="x", pady=(18, 0))

        def retry() -> None:
            result["action"] = "retry"
            root.destroy()

        def confirm() -> None:
            result["action"] = "confirm"
            root.destroy()

        def cancel() -> None:
            result["action"] = "cancel"
            root.destroy()

        ttk.Button(
            buttons,
            text="Проверить автоматически",
            command=retry,
        ).pack(side="left")

        if bool(
            CONFIG["install_path"].get(
                "manual_allow_confirm_without_read",
                True,
            )
        ):
            ttk.Button(
                buttons,
                text=f"Я вижу {required_drive}: — продолжить",
                command=confirm,
            ).pack(side="left", padx=(12, 0))

        ttk.Button(
            buttons,
            text="Отмена",
            command=cancel,
        ).pack(side="right")

        root.protocol("WM_DELETE_WINDOW", cancel)
        root.mainloop()

        return str(result["action"])

    except Exception as exc:
        LOGGER.warning("Не удалось открыть окно ручной проверки: %s", exc)

        print()
        print("=" * 76)
        print("НЕОБХОДИМО ПРОВЕРИТЬ ПУТЬ УСТАНОВКИ")
        print("=" * 76)
        print(f"Измените только букву диска на латинскую {required_drive}.")
        print(f"Последнее значение: {detected_path or 'не прочитано'}")
        print(f"Причина: {error_message}")
        print()

        answer = input(
            "Введите RETRY для повторной проверки, "
            "YES если визуально видите C:, или CANCEL: "
        ).strip().casefold()

        if answer == "retry":
            return "retry"

        if answer in {"yes", "y", "да"}:
            return "confirm"

        return "cancel"


def ensure_install_drive(window_location: Any) -> str:
    """
    Проверяет путь, меняет только букву диска и подтверждает результат.

    Если автоматическое чтение невозможно, пользователь может визуально
    подтвердить, что поле уже начинается с латинского C:.
    """

    if pyautogui is None:
        raise RuntimeError("PyAutoGUI недоступен.")

    path_config = CONFIG["install_path"]
    required_drive = str(path_config["required_drive"]).strip().upper()

    if (
        len(required_drive) != 1
        or required_drive < "A"
        or required_drive > "Z"
    ):
        raise ValueError(
            "install_path.required_drive должен содержать одну латинскую букву A-Z."
        )

    installer_rect = get_installer_window_rect(window_location)

    saved_clipboard = (
        try_get_clipboard_text()
        if bool(path_config["restore_clipboard"])
        else None
    )

    last_detected_path = ""
    last_error = ""

    try:
        # Автоматические попытки чтения и исправления.
        for attempt in range(1, int(path_config["auto_attempts"]) + 1):
            try:
                LOGGER.info("Автоматическая проверка пути: попытка %s.", attempt)

                current_path, field_point = copy_install_path_from_field(
                    installer_rect
                )

                last_detected_path = current_path
                current_drive = get_windows_path_drive(current_path)

                if current_drive == required_drive:
                    LOGGER.info(
                        "Путь уже находится на диске %s: %s",
                        required_drive,
                        current_path,
                    )
                    return current_path

                corrected_path = replace_only_drive_letter(
                    current_path,
                    required_drive,
                )

                LOGGER.info(
                    "Меняем только букву диска: %s -> %s",
                    current_path,
                    corrected_path,
                )

                # Пишем в ту же точку, где поле уже успешно прочиталось.
                write_install_path_to_field(
                    field_point,
                    corrected_path,
                )

                verified_path, _ = copy_install_path_from_field(
                    installer_rect
                )

                last_detected_path = verified_path
                verified_drive = get_windows_path_drive(verified_path)

                same_full_path = (
                    normalize_windows_path_for_compare(verified_path)
                    == normalize_windows_path_for_compare(corrected_path)
                )

                if verified_drive == required_drive and same_full_path:
                    LOGGER.info(
                        "Путь успешно изменён и проверен: %s",
                        verified_path,
                    )
                    save_screenshot("install_path_verified")
                    return verified_path

                raise RuntimeError(
                    "После вставки поле вернуло другое значение: "
                    f"{verified_path!r}; ожидалось {corrected_path!r}"
                )

            except Exception as exc:
                last_error = str(exc)

                LOGGER.warning(
                    "Автоматическая попытка %s не удалась: %s",
                    attempt,
                    exc,
                )

                time.sleep(0.40)

        # Ручной режим.
        for manual_attempt in range(
            1,
            int(path_config["manual_max_checks"]) + 1,
        ):
            LOGGER.warning(
                "Переходим к ручной проверке пути, попытка %s.",
                manual_attempt,
            )

            action = show_manual_install_path_dialog(
                required_drive=required_drive,
                detected_path=last_detected_path,
                error_message=last_error,
            )

            if action == "cancel":
                raise RuntimeError(
                    "Пользователь отменил установку на этапе проверки пути."
                )

            if action == "confirm":
                LOGGER.warning(
                    "Путь подтверждён пользователем визуально. "
                    "Автоматическое чтение поля не удалось."
                )

                save_screenshot(
                    "install_path_confirmed_by_user"
                )

                # Это значение используется только в логе.
                # Фактический путь уже установлен пользователем в интерфейсе.
                return (
                    f"{required_drive}:\\"
                    "[подтверждено пользователем в окне LDPlayer]"
                )

            # action == "retry": повторно перебираем все возможные точки.
            try:
                verified_path, _ = copy_install_path_from_field(
                    installer_rect
                )

                last_detected_path = verified_path
                verified_drive = get_windows_path_drive(verified_path)

                if verified_drive == required_drive:
                    LOGGER.info(
                        "Ручное исправление пути подтверждено автоматически: %s",
                        verified_path,
                    )

                    save_screenshot(
                        "manual_install_path_verified"
                    )

                    return verified_path

                last_error = (
                    f"В поле всё ещё указан диск "
                    f"{verified_drive or 'не определён'}, "
                    f"а требуется {required_drive}."
                )

                LOGGER.warning(
                    "%s Текущее значение: %s",
                    last_error,
                    verified_path,
                )

            except Exception as exc:
                last_error = str(exc)
                LOGGER.warning(
                    "Не удалось автоматически перепроверить путь: %s",
                    exc,
                )

        raise RuntimeError(
            "Путь установки не подтверждён после "
            f"{path_config['manual_max_checks']} ручных попыток."
        )

    finally:
        if saved_clipboard is not None and bool(path_config["restore_clipboard"]):
            try:
                set_clipboard_text(saved_clipboard)
                LOGGER.debug("Исходный текстовый буфер обмена восстановлен.")
            except Exception as exc:
                LOGGER.warning("Не удалось восстановить буфер обмена: %s", exc)


# =============================================================================
# HEALTH-CHECK
# =============================================================================


def format_bytes(value: int) -> str:
    """Форматирует размер в удобный вид."""

    size = float(value)

    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return f"{size:.2f} {unit}"
        size /= 1024

    return f"{size:.2f} TB"


def installation_present() -> bool:
    """Проверяет наличие реальной установки LDPlayer, не считая одну vms."""

    if PATHS["uninstaller"].is_file():
        return True

    return any(
        (PATHS["target_dir"] / str(relative)).is_file()
        for relative in CONFIG["verification"]["expected_files_any"]
    )


def health_check() -> None:
    """Проверяет критические условия до первого изменения системы."""

    global TEMPLATE_PATHS

    errors: list[str] = []

    def check(
        name: str,
        condition: bool,
        details: str,
        warning: bool = False,
    ) -> None:
        if condition:
            LOGGER.info("[OK] %s — %s", name, details)
        elif warning:
            LOGGER.warning("[WARN] %s — %s", name, details)
        else:
            LOGGER.error("[FAIL] %s — %s", name, details)
            errors.append(f"{name}: {details}")

    LOGGER.info("=" * 76)
    LOGGER.info("HEALTH-CHECK")
    LOGGER.info("=" * 76)

    check("Windows", os.name == "nt", os.name)
    check("Администратор", is_admin(), "права получены")
    check("Python", sys.version_info >= (3, 10), sys.version.split()[0])

    check(
        "PyAutoGUI",
        PYAUTOGUI_ERROR is None,
        "импортирован" if PYAUTOGUI_ERROR is None else str(PYAUTOGUI_ERROR),
    )
    check(
        "OpenCV",
        CV2_ERROR is None,
        getattr(cv2, "__version__", str(CV2_ERROR)),
    )
    check(
        "taskkill.exe",
        shutil.which("taskkill.exe") is not None,
        shutil.which("taskkill.exe") or "не найден",
    )
    check(
        "tasklist.exe",
        shutil.which("tasklist.exe") is not None,
        shutil.which("tasklist.exe") or "не найден",
    )
    check(
        "PowerShell",
        shutil.which("powershell.exe") is not None,
        shutil.which("powershell.exe") or "не найден",
    )
    check(
        "Инсталлятор",
        PATHS["installer"].is_file(),
        str(PATHS["installer"]),
    )

    if installation_present() and not PATHS["uninstaller"].is_file():
        allowed = bool(
            CONFIG["safety"]["allow_install_over_existing_without_uninstaller"]
        )

        check(
            "Деинсталлятор",
            allowed,
            f"не найден: {PATHS['uninstaller']}",
            warning=allowed,
        )
    else:
        check(
            "Деинсталлятор",
            PATHS["uninstaller"].is_file() or not installation_present(),
            str(PATHS["uninstaller"]),
        )

    check("state.json", not STATE_PATH.exists(), str(STATE_PATH))
    check(
        "Свободный backup_vms",
        not lexists(PATHS["backup_vms"]),
        str(PATHS["backup_vms"]),
    )

    backup_is_dangerous = (
        os.path.normcase(str(PATHS["backup_vms"]))
        == os.path.normcase(str(PATHS["vms_dir"]))
        or is_subpath(PATHS["backup_vms"], PATHS["target_dir"])
    )

    check(
        "Размещение бэкапа",
        not backup_is_dangerous,
        f"vms={PATHS['vms_dir']}; backup={PATHS['backup_vms']}",
    )

    try:
        PATHS["backup_vms"].parent.mkdir(parents=True, exist_ok=True)
        probe = PATHS["backup_vms"].parent / f".write_test_{RUN_ID}"
        probe.write_text("ok", encoding="utf-8")
        probe.unlink()
        check("Запись в backup", True, str(PATHS["backup_vms"].parent))
    except Exception as exc:
        check("Запись в backup", False, str(exc))

    try:
        info = inspect_vms(PATHS["vms_dir"])
        manifest = info["manifest"]
        mode = info["mode"]

        if mode == "none":
            check(
                "vms",
                False,
                f"не найдена: {PATHS['vms_dir']}; перенос данных не потребуется",
                warning=True,
            )
        else:
            check(
                "vms",
                True,
                (
                    f"mode={mode}; files={manifest['files']}; "
                    f"dirs={manifest['directories']}; "
                    f"size={format_bytes(manifest['bytes'])}"
                ),
            )

            require_nonempty = bool(CONFIG["safety"]["require_nonempty_vms"])

            check(
                "Непустая vms",
                not require_nonempty or manifest_nonempty(manifest),
                f"require_nonempty_vms={require_nonempty}",
            )

            if mode == "junction":
                check(
                    "Цель junction",
                    Path(info["link_target"]).is_dir(),
                    info["link_target"],
                )

            if mode == "physical":
                volumes_match = same_volume(
                    PATHS["vms_dir"],
                    PATHS["backup_vms"],
                )
                allow_cross = bool(
                    CONFIG["safety"]["allow_cross_volume_backup"]
                )

                check(
                    "Том бэкапа",
                    volumes_match or allow_cross,
                    (
                        "один том"
                        if volumes_match
                        else f"разные тома; allow={allow_cross}"
                    ),
                )

                if not volumes_match and allow_cross:
                    required_bytes = int(
                        manifest["bytes"]
                        * float(
                            CONFIG["safety"]["minimum_free_space_factor"]
                        )
                    )

                    existing_parent = PATHS["backup_vms"].parent

                    while (
                        not existing_parent.exists()
                        and existing_parent.parent != existing_parent
                    ):
                        existing_parent = existing_parent.parent

                    free_bytes = shutil.disk_usage(existing_parent).free

                    check(
                        "Свободное место",
                        free_bytes >= required_bytes,
                        (
                            f"free={format_bytes(free_bytes)}; "
                            f"need≈{format_bytes(required_bytes)}"
                        ),
                    )

    except Exception as exc:
        check("Анализ vms", False, str(exc))

    try:
        TEMPLATE_PATHS = build_template_paths()

        for key, path in TEMPLATE_PATHS.items():
            check(f"PNG {key}", path.is_file(), str(path))
    except Exception as exc:
        check("PNG-шаблоны", False, str(exc))

    install_path_config = CONFIG["install_path"]
    required_drive = str(
        install_path_config["required_drive"]
    ).strip().upper()

    check(
        "Обязательный диск установки",
        len(required_drive) == 1 and "A" <= required_drive <= "Z",
        f"{required_drive!r}; должна использоваться латинская буква A-Z",
    )

    relative = install_path_config["field_relative"]

    check(
        "Координаты поля пути",
        (
            isinstance(relative, list)
            and len(relative) == 2
            and all(isinstance(value, (int, float)) for value in relative)
            and all(0 <= float(value) <= 1 for value in relative)
        ),
        str(relative),
    )

    check(
        "Попытки проверки пути",
        (
            int(install_path_config["auto_attempts"]) >= 1
            and int(install_path_config["manual_max_checks"]) >= 1
        ),
        (
            f"auto={install_path_config['auto_attempts']}; "
            f"manual={install_path_config['manual_max_checks']}"
        ),
    )

    bottom_offsets = install_path_config.get(
        "field_bottom_offsets",
        [],
    )

    x_candidates = install_path_config.get(
        "field_x_candidates",
        [],
    )

    check(
        "Кандидаты поля пути",
        (
            isinstance(bottom_offsets, list)
            and len(bottom_offsets) >= 1
            and all(int(value) > 0 for value in bottom_offsets)
            and isinstance(x_candidates, list)
            and len(x_candidates) >= 1
            and all(0 < float(value) < 0.80 for value in x_candidates)
        ),
        (
            f"bottom_offsets={bottom_offsets}; "
            f"x_candidates={x_candidates}"
        ),
    )

    if pyautogui is not None:
        try:
            width, height = pyautogui.size()
            check("Экран", width > 0 and height > 0, f"{width}x{height}")

            if RUN_DIR is None:
                raise RuntimeError("RUN_DIR не создан.")

            test_image = RUN_DIR / "healthcheck_screen.png"
            pyautogui.screenshot(str(test_image))

            check(
                "Скриншот",
                test_image.is_file() and test_image.stat().st_size > 0,
                str(test_image),
            )

            if not bool(CONFIG["debug"]["keep_healthcheck_screenshot"]):
                test_image.unlink(missing_ok=True)

        except Exception as exc:
            check("PyAutoGUI screen", False, str(exc))

    if errors:
        LOGGER.error("Health-check не пройден. Ошибок: %s", len(errors))

        for error in errors:
            LOGGER.error("  - %s", error)

        raise RuntimeError("Health-check не пройден. Смотрите run.log.")

    LOGGER.info("HEALTH-CHECK ПРОЙДЕН.")
    LOGGER.info("=" * 76)


# =============================================================================
# РЕЗЕРВИРОВАНИЕ И ВОССТАНОВЛЕНИЕ VMS
# =============================================================================


def protect_vms() -> None:
    """Перемещает обычную vms или временно снимает junction."""

    vms = PATHS["vms_dir"]
    backup = PATHS["backup_vms"]
    info = inspect_vms(vms)

    if info["mode"] == "physical" and lexists(backup):
        raise FileExistsError(
            f"Бэкап уже существует и не будет изменён: {backup}"
        )

    state: dict[str, Any] = {
        "version": 3,
        "run_id": RUN_ID,
        "status": "protecting",
        "mode": info["mode"],
        "original_vms": str(vms),
        "backup_vms": str(backup),
        "manifest": info["manifest"],
    }

    if info["mode"] == "junction":
        state["link_target"] = info["link_target"]

    save_state(state)
    LOGGER.info("Создан аварийный файл: %s", STATE_PATH)

    if info["mode"] == "none":
        update_state(status="protected")
        return

    if info["mode"] == "junction":
        LOGGER.info("Снимаем junction %s -> %s", vms, info["link_target"])
        remove_link(vms)
        update_state(status="protected")
        return

    backup.parent.mkdir(parents=True, exist_ok=True)
    expected = info["manifest"]

    if same_volume(vms, backup):
        LOGGER.info("Перемещаем vms: %s -> %s", vms, backup)
        os.replace(vms, backup)
    else:
        if not bool(CONFIG["safety"]["allow_cross_volume_backup"]):
            raise RuntimeError(
                "vms и backup находятся на разных дисках, "
                "а междисковый перенос запрещён config.json."
            )

        LOGGER.info("Копируем vms на другой диск с последующей проверкой...")
        copy_verified(vms, backup, expected)
        remove_physical_directory(vms)

    actual = build_manifest(backup)

    if not manifests_equal(expected, actual):
        raise RuntimeError(
            "Бэкап не прошёл проверку: "
            f"expected={expected}; actual={actual}"
        )

    update_state(status="protected")
    LOGGER.info("Бэкап vms проверен: %s", actual)


def try_manifest(path: Path) -> dict[str, int] | None:
    """Строит манифест только для обычной существующей папки."""

    if (
        not lexists(path)
        or is_reparse_point(path)
        or not path.is_dir()
    ):
        return None

    return build_manifest(path)


def restore_physical(
    backup: Path,
    original: Path,
    expected: dict[str, Any],
) -> None:
    """Восстанавливает физическую папку vms и проверяет результат."""

    original.parent.mkdir(parents=True, exist_ok=True)

    if same_volume(backup, original):
        os.replace(backup, original)
    else:
        temp = original.parent / f".vms_restore_{RUN_ID}"

        if lexists(temp):
            quarantine_path(temp, "old_restore_temp", backup)

        copy_verified(backup, temp, expected)
        os.replace(temp, original)

    actual = build_manifest(original)

    if not manifests_equal(expected, actual):
        raise RuntimeError(
            f"Восстановленная vms не прошла проверку: {actual}"
        )

    # При междисковом восстановлении backup остаётся копией.
    # Удаляем его только после успешной проверки оригинала.
    if lexists(backup):
        remove_physical_directory(backup)


def recover_physical(state: dict[str, Any]) -> None:
    """Восстанавливает обычную vms, включая частично завершённый запуск."""

    original = Path(state["original_vms"])
    backup = Path(state["backup_vms"])
    expected = dict(state["manifest"])

    original_manifest = try_manifest(original)
    backup_manifest = try_manifest(backup)

    original_valid = (
        original_manifest is not None
        and manifests_equal(expected, original_manifest)
    )
    backup_valid = (
        backup_manifest is not None
        and manifests_equal(expected, backup_manifest)
    )

    # Сбой произошёл до переноса: оригинал уже находится на месте.
    if original_valid:
        if lexists(backup):
            quarantine_path(
                backup,
                "duplicate_or_partial_backup",
                backup,
            )

        clear_state()
        LOGGER.info("Исходная vms уже была цела; state.json очищен.")
        return

    # Нормальный случай: бэкап цел, а установщик создал новую vms.
    if backup_valid:
        if lexists(original):
            quarantine_path(original, "generated_vms", backup)

        restore_physical(backup, original, expected)
        clear_state()
        LOGGER.info("Физическая vms восстановлена.")
        return

    # Восстановление могло завершиться, а state.json остаться после сбоя.
    if original_manifest is not None and not lexists(backup):
        if manifest_nonempty(original_manifest) or not manifest_nonempty(expected):
            LOGGER.warning(
                "Бэкап отсутствует, но исходная vms существует. "
                "Считаем state.json устаревшим и не удаляем данные."
            )
            clear_state()
            return

    raise RuntimeError(
        "Автовосстановление невозможно без риска. "
        f"original_manifest={original_manifest}; "
        f"backup_manifest={backup_manifest}; expected={expected}. "
        "state.json сохранён."
    )


def recover_junction(state: dict[str, Any]) -> None:
    """Восстанавливает junction на сохранённую целевую папку."""

    original = Path(state["original_vms"])
    backup = Path(state["backup_vms"])
    target = Path(state["link_target"])

    if not target.is_dir():
        raise FileNotFoundError(f"Цель junction отсутствует: {target}")

    if lexists(original):
        if is_reparse_point(original):
            current_target = resolve_link_target(original)

            if os.path.normcase(str(current_target)) == os.path.normcase(
                str(target)
            ):
                clear_state()
                return

        quarantine_path(
            original,
            "generated_before_junction",
            backup,
        )

    create_junction(original, target)
    clear_state()
    LOGGER.info("Junction восстановлен: %s -> %s", original, target)


def recover_pending_state(required: bool = False) -> bool:
    """Восстанавливает vms по state.json после ошибки или перезагрузки."""

    state = load_state()

    if state is None:
        if required:
            raise FileNotFoundError(STATE_PATH)
        return False

    LOGGER.warning("Найден незавершённый запуск: %s", STATE_PATH)
    update_state(status="restoring")

    mode = state.get("mode")

    if mode == "physical":
        recover_physical(state)
    elif mode == "junction":
        recover_junction(state)
    elif mode == "none":
        clear_state()
    else:
        raise RuntimeError(f"Неизвестный mode в state.json: {mode!r}")

    return True


# =============================================================================
# ДЕИНСТАЛЛЯЦИЯ И УСТАНОВКА
# =============================================================================


def start_gui(executable: Path) -> subprocess.Popen[Any]:
    """Запускает GUI-приложение из его собственной папки."""

    LOGGER.info("Запускаем: %s", executable)

    return subprocess.Popen(
        [str(executable)],
        cwd=str(executable.parent),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def wait_process(
    process: subprocess.Popen[Any],
    timeout: float,
    label: str,
) -> None:
    """Ждёт завершения процесса и обрабатывает зависание."""

    try:
        process.wait(timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        save_screenshot(f"{label}_timeout")

        try:
            process.terminate()
            process.wait(timeout=10)
        except Exception:
            try:
                process.kill()
            except Exception:
                pass

        raise TimeoutError(
            f"{label} не завершился за {timeout:.1f} сек."
        ) from exc


def uninstall_ldplayer() -> None:
    """Выполняет штатную деинсталляцию через PNG-шаблоны."""

    uninstaller = PATHS["uninstaller"]

    if not uninstaller.is_file():
        if not installation_present():
            LOGGER.info("LDPlayer не установлен — удаление пропущено.")
            return

        if bool(
            CONFIG["safety"]["allow_install_over_existing_without_uninstaller"]
        ):
            LOGGER.warning(
                "Деинсталлятор отсутствует; установка поверх разрешена config.json."
            )
            return

        raise FileNotFoundError(f"Деинсталлятор не найден: {uninstaller}")

    process = start_gui(uninstaller)

    delete_button = locate_template("delete")
    click_template(delete_button, "uninstall_delete")

    time.sleep(float(CONFIG["timeouts"]["after_delete_click"]))

    done_button = locate_template("delete_done")
    click_template(done_button, "uninstall_done")

    time.sleep(float(CONFIG["timeouts"]["after_delete_done_click"]))

    wait_process(
        process,
        float(CONFIG["timeouts"]["uninstaller_exit"]),
        "uninstaller",
    )

    LOGGER.info("Деинсталляция завершена.")


def verify_installation() -> None:
    """Проверяет появление хотя бы одного ожидаемого EXE-файла."""

    target = PATHS["target_dir"]
    expected = [
        target / str(value)
        for value in CONFIG["verification"]["expected_files_any"]
    ]

    deadline = (
        time.monotonic()
        + float(CONFIG["timeouts"]["verify_installation"])
    )

    while time.monotonic() < deadline:
        existing = [path for path in expected if path.is_file()]

        if target.is_dir() and existing:
            LOGGER.info(
                "Проверка установки успешна: %s",
                ", ".join(map(str, existing)),
            )
            return

        time.sleep(1)

    raise RuntimeError(
        f"В {target} не найден ни один ожидаемый файл: "
        + ", ".join(path.name for path in expected)
    )


def install_ldplayer() -> None:
    """Запускает установщик, проверяет диск C и ждёт завершения."""

    if pyautogui is None:
        raise RuntimeError("PyAutoGUI недоступен.")

    installer = PATHS["installer"]

    if not installer.is_file():
        raise FileNotFoundError(installer)

    start_gui(installer)
    time.sleep(float(CONFIG["timeouts"]["after_installer_start"]))

    window = locate_template("window")

    region = expand_region(
        window,
        int(CONFIG["ui"]["window_region_padding"]),
    )

    settings = locate_template(
        "settings",
        region=region,
        required=False,
    )

    if settings:
        click_template(settings, "installer_settings")
    elif bool(CONFIG["ui"]["allow_enter_if_settings_missing"]):
        LOGGER.warning(
            "settings.png не найден — отправляем Enter по настройке config.json."
        )
        pyautogui.press("enter")
    else:
        raise TimeoutError(
            "settings.png не найден; слепое нажатие Enter отключено."
        )

    time.sleep(float(CONFIG["timeouts"]["after_settings_click"]))

    # Читаем путь прямо из поля и меняем только букву диска на C.
    verified_install_path = ensure_install_drive(window)

    LOGGER.info(
        "Итоговый путь установки подтверждён: %s",
        verified_install_path,
    )

    install_timeout = max(
        float(CONFIG["templates"]["install"]["timeout"]),
        float(CONFIG["templates"]["install_alt"]["timeout"]),
    )

    key, install_button = locate_any(
        ("install", "install_alt"),
        region=region,
        timeout=install_timeout,
    )

    LOGGER.info("Кнопка установки найдена по шаблону: %s", key)
    click_template(install_button, "installer_install")

    time.sleep(float(CONFIG["timeouts"]["after_install_click"]))

    locate_template("done")
    LOGGER.info("done.png найден.")

    verify_installation()


# =============================================================================
# ОСНОВНОЙ СЦЕНАРИЙ
# =============================================================================


def reinstall_workflow() -> None:
    """Выполняет переустановку и всегда пытается вернуть vms через finally."""

    main_error: BaseException | None = None
    main_traceback = None

    try:
        # Сначала останавливаем процессы и только потом трогаем vms.
        stop_ldplayer_processes()
        protect_vms()

        update_state(status="uninstalling")
        uninstall_ldplayer()

        stop_ldplayer_processes()

        update_state(status="installing")
        install_ldplayer()

        update_state(status="installation_verified")

    except BaseException as exc:
        main_error = exc
        main_traceback = sys.exc_info()[2]
        LOGGER.exception("Ошибка основного сценария.")

    finally:
        restore_error: BaseException | None = None

        if STATE_PATH.exists():
            try:
                LOGGER.info("Обязательное восстановление vms...")

                # Установщик может запустить LDPlayer автоматически.
                stop_ldplayer_processes()
                recover_pending_state(required=True)

            except BaseException as exc:
                restore_error = exc
                LOGGER.exception("КРИТИЧЕСКАЯ ОШИБКА восстановления vms.")

        if restore_error is not None:
            if main_error is not None:
                raise RuntimeError(
                    "Переустановка завершилась ошибкой, и vms не удалось "
                    "автоматически восстановить. state.json сохранён."
                ) from restore_error

            raise restore_error

    if main_error is not None:
        raise main_error.with_traceback(main_traceback)


def main() -> int:
    """Подготавливает окружение и запускает выбранный режим работы."""

    global CONFIG
    global PATHS

    configure_console()
    set_dpi_awareness()

    arguments = parse_arguments()

    if os.name != "nt":
        raise OSError("Скрипт предназначен только для Windows.")

    if not is_admin():
        elevate_self()
        return 0

    acquire_mutex()

    CONFIG, created, updated = load_config()
    PATHS = resolve_paths(CONFIG)

    log_path = setup_logging(PATHS["logs_dir"])
    configure_pyautogui()

    LOGGER.info("LDPlayer Reinstall v3.2")
    LOGGER.info("Папка скрипта: %s", BASE_DIR)
    LOGGER.info("Конфигурация: %s", CONFIG_PATH)
    LOGGER.info("Лог: %s", log_path)

    if created:
        LOGGER.info("Создан config.json с настройками по умолчанию.")
    elif updated:
        LOGGER.info("В config.json добавлены новые параметры по умолчанию.")

    # Перед новым запуском восстанавливаем незавершённую прошлую операцию.
    if STATE_PATH.exists():
        if not bool(CONFIG["safety"]["auto_recover_previous_state"]):
            raise RuntimeError(
                "Найден state.json, но автовосстановление отключено."
            )

        stop_ldplayer_processes()
        recover_pending_state(required=True)

    if arguments.recover_only:
        LOGGER.info("Незавершённого state.json больше нет.")
        show_message("LDPlayer", "Восстановление завершено.")
        return 0

    health_check()

    if arguments.health_check_only:
        show_message(
            "LDPlayer",
            "Health-check пройден. Изменения не выполнялись.",
        )
        return 0

    reinstall_workflow()

    LOGGER.info("ГОТОВО: LDPlayer переустановлен, vms восстановлена.")

    show_message(
        "LDPlayer",
        "ГОТОВО! LDPlayer переустановлен, vms восстановлена.",
    )

    return 0


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    exit_code = 0

    try:
        exit_code = main()

    except KeyboardInterrupt:
        exit_code = 130

        if LOGGER.handlers:
            LOGGER.error("Операция прервана пользователем.")

        show_message(
            "LDPlayer",
            "Операция прервана пользователем.",
            error=True,
        )

    except Exception as exc:
        exit_code = 1

        if LOGGER.handlers:
            LOGGER.error("ФАТАЛЬНАЯ ОШИБКА:\n%s", traceback.format_exc())

            if bool(
                CONFIG.get("debug", {}).get("screenshot_on_error", True)
            ):
                save_screenshot("fatal_error")

            details = str(RUN_DIR / "run.log") if RUN_DIR else "консоль"
        else:
            traceback.print_exc()
            details = "консоль"

        show_message(
            "Ошибка LDPlayer",
            f"{exc}\n\nПодробности: {details}",
            error=True,
        )

    finally:
        release_mutex()

    raise SystemExit(exit_code)
