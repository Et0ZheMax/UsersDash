"""Повторная авторизация существующего LDPlayer в Viking Rise.

Утилита связывает экземпляр LDPlayer с записью активного профиля GnBots по
``InstanceId`` и повторно выполняет вход через IGG. Она не создаёт, не
удаляет и не переименовывает эмуляторы и не изменяет профиль бота.
"""

from __future__ import annotations

import argparse
import logging
import sys
import tempfile
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from viking_recovery import (
    GNBOTS_DIR,
    LDPLAYER_DIR,
    TEMPLATE_INDEX,
    Farm,
    Instance,
    RecoveryEngine,
    RecoveryError,
    SingleRunLock,
    find_farm,
    load_farms,
)


@dataclass(frozen=True)
class LoginTarget:
    """Экземпляр LDPlayer и привязанная к нему запись фермы GnBots."""

    instance: Instance
    farm: Farm | None
    issue: str = ""

    @property
    def ready(self) -> bool:
        return self.farm is not None and self.farm.ready and not self.issue


def build_login_targets(instances: Sequence[Instance], farms: Sequence[Farm]) -> list[LoginTarget]:
    """Связать существующие эмуляторы с фермами строго по ``InstanceId``."""

    farms_by_index: dict[int, list[Farm]] = {}
    for farm in farms:
        if farm.instance_id is not None:
            farms_by_index.setdefault(farm.instance_id, []).append(farm)

    targets: list[LoginTarget] = []
    for instance in sorted(instances, key=lambda item: item.index):
        if instance.index == TEMPLATE_INDEX:
            continue
        matches = farms_by_index.get(instance.index, [])
        if not matches:
            targets.append(LoginTarget(instance, None, "Нет фермы с таким InstanceId"))
        else:
            for farm in sorted(matches, key=lambda item: item.name.casefold()):
                issue = "" if farm.ready else "Не заполнены Email/Password или Slot не igg"
                targets.append(LoginTarget(instance, farm, issue))
    return targets


class LoginEngine(RecoveryEngine):
    """Контроллер повторного входа в уже существующий экземпляр LDPlayer."""

    def validate_login_environment(self) -> None:
        """Проверить только компоненты входа, не требуя чистого образа и места для клона."""

        missing = [path for path in (self.ldconsole, self.adb, self.profile_path) if not path.exists()]
        if missing:
            raise RecoveryError("Не найдены обязательные файлы: " + ", ".join(map(str, missing)))
        ocr = self.gnbots_dir / "Tesseract.dll"
        trained = self.gnbots_dir / "tessdata" / "eng.traineddata"
        helper = Path(__file__).with_name("ocr_words.ps1")
        if not ocr.exists() or not trained.exists() or not helper.exists():
            raise RecoveryError("Не найдены OCR-компоненты VikingLogin/GnBots")

    def login(self, farm: Farm, instance_index: int) -> Instance:
        """Запустить существующий экземпляр и повторно авторизовать выбранную ферму."""

        if not farm.ready:
            raise RecoveryError("Для фермы нужны непустые Email/Password и Slot=igg")
        if instance_index == TEMPLATE_INDEX:
            raise RecoveryError("Авторизация в чистом образе ID 0 запрещена")
        if farm.instance_id != instance_index:
            raise RecoveryError(
                f"Профиль GnBots связывает ферму {farm.name} с LDPlayer ID {farm.instance_id}, "
                f"а выбран ID {instance_index}"
            )

        self.validate_login_environment()
        matches = [item for item in self.list_instances() if item.index == instance_index]
        if len(matches) != 1:
            raise RecoveryError(f"LDPlayer ID {instance_index} больше не найден однозначно")

        self.new_index = instance_index
        self._validate_instance_storage(instance_index)
        self.launch_and_wait()
        self.open_login()
        self.submit_credentials(farm)
        self.select_game_id(farm)
        self.wait_for_game()
        self.logger.info(
            "Повторная авторизация завершена: ферма %s, LDPlayer ID %s",
            farm.name,
            instance_index,
        )
        return matches[0]


def configure_login_logging(log_path: Path | None = None) -> logging.Logger:
    """Создать отдельный журнал VikingLogin без секретных данных."""

    path = log_path or Path(__file__).with_name("logs") / "viking_login.log"
    path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("viking_login")
    logger.setLevel(logging.DEBUG)
    if not logger.handlers:
        handler = logging.FileHandler(path, encoding="utf-8")
        handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        logger.addHandler(handler)
    return logger


def main(argv: Sequence[str] | None = None) -> int:
    """Запустить консольный интерфейс VikingLogin."""

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--list", action="store_true", help="показать экземпляры и связанные фермы")
    parser.add_argument("--farm", help="имя фермы из активного профиля GnBots")
    parser.add_argument("--dry-run", action="store_true", help="проверить привязку без запуска")
    args = parser.parse_args(argv)
    logger = configure_login_logging()
    try:
        farms = load_farms()
        engine = LoginEngine(logger, status=print, ldplayer_dir=LDPLAYER_DIR, gnbots_dir=GNBOTS_DIR)
        targets = build_login_targets(engine.list_instances(), farms)
        if args.list:
            for target in targets:
                farm_name = target.farm.name if target.farm else "—"
                state = "готов" if target.ready else target.issue
                print(f"ID {target.instance.index}\t{target.instance.name}\t{farm_name}\t{state}")
            return 0
        if args.farm:
            farm = find_farm(args.farm, farms)
            target = next(
                (item for item in targets if item.farm and item.farm.record_id == farm.record_id),
                None,
            )
            if target is None or not target.ready:
                raise RecoveryError(target.issue if target else "Эмулятор фермы не найден")
            if args.dry_run:
                engine.validate_login_environment()
                engine._validate_instance_storage(target.instance.index)
                print(
                    f"Привязка проверена: {farm.name} -> {target.instance.name}, "
                    f"LDPlayer ID {target.instance.index}"
                )
                return 0
            lock_path = Path(tempfile.gettempdir()) / "VikingRecovery" / "run.lock"
            with SingleRunLock(lock_path):
                instance = engine.login(farm, target.instance.index)
            print(f"Готово: {farm.name} повторно авторизована в {instance.name}, ID {instance.index}")
            return 0
        parser.print_help()
        return 0
    except RecoveryError as exc:
        logger.exception("Повторная авторизация завершилась ошибкой: %s", exc)
        print(f"Ошибка: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
