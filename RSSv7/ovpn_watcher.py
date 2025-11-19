#!/usr/bin/env python3
"""
openvpn_watchdog.py  –  мониторит OpenVPN-GUI, умеет переключаться на резервный
профиль, шлёт уведомления в Telegram и перезапускает туннель при сбое.

pip install psutil requests
"""
from __future__ import annotations
import time, subprocess, sys
from pathlib import Path
from typing import Final

import psutil
import requests

###############################################################################
# CONFIG
###############################################################################
OPENVPN_GUI_PATH: Final = r"C:\Program Files\OpenVPN\bin\openvpn-gui.exe"

# Профили в порядке приоритета
PROFILES:        Final[list[str]] = ["sde", "xnl"]

INTERVAL_SEC:    Final = 30          # период проверки
CONNECT_WAIT_SEC:Final = 15          # сколько секунд ждать, что туннель поднимется

TELEGRAM_TOKEN:  Final = "7460479135:AAEUcUZdO01AEOVxgA0xlV8ZoLOmZcKw-Uc"
CHAT_ID:         Final = "275483461"

VERBOSE:         Final = False
###############################################################################


class Watchdog:
    def __init__(self) -> None:
        self.last_ok = True
        self.active_profile: str | None = None

    # --------------------------- helpers ---------------------------------- #
    @staticmethod
    def _find(name: str) -> list[psutil.Process]:
        return [p for p in psutil.process_iter(("name",))
                if p.info["name"].lower() == name.lower()]

    @staticmethod
    def _profile_up(profile: str) -> bool:
        needle = f"{profile}.ovpn".lower()
        for p in psutil.process_iter(("name", "cmdline")):
            if p.info["name"].lower() != "openvpn.exe":
                continue
            if needle in " ".join(p.info.get("cmdline") or []).lower():
                return True
        return False

    # --------------------------- telegram --------------------------------- #
    @staticmethod
    def _notify(msg: str) -> None:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        try:
            requests.post(url,
                          json={"chat_id": CHAT_ID, "text": msg},
                          timeout=10).raise_for_status()
        except requests.RequestException as e:
            print(f"[!] Telegram error: {e}", file=sys.stderr)

    # --------------------------- restart ---------------------------------- #
    def _restart(self) -> None:
        self._notify("F99⚠️ OpenVPN down — attempting fallback…")

        # 1. убить остатки
        for p in self._find("openvpn-gui.exe") + self._find("openvpn.exe"):
            try:
                p.terminate()
            except Exception:
                pass
        psutil.wait_procs(self._find("openvpn.exe"), timeout=5)

        # 2. пробуем профили по очереди
        for idx, prof in enumerate(PROFILES):
            if idx == 0:
                # запускаем GUI и сразу коннектим
                subprocess.Popen([OPENVPN_GUI_PATH, "--connect", prof],
                                 stdout=subprocess.DEVNULL,
                                 stderr=subprocess.DEVNULL)
            else:
                # GUI уже запущен – отдаём команду
                subprocess.Popen([OPENVPN_GUI_PATH, "--command", "connect", prof],
                                 stdout=subprocess.DEVNULL,
                                 stderr=subprocess.DEVNULL)

            for _ in range(CONNECT_WAIT_SEC):
                if self._profile_up(prof):
                    self.active_profile = prof
                    self._notify(f"F99✅ Connected using profile *{prof}*.")
                    return
                time.sleep(1)

            self._notify(f"❌ Profile *{prof}* failed, trying next…")

        # если до сюда дошли – ни один не поднялся
        self.active_profile = None
        self._notify("🚫 All profiles failed. Will retry in next cycle.")

    # ---------------------------- loop ------------------------------------ #
    def run(self) -> None:
        while True:
            # активен ли хоть один профиль?
            ok = False
            for prof in PROFILES:
                if self._profile_up(prof):
                    ok, self.active_profile = True, prof
                    break
            if VERBOSE:
                print(f"OK={ok} active={self.active_profile}")

            if ok and not self.last_ok:
                self._notify(f"F99✅ Tunnel restored with *{self.active_profile}*.")
            elif not ok and self.last_ok:
                self._restart()

            self.last_ok = ok
            time.sleep(max(5, INTERVAL_SEC))

# ------------------------------------------------------------------------- #
if __name__ == "__main__":
    if not Path(OPENVPN_GUI_PATH).exists():
        sys.exit(f"openvpn-gui.exe not found: {OPENVPN_GUI_PATH}")

    try:
        Watchdog().run()
    except KeyboardInterrupt:
        print("\nStopped by user.")
