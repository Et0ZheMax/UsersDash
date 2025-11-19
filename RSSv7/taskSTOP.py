#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations  # если нужно поддерживать 3.7–3.8
import os
import sys
import subprocess
import ctypes
from typing import List, Tuple  # опционально, если не используете __future__.annotations

def is_admin() -> bool:
    try:
        return ctypes.windll.shell32.IsUserAnAdmin()
    except Exception:
        return False

if not is_admin():
    script = os.path.abspath(sys.argv[0])
    params = " ".join([f'"{script}"'] + [f'"{arg}"' for arg in sys.argv[1:]])
    ctypes.windll.shell32.ShellExecuteW(None, "runas", sys.executable, params, None, 1)
    sys.exit(0)

# Список ваших задач
TASKS = [
    r"\LD СЛЁТ",
    r"\LD РАЗЛОГ",
    r"\LD GN ПРОВЕРКА",
]

def run_schtasks(cmd: List[str]) -> Tuple[int, str]:
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    out = proc.stdout.decode('cp866', errors='replace').strip()
    return proc.returncode, out

def change_task_state(task_name: str, disable: bool) -> bool:
    action = "/disable" if disable else "/enable"
    cmd = ["schtasks", "/Change", "/TN", task_name, action]
    code, out = run_schtasks(cmd)
    if code == 0:
        verb = "Приостановлена" if disable else "Включена"
        print(f"✅ {verb} задача: {task_name}")
        return True
    else:
        verb = "приостановке" if disable else "включении"
        print(f"❌ Ошибка при {verb} задачи {task_name}:")
        print(out or "(нет вывода)")
        return False

def main():
    if len(sys.argv) != 2 or sys.argv[1] not in ("disable", "enable"):
        print("Usage: taskSTOP.py <disable|enable>")
        sys.exit(1)

    disable = (sys.argv[1] == "disable")
    print(f"{'Приостанавливаем' if disable else 'Включаем'} задачи…")
    for task in TASKS:
        change_task_state(task, disable=disable)

if __name__ == "__main__":
    main()
