# launcher.py
import subprocess
import pathlib
import tkinter as tk
from tkinter import ttk, messagebox

# --- здесь словарь: имя кнопки -> список аргументов для Popen ------------
COMMANDS = {
    "BOT_RESTART": [
        "python",
        r"C:\Users\Administrator\Desktop\RssCounterV8\BOT_RESTART.py",
    ],
    "Проверка Бот\\Окна": [
        "python",
        r"C:\Users\Administrator\Desktop\RssCounterV8\Gn_Ld_Check.py",
    ],
    "Проверка логов на ошибки": [
        "python",
        r"C:\Users\Administrator\Desktop\RssCounterV8\LD_problems.py",
    ],
    "Остановить задачи": [
        "python",
        r"C:\Users\Administrator\Desktop\RssCounterV8\taskSTOP.py",
        "disable",
    ],
    "Включить задачи": [
        "python",
        r"C:\Users\Administrator\Desktop\RssCounterV8\taskSTOP.py",
        "enable",
    ],
    "Спарсить IGG id": [
        "python",
        r"C:\Users\Administrator\Desktop\RssCounterV8\IGG_ID_PARSER.py",
        "enable",
    ],
    "Обновить VR": [
        "python",
        r"C:\Users\Administrator\Desktop\RssCounterV8\LdUPD\LdUPD.py",
        "enable",
    ],
}

def run_script(cmd_args: list[str]):
    try:
        # Если нужно, открываем новое окно консоли
        subprocess.Popen(cmd_args, creationflags=subprocess.CREATE_NEW_CONSOLE)
    except Exception as e:
        messagebox.showerror("Ошибка запуска", str(e))

root = tk.Tk()
root.title("F99 – Launcher")

# Для каждого пункта из COMMANDS создаём кнопку
for label, cmd in COMMANDS.items():
    btn = ttk.Button(
        root,
        text=label,
        width=30,
        command=lambda c=cmd: run_script(c)
    )
    btn.pack(padx=8, pady=4)

root.mainloop()
