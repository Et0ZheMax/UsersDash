# launcher.py

import json
import os
import sys
import ctypes
import subprocess
import pathlib
import tkinter as tk
from tkinter import ttk, messagebox, filedialog

CONFIG_PATH = pathlib.Path(__file__).parent / "config.json"

DEFAULT_CONFIG = {
    "entries": [
        {
            "type": "script",
            "label": "Остановить задачи",
            "path": r"C:\Users\Administrator\Desktop\RssCounterV8\taskSTOP.py",
            "args": ["disable"]
        },
        {
            "type": "script",
            "label": "Включить задачи",
            "path": r"C:\Users\Administrator\Desktop\RssCounterV8\taskSTOP.py",
            "args": ["enable"]
        }
    ],
    "window": {}
}


class Launcher(tk.Tk):
    POLL_INTERVAL = 2000  # milliseconds
    TASK_NAMES = ["\\LD СЛЁТ", "\\LD РАЗЛОГ", "\\LD GN ПРОВЕРКА"]

    def __init__(self):
        super().__init__()
        self.title("F99 – Launcher")
        self.protocol("WM_DELETE_WINDOW", self.on_close)

        # Load configuration
        self.cfg = load_config()
        self.entries = self.cfg["entries"]

        # Restore window geometry
        geom = self.cfg["window"].get("geometry")
        if geom:
            self.geometry(geom)

        # Top frame: status indicator + settings button
        top_frame = ttk.Frame(self)
        top_frame.pack(fill="x", anchor="ne", padx=8, pady=4)

        # новое: чёрная точка + красный цвет по умолчанию
        self.status_label = ttk.Label(
            top_frame,
            text="●",
            font=("Segoe UI Emoji", 14),
            foreground="red"
        )
        self.status_label.pack(side="left", padx=(0, 8))

        # Settings button on the right
        settings_btn = ttk.Button(top_frame, text="⚙ Настройки", command=self.open_settings)
        settings_btn.pack(side="right")

        # Buttons panel
        self.buttons_frame = ttk.Frame(self)
        self.buttons_frame.pack(fill="both", expand=True, padx=8, pady=8)

        self.buttons: list[ttk.Button] = []
        self.build_buttons()
        self.buttons_frame.bind("<Configure>", lambda e: self.reflow())

        # Start polling task statuses
        self.after(self.POLL_INTERVAL, self.poll_task_status)

    def poll_task_status(self):
        all_enabled = True
        for task in self.TASK_NAMES:
            print(f"[DEBUG] Querying task: {task}")
            try:
                output = subprocess.check_output([
                    "schtasks", "/Query", "/TN", task, "/FO", "LIST", "/V"
                ], stderr=subprocess.STDOUT)
                try:
                    text = output.decode('cp866')
                except:
                    try:
                        text = output.decode('cp437')
                    except:
                        text = output.decode('utf-8', errors='ignore')

                state_val = None
                for line in text.splitlines():
                    left = line.split(":", 1)[0].strip().lower()
                    right = line.split(":", 1)[1].strip().lower() if ":" in line else ""
                    # английский
                    if "scheduled task state" in left or left.startswith("enabled"):
                        state_val = right
                        break
                    # русская локаль
                    if "состояние задачи" in left or "запланированной задачи" in left:
                        state_val = right
                        break

                # дополнительный общий fallback
                if state_val is None:
                    if "enabled" in text.lower() or "включено" in text.lower():
                        state_val = "enabled"
                    elif "disabled" in text.lower() or "отключено" in text.lower():
                        state_val = "disabled"

                print(f"[DEBUG] Task {task} state_val={state_val}")
                if state_val not in ("enabled", "yes", "true", "да", "1"):
                    all_enabled = False

            except subprocess.CalledProcessError as e:
                print(f"[DEBUG] Task {task} query error: {e}")
                all_enabled = False

        # update indicator
        color = "green" if all_enabled else "red"
        self.status_label.config(text="●", foreground=color)
        self.after(self.POLL_INTERVAL, self.poll_task_status)

    def build_buttons(self):
        for b in self.buttons:
            b.destroy()
        self.buttons.clear()

        for ent in self.entries:
            b = ttk.Button(
                self.buttons_frame,
                text=ent.get("label", ""),
                command=lambda e=ent: self.run_entry(e)
            )
            self.buttons.append(b)

        self.reflow()

    def reflow(self):
        for b in self.buttons:
            b.grid_forget()

        w = max(1, self.buttons_frame.winfo_width())
        minw = 150
        cols = max(1, w // minw)
        for c in range(cols):
            self.buttons_frame.columnconfigure(c, weight=1)

        for i, b in enumerate(self.buttons):
            r, c = divmod(i, cols)
            b.grid(row=r, column=c, sticky="nsew", padx=4, pady=4)

    def run_entry(self, ent):
        t, path = ent["type"], ent["path"]
        args = ent.get("args", [])
        try:
            if t == "folder":
                os.startfile(path)
            else:
                if t == "script":
                    lpFile = sys.executable
                    lpParams = f"\"{path}\"" + ("" if not args else " " + " ".join(args))
                else:
                    lpFile = path
                    lpParams = None

                ret = ctypes.windll.shell32.ShellExecuteW(
                    None, "runas", lpFile, lpParams, None, 1
                )
                if ret <= 32:
                    raise RuntimeError(f"ShellExecute вернул {ret}")
        except Exception as e:
            messagebox.showerror("Ошибка запуска", str(e))

    def open_settings(self):
        Settings(self)

    def on_close(self):
        self.cfg["window"]["geometry"] = self.geometry()
        save_config(self.cfg)
        self.destroy()


class Settings(tk.Toplevel):
    def __init__(self, parent: Launcher):
        super().__init__(parent)
        self.parent = parent
        self.title("Настройки")
        self.geometry("600x400")

        self.entries = list(parent.entries)

        lf = ttk.Frame(self)
        lf.pack(fill="both", expand=True, padx=8, pady=8)
        self.listbox = tk.Listbox(lf)
        self.listbox.pack(side="left", fill="both", expand=True)
        sb = ttk.Scrollbar(lf, command=self.listbox.yview)
        sb.pack(side="right", fill="y")
        self.listbox.config(yscrollcommand=sb.set)
        self.refresh()

        bf = ttk.Frame(self)
        bf.pack(pady=4)
        ttk.Button(bf, text="Добавить",    command=self.add).pack(side="left", padx=4)
        ttk.Button(bf, text="Редактировать",command=self.edit).pack(side="left", padx=4)
        ttk.Button(bf, text="Удалить",     command=self.remove).pack(side="left", padx=4)
        ttk.Button(bf, text="Вверх",       command=self.move_up).pack(side="left", padx=4)
        ttk.Button(bf, text="Вниз",        command=self.move_down).pack(side="left", padx=4)

        af = ttk.Frame(self)
        af.pack(pady=8)
        ttk.Button(af, text="Сохранить", command=self.save).pack(side="left", padx=4)
        ttk.Button(af, text="Отмена",    command=self.destroy).pack(side="left", padx=4)

    def refresh(self):
        self.listbox.delete(0, tk.END)
        for i, e in enumerate(self.entries, 1):
            args = " " + " ".join(e.get("args", [])) if e.get("args") else ""
            self.listbox.insert(
                tk.END,
                f"{i}. [{e['type']}] {e['label']} → {e['path']}{args}"
            )

    def add(self):
        EntryDlg(self, None)

    def edit(self):
        sel = self.listbox.curselection()
        if sel:
            EntryDlg(self, sel[0])

    def remove(self):
        sel = self.listbox.curselection()
        if sel and messagebox.askyesno("Удалить?", "Удалить запись?"):
            self.entries.pop(sel[0])
            self.refresh()

    def move_up(self):
        sel = self.listbox.curselection()
        if not sel or sel[0] == 0:
            return
        i = sel[0]
        self.entries[i-1], self.entries[i] = self.entries[i], self.entries[i-1]
        self.refresh()
        self.listbox.selection_set(i-1)

    def move_down(self):
        sel = self.listbox.curselection()
        if not sel or sel[0] == len(self.entries)-1:
            return
        i = sel[0]
        self.entries[i+1], self.entries[i] = self.entries[i], self.entries[i+1]
        self.refresh()
        self.listbox.selection_set(i+1)

    def save(self):
        self.parent.cfg["entries"] = self.entries
        save_config(self.parent.cfg)
        self.parent.entries = self.entries
        self.parent.build_buttons()
        self.destroy()


class EntryDlg(tk.Toplevel):
    def __init__(self, parent: Settings, idx):
        super().__init__(parent)
        self.parent = parent
        self.idx = idx
        self.title("Новая запись" if idx is None else "Редактирование")
        self.resizable(False, False)

        ttk.Label(self, text="Тип:").grid(row=0, column=0, padx=4, pady=4, sticky="e")
        self.type_v = tk.StringVar(value="script")
        ttk.Combobox(
            self, textvariable=self.type_v,
            values=["script", "program", "folder"], state="readonly"
        ).grid(row=0, column=1, padx=4, pady=4, sticky="w")

        ttk.Label(self, text="Имя кнопки:").grid(row=1, column=0, padx=4, pady=4, sticky="e")
        self.label_v = tk.StringVar()
        ttk.Entry(self, textvariable=self.label_v).grid(row=1, column=1, padx=4, pady=4, sticky="we")

        ttk.Label(self, text="Путь:").grid(row=2, column=0, padx=4, pady=4, sticky="e")
        self.path_v = tk.StringVar()
        ttk.Entry(self, textvariable=self.path_v, width=50).grid(row=2, column=1, padx=4, pady=4, sticky="we")
        ttk.Button(self, text="…", width=3, command=self.browse).grid(row=2, column=2, padx=4)

        ttk.Label(self, text="Аргументы:").grid(row=3, column=0, padx=4, pady=4, sticky="e")
        self.args_v = tk.StringVar()
        ttk.Entry(self, textvariable=self.args_v).grid(row=3, column=1, padx=4, pady=4, sticky="we")

        ff = ttk.Frame(self)
        ff.grid(row=4, column=0, columnspan=3, pady=8)
        ttk.Button(ff, text="OK",     command=self.ok).pack(side="left", padx=4)
        ttk.Button(ff, text="Отмена", command=self.destroy).pack(side="left", padx=4)

        if idx is not None:
            e = parent.entries[idx]
            self.type_v.set(e["type"])
            self.label_v.set(e["label"])
            self.path_v.set(e["path"])
            self.args_v.set(" ".join(e.get("args", [])))

    def browse(self):
        if self.type_v.get() == "folder":
            p = filedialog.askdirectory()
        else:
            p = filedialog.askopenfilename()
        if p:
            self.path_v.set(p)

    def ok(self):
        typ = self.type_v.get()
        lbl = self.label_v.get().strip()
        pth = self.path_v.get().strip()
        args = self.args_v.get().split() if self.args_v.get().strip() else []

        if not lbl or not pth:
            messagebox.showerror("Ошибка", "Заполните все поля")
            return

        rec = {"type": typ, "label": lbl, "path": pth}
        if args:
            rec["args"] = args

        if self.idx is None:
            self.parent.entries.append(rec)
        else:
            self.parent.entries[self.idx] = rec

        self.parent.refresh()
        self.destroy()


def load_config():
    if not CONFIG_PATH.exists():
        save_config(DEFAULT_CONFIG)
        return DEFAULT_CONFIG.copy()
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    cfg.setdefault("entries", [])
    cfg.setdefault("window", {})
    return cfg


def save_config(cfg):
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    app = Launcher()
    app.mainloop()
