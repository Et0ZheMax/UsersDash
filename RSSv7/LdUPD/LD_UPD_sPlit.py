import os
import json
import subprocess
import threading
import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext
from datetime import datetime

# Путь к папке скрипта, файлам настроек и логов
APP_DIR = os.path.dirname(os.path.abspath(__file__))
SETTINGS_PATH = os.path.join(APP_DIR, 'settings.json')
HISTORY_LOG = os.path.join(APP_DIR, 'installed_history.log')

# Инициализация настроек
default_settings = {
    'config_dir': '',
    'arch_file': '',
    'main_file': '',
    'pack_file': '',
    'ldconsole': 'ldconsole.exe'
}
try:
    with open(SETTINGS_PATH, 'r', encoding='utf-8') as sf:
        settings = json.load(sf)
except Exception:
    settings = default_settings.copy()


def save_settings():
    with open(SETTINGS_PATH, 'w', encoding='utf-8') as sf:
        json.dump(settings, sf, ensure_ascii=False, indent=2)


def log_history(emulator_name):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(HISTORY_LOG, 'a', encoding='utf-8') as lf:
        lf.write(f"{timestamp} Installed on: {emulator_name}\n")


def find_key_recursive(d, key):
    if isinstance(d, dict):
        for k, v in d.items():
            if k == key:
                return v
            if isinstance(v, (dict, list)):
                found = find_key_recursive(v, key)
                if found is not None:
                    return found
    elif isinstance(d, list):
        for item in d:
            found = find_key_recursive(item, key)
            if found is not None:
                return found
    return None


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("LDPlayer Split-APK Installer")
        self.geometry("800x600")

        # Переменные интерфейса
        self.config_dir = tk.StringVar(value=settings.get('config_dir', ''))
        self.arch_file = tk.StringVar(value=settings.get('arch_file', ''))
        self.main_file = tk.StringVar(value=settings.get('main_file', ''))
        self.pack_file = tk.StringVar(value=settings.get('pack_file', ''))
        self.ldconsole = tk.StringVar(value=settings.get('ldconsole', 'ldconsole.exe'))
        self.emu_names = []
        self.check_vars = []
        self.check_widgets = []

        self._build_ui()

    def _build_ui(self):
        frm = tk.Frame(self)
        frm.pack(fill='x', padx=10, pady=5)

        # Папка конфигов эмуляторов
        tk.Label(frm, text="Config dir:").grid(row=0, column=0, sticky='e')
        tk.Entry(frm, textvariable=self.config_dir, width=60).grid(row=0, column=1, padx=5)
        tk.Button(frm, text="Browse…", command=self.browse_config).grid(row=0, column=2)

        # Архитектурный APK
        tk.Label(frm, text="Arch APK:").grid(row=1, column=0, sticky='e')
        tk.Entry(frm, textvariable=self.arch_file, width=60).grid(row=1, column=1, padx=5)
        tk.Button(frm, text="Browse…", command=self.browse_arch).grid(row=1, column=2)

        # Базовый APK
        tk.Label(frm, text="Main APK:").grid(row=2, column=0, sticky='e')
        tk.Entry(frm, textvariable=self.main_file, width=60).grid(row=2, column=1, padx=5)
        tk.Button(frm, text="Browse…", command=self.browse_main).grid(row=2, column=2)

        # Пакет ресурсов
        tk.Label(frm, text="Pack APK:").grid(row=3, column=0, sticky='e')
        tk.Entry(frm, textvariable=self.pack_file, width=60).grid(row=3, column=1, padx=5)
        tk.Button(frm, text="Browse…", command=self.browse_pack).grid(row=3, column=2)

        # Путь к ldconsole
        tk.Label(frm, text="ldconsole:").grid(row=4, column=0, sticky='e')
        tk.Entry(frm, textvariable=self.ldconsole, width=60).grid(row=4, column=1, padx=5)
        tk.Button(frm, text="Browse…", command=self.browse_ldconsole).grid(row=4, column=2)

        # Кнопка сканирования
        tk.Button(frm, text="Scan emulators", command=self.scan_emus).grid(row=5, column=1, pady=10)

        # Список чекбоксов
        list_frame = tk.Frame(self)
        list_frame.pack(fill='both', expand=True, padx=10, pady=(0,5))
        vsb = tk.Scrollbar(list_frame, orient="vertical")
        vsb.pack(side="left", fill="y")
        canvas = tk.Canvas(list_frame, borderwidth=1, relief='sunken', yscrollcommand=vsb.set)
        canvas.pack(side="left", fill="both", expand=True)
        vsb.config(command=canvas.yview)
        self.box_frame = tk.Frame(canvas)
        canvas.create_window((0,0), window=self.box_frame, anchor='nw')
        self.box_frame.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))

        # Кнопки управления
        ctrl = tk.Frame(self)
        ctrl.pack(fill='x', padx=10, pady=(0,5))
        tk.Button(ctrl, text="Select All",   command=lambda:self._toggle_all(True)).pack(side='left')
        tk.Button(ctrl, text="Deselect All", command=lambda:self._toggle_all(False)).pack(side='left')
        tk.Button(ctrl, text="Install",      command=self.start_install).pack(side='right')

        # Лог
        self.log = scrolledtext.ScrolledText(self, height=8)
        self.log.pack(fill='both', padx=10, pady=(0,10), expand=False)

    def browse_config(self):
        d = filedialog.askdirectory(title=r"Select LDPlayer vms\config folder")
        if d:
            self.config_dir.set(d)
            settings['config_dir'] = d
            save_settings()

    def browse_arch(self):
        f = filedialog.askopenfilename(title="Select arch APK", filetypes=[("APK files","*.apk")])
        if f:
            self.arch_file.set(f)
            settings['arch_file'] = f
            save_settings()

    def browse_main(self):
        f = filedialog.askopenfilename(title="Select main APK", filetypes=[("APK files","*.apk")])
        if f:
            self.main_file.set(f)
            settings['main_file'] = f
            save_settings()

    def browse_pack(self):
        f = filedialog.askopenfilename(title="Select pack APK", filetypes=[("APK files","*.apk")])
        if f:
            self.pack_file.set(f)
            settings['pack_file'] = f
            save_settings()

    def browse_ldconsole(self):
        f = filedialog.askopenfilename(title="Select ldconsole.exe", filetypes=[("Executable","*.exe")])
        if f:
            self.ldconsole.set(f)
            settings['ldconsole'] = f
            save_settings()

    def scan_emus(self):
        cfg = self.config_dir.get()
        if not os.path.isdir(cfg):
            messagebox.showerror("Error", "Invalid config directory")
            return
        names = []
        for fn in os.listdir(cfg):
            if not fn.endswith('.config'): continue
            path = os.path.join(cfg, fn)
            try:
                data = json.load(open(path, encoding='utf-8'))
                name = find_key_recursive(data, 'statusSettings.playerName')
                if name and name.lower() != 'ldplayer':
                    names.append(str(name))
            except:
                pass
        self.emu_names = list(dict.fromkeys(names))
        self._populate_checkboxes()

    def _populate_checkboxes(self):
        for w in self.box_frame.winfo_children():
            w.destroy()
        self.check_vars = []
        self.check_widgets = []
        for name in self.emu_names:
            var = tk.BooleanVar(value=False)
            chk = tk.Checkbutton(self.box_frame, text=name, variable=var, anchor='w')
            chk.pack(fill='x', padx=5, pady=2)
            self.check_vars.append((name, var))
            self.check_widgets.append((name, chk))

    def _toggle_all(self, val):
        for _, v in self.check_vars:
            v.set(val)

    def start_install(self):
        arch = self.arch_file.get()
        main = self.main_file.get()
        pack = self.pack_file.get()
        ld = self.ldconsole.get()
        if not (os.path.isfile(arch) and os.path.isfile(main) and os.path.isfile(pack) and os.path.isfile(ld)):
            messagebox.showerror("Error", "One or more APK files or ldconsole.exe not found")
            return
        selected = [n for n, v in self.check_vars if v.get()]
        if not selected:
            messagebox.showwarning("No targets", "No emulators selected")
            return

        threading.Thread(
            target=self._do_install,
            args=(selected, ld, arch, main, pack),
            daemon=True
        ).start()

    def _do_install(self, targets, ldpath, arch, main, pack):
        for name in targets:
            self._log(f"\n>>> Installing on: {name}")
            cmd = [
                ldpath,
                'adb',
                '--name', name,
                '--command', f"install-multiple {arch} {main} {pack}"
            ]
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            for line in proc.stdout:
                self._log(f"[{name}] {line.strip()}")
            proc.wait()
            if proc.returncode == 0:
                self._mark_installed(name)
                log_history(name)
        self._log("\n=== All done ===")

    def _log(self, text):
        self.log.insert('end', text + "\n")
        self.log.see('end')

    def _mark_installed(self, name):
        for nm, widget in self.check_widgets:
            if nm == name:
                widget.config(fg='green')
                break


if __name__ == '__main__':
    App().mainloop()
