import os
import re
import sys
import json
import shutil
import datetime
import subprocess
from pathlib import Path
import tkinter as tk
from tkinter import ttk, filedialog, messagebox

# ---- корзина (безопаснее)
try:
    from send2trash import send2trash
    USE_TRASH = True
except Exception:
    USE_TRASH = False

APP_TITLE = "Мини-утилита: пустые папки & конфиги LDPlayer"
CONFIG_FILE = Path.home() / ".clean_empty_dirs_config.json"

# ====================== Общие утилиты ======================

def load_settings() -> dict:
    if CONFIG_FILE.exists():
        try:
            return json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"backup_root": ""}

def save_settings(data: dict) -> None:
    try:
        CONFIG_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass

def is_dir_really_empty(p: Path) -> bool:
    """
    Папка считается пустой только если в ней нет НИЧЕГО (ни файлов, ни подпапок).
    Для симлинка к папке — проверяется целевая папка.
    """
    try:
        if not p.exists():
            return False
        # Path.is_dir() следует по симлинку — это то, что нам нужно
        if not p.is_dir():
            return False
        with os.scandir(p) as it:
            for _ in it:
                return False
        return True
    except PermissionError:
        return False
    except FileNotFoundError:
        # битый симлинк: не считаем пустым (по ТЗ удаляем только пустые реальные папки/ссылки на пустые папки)
        return False

def safe_remove_dir_empty(p: Path) -> bool:
    """
    Удаляет пустую папку или симлинк на папку (ТОЛЬКО если целевая папка пуста).
    """
    if not is_dir_really_empty(p):
        return False
    if USE_TRASH:
        send2trash(str(p))
    else:
        os.rmdir(p)  # для симлинка к папке на Windows это удалит ссылку
    return True

def safe_remove_file(p: Path) -> bool:
    """Удаляет файл (в корзину, если доступно)."""
    if not p.exists() or not p.is_file():
        return False
    if USE_TRASH:
        send2trash(str(p))
    else:
        p.unlink()
    return True

def open_in_explorer(path: Path) -> None:
    try:
        if sys.platform == "win32":
            os.startfile(str(path))  # type: ignore[attr-defined]
        elif sys.platform == "darwin":
            subprocess.Popen(["open", str(path)])
        else:
            subprocess.Popen(["xdg-open", str(path)])
    except Exception:
        pass

def ensure_unique_dir(base: Path) -> Path:
    if not base.exists():
        return base
    suffix = datetime.datetime.now().strftime("_%H%M%S")
    return base.with_name(base.name + suffix)

def safe_relpath(child: Path, base: Path) -> Path | None:
    try:
        return child.relative_to(base)
    except Exception:
        return None

# ======== Бэкап ========

def get_backup_subdir(root: Path, kind: str) -> Path:
    date_part = datetime.datetime.now().strftime("%d.%m")
    name = f"{'config' if kind=='config' else 'folders'}_bckp_{date_part}"
    dest = ensure_unique_dir(root / name)
    dest.mkdir(parents=True, exist_ok=True)
    return dest

def backup_files(files: list[tuple[Path, Path | None]], backup_root: Path) -> list[str]:
    errors = []
    dest_root = get_backup_subdir(backup_root, "config")
    for path, base in files:
        try:
            if not path.exists() or not path.is_file():
                continue
            if base:
                rel = safe_relpath(path, base)
                target = dest_root / (rel if rel else path.name)
            else:
                target = dest_root / path.name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, target)
        except Exception as e:
            errors.append(f"{path}: {e}")
    return errors

def backup_dirs_empty(dirs: list[tuple[Path, Path | None]], backup_root: Path) -> list[str]:
    errors = []
    dest_root = get_backup_subdir(backup_root, "folders")
    for d, base in dirs:
        try:
            if not d.exists() or not d.is_dir():
                continue
            if not is_dir_really_empty(d):
                continue
            if base:
                rel = safe_relpath(d, base)
                target = dest_root / (rel if rel else d.name)
            else:
                target = dest_root / d.name
            target.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            errors.append(f"{d}: {e}")
    return errors

# ====================== Обход с учётом симлинков ======================

def find_empty_dirs_following_symlinks(root: Path) -> list[Path]:
    """
    Пост-ордер обход (bottom-up) с переходом по симлинкам и защитой от циклов.
    Возвращает список реально пустых папок (включая симлинки на пустые папки).
    """
    empty: list[Path] = []
    stack: list[tuple[Path, bool]] = []
    seen_real: set[str] = set()

    if not root.exists() or not root.is_dir():
        return empty

    stack.append((root, False))
    while stack:
        node, visited = stack.pop()
        try:
            real = os.path.realpath(node)
        except Exception:
            real = str(node)

        if not visited:
            # защита от циклов (A -> B -> A через симлинки)
            if real in seen_real:
                # уже обошли эту реальную директорию
                continue
            seen_real.add(real)
            stack.append((node, True))
            try:
                with os.scandir(node) as it:
                    for entry in it:
                        # интересуют только директории/симлинки к директориям
                        try:
                            if entry.is_dir(follow_symlinks=True):
                                stack.append((Path(entry.path), False))
                        except PermissionError:
                            continue
            except (PermissionError, FileNotFoundError):
                continue
        else:
            # после детей — проверяем пустоту
            if is_dir_really_empty(node):
                empty.append(node)

    # дочерние раньше родительских
    empty.sort(key=lambda p: len(p.as_posix()), reverse=True)
    return empty

# ====================== Вкладка 1: Пустые папки ======================

def format_path_with_symlink_mark(p: Path, base: Path | None) -> str:
    rel = p
    if base and (base in p.parents or p == base):
        rel = p.relative_to(base)
    s = str(rel)
    if p.is_symlink():
        s += " [symlink]"
    return s

class TabEmptyDirs(ttk.Frame):
    def __init__(self, master, settings: dict):
        super().__init__(master, padding=10)
        self.settings = settings
        self.current_root: Path | None = None
        self.all_empty: list[Path] = []
        self.filtered_indices: list[int] = []

        # Верхняя панель
        top = ttk.Frame(self); top.pack(fill="x")
        self.root_label = ttk.Label(top, text="Корневая папка: не выбрана")
        self.root_label.pack(side="left", padx=(0,10))
        ttk.Button(top, text="Сканировать…", command=self.pick_and_scan).pack(side="right")

        # Поиск
        search_bar = ttk.Frame(self); search_bar.pack(fill="x", pady=(8,0))
        ttk.Label(search_bar, text="Фильтр:").pack(side="left")
        self.search_var = tk.StringVar()
        ent = ttk.Entry(search_bar, textvariable=self.search_var); ent.pack(side="left", fill="x", expand=True, padx=(6,0))
        ent.bind("<KeyRelease>", lambda e: self.apply_filter())

        # Список
        mid = ttk.Frame(self); mid.pack(fill="both", expand=True, pady=(8,0))
        self.listbox = tk.Listbox(mid, selectmode=tk.EXTENDED, activestyle="none", borderwidth=0, highlightthickness=1)
        self.listbox.pack(side="left", fill="both", expand=True)
        scroll = ttk.Scrollbar(mid, orient="vertical", command=self.listbox.yview); scroll.pack(side="right", fill="y")
        self.listbox.config(yscrollcommand=scroll.set)
        self.listbox.bind("<Double-Button-1>", self.open_selected)
        self.listbox.bind("<Control-a>", lambda e: (self.select_all(), "break"))
        self.listbox.bind("<Delete>", lambda e: (self.delete_selected(), "break"))
        self.listbox.bind("<<ListboxSelect>>", lambda e: self.update_selection_count())

        # Нижняя панель
        bottom = ttk.Frame(self); bottom.pack(fill="x", pady=(8,0))
        self.count_label = ttk.Label(bottom, text="Найдено пустых папок: 0 | Выбрано: 0")
        self.count_label.pack(side="left")
        actions = ttk.Frame(bottom); actions.pack(side="right")
        ttk.Button(actions, text="Выделить всё", command=self.select_all).pack(side="left", padx=(0,6))
        ttk.Button(actions, text="Обновить", command=self.rescan_current).pack(side="left", padx=(0,6))
        ttk.Button(actions, text="Удалить выбранные", command=self.delete_selected).pack(side="left")

        ttk.Label(self, padding=(0,6), foreground="#666",
                  text=("Учёт симлинков: если ссылка указывает на непустую папку — удаление запрещено; "
                        "в списке помечается как [symlink].")).pack(fill="x", anchor="w")

    def pick_and_scan(self):
        folder = filedialog.askdirectory(title="Выберите папку для сканирования")
        if not folder:
            return
        self.current_root = Path(folder)
        self.root_label.config(text=f"Корневая папка: {self.current_root}")
        self.scan()

    def scan(self):
        if not self.current_root or not self.current_root.exists():
            messagebox.showinfo("Сканирование", "Сначала выберите корректную папку.")
            return
        self.all_empty = find_empty_dirs_following_symlinks(self.current_root)
        self.apply_filter()
        self.update_selection_count()

    def apply_filter(self):
        query = self.search_var.get().strip().lower()
        self.listbox.delete(0, tk.END)
        self.filtered_indices = []
        for idx, p in enumerate(self.all_empty):
            s = format_path_with_symlink_mark(p, self.current_root)
            if query in s.lower():
                self.listbox.insert(tk.END, s)
                self.filtered_indices.append(idx)
        self.count_label.config(text=f"Найдено пустых папок: {len(self.filtered_indices)} | Выбрано: {len(self.listbox.curselection())}")

    def rescan_current(self):
        if not self.current_root:
            messagebox.showinfo("Обновление", "Сначала выберите папку.")
            return
        self.scan()

    def select_all(self):
        self.listbox.select_set(0, tk.END)
        self.update_selection_count()

    def update_selection_count(self):
        self.count_label.config(text=f"Найдено пустых папок: {len(self.filtered_indices)} | Выбрано: {len(self.listbox.curselection())}")

    def selected_paths(self) -> list[Path]:
        sel = self.listbox.curselection()
        return [self.all_empty[self.filtered_indices[i]] for i in sel]

    def open_selected(self, _event=None):
        for p in self.selected_paths():
            open_in_explorer(p)

    def ask_backup_then_delete_dirs(self, dirs: list[Path]) -> tuple[list[str], list[str], int]:
        use_trash_text = "в корзину" if USE_TRASH else "без корзины"
        if not messagebox.askyesno("Подтверждение", f"Удалить выбранные пустые папки ({use_trash_text})?\nПеред удалением каждая папка будет повторно проверена на пустоту (с учётом симлинков)."):
            return [], [], 0

        do_backup = messagebox.askyesno("Бэкап", "Сделать бэкап перед удалением?\nБудет создана пустая структура в 'folders_bckp_DD.MM'.")
        if do_backup:
            backup_root = self.settings.get("backup_root") or ""
            if not backup_root:
                if messagebox.askyesno("Путь бэкапа", "Путь для бэкапов не задан. Указать сейчас?"):
                    chosen = filedialog.askdirectory(title="Выберите папку для бэкапов")
                    if not chosen:
                        messagebox.showinfo("Бэкап", "Бэкап отменён, удаление прервано.")
                        return [], [], 0
                    self.settings["backup_root"] = chosen
                    save_settings(self.settings)
                else:
                    messagebox.showinfo("Бэкап", "Бэкап отменён, удаление прервано.")
                    return [], [], 0
            base = self.current_root if self.current_root else None
            b_errors = backup_dirs_empty([(d, base) for d in dirs], Path(self.settings["backup_root"]))
            if b_errors:
                messagebox.showwarning("Бэкап: ошибки", "\n".join(b_errors))

        deleted = 0
        skipped, errors = [], []
        for p in dirs:
            try:
                if safe_remove_dir_empty(p):
                    deleted += 1
                else:
                    # укажем пометку для симлинка
                    mark = " (symlink)" if p.is_symlink() else ""
                    skipped.append(f"{p}{mark}")
            except Exception as e:
                errors.append(f"{p}: {e}")
        return skipped, errors, deleted

    def delete_selected(self):
        dirs = self.selected_paths()
        if not dirs:
            messagebox.showinfo("Удаление", "Сначала выделите папки в списке.")
            return
        skipped, errors, deleted = self.ask_backup_then_delete_dirs(dirs)
        msg = [f"Удалено: {deleted}"]
        if skipped: msg.append(f"Пропущено (не пустые): {len(skipped)}")
        if errors: msg.append(f"Ошибки: {len(errors)}")
        messagebox.showinfo("Готово", "\n".join(msg))
        self.scan()
        details = []
        if skipped: details.append("Не удалены:\n" + "\n".join(skipped))
        if errors:  details.append("Ошибки:\n" + "\n".join(errors))
        if details:
            messagebox.showwarning("Подробности", "\n\n".join(details))

# ====================== Вкладка 2: LDPlayer ======================

LEIDIAN_CONFIG_RE = re.compile(r"^leidian(\d+)\.config$", re.IGNORECASE)

class TabLdplayer(ttk.Frame):
    def __init__(self, master, settings: dict):
        super().__init__(master, padding=10)
        self.settings = settings
        self.configs_dir: Path | None = None
        self.emus_dir: Path | None = None
        self.rows: list[dict] = []  # {path, kind: 'file'|'dir', reason, base: Path|None, is_symlink: bool}

        # Папки
        row1 = ttk.Frame(self); row1.pack(fill="x")
        self.lbl_configs = ttk.Label(row1, text="Папка конфигов: не выбрана")
        self.lbl_configs.pack(side="left", padx=(0,10))
        ttk.Button(row1, text="Выбрать…", command=self.pick_configs).pack(side="right")

        row2 = ttk.Frame(self); row2.pack(fill="x", pady=(8,0))
        self.lbl_emus = ttk.Label(row2, text="Папка эмуляторов: не выбрана")
        self.lbl_emus.pack(side="left", padx=(0,10))
        ttk.Button(row2, text="Выбрать…", command=self.pick_emus).pack(side="right")

        # Поиск + Скан
        top_actions = ttk.Frame(self); top_actions.pack(fill="x", pady=(8,0))
        ttk.Label(top_actions, text="Фильтр:").pack(side="left")
        self.search_var = tk.StringVar()
        ent = ttk.Entry(top_actions, textvariable=self.search_var); ent.pack(side="left", fill="x", expand=True, padx=(6,0))
        ent.bind("<KeyRelease>", lambda e: self.apply_filter())
        ttk.Button(top_actions, text="Сканировать", command=self.scan).pack(side="right")

        # Таблица
        table = ttk.Frame(self); table.pack(fill="both", expand=True, pady=(8,0))
        self.tree = ttk.Treeview(table, columns=("path","type","reason"), show="headings", selectmode="extended")
        self.tree.heading("path", text="Путь")
        self.tree.heading("type", text="Тип")
        self.tree.heading("reason", text="Пометка")
        self.tree.column("path", width=520, anchor="w")
        self.tree.column("type", width=120, anchor="w")
        self.tree.column("reason", width=200, anchor="w")
        self.tree.pack(side="left", fill="both", expand=True)
        scroll = ttk.Scrollbar(table, orient="vertical", command=self.tree.yview); scroll.pack(side="right", fill="y")
        self.tree.configure(yscrollcommand=scroll.set)
        self.tree.bind("<Double-Button-1>", self.open_selected)
        self.tree.bind("<Control-a>", lambda e: (self.select_all(), "break"))
        self.tree.bind("<Delete>", lambda e: (self.delete_selected(), "break"))
        self.tree.bind("<<TreeviewSelect>>", lambda e: self.update_selection_count())

        # Нижняя панель
        bottom = ttk.Frame(self); bottom.pack(fill="x", pady=(8,0))
        self.count_label = ttk.Label(bottom, text="Найдено проблем: 0 | Выбрано: 0")
        self.count_label.pack(side="left")
        actions = ttk.Frame(bottom); actions.pack(side="right")
        ttk.Button(actions, text="Выделить всё", command=self.select_all).pack(side="left", padx=(0,6))
        ttk.Button(actions, text="Обновить", command=self.scan).pack(side="left", padx=(0,6))
        ttk.Button(actions, text="Удалить выбранные", command=self.delete_selected).pack(side="left")

        ttk.Label(self, padding=(0,6), foreground="#666",
                  text=("Лишний конфиг — когда нет папки leidianN. "
                        "Пустая папка — leidianN существует, но пуста. "
                        "Симлинки помечаются в столбце «Тип».")).pack(fill="x", anchor="w")

        self.filtered_iids: list[str] = []

    def pick_configs(self):
        folder = filedialog.askdirectory(title="Выберите папку с конфигами (leidianN.config)")
        if not folder:
            return
        self.configs_dir = Path(folder)
        self.lbl_configs.config(text=f"Папка конфигов: {self.configs_dir}")

    def pick_emus(self):
        folder = filedialog.askdirectory(title="Выберите папку с папками эмуляторов (leidianN)")
        if not folder:
            return
        self.emus_dir = Path(folder)
        self.lbl_emus.config(text=f"Папка эмуляторов: {self.emus_dir}")

    def scan(self):
        if not self.configs_dir or not self.configs_dir.exists():
            messagebox.showinfo("Сканирование", "Выберите корректную папку конфигов.")
            return
        if not self.emus_dir or not self.emus_dir.exists():
            messagebox.showinfo("Сканирование", "Выберите корректную папку эмуляторов.")
            return

        self.tree.delete(*self.tree.get_children())
        self.rows.clear()

        # Собираем конфиги
        cfgs: dict[str, Path] = {}
        try:
            for entry in os.scandir(self.configs_dir):
                if entry.is_file() and LEIDIAN_CONFIG_RE.match(entry.name):
                    stem = entry.name[:-7]  # без ".config"
                    cfgs[stem.lower()] = Path(entry.path)
        except PermissionError:
            pass

        # Проверяем соответствующие папки (учитываем симлинки)
        for stem_lower, cfg_path in sorted(cfgs.items()):
            emu_path = (self.emus_dir / stem_lower)
            has_dir = emu_path.exists() and (emu_path.is_dir() or emu_path.is_symlink())
            if not has_dir:
                self._add_row(cfg_path, "file", "лишний конфиг", self.configs_dir, False)
            else:
                # если есть папка/симлинк — проверим пустоту по целевой директории
                if is_dir_really_empty(emu_path):
                    self._add_row(cfg_path, "file", "лишний конфиг", self.configs_dir, False)
                    self._add_row(emu_path, "dir", "пустая папка", self.emus_dir, emu_path.is_symlink())

        self.apply_filter()

    def _add_row(self, path: Path, kind: str, reason: str, base_for_rel: Path | None, is_symlink: bool):
        self.rows.append({"path": path, "kind": kind, "reason": reason, "base": base_for_rel, "is_symlink": is_symlink})

    def apply_filter(self):
        q = self.search_var.get().strip().lower()
        self.tree.delete(*self.tree.get_children())
        self.filtered_iids.clear()
        for row in self.rows:
            s = str(row["path"])
            if q in s.lower() or q in row["reason"]:
                type_text = ("файл" if row["kind"]=="file" else "папка") + (" (symlink)" if row.get("is_symlink") else "")
                iid = self.tree.insert("", "end", values=(s, type_text, row["reason"]))
                self.filtered_iids.append(iid)
        self.update_selection_count()

    def update_selection_count(self):
        sel = set(self.tree.selection())
        visible_sel = len([i for i in self.filtered_iids if i in sel])
        self.count_label.config(text=f"Найдено проблем: {len(self.filtered_iids)} | Выбрано: {visible_sel}")

    def select_all(self):
        for iid in self.filtered_iids:
            self.tree.selection_add(iid)
        self.update_selection_count()

    def selected_rows(self) -> list[dict]:
        iids = self.tree.selection()
        selected_paths = {self.tree.set(iid, "path") for iid in iids}
        return [r for r in self.rows if str(r["path"]) in selected_paths]

    def open_selected(self, _event=None):
        for r in self.selected_rows():
            open_in_explorer(Path(r["path"]))

    def ask_backup_then_delete_items(self, rows: list[dict]) -> tuple[list[str], list[str], int]:
        use_trash_text = "в корзину" if USE_TRASH else "без корзины"
        if not messagebox.askyesno("Подтверждение", f"Удалить выбранные элементы ({use_trash_text})?\nПапки/симлинки удаляются только если целевая папка пуста."):
            return [], [], 0

        do_backup = messagebox.askyesno("Бэкап", "Сделать бэкап перед удалением?\nФайлы → 'config_bckp_DD.MM', папки → 'folders_bckp_DD.MM'.")
        if do_backup:
            backup_root = self.settings.get("backup_root") or ""
            if not backup_root:
                if messagebox.askyesno("Путь бэкапа", "Путь для бэкапов не задан. Указать сейчас?"):
                    chosen = filedialog.askdirectory(title="Выберите папку для бэкапов")
                    if not chosen:
                        messagebox.showinfo("Бэкап", "Бэкап отменён, удаление прервано.")
                        return [], [], 0
                    self.settings["backup_root"] = chosen
                    save_settings(self.settings)
                else:
                    messagebox.showinfo("Бэкап", "Бэкап отменён, удаление прервано.")
                    return [], [], 0

            files = [(r["path"], r["base"]) for r in rows if r["kind"] == "file"]
            dirs  = [(r["path"], r["base"]) for r in rows if r["kind"] == "dir"]
            b_err1 = backup_files(files, Path(self.settings["backup_root"])) if files else []
            b_err2 = backup_dirs_empty(dirs, Path(self.settings["backup_root"])) if dirs else []
            b_errors = b_err1 + b_err2
            if b_errors:
                messagebox.showwarning("Бэкап: ошибки", "\n".join(b_errors))

        deleted = 0
        skipped, errors = [], []

        for r in rows:
            p = Path(r["path"])
            try:
                if r["kind"] == "file":
                    if safe_remove_file(p):
                        deleted += 1
                    else:
                        skipped.append(f"{p} (файл не найден)")
                else:
                    if safe_remove_dir_empty(p):
                        deleted += 1
                    else:
                        mark = " (symlink)" if p.is_symlink() else ""
                        skipped.append(f"{p}{mark} (папка не пустая)")
            except Exception as e:
                errors.append(f"{p}: {e}")

        return skipped, errors, deleted

    def delete_selected(self):
        rows = self.selected_rows()
        if not rows:
            messagebox.showinfo("Удаление", "Сначала выделите элементы в списке.")
            return
        skipped, errors, deleted = self.ask_backup_then_delete_items(rows)
        msg = [f"Удалено: {deleted}"]
        if skipped: msg.append(f"Пропущено: {len(skipped)}")
        if errors:  msg.append(f"Ошибки: {len(errors)}")
        messagebox.showinfo("Готово", "\n".join(msg))
        self.scan()
        details = []
        if skipped: details.append("Не удалены:\n" + "\n".join(skipped))
        if errors:  details.append("Ошибки:\n" + "\n".join(errors))
        if details:
            messagebox.showwarning("Подробности", "\n\n".join(details))

# ====================== Окно Настроек ======================

class SettingsDialog(tk.Toplevel):
    def __init__(self, master, settings: dict):
        super().__init__(master)
        self.settings = settings
        self.title("Настройки")
        self.resizable(False, False)
        self.transient(master)
        self.grab_set()

        frm = ttk.Frame(self, padding=12); frm.pack(fill="both", expand=True)
        ttk.Label(frm, text="Папка для бэкапов:").grid(row=0, column=0, sticky="w")
        self.var_path = tk.StringVar(value=self.settings.get("backup_root",""))
        ent = ttk.Entry(frm, textvariable=self.var_path, width=48)
        ent.grid(row=1, column=0, sticky="we", pady=(4,0))
        ttk.Button(frm, text="Выбрать…", command=self.pick).grid(row=1, column=1, padx=(6,0))
        note = ttk.Label(frm, text="Внутри неё будут создаваться папки config_bckp_DD.MM / folders_bckp_DD.MM.", foreground="#666")
        note.grid(row=2, column=0, columnspan=2, sticky="w", pady=(8,0))

        btns = ttk.Frame(frm); btns.grid(row=3, column=0, columnspan=2, sticky="e", pady=(12,0))
        ttk.Button(btns, text="Отмена", command=self.destroy).pack(side="right")
        ttk.Button(btns, text="Сохранить", command=self.save).pack(side="right", padx=(0,6))

        for i in range(2):
            frm.grid_columnconfigure(i, weight=0)
        frm.grid_columnconfigure(0, weight=1)

    def pick(self):
        folder = filedialog.askdirectory(title="Выберите папку для бэкапов")
        if folder:
            self.var_path.set(folder)

    def save(self):
        self.settings["backup_root"] = self.var_path.get().strip()
        save_settings(self.settings)
        self.destroy()

# ====================== Приложение ======================

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(APP_TITLE)
        self.geometry("900x620")
        self.minsize(720, 500)
        self.configure(bg="#FFFFFF")

        style = ttk.Style(self)
        try:
            if sys.platform == "win32":
                style.theme_use("vista")
            else:
                style.theme_use("clam")
        except Exception:
            pass

        self.settings = load_settings()

        header = ttk.Frame(self, padding=(10,8)); header.pack(fill="x")
        ttk.Label(header, text=APP_TITLE).pack(side="left")
        ttk.Button(header, text="Настройки", command=self.open_settings).pack(side="right")

        nb = ttk.Notebook(self); nb.pack(fill="both", expand=True)

        self.tab1 = TabEmptyDirs(nb, self.settings)
        self.tab2 = TabLdplayer(nb, self.settings)

        nb.add(self.tab1, text="Пустые папки")
        nb.add(self.tab2, text="LDPlayer: конфиги ↔ папки")

        hint = ttk.Label(self, padding=10, foreground="#666",
                         text=("Подсказки: двойной клик — открыть; Ctrl+A — выделить всё; Delete — удаление. "
                               "Симлинки помечаются и проверяются по целевой папке. "
                               f"{'Удаление в корзину активно.' if USE_TRASH else 'Удаление без корзины (send2trash не найден).'}"))
        hint.pack(fill="x", anchor="w")

    def open_settings(self):
        SettingsDialog(self, self.settings)

if __name__ == "__main__":
    app = App()
    app.mainloop()
