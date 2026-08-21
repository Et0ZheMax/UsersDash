"""Графический интерфейс восстановления фермы Viking Rise."""

from __future__ import annotations

import copy
import logging
import queue
import tempfile
import threading
import tkinter as tk
import winsound
from pathlib import Path
from tkinter import messagebox, ttk

from viking_recovery import (
    CancelledError,
    Farm,
    RecoveryEngine,
    RecoveryError,
    SingleRunLock,
    configure_logging,
    load_farms,
    resolve_profile_path,
)


class QueueHandler(logging.Handler):
    """Передаёт записи logging в безопасную для Tkinter очередь."""

    def __init__(self, output: queue.Queue[tuple[str, str]]):
        super().__init__(logging.INFO)
        self.output = output

    def emit(self, record: logging.LogRecord) -> None:
        clean_record = copy.copy(record)
        clean_record.exc_info = None
        clean_record.exc_text = None
        clean_record.stack_info = None
        self.output.put(("log", self.format(clean_record)))


class RecoveryApp(tk.Tk):
    """Окно выбора фермы и наблюдения за восстановлением."""

    def __init__(self) -> None:
        super().__init__()
        self.title("Viking Rise — восстановление фермы")
        self.geometry("780x620")
        self.minsize(700, 520)
        self.events: queue.Queue[tuple[str, object]] = queue.Queue()
        self.cancel_event = threading.Event()
        self.worker: threading.Thread | None = None
        self.farms: dict[str, Farm] = {}
        self.logger = configure_logging()
        handler = QueueHandler(self.events)
        handler.setFormatter(logging.Formatter("%(asctime)s  %(message)s", "%H:%M:%S"))
        self.logger.addHandler(handler)
        self._build_ui()
        self.reload_farms()
        self.after(150, self._drain_events)

    def _build_ui(self) -> None:
        header = ttk.Frame(self, padding=12)
        header.pack(fill=tk.X)
        ttk.Label(header, text="Восстановление Viking Rise", font=("Segoe UI", 16, "bold")).pack(anchor=tk.W)
        ttk.Label(
            header,
            text="Выберите ферму из активного профиля GnBots. Старый эмулятор будет сохранён с суффиксом _OLD.",
        ).pack(anchor=tk.W, pady=(4, 0))

        controls = ttk.Frame(self, padding=(12, 0, 12, 8))
        controls.pack(fill=tk.X)
        ttk.Label(controls, text="Поиск:").pack(side=tk.LEFT)
        self.search_var = tk.StringVar()
        search = ttk.Entry(controls, textvariable=self.search_var, width=32)
        search.pack(side=tk.LEFT, padx=(6, 10))
        self.search_var.trace_add("write", lambda *_: self._fill_tree())
        ttk.Button(controls, text="Обновить профиль", command=self.reload_farms).pack(side=tk.LEFT)

        columns = ("name", "custom", "active", "ready")
        self.tree = ttk.Treeview(self, columns=columns, show="headings", height=13, selectmode="browse")
        self.tree.heading("name", text="Ферма")
        self.tree.heading("custom", text="IGG ID / Custom")
        self.tree.heading("active", text="Активна")
        self.tree.heading("ready", text="Данные готовы")
        self.tree.column("name", width=220)
        self.tree.column("custom", width=220)
        self.tree.column("active", width=90, anchor=tk.CENTER)
        self.tree.column("ready", width=120, anchor=tk.CENTER)
        self.tree.pack(fill=tk.BOTH, expand=True, padx=12)

        actions = ttk.Frame(self, padding=12)
        actions.pack(fill=tk.X)
        self.start_button = ttk.Button(actions, text="Восстановить выбранную ферму", command=self.start)
        self.start_button.pack(side=tk.LEFT)
        self.cancel_button = ttk.Button(actions, text="Отменить", command=self.cancel, state=tk.DISABLED)
        self.cancel_button.pack(side=tk.LEFT, padx=8)
        self.progress = ttk.Progressbar(actions, mode="indeterminate")
        self.progress.pack(side=tk.RIGHT, fill=tk.X, expand=True, padx=(20, 0))

        self.status_var = tk.StringVar(value="Готово к работе")
        self.status_label = tk.Label(
            self,
            textvariable=self.status_var,
            anchor=tk.W,
            padx=12,
            pady=6,
            font=("Segoe UI", 10, "bold"),
        )
        self.status_label.pack(fill=tk.X)
        self.log = tk.Text(self, height=10, wrap=tk.WORD, state=tk.DISABLED, font=("Consolas", 9))
        self.log.pack(fill=tk.BOTH, expand=False, padx=12, pady=(0, 12))

    def reload_farms(self) -> None:
        try:
            farms = load_farms()
        except RecoveryError as exc:
            messagebox.showerror("Ошибка профиля", str(exc), parent=self)
            return
        self.farms = {farm.name: farm for farm in farms}
        self._fill_tree()
        profile = resolve_profile_path()
        self.status_var.set(f"Профиль {profile.name}; загружено ферм: {len(farms)}")

    def _fill_tree(self) -> None:
        selected = self.tree.selection()
        selected_name = self.tree.item(selected[0], "values")[0] if selected else None
        self.tree.delete(*self.tree.get_children())
        needle = self.search_var.get().strip().casefold()
        for farm in self.farms.values():
            if needle and needle not in farm.name.casefold() and needle not in farm.custom.casefold():
                continue
            item = self.tree.insert(
                "",
                tk.END,
                values=(
                    farm.name,
                    farm.custom or "—",
                    "Да" if farm.active else "Нет",
                    "Да" if farm.ready else "Нет",
                ),
            )
            if farm.name == selected_name:
                self.tree.selection_set(item)

    def _selected_farm(self) -> Farm | None:
        selected = self.tree.selection()
        if not selected:
            messagebox.showwarning("Ферма не выбрана", "Выберите одну ферму из списка.", parent=self)
            return None
        name = str(self.tree.item(selected[0], "values")[0])
        return self.farms.get(name)

    def start(self) -> None:
        if self.worker and self.worker.is_alive():
            return
        farm = self._selected_farm()
        if not farm:
            return
        if not farm.ready:
            messagebox.showerror(
                "Нет данных для входа",
                "У фермы должны быть заполнены Email, Password и Slot=igg.",
                parent=self,
            )
            return
        text = (
            f"Восстановить ферму {farm.name}?\n\n"
            "Будет создан клон чистого образа ID 0 на диске C. "
            f"Если {farm.name} уже существует, старые экземпляры будут сохранены как {farm.name}_OLD."
        )
        if not messagebox.askyesno("Подтверждение", text, parent=self):
            return
        self.cancel_event.clear()
        self.status_label.configure(background="SystemButtonFace", foreground="SystemWindowText")
        self.status_var.set(f"Запуск восстановления {farm.name}")
        self.start_button.configure(state=tk.DISABLED)
        self.cancel_button.configure(state=tk.NORMAL)
        self.progress.start(12)
        self.worker = threading.Thread(target=self._worker, args=(farm,), daemon=True)
        self.worker.start()

    def cancel(self) -> None:
        self.cancel_event.set()
        self.status_var.set("Запрошена отмена; ожидается безопасная точка остановки")
        self.cancel_button.configure(state=tk.DISABLED)

    def _worker(self, farm: Farm) -> None:
        try:
            lock_path = Path(tempfile.gettempdir()) / "VikingRecovery" / "run.lock"
            with SingleRunLock(lock_path):
                engine = RecoveryEngine(
                    self.logger,
                    status=lambda message: self.events.put(("status", message)),
                    cancel_event=self.cancel_event,
                )
                backup, index = engine.recover(farm)
            result = f"Ферма {farm.name} восстановлена. Новый LDPlayer ID {index}."
            if backup:
                result += f" Старый эмулятор сохранён как {backup}."
            self.events.put(("success", result))
        except CancelledError as exc:
            self.events.put(("cancelled", str(exc)))
        except Exception as exc:
            self.logger.exception("Ошибка восстановления")
            self.events.put(("error", str(exc)))
        finally:
            self.events.put(("finished", None))

    def _append_log(self, message: str) -> None:
        self.log.configure(state=tk.NORMAL)
        self.log.insert(tk.END, message + "\n")
        self.log.see(tk.END)
        self.log.configure(state=tk.DISABLED)

    def _show_success(self, message: str) -> None:
        """Сделать успешное завершение заметным поверх окна LDPlayer."""

        self.status_var.set(f"✓ ГОТОВО — {message}")
        self.status_label.configure(background="#188038", foreground="white")
        self._append_log("✓ ВОССТАНОВЛЕНИЕ УСПЕШНО ЗАВЕРШЕНО")
        try:
            winsound.MessageBeep(winsound.MB_ICONASTERISK)
        except RuntimeError:
            self.bell()
        self.deiconify()
        self.lift()
        self.attributes("-topmost", True)
        self.focus_force()
        try:
            messagebox.showinfo("Восстановление успешно завершено", message, parent=self)
        finally:
            self.attributes("-topmost", False)

    def _drain_events(self) -> None:
        try:
            while True:
                kind, payload = self.events.get_nowait()
                if kind == "log":
                    self._append_log(str(payload))
                elif kind == "status":
                    self.status_var.set(str(payload))
                elif kind == "success":
                    self._show_success(str(payload))
                elif kind == "cancelled":
                    self.status_var.set(str(payload))
                    self.status_label.configure(background="#f9ab00", foreground="black")
                elif kind == "error":
                    self.status_var.set("Восстановление завершилось ошибкой")
                    self.status_label.configure(background="#d93025", foreground="white")
                    messagebox.showerror("Ошибка", str(payload), parent=self)
                elif kind == "finished":
                    self.progress.stop()
                    self.start_button.configure(state=tk.NORMAL)
                    self.cancel_button.configure(state=tk.DISABLED)
        except queue.Empty:
            pass
        self.after(150, self._drain_events)


if __name__ == "__main__":
    RecoveryApp().mainloop()
