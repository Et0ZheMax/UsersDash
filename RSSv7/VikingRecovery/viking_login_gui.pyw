"""Графический интерфейс повторной авторизации Viking Rise."""

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

from viking_login import (
    LoginEngine,
    LoginTarget,
    build_login_targets,
    configure_login_logging,
)
from viking_recovery import CancelledError, RecoveryError, SingleRunLock, load_farms, resolve_profile_path


class QueueHandler(logging.Handler):
    """Передавать безопасные записи журнала в поток Tkinter."""

    def __init__(self, output: queue.Queue[tuple[str, object]]):
        super().__init__(logging.INFO)
        self.output = output

    def emit(self, record: logging.LogRecord) -> None:
        clean_record = copy.copy(record)
        clean_record.exc_info = None
        clean_record.exc_text = None
        clean_record.stack_info = None
        self.output.put(("log", self.format(clean_record)))


class LoginApp(tk.Tk):
    """Окно выбора существующего LDPlayer и повторного входа."""

    def __init__(self) -> None:
        super().__init__()
        self.title("VikingLogin — повторная авторизация")
        self.geometry("860x620")
        self.minsize(760, 520)
        self.events: queue.Queue[tuple[str, object]] = queue.Queue()
        self.cancel_event = threading.Event()
        self.worker: threading.Thread | None = None
        self.targets: dict[str, LoginTarget] = {}
        self.logger = configure_login_logging()
        handler = QueueHandler(self.events)
        handler.setFormatter(logging.Formatter("%(asctime)s  %(message)s", "%H:%M:%S"))
        self.logger.addHandler(handler)
        self._build_ui()
        self.reload_targets()
        self.after(150, self._drain_events)

    def _build_ui(self) -> None:
        header = ttk.Frame(self, padding=12)
        header.pack(fill=tk.X)
        ttk.Label(header, text="VikingLogin", font=("Segoe UI", 16, "bold")).pack(anchor=tk.W)
        ttk.Label(
            header,
            text=(
                "Повторный вход в существующий LDPlayer. Эмуляторы не клонируются, "
                "не удаляются и не переименовываются."
            ),
        ).pack(anchor=tk.W, pady=(4, 0))

        controls = ttk.Frame(self, padding=(12, 0, 12, 8))
        controls.pack(fill=tk.X)
        ttk.Label(controls, text="Поиск:").pack(side=tk.LEFT)
        self.search_var = tk.StringVar()
        search = ttk.Entry(controls, textvariable=self.search_var, width=32)
        search.pack(side=tk.LEFT, padx=(6, 10))
        self.search_var.trace_add("write", lambda *_: self._fill_tree())
        ttk.Button(controls, text="Обновить список", command=self.reload_targets).pack(side=tk.LEFT)

        columns = ("instance", "index", "farm", "custom", "state")
        self.tree = ttk.Treeview(self, columns=columns, show="headings", height=14, selectmode="browse")
        self.tree.heading("instance", text="Эмулятор LDPlayer")
        self.tree.heading("index", text="ID")
        self.tree.heading("farm", text="Ферма GnBots")
        self.tree.heading("custom", text="IGG ID / Custom")
        self.tree.heading("state", text="Готовность")
        self.tree.column("instance", width=190)
        self.tree.column("index", width=55, anchor=tk.CENTER)
        self.tree.column("farm", width=160)
        self.tree.column("custom", width=160)
        self.tree.column("state", width=260)
        self.tree.pack(fill=tk.BOTH, expand=True, padx=12)
        self.tree.bind("<Double-1>", lambda _event: self.start())

        actions = ttk.Frame(self, padding=12)
        actions.pack(fill=tk.X)
        self.start_button = ttk.Button(actions, text="Авторизовать выбранный эмулятор", command=self.start)
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

    def reload_targets(self) -> None:
        try:
            engine = LoginEngine(self.logger)
            targets = build_login_targets(engine.list_instances(), load_farms())
        except RecoveryError as exc:
            messagebox.showerror("Ошибка загрузки", str(exc), parent=self)
            return
        self.targets = {self._target_key(target): target for target in targets}
        self._fill_tree()
        profile = resolve_profile_path()
        ready = sum(target.ready for target in targets)
        self.status_var.set(f"Профиль {profile.name}; эмуляторов готово к входу: {ready}/{len(targets)}")

    def _fill_tree(self) -> None:
        selected = self.tree.selection()
        selected_key = selected[0] if selected else None
        self.tree.delete(*self.tree.get_children())
        needle = self.search_var.get().strip().casefold()
        for target in self.targets.values():
            farm_name = target.farm.name if target.farm else "—"
            custom = target.farm.custom if target.farm and target.farm.custom else "—"
            haystack = f"{target.instance.name} {target.instance.index} {farm_name} {custom}".casefold()
            if needle and needle not in haystack:
                continue
            item = self.tree.insert(
                "",
                tk.END,
                iid=self._target_key(target),
                values=(
                    target.instance.name,
                    target.instance.index,
                    farm_name,
                    custom,
                    "Да" if target.ready else target.issue,
                ),
            )
            if self._target_key(target) == selected_key:
                self.tree.selection_set(item)

    @staticmethod
    def _target_key(target: LoginTarget) -> str:
        farm_key = (target.farm.record_id or target.farm.name) if target.farm else "unassigned"
        return f"{target.instance.index}:{farm_key}"

    def _selected_target(self) -> LoginTarget | None:
        selected = self.tree.selection()
        if not selected:
            messagebox.showwarning("Эмулятор не выбран", "Выберите эмулятор из списка.", parent=self)
            return None
        return self.targets.get(selected[0])

    def start(self) -> None:
        if self.worker and self.worker.is_alive():
            return
        target = self._selected_target()
        if not target:
            return
        if not target.ready or target.farm is None:
            messagebox.showerror("Вход невозможен", target.issue, parent=self)
            return
        text = (
            f"Повторно авторизовать ферму {target.farm.name} в эмуляторе "
            f"{target.instance.name} (ID {target.instance.index})?\n\n"
            "Скрипт только запустит существующий LDPlayer и выполнит вход через IGG. "
            "Файлы, имена и профиль GnBots изменяться не будут."
        )
        if not messagebox.askyesno("Подтверждение", text, parent=self):
            return
        self.cancel_event.clear()
        self.status_label.configure(background="SystemButtonFace", foreground="SystemWindowText")
        self.status_var.set(f"Запуск повторного входа {target.farm.name}")
        self.start_button.configure(state=tk.DISABLED)
        self.cancel_button.configure(state=tk.NORMAL)
        self.progress.start(12)
        self.worker = threading.Thread(target=self._worker, args=(target,), daemon=True)
        self.worker.start()

    def cancel(self) -> None:
        self.cancel_event.set()
        self.status_var.set("Запрошена отмена; ожидается безопасная точка остановки")
        self.cancel_button.configure(state=tk.DISABLED)

    def _worker(self, target: LoginTarget) -> None:
        try:
            if target.farm is None:
                raise RecoveryError("Для выбранного эмулятора не найдена ферма")
            lock_path = Path(tempfile.gettempdir()) / "VikingRecovery" / "run.lock"
            with SingleRunLock(lock_path):
                engine = LoginEngine(
                    self.logger,
                    status=lambda message: self.events.put(("status", message)),
                    cancel_event=self.cancel_event,
                )
                instance = engine.login(target.farm, target.instance.index)
            self.events.put(
                (
                    "success",
                    f"Ферма {target.farm.name} авторизована в {instance.name}, LDPlayer ID {instance.index}.",
                )
            )
        except CancelledError as exc:
            self.events.put(("cancelled", str(exc)))
        except Exception as exc:
            self.logger.exception("Ошибка повторной авторизации")
            self.events.put(("error", str(exc)))
        finally:
            self.events.put(("finished", None))

    def _append_log(self, message: str) -> None:
        self.log.configure(state=tk.NORMAL)
        self.log.insert(tk.END, message + "\n")
        self.log.see(tk.END)
        self.log.configure(state=tk.DISABLED)

    def _show_success(self, message: str) -> None:
        self.status_var.set(f"✓ ГОТОВО — {message}")
        self.status_label.configure(background="#188038", foreground="white")
        self._append_log("✓ ПОВТОРНАЯ АВТОРИЗАЦИЯ УСПЕШНО ЗАВЕРШЕНА")
        try:
            winsound.MessageBeep(winsound.MB_ICONASTERISK)
        except RuntimeError:
            self.bell()
        self.deiconify()
        self.lift()
        self.attributes("-topmost", True)
        self.focus_force()
        try:
            messagebox.showinfo("Авторизация успешно завершена", message, parent=self)
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
                    self.status_var.set("Повторная авторизация завершилась ошибкой")
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
    LoginApp().mainloop()
