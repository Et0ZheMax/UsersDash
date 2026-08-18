"""Graphical interface for LDPlayer VM Mover."""

from __future__ import annotations

import queue
import shutil
import threading
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, ttk

import ldplayer_mover as mover


class MoverGUI:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("LDPlayer VM Mover")
        self.root.geometry("980x690")
        self.root.minsize(820, 560)

        self.events: queue.Queue[tuple] = queue.Queue()
        self.vms: list[mover.VM] = []
        self.selected: set[str] = set()
        self.busy = False
        self.scanned_source: Path | None = None
        self.scanned_destination: Path | None = None

        self.source_var = tk.StringVar(value=str(mover.DEFAULT_SOURCE))
        self.destination_var = tk.StringVar(value=str(mover.DEFAULT_DESTINATION))
        self.summary_var = tk.StringVar(value="Сначала выполните сканирование")
        self.status_var = tk.StringVar(value="Готово")

        self._build()
        self.root.after(100, self._poll_events)
        self.root.after(250, self.scan)

    def _build(self) -> None:
        outer = ttk.Frame(self.root, padding=14)
        outer.pack(fill="both", expand=True)
        outer.columnconfigure(1, weight=1)
        outer.rowconfigure(3, weight=1)

        ttk.Label(outer, text="Папка LDPlayer:").grid(row=0, column=0, sticky="w", pady=4)
        self.source_entry = ttk.Entry(outer, textvariable=self.source_var)
        self.source_entry.grid(row=0, column=1, sticky="ew", padx=8, pady=4)
        self.source_browse = ttk.Button(
            outer, text="Обзор…", command=lambda: self._browse(self.source_var)
        )
        self.source_browse.grid(row=0, column=2, pady=4)

        ttk.Label(outer, text="Резервная папка:").grid(row=1, column=0, sticky="w", pady=4)
        self.destination_entry = ttk.Entry(outer, textvariable=self.destination_var)
        self.destination_entry.grid(row=1, column=1, sticky="ew", padx=8, pady=4)
        self.destination_browse = ttk.Button(
            outer, text="Обзор…", command=lambda: self._browse(self.destination_var)
        )
        self.destination_browse.grid(row=1, column=2, pady=4)

        toolbar = ttk.Frame(outer)
        toolbar.grid(row=2, column=0, columnspan=3, sticky="ew", pady=(8, 8))
        self.scan_button = ttk.Button(toolbar, text="Сканировать", command=self.scan)
        self.scan_button.pack(side="left")
        self.recommended_button = ttk.Button(
            toolbar, text="Выбрать рекомендуемые", command=self.select_recommended
        )
        self.recommended_button.pack(side="left", padx=(8, 0))
        self.all_button = ttk.Button(toolbar, text="Выбрать все", command=self.select_all)
        self.all_button.pack(side="left", padx=(8, 0))
        self.clear_button = ttk.Button(toolbar, text="Снять выбор", command=self.clear_selection)
        self.clear_button.pack(side="left", padx=(8, 0))

        table_frame = ttk.Frame(outer)
        table_frame.grid(row=3, column=0, columnspan=3, sticky="nsew")
        table_frame.columnconfigure(0, weight=1)
        table_frame.rowconfigure(0, weight=1)

        columns = ("pick", "name", "size", "state")
        self.tree = ttk.Treeview(table_frame, columns=columns, show="headings", selectmode="browse")
        self.tree.heading("pick", text="Перенос")
        self.tree.heading("name", text="Эмулятор")
        self.tree.heading("size", text="Освободится")
        self.tree.heading("state", text="Состояние")
        self.tree.column("pick", width=75, anchor="center", stretch=False)
        self.tree.column("name", width=150, anchor="w", stretch=False)
        self.tree.column("size", width=120, anchor="e", stretch=False)
        self.tree.column("state", width=480, anchor="w")
        self.tree.tag_configure("recommended", background="#e8f5e9")
        self.tree.tag_configure("unavailable", foreground="#777777")
        self.tree.grid(row=0, column=0, sticky="nsew")
        scrollbar = ttk.Scrollbar(table_frame, orient="vertical", command=self.tree.yview)
        scrollbar.grid(row=0, column=1, sticky="ns")
        self.tree.configure(yscrollcommand=scrollbar.set)
        self.tree.bind("<Double-1>", self._toggle_event)
        self.tree.bind("<space>", self._toggle_event)

        lower = ttk.Frame(outer)
        lower.grid(row=4, column=0, columnspan=3, sticky="ew", pady=(10, 0))
        lower.columnconfigure(0, weight=1)
        ttk.Label(lower, textvariable=self.summary_var).grid(row=0, column=0, sticky="w")
        self.move_button = ttk.Button(
            lower, text="Перенести выбранные", command=self.start_transfer
        )
        self.move_button.grid(row=0, column=1, rowspan=2, padx=(12, 0), sticky="ns")
        self.progress = ttk.Progressbar(lower, mode="determinate", maximum=100)
        self.progress.grid(row=1, column=0, sticky="ew", pady=(7, 0))

        ttk.Label(outer, textvariable=self.status_var).grid(
            row=5, column=0, columnspan=3, sticky="w", pady=(8, 3)
        )
        self.log = tk.Text(outer, height=7, wrap="word", state="disabled")
        self.log.grid(row=6, column=0, columnspan=3, sticky="ew")

    def _browse(self, variable: tk.StringVar) -> None:
        initial = Path(variable.get())
        initial_dir = str(initial if initial.exists() else Path(initial.anchor or "C:\\"))
        chosen = filedialog.askdirectory(initialdir=initial_dir)
        if chosen:
            variable.set(chosen)

    def _set_busy(self, value: bool) -> None:
        self.busy = value
        state = "disabled" if value else "normal"
        for widget in (
            self.scan_button,
            self.recommended_button,
            self.all_button,
            self.clear_button,
            self.move_button,
            self.source_entry,
            self.destination_entry,
            self.source_browse,
            self.destination_browse,
        ):
            widget.configure(state=state)

    def _log(self, text: str) -> None:
        self.log.configure(state="normal")
        self.log.insert("end", text.rstrip() + "\n")
        self.log.see("end")
        self.log.configure(state="disabled")

    def scan(self) -> None:
        if self.busy:
            return
        source = Path(self.source_var.get().strip()).absolute()
        destination = Path(self.destination_var.get().strip()).absolute()
        self._set_busy(True)
        self.status_var.set("Сканирование папок и расчёт размера…")
        self.progress.configure(mode="indeterminate")
        self.progress.start(12)

        def worker() -> None:
            try:
                mover.validate_roots(source, destination)
                mover.recover_journals(
                    source, destination, lambda text: self.events.put(("log", text))
                )
                found = mover.scan_vms(source, destination)
                self.events.put(("scan_done", found, source, destination))
            except Exception as exc:
                self.events.put(("error", "Ошибка сканирования", str(exc)))

        threading.Thread(target=worker, daemon=True).start()

    def _populate(self) -> None:
        self.tree.delete(*self.tree.get_children())
        for vm in self.vms:
            mark = "☑" if vm.name in self.selected else "☐" if vm.movable else "—"
            size = mover.human_size(vm.bytes_used) if not vm.is_link and not vm.scan_error else "—"
            tags = ("recommended",) if vm.recommended else ("unavailable",) if not vm.movable else ()
            self.tree.insert(
                "", "end", iid=vm.name, values=(mark, vm.name, size, mover.status_text(vm)), tags=tags
            )
        self._update_summary()

    def _toggle_event(self, event: tk.Event) -> str:
        if self.busy:
            return "break"
        item = self.tree.identify_row(event.y) if getattr(event, "y", 0) else self.tree.focus()
        if item:
            vm = next((entry for entry in self.vms if entry.name == item), None)
            if vm and vm.movable:
                if item in self.selected:
                    self.selected.remove(item)
                else:
                    self.selected.add(item)
                self._populate()
                self.tree.focus(item)
                self.tree.selection_set(item)
        return "break"

    def select_recommended(self) -> None:
        self.selected = {vm.name for vm in self.vms if vm.recommended}
        self._populate()

    def select_all(self) -> None:
        self.selected = {vm.name for vm in self.vms if vm.movable}
        self._populate()

    def clear_selection(self) -> None:
        self.selected.clear()
        self._populate()

    def _selected_vms(self) -> list[mover.VM]:
        return [vm for vm in self.vms if vm.name in self.selected and vm.movable]

    def _update_summary(self) -> None:
        chosen = self._selected_vms()
        total = sum(vm.bytes_used for vm in chosen)
        self.summary_var.set(
            f"Выбрано: {len(chosen)}   •   Освободится на C: примерно {mover.human_size(total)}"
        )

    def start_transfer(self) -> None:
        if self.busy:
            return
        chosen = self._selected_vms()
        if not chosen:
            messagebox.showinfo("LDPlayer VM Mover", "Выберите хотя бы один эмулятор.")
            return
        source = Path(self.source_var.get().strip()).absolute()
        destination = Path(self.destination_var.get().strip()).absolute()
        if source != self.scanned_source or destination != self.scanned_destination:
            messagebox.showinfo(
                "Нужно повторное сканирование",
                "Пути изменились после последнего сканирования. Нажмите «Сканировать» ещё раз.",
            )
            return
        running = mover.running_ldplayer_processes()
        if running:
            messagebox.showerror(
                "LDPlayer запущен",
                "Полностью закройте LDPlayer и менеджер мультиокон:\n\n" + ", ".join(running),
            )
            return
        total = sum(vm.bytes_used for vm in chosen)
        try:
            mover.validate_roots(source, destination)
            free = shutil.disk_usage(destination.anchor).free
        except Exception as exc:
            messagebox.showerror("Ошибка доступа к диску", str(exc))
            return
        reserve = max(2 * 1024**3, int(total * 0.05))
        if total + reserve > free:
            messagebox.showerror(
                "Недостаточно места",
                f"Свободно: {mover.human_size(free)}\n"
                f"Нужно с запасом: {mover.human_size(total + reserve)}",
            )
            return
        names = ", ".join(vm.name for vm in chosen)
        if not messagebox.askyesno(
            "Подтверждение переноса",
            f"Будут перенесены:\n{names}\n\n"
            f"На диске C освободится примерно {mover.human_size(total)}.\n\nПродолжить?",
        ):
            return

        self._set_busy(True)
        self.progress.configure(mode="determinate", value=0)
        self._log("Запуск переноса: " + names)

        def worker() -> None:
            results: list[tuple[str, bool, str | None]] = []
            fatal_error: str | None = None
            try:
                destination.mkdir(parents=True, exist_ok=True)
                for position, vm in enumerate(chosen, 1):
                    running_now = mover.running_ldplayer_processes()
                    if running_now:
                        raise mover.MoveError(
                            "LDPlayer запущен: " + ", ".join(running_now)
                        )

                    def on_progress(copied: int, size: int, name: str = vm.name) -> None:
                        self.events.put(("progress", name, position, len(chosen), copied, size))

                    def on_status(stage: str, name: str = vm.name) -> None:
                        self.events.put(("stage", name, position, len(chosen), stage))

                    try:
                        warning = mover.move_one(
                            vm,
                            destination,
                            "junction",
                            progress_callback=on_progress,
                            status_callback=on_status,
                        )
                        results.append((vm.name, True, warning))
                        self.events.put(("log", f"{vm.name}: перенос завершён."))
                        if warning:
                            self.events.put(("log", f"{vm.name}: ПРЕДУПРЕЖДЕНИЕ — {warning}"))
                    except Exception as exc:
                        results.append((vm.name, False, str(exc)))
                        self.events.put(("log", f"{vm.name}: ОШИБКА — {exc}"))
            except Exception as exc:
                fatal_error = str(exc)
                self.events.put(("log", f"Операция остановлена: {exc}"))
            self.events.put(("transfer_done", results, fatal_error))

        threading.Thread(target=worker, daemon=True).start()

    def _poll_events(self) -> None:
        try:
            while True:
                event = self.events.get_nowait()
                kind = event[0]
                if kind == "log":
                    self._log(event[1])
                elif kind == "scan_done":
                    self.vms = event[1]
                    self.scanned_source = event[2]
                    self.scanned_destination = event[3]
                    self.selected = {vm.name for vm in self.vms if vm.recommended}
                    self.progress.stop()
                    self.progress.configure(mode="determinate", value=0)
                    self._set_busy(False)
                    self._populate()
                    self.status_var.set(f"Найдено эмуляторов: {len(self.vms)}")
                elif kind == "progress":
                    _, name, position, count, copied, size = event
                    percent = 100 if size == 0 else min(100, copied * 100 / size)
                    self.progress.configure(value=percent)
                    self.status_var.set(
                        f"[{position}/{count}] {name}: копирование {percent:.1f}% — "
                        f"{mover.human_size(copied)} / {mover.human_size(size)}"
                    )
                elif kind == "stage":
                    _, name, position, count, stage = event
                    self.status_var.set(f"[{position}/{count}] {name}: {stage}")
                elif kind == "transfer_done":
                    results = event[1]
                    fatal_error = event[2]
                    self._set_busy(False)
                    successes = [name for name, ok, _ in results if ok]
                    failures = [(name, info) for name, ok, info in results if not ok]
                    if failures or fatal_error:
                        details = "\n".join(f"{name}: {info}" for name, info in failures)
                        if fatal_error:
                            details = (details + "\n" + fatal_error).strip()
                        messagebox.showwarning(
                            "Перенос завершён с ошибками",
                            f"Успешно: {len(successes)}\n\n{details}",
                        )
                    else:
                        messagebox.showinfo(
                            "Готово", f"Успешно перенесено: {len(successes)}."
                        )
                    self.scan()
                elif kind == "error":
                    self.progress.stop()
                    self.progress.configure(mode="determinate", value=0)
                    self._set_busy(False)
                    self.status_var.set(event[2])
                    messagebox.showerror(event[1], event[2])
        except queue.Empty:
            pass
        self.root.after(100, self._poll_events)


def main() -> None:
    root = tk.Tk()
    style = ttk.Style(root)
    if "vista" in style.theme_names():
        style.theme_use("vista")
    MoverGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()
