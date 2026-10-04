"""Единый интерфейс восстановления, входа и пакетного создания ферм Viking Rise."""

from __future__ import annotations

import logging
import copy
import queue
import tempfile
import threading
import tkinter as tk
import winsound
from pathlib import Path
from tkinter import messagebox, ttk

from viking_login import build_login_targets
from viking_recovery import CancelledError, RecoveryError, SingleRunLock, configure_logging, load_farms
from viking_tools import (
    Maintenance, NewFarm, Provisioner, TARIFFS, ToolsConfig, ToolsLoginEngine, ToolsRecoveryEngine,
)


class QueueHandler(logging.Handler):
    """Передавать сообщения журнала в поток интерфейса без traceback с реквизитами."""

    def __init__(self, output):
        super().__init__(logging.INFO)
        self.output = output

    def emit(self, record):
        clean = copy.copy(record)
        clean.exc_info = clean.exc_text = clean.stack_info = None
        self.output.put(('log', self.format(clean)))


class VikingToolsApp(tk.Tk):
    """Три режима в одном окне, общий журнал и последовательная очередь операций."""

    def __init__(self):
        super().__init__()
        self.title('Viking Tools — управление фермами')
        self.geometry('1000x760')
        self.minsize(860, 640)
        self.events = queue.Queue()
        self.cancel_event = threading.Event()
        self.worker = None
        self.rows = []
        self.resume_items = None
        self.logger = configure_logging(Path(__file__).with_name('logs') / 'viking_tools.log')
        handler = QueueHandler(self.events)
        handler.setFormatter(logging.Formatter('%(asctime)s  %(message)s', '%H:%M:%S'))
        self.logger.addHandler(handler)
        self.protocol('WM_DELETE_WINDOW', self.close)
        self.notebook = ttk.Notebook(self)
        self.notebook.pack(fill=tk.BOTH, expand=True, padx=12, pady=12)
        self.recovery_page = self.selection_page('Восстановление', 'Восстановить ферму', self.recover)
        self.login_page = self.selection_page('Повторный вход', 'Авторизовать эмулятор', self.login)
        self.build_creation_page()
        footer = ttk.Frame(self, padding=(12, 0, 12, 12))
        footer.pack(fill=tk.X)
        self.status = tk.StringVar(value='Готово к работе')
        self.status_label = tk.Label(footer, textvariable=self.status, anchor=tk.W, wraplength=950)
        self.status_label.pack(fill=tk.X)
        self.progress = ttk.Progressbar(footer, mode='indeterminate')
        self.progress.pack(side=tk.LEFT, fill=tk.X, expand=True, pady=8)
        self.cancel_button = ttk.Button(footer, text='Отменить', command=self.cancel, state=tk.DISABLED)
        self.cancel_button.pack(side=tk.RIGHT, padx=8)
        self.log = tk.Text(self, height=11, state=tk.DISABLED, font=('Consolas', 9), wrap=tk.WORD)
        self.log.pack(fill=tk.X, padx=12, pady=(0, 12))
        self.after(150, self.drain)
        self.after(300, self.reload)

    def selection_page(self, title, button, callback):
        frame = ttk.Frame(self.notebook, padding=12)
        self.notebook.add(frame, text=title)
        controls = ttk.Frame(frame)
        controls.pack(fill=tk.X, pady=(0, 8))
        ttk.Label(controls, text='Поиск:').pack(side=tk.LEFT)
        search = tk.StringVar()
        ttk.Entry(controls, textvariable=search, width=30).pack(side=tk.LEFT, padx=8)
        ttk.Button(controls, text='Обновить список', command=self.reload).pack(side=tk.LEFT)
        columns = ('farm', 'instance', 'id', 'igg', 'ready')
        tree = ttk.Treeview(frame, columns=columns, show='headings', selectmode='browse')
        for column, label, width in zip(columns, ['Ферма', 'Эмулятор', 'LDPlayer ID', 'IGG ID', 'Готовность'],
                                         [190, 190, 85, 160, 230]):
            tree.heading(column, text=label)
            tree.column(column, width=width)
        tree.pack(fill=tk.BOTH, expand=True)
        ttk.Button(frame, text=button, command=callback).pack(anchor=tk.W, pady=10)
        page = dict(frame=frame, tree=tree, search=search, records={})
        search.trace_add('write', lambda *_: self.fill(page))
        return page

    def reload(self):
        if self.worker and self.worker.is_alive():
            return
        try:
            engine = ToolsRecoveryEngine(self.logger)
            farms = load_farms(engine.profile_path)
            instances = engine.list_instances()
            by_index = {i.index: i for i in instances}
            self.recovery_page['records'] = {
                str(i): (farm, by_index.get(farm.instance_id), '' if farm.ready else 'Нет данных входа')
                for i, farm in enumerate(farms)
            }
            targets = build_login_targets(instances, farms)
            self.login_page['records'] = {str(i): (t.farm, t.instance, t.issue) for i, t in enumerate(targets)}
            self.fill(self.recovery_page)
            self.fill(self.login_page)
            self.status.set(f'Активный профиль: {engine.profile_path.name}')
        except Exception as exc:
            messagebox.showerror('Ошибка загрузки профиля', str(exc), parent=self)

    def fill(self, page):
        tree = page['tree']
        tree.delete(*tree.get_children())
        needle = page['search'].get().strip().casefold()
        for key, (farm, instance, issue) in page['records'].items():
            values = (farm.name if farm else '—', instance.name if instance else '—',
                      instance.index if instance else '—', farm.custom or 'Первый в списке' if farm else '—',
                      issue or 'Да')
            if needle and needle not in ' '.join(map(str, values)).casefold():
                continue
            tree.insert('', tk.END, iid=key, values=values)

    def selected(self, page):
        selection = page['tree'].selection()
        if not selection:
            raise RecoveryError('Выберите ферму из списка')
        farm, instance, issue = page['records'][selection[0]]
        if issue or farm is None:
            raise RecoveryError(issue or 'Не найдены данные фермы')
        return farm, instance

    def build_creation_page(self):
        frame = ttk.Frame(self.notebook, padding=12)
        self.notebook.add(frame, text='Создание новых ферм')
        ttk.Label(frame, text=(
            'Заполните аккаунты. Перед созданием бот и эмуляторы будут остановлены; '
            'после завершения работа бота возобновится.'
        ), wraplength=920).pack(anchor=tk.W, pady=(0, 10))
        canvas = tk.Canvas(frame, highlightthickness=0)
        scrollbar = ttk.Scrollbar(frame, orient=tk.VERTICAL, command=canvas.yview)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        canvas.pack(fill=tk.BOTH, expand=True)
        canvas.configure(yscrollcommand=scrollbar.set)
        self.accounts_frame = ttk.Frame(canvas)
        window = canvas.create_window((0, 0), window=self.accounts_frame, anchor=tk.NW)
        canvas.bind('<Configure>', lambda event: canvas.itemconfigure(window, width=event.width))
        self.accounts_frame.bind('<Configure>', lambda _event: canvas.configure(scrollregion=canvas.bbox('all')))
        controls = ttk.Frame(frame)
        controls.pack(fill=tk.X, pady=10)
        ttk.Button(controls, text='Добавить ещё один акк', command=self.add_row).pack(side=tk.LEFT)
        ttk.Button(controls, text='Создать фермы', command=self.create).pack(side=tk.LEFT, padx=8)
        ttk.Button(controls, text='Продолжить пакет', command=self.resume).pack(side=tk.LEFT)
        ttk.Button(frame, text='Восстановить работу бота', command=self.restore).pack(anchor=tk.W)
        ttk.Label(frame, text='IGG ID можно оставить пустым. Для сверки достаточно первых пяти цифр.').pack(
            anchor=tk.W, pady=(8, 0)
        )
        self.add_row()

    def add_row(self):
        if self.worker and self.worker.is_alive():
            return
        frame = ttk.LabelFrame(self.accounts_frame, text=f'Аккаунт {len(self.rows) + 1}', padding=8)
        frame.pack(fill=tk.X, pady=5)
        variables = {key: tk.StringVar() for key in ('name', 'email', 'password', 'igg_id')}
        labels = ['Имя фермы', 'Почта', 'Пароль', 'IGG ID (необязательно)']
        for index, (key, label) in enumerate(zip(variables, labels)):
            ttk.Label(frame, text=label).grid(row=0, column=index, sticky=tk.W, padx=4)
            ttk.Entry(frame, textvariable=variables[key], show='•' if key == 'password' else '', width=23).grid(
                row=1, column=index, sticky=tk.EW, padx=4
            )
            frame.columnconfigure(index, weight=1)
        tariff = tk.StringVar(value=TARIFFS[500][0])
        ttk.Label(frame, text='Тариф').grid(row=2, column=0, sticky=tk.W, pady=(8, 0))
        ttk.Combobox(frame, textvariable=tariff, values=[value[0] for value in TARIFFS.values()],
                     state='readonly', width=22).grid(row=3, column=0, sticky=tk.W)
        entry = dict(frame=frame, variables=variables, tariff=tariff)
        self.rows.append(entry)
        ttk.Button(frame, text='Убрать аккаунт', command=lambda: self.remove_row(entry)).grid(row=3, column=3)

    def remove_row(self, row):
        if not (self.worker and self.worker.is_alive()) and len(self.rows) > 1:
            self.rows.remove(row)
            row['frame'].destroy()

    def items_from_form(self):
        items = []
        for row in self.rows:
            values = {
                key: var.get().strip() if key != 'password' else var.get() for key, var in row['variables'].items()
            }
            tariff = next(price for price, value in TARIFFS.items() if value[0] == row['tariff'].get())
            item = NewFarm(**values, tariff=tariff)
            item.validate()
            items.append(item)
        return items

    def engine(self):
        return ToolsRecoveryEngine(self.logger, status=lambda value: self.events.put(('status', value)),
                                   cancel_event=self.cancel_event)

    def recover(self):
        try:
            farm, _ = self.selected(self.recovery_page)
            text = f'Восстановить {farm.name}? Старый эмулятор будет сохранён с _OLD.'
            if not messagebox.askyesno('Восстановление', text, parent=self):
                return
            self.start(lambda: self.recover_work(farm))
        except RecoveryError as exc:
            messagebox.showerror('Ошибка', str(exc), parent=self)

    def recover_work(self, farm):
        backup, index = self.engine().recover(farm)
        return f'{farm.name} восстановлена, LDPlayer ID {index}; старый экземпляр: {backup or "не найден"}'

    def login(self):
        try:
            farm, instance = self.selected(self.login_page)
            if not messagebox.askyesno('Повторный вход', f'Авторизовать {farm.name} в LDPlayer ID {instance.index}?',
                                      parent=self):
                return
            def work():
                engine = ToolsLoginEngine(self.logger, status=lambda value: self.events.put(('status', value)),
                                          cancel_event=self.cancel_event)
                engine.login(farm, instance.index)
                return f'{farm.name} авторизована в LDPlayer ID {instance.index}'
            self.start(work)
        except RecoveryError as exc:
            messagebox.showerror('Ошибка', str(exc), parent=self)

    def create(self):
        try:
            items = self.items_from_form()
            names = ', '.join(i.name for i in items)
            text = (f'Создать {len(items)} ферм: {names}?\n\n'
                    'GnBots, эмуляторы и задачи перезапуска будут временно остановлены.')
            if not messagebox.askyesno('Создание новых ферм', text, parent=self):
                return
            self.resume_items = items
            self.start(lambda: self.create_work(items, False))
        except Exception as exc:
            messagebox.showerror('Проверьте данные', str(exc), parent=self)

    def create_work(self, items, resume):
        provisioner = Provisioner(self.engine(), ToolsConfig())
        if resume:
            items = provisioner.pending() or items
        if not items:
            raise RecoveryError('Незавершённого пакета нет')
        result = provisioner.create(items, resume=resume)
        return f'Создано ферм: {len(result)}. Бот запущен, задачи восстановлены, Telegram подтвердил доставку.'

    def resume(self):
        self.start(lambda: self.create_work(self.resume_items or [], True))

    def restore(self):
        def work():
            config = ToolsConfig()
            path = config.state_dir / 'maintenance.json'
            if not path.exists():
                raise RecoveryError('Нет сохранённого состояния остановки сервера')
            Maintenance(self.engine(), path).resume()
            return 'Задачи восстановлены, запуск GnBots подтверждён'
        self.start(work)

    def start(self, operation):
        if self.worker and self.worker.is_alive():
            return
        self.cancel_event.clear()
        self.status_label.configure(background='SystemButtonFace', foreground='SystemWindowText')
        self.status.set('Проверка и запуск операции')
        self.progress.start(12)
        self.cancel_button.configure(state=tk.NORMAL)
        self.worker = threading.Thread(target=self.run, args=(operation,), daemon=True)
        self.worker.start()

    def run(self, operation):
        try:
            with SingleRunLock(Path(tempfile.gettempdir()) / 'VikingRecovery' / 'run.lock'):
                result = operation()
            self.events.put(('success', result))
        except CancelledError as exc:
            self.events.put(('cancelled', str(exc)))
        except Exception as exc:
            # Только текст ошибки: не включаем значения реквизитов и SQL-параметры в лог.
            self.logger.error('Операция не завершена: %s', exc)
            self.events.put(('error', str(exc)))
        finally:
            self.events.put(('finished', None))

    def cancel(self):
        self.cancel_event.set()
        self.status.set('Отмена запрошена; создание остановится в безопасной точке, работа бота восстановится')
        self.cancel_button.configure(state=tk.DISABLED)

    def close(self):
        if self.worker and self.worker.is_alive():
            messagebox.showinfo('Операция выполняется', 'Сначала нажмите «Отменить» и дождитесь восстановления бота.',
                                parent=self)
            return
        self.destroy()

    def drain(self):
        try:
            while True:
                kind, payload = self.events.get_nowait()
                if kind == 'log':
                    self.log.configure(state=tk.NORMAL)
                    self.log.insert(tk.END, str(payload) + '\n')
                    self.log.see(tk.END)
                    self.log.configure(state=tk.DISABLED)
                elif kind == 'status':
                    self.status.set(str(payload))
                elif kind == 'success':
                    self.status.set('✓ ГОТОВО · ' + str(payload))
                    self.status_label.configure(background='#188038', foreground='white')
                    winsound.MessageBeep(winsound.MB_ICONASTERISK)
                    self.lift()
                    messagebox.showinfo('Операция завершена', str(payload), parent=self)
                elif kind in ('error', 'cancelled'):
                    self.status.set(str(payload))
                    self.status_label.configure(
                        background='#d93025' if kind == 'error' else '#f9ab00', foreground='white',
                    )
                    messagebox.showerror('Операция не завершена', str(payload), parent=self)
                elif kind == 'finished':
                    self.progress.stop()
                    self.cancel_button.configure(state=tk.DISABLED)
        except queue.Empty:
            pass
        self.after(150, self.drain)


if __name__ == '__main__':
    VikingToolsApp().mainloop()
