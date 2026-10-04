"""Создание ферм и общий контроллер Viking Tools: Recovery, Login, New farm."""

from __future__ import annotations

import copy
import base64
import ctypes
import hashlib
import json
import logging
import os
import re
import shutil
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass, field, replace
from datetime import datetime
from pathlib import Path

from viking_login import LoginEngine
from viking_recovery import (
    Farm, OcrWord, RecoveryEngine, RecoveryError, load_farms, required_free_space,
)

TARIFFS = {500: ('Только Фарм', 'OnlyFarm'), 1000: ('Расширенный', 'Extended'), 1400: ('Премиум', 'Premium')}


def atomic_json(path: Path, value) -> None:
    """Записать JSON с fsync и заменой файла на том же томе."""

    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(dir=path.parent, prefix='.' + path.name, suffix='.tmp')
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def read_json(path: Path):
    return json.loads(path.read_text(encoding='utf-8-sig'))


def protect_credentials(value: bytes, decrypt: bool = False) -> bytes:
    """Защитить реквизиты Windows DPAPI для текущего пользователя сервера."""

    class Blob(ctypes.Structure):
        _fields_ = [('length', ctypes.c_uint32), ('data', ctypes.POINTER(ctypes.c_ubyte))]

    buffer = ctypes.create_string_buffer(value)
    source = Blob(len(value), ctypes.cast(buffer, ctypes.POINTER(ctypes.c_ubyte)))
    target = Blob()
    library = ctypes.WinDLL('crypt32', use_last_error=True)
    function = library.CryptUnprotectData if decrypt else library.CryptProtectData
    function.argtypes = [ctypes.POINTER(Blob), ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p,
                         ctypes.c_void_p, ctypes.c_uint32, ctypes.POINTER(Blob)]
    function.restype = ctypes.c_int
    if not function(ctypes.byref(source), None, None, None, None, 1, ctypes.byref(target)):
        raise RecoveryError('Windows не смог защитить или открыть сохранённые реквизиты текущего пользователя')
    try:
        return ctypes.string_at(target.data, target.length)
    finally:
        free = ctypes.WinDLL('kernel32').LocalFree
        free.argtypes = [ctypes.c_void_p]
        free.restype = ctypes.c_void_p
        free(target.data)


@dataclass(frozen=True)
class NewFarm:
    """Введённый аккаунт с постоянным ID операции для безопасного повторения."""

    name: str
    email: str
    password: str = field(repr=False)
    igg_id: str = ''
    tariff: int = 500
    operation_id: str = field(default_factory=lambda: uuid.uuid4().hex)

    def validate(self) -> None:
        if not self.name.strip() or self.name != self.name.strip() or len(self.name) > 128:
            raise RecoveryError('Имя фермы должно быть непустым и не длиннее 128 символов')
        if any(ord(c) < 32 for c in self.name) or re.search(r'[\\/:*?"<>|,]', self.name):
            raise RecoveryError('Имя фермы содержит недопустимые символы')
        if '@' not in self.email or not self.password or self.tariff not in TARIFFS:
            raise RecoveryError(f'{self.name}: заполните почту, пароль и тариф')
        if self.igg_id and (not self.igg_id.isdigit() or len(self.igg_id) < 5):
            raise RecoveryError(f'{self.name}: IGG ID должен содержать минимум пять цифр или быть пустым')

    def payload(self, igg_id: str | None = None) -> dict:
        return dict(
            internal_id=self.operation_id, name=self.name, email=self.email, password=self.password,
            igg_id=self.igg_id if igg_id is None else igg_id, tariff=self.tariff,
        )


def receipt(payload: dict) -> str:
    return hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode()).hexdigest()


def choose_game_row(words: list[OcrWord], entered_id: str) -> tuple[int, int, str]:
    """Выбрать первый IGG ID или однозначно сравнить первые пять цифр."""

    rows: list[list[OcrWord]] = []
    for word in sorted((w for w in words if 50 <= w.y1 <= 420), key=lambda w: (w.y1, w.x1)):
        row = next((r for r in rows if abs(sum(w.y1 for w in r) / len(r) - word.y1) <= 9), None)
        if row is None:
            rows.append([word])
        else:
            row.append(word)
    candidates = []
    for row in rows:
        groups: list[list[OcrWord]] = []
        for word in sorted(row, key=lambda w: w.x1):
            if not word.text.strip(' ,:.').isdigit():
                continue
            if groups and word.x1 - groups[-1][-1].x2 <= 16:
                groups[-1].append(word)
            else:
                groups.append([word])
        for group in groups:
            digits = ''.join(w.text.strip(' ,:.') for w in group)
            if 6 <= len(digits) <= 12:
                candidates.append((round(sum((w.y1 + w.y2) / 2 for w in group) / len(group)), digits))
    if not candidates:
        raise RecoveryError('На экране выбора не распознан ни один IGG ID')
    candidates.sort()
    if not entered_id:
        y, digits = candidates[0]
    else:
        if not entered_id.isdigit() or len(entered_id) < 5:
            raise RecoveryError('Для сверки IGG ID нужно минимум пять цифр')
        matches = [(y, digits) for y, digits in candidates if digits.startswith(entered_id[:5])]
        if len(matches) != 1:
            raise RecoveryError('Первые пять цифр IGG ID не найдены однозначно на экране выбора')
        y, digits = matches[0]
    return 300, y, digits


class GameSelection:
    """Правила выбора игрового ID для всех режимов нового мультитула."""

    selected_igg_id = ''

    def select_game_id(self, farm: Farm) -> None:
        self._step('Выбор игрового IGG ID')
        words = self._wait_screen(['select igg id', 'select 166 1d'], 90, 'список IGG ID')
        x, y, self.selected_igg_id = choose_game_row(words, farm.custom)
        self._adb('logcat', '-c', timeout=30)
        self._tap(x, y)


class ToolsRecoveryEngine(GameSelection, RecoveryEngine):
    pass


class ToolsLoginEngine(GameSelection, LoginEngine):
    pass


class DashClient:
    """UsersDash API с токеном в заголовке и обязательной проверкой сохранённых данных."""

    def __init__(self, url: str, server: str, token: str):
        self.url, self.server, self.token = url.rstrip('/'), server, token
        if not url or not server or not token:
            raise RecoveryError('Не настроена связь с UsersDash в конфигурации RSSv7')
        parsed = urllib.parse.urlsplit(url)
        if parsed.scheme != 'https' and not (parsed.scheme == 'http' and parsed.hostname in ('localhost', '127.0.0.1')):
            raise RecoveryError('Для удалённого UsersDash требуется HTTPS')

    def call(self, endpoint: str, payload: dict | None = None) -> dict:
        address = (self.url + '/api/farms/v1/provision' + endpoint + '?'
                   + urllib.parse.urlencode({'server': self.server}))
        request = urllib.request.Request(
            address, data=json.dumps(payload).encode() if payload is not None else None,
            headers={'Authorization': 'Bearer ' + self.token, 'Content-Type': 'application/json'},
        )
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                data = json.load(response)
        except urllib.error.HTTPError as exc:
            try:
                reason = json.load(exc).get('error', 'ошибка API')
            except (ValueError, AttributeError):
                reason = 'API Viking Tools отсутствует или недоступен'
            raise RecoveryError(f'UsersDash: HTTP {exc.code}; {reason}') from None
        except (OSError, ValueError):
            raise RecoveryError('UsersDash не подтвердил запрос; проверьте соединение и повторите') from None
        if not isinstance(data, dict) or not data.get('ok'):
            raise RecoveryError('UsersDash не подтвердил выполнение запроса')
        return data

    def reserve(self, item: NewFarm) -> dict:
        data = self.call('/reserve', item.payload())
        # После завершения сервер возвращает уже распознанный полный ID.
        if data['status'] != 'completed' and data.get('receipt') != receipt(item.payload()):
            raise RecoveryError('UsersDash сохранил данные, отличающиеся от введённых')
        return data

    def complete(self, item: NewFarm, index: int, igg_id: str) -> None:
        data = self.call('/complete', dict(
            internal_id=item.operation_id, name=item.name, instance_id=index, igg_id=igg_id,
        ))
        if data.get('instance_id') != index or data.get('receipt') != receipt(item.payload(igg_id)):
            raise RecoveryError('UsersDash не подтвердил финальные данные фермы')


class ToolsConfig:
    """Локальная конфигурация: пути к действующим RSSv7 и тарифным шаблонам."""

    def __init__(self, config_path: Path | None = None):
        self.folder = Path(__file__).resolve().parent
        path = config_path or self.folder / 'viking_tools.json'
        settings = read_json(path) if path.exists() else {
            'rss_config': str(self.folder.parent / 'config.json'),
            'templates_dir': str(self.folder.parent / 'settings' / 'templates'),
        }
        self.rss_config = Path(settings['rss_config'])
        self.templates_dir = Path(settings['templates_dir'])
        self.state_dir = self.folder / 'state'
        self.state_dir.mkdir(exist_ok=True)
        # Общий загрузчик обязателен для runtime-переменных репозитория.
        repo = self.rss_config.parent.parent
        if str(repo) not in sys.path:
            sys.path.insert(0, str(repo))
        from shared.env_loader import load_root_env_file
        load_root_env_file(self.rss_config)
        rss = read_json(self.rss_config)
        self.dash = DashClient(
            os.getenv('USERSDASH_API_URL') or rss.get('USERSDASH_API_URL', ''),
            os.getenv('SERVER_NAME') or rss.get('SERVER_NAME', ''),
            os.getenv('USERSDASH_API_TOKEN') or rss.get('USERSDASH_API_TOKEN', ''),
        )

    def template(self, tariff: int) -> list[dict]:
        path = self.templates_dir / (TARIFFS[tariff][1] + '.json')
        steps = read_json(path)
        if isinstance(steps, dict):
            steps = steps.get('Data', [])
        if isinstance(steps, str):
            steps = json.loads(steps)
        if not isinstance(steps, list) or not steps:
            raise RecoveryError(f'Пустой или неверный тарифный шаблон: {path.name}')
        # AccountLib.dll падает при IsActive=false внутри Data: исключаем такие действия.
        steps = [copy.deepcopy(step) for step in steps if isinstance(step, dict) and step.get('IsActive', True)]
        if not steps:
            raise RecoveryError('В тарифном шаблоне нет активных действий')
        for index, step in enumerate(steps):
            step.update(Id=index, OrderId=index)
            if 'ScheduleData' in step:
                step['ScheduleData']['Last'] = '0001-01-01T00:00:00'
        return steps


class Maintenance:
    """Остановка помех с сохранением задач и обязательным восстановлением в finally."""

    def __init__(self, engine: RecoveryEngine, state_path: Path):
        self.engine, self.state_path = engine, state_path
        self.helper = Path(__file__).with_name('viking_maintenance.ps1')

    def call(self, action: str) -> dict:
        result = self.engine.runner.run([
            'powershell.exe', '-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass',
            '-File', self.helper, '-Action', action, '-StatePath', self.state_path,
            '-GnBotsDir', self.engine.gnbots_dir,
        ], timeout=120)
        return json.loads(result.stdout.lstrip('\ufeff'))

    def pause(self) -> None:
        self.engine._step('Остановка GnBots, мониторов и задач перезапуска')
        self.call('Pause')
        self.engine.runner.run([self.engine.ldconsole, 'quitall'], timeout=60)
        def stopped():
            result = self.engine.runner.run([self.engine.ldconsole, 'list2'], timeout=30).stdout
            return all(line.split(',')[4] == '0' for line in result.splitlines() if len(line.split(',')) >= 7)
        self.engine._wait_until(stopped, 120, 'остановка всех эмуляторов')

    def resume(self) -> None:
        self.engine.logger.info('Восстановление задач и запуск GnBots')
        self.engine.status('Восстановление задач и запуск GnBots')
        log = self.engine.gnbots_dir / 'logs' / f'bot{datetime.now():%Y%m%d}.txt'
        before = log.stat().st_mtime_ns if log.exists() else 0
        self.call('Resume')
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            if log.exists() and log.stat().st_mtime_ns > before:
                return
            time.sleep(2)
        raise RecoveryError('Задачи восстановлены, но GnBots не подтвердил запуск свежей записью в журнале')


def install_profile_record(engine: RecoveryEngine, item: NewFarm, steps: list[dict], index: int = -1,
                           igg_id: str | None = None, active: bool = False) -> None:
    """Добавить новую запись без замены остальных ферм; перепроверить атомарную запись."""

    records = read_json(engine.profile_path)
    if not isinstance(records, list):
        raise RecoveryError('Неверный формат профиля GnBots')
    matches = [r for r in records if r.get('Id') == item.operation_id]
    collision = [r for r in records if str(r.get('Name', '')).casefold() == item.name.casefold()
                 and r.get('Id') != item.operation_id]
    if collision or len(matches) > 1:
        raise RecoveryError(f'{item.name}: имя или ID заняты в профиле GnBots')
    if matches:
        record = matches[0]
        if record.get('VikingTools', {}).get('operation_id') != item.operation_id:
            raise RecoveryError('Запись профиля принадлежит другой операции')
    else:
        source = next((r for r in records if r.get('AppId') == 'vikingbot' and r.get('MenuData')), None)
        if source is None:
            raise RecoveryError('В профиле нет базовой записи Viking Rise')
        record = copy.deepcopy(source)
        records.append(record)
    menu = record['MenuData']
    menu = json.loads(menu) if isinstance(menu, str) else copy.deepcopy(menu)
    menu['Config'].update(Email=item.email, Password=item.password, Slot='igg',
                          Custom=item.igg_id if igg_id is None else igg_id)
    record.update(
        Id=item.operation_id, Name=item.name, AppId='vikingbot', InstanceId=index, SessionId=-1,
        Active=active, LastRun='0001-01-01T00:00:00', HealthCheckPassed=False, LastUsedActions=[],
        MenuData=json.dumps(menu, ensure_ascii=False), Data=json.dumps(steps, ensure_ascii=False),
        VikingTools={'operation_id': item.operation_id, 'tariff': item.tariff,
                     'entered_igg_id': item.igg_id, 'status': 'ready' if active else 'prepared'},
    )
    backup = engine.profile_path.with_name(
        engine.profile_path.name + f'.before_tools_{datetime.now():%Y%m%d_%H%M%S_%f}'
    )
    shutil.copy2(engine.profile_path, backup)
    atomic_json(engine.profile_path, records)
    saved = next((r for r in read_json(engine.profile_path) if r.get('Id') == item.operation_id), None)
    if saved != record:
        raise RecoveryError('Профиль GnBots не подтвердил новую запись')


class Provisioner:
    """Последовательный пакет создания ферм с возобновляемыми этапами."""

    def __init__(self, engine: ToolsRecoveryEngine, config: ToolsConfig):
        self.engine, self.config = engine, config
        self.journal_path = config.state_dir / 'provision.json'
        self.maintenance = Maintenance(engine, config.state_dir / 'maintenance.json')

    def save(self, journal: dict) -> None:
        atomic_json(self.journal_path, journal)

    def pending(self) -> list[NewFarm]:
        if not self.journal_path.exists():
            return []
        journal = read_json(self.journal_path)
        if journal.get('status') == 'completed':
            return []
        encrypted = base64.b64decode(journal['credentials_dpapi'])
        rows = json.loads(protect_credentials(encrypted, decrypt=True))
        return [NewFarm(row['name'], row['email'], row['password'], row['igg_id'], row['tariff'], row['internal_id'])
                for row in rows]

    def preflight(self, items: list[NewFarm], journal: dict) -> None:
        self.engine.validate()
        info = self.config.dash.call('')
        if not info.get('telegram_ready'):
            raise RecoveryError('В UsersDash не настроены Telegram-уведомления')
        inspected = self.maintenance.call('Inspect')
        if not inspected['administrator']:
            raise RecoveryError('Запустите Viking Tools от имени администратора для управления задачами')
        if inspected.get('session_id') == 0:
            raise RecoveryError('Создание ферм запускается из рабочего стола Windows, не из SSH-сессии')
        for item in items:
            item.validate()
            self.config.template(item.tariff)
        if len({item.name.casefold() for item in items}) != len(items) or not items:
            raise RecoveryError('В пакете должны быть непустые уникальные имена ферм')
        needed = sum(state.get('index') is None for state in journal.get('items', [])) if journal else len(items)
        free = shutil.disk_usage(self.engine.ldplayer_dir).free
        size = required_free_space((self.engine.ldplayer_dir / 'vms' / 'leidian0').resolve())
        if free < size * needed:
            raise RecoveryError(f'Для пакета нужен запас места {size * needed / 1024**3:.1f} ГБ на диске LDPlayer')
        if not journal:
            names = {f.name.casefold() for f in load_farms(self.engine.profile_path)}
            names.update(i.name.casefold() for i in self.engine.list_instances())
            if any(item.name.casefold() in names for item in items):
                raise RecoveryError('Имя новой фермы уже существует; используйте Recovery или другое имя')

    def create(self, items: list[NewFarm], resume: bool = False) -> list[dict]:
        journal = read_json(self.journal_path) if resume and self.journal_path.exists() else {}
        if not resume and self.journal_path.exists() and read_json(self.journal_path).get('status') != 'completed':
            raise RecoveryError('Есть незавершённый пакет. Нажмите «Продолжить пакет»')
        if journal and [item.operation_id for item in items] != [state['operation_id'] for state in journal['items']]:
            raise RecoveryError('Пакет не совпадает с сохранёнными операциями')
        if journal and all(state['stage'] == 'completed' for state in journal['items']):
            # Повтор доставки не останавливает работающие фермы и не требует места для новых клонов.
            self.maintenance.resume()
            self.finish(items, journal)
            return journal['items']
        self.preflight(items, journal)
        if not journal:
            journal = {'status': 'running', 'items': [dict(operation_id=i.operation_id, name=i.name,
                       tariff=i.tariff, stage='new', index=None, igg_id='') for i in items]}
            secured = protect_credentials(json.dumps([item.payload() for item in items]).encode())
            journal['credentials_dpapi'] = base64.b64encode(secured).decode()
            self.save(journal)
        failure = None
        try:
            self.maintenance.pause()
            # UsersDash резервируется прежде профиля, чтобы фоновый импорт не создал дубль.
            for item, state in zip(items, journal['items']):
                if state['stage'] == 'new':
                    self.engine._check_cancel()
                    self.config.dash.reserve(item)
                    install_profile_record(self.engine, item, self.config.template(item.tariff))
                    state['stage'] = 'reserved'
                    self.save(journal)
            for item, state in zip(items, journal['items']):
                self.engine._check_cancel()
                if state['stage'] == 'completed':
                    continue
                self.engine._step(f'Подготовка {item.name} · {TARIFFS[item.tariff][0]}')
                steps = self.config.template(item.tariff)
                farm = Farm(item.name, item.email, item.password, item.igg_id, 'igg', False, item.operation_id)
                if state['stage'] == 'reserved':
                    owned_farm = replace(farm, name=item.name + '__new_' + item.operation_id)
                    state['index'] = self.engine.clone_template(owned_farm)
                    state['stage'] = 'cloned'
                    self.save(journal)
                self.engine.new_index = state['index']
                if state['stage'] == 'cloned':
                    self.engine._validate_instance_storage(state['index'])
                    self.engine.launch_and_wait()
                    self.engine.open_login()
                    self.engine.submit_credentials(farm)
                    self.engine.select_game_id(farm)
                    self.engine.wait_for_game()
                    state['igg_id'] = self.engine.selected_igg_id
                    state['stage'] = 'authorized'
                    self.save(journal)
                if state['stage'] == 'authorized':
                    # В новом режиме нет переименования старых эмуляторов.
                    existing = self.engine.list_instances()
                    if any(i.name.casefold() == item.name.casefold() and i.index != state['index'] for i in existing):
                        raise RecoveryError('Имя нового эмулятора занято другим экземпляром')
                    self.engine.runner.run([self.engine.ldconsole, 'rename', '--index', str(state['index']),
                                            '--title', item.name], timeout=60)
                    if not any(i.index == state['index'] and i.name == item.name for i in self.engine.list_instances()):
                        raise RecoveryError('Не подтверждено переименование нового LDPlayer')
                    install_profile_record(self.engine, item, steps, state['index'], state['igg_id'])
                    self.config.dash.complete(item, state['index'], state['igg_id'])
                    install_profile_record(self.engine, item, steps, state['index'], state['igg_id'], True)
                    state['stage'] = 'completed'
                    self.save(journal)
                self.engine.runner.run([self.engine.ldconsole, 'quit', '--index', str(state['index'])], timeout=60)
                self.engine._step(f'{item.name}: создана и проверена, LDPlayer ID {state["index"]}')
        except BaseException as exc:
            failure = exc
            journal['status'] = 'interrupted'
            self.save(journal)
        finally:
            if self.maintenance.state_path.exists():
                try:
                    self.maintenance.resume()
                except Exception:
                    journal['status'] = 'restore_required'
                    self.save(journal)
                    raise RecoveryError('Не подтверждено восстановление сервера; нажмите «Восстановить работу бота»')
        if failure:
            raise failure
        self.finish(items, journal)
        return journal['items']

    def finish(self, items: list[NewFarm], journal: dict) -> None:
        self.config.dash.call('/notify', {'internal_ids': [i.operation_id for i in items]})
        journal['status'] = 'completed'
        journal.pop('credentials_dpapi', None)
        self.save(journal)
