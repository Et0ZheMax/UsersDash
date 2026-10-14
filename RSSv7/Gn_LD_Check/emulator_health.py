"""Проверка Android и имён аккаунтов без перезапуска приложений и Windows."""
import asyncio
import json
import os
import platform
import re
import subprocess
import time
from pathlib import Path

import Gn_Ld_Check as watchdog

STATE = Path(__file__).parent / 'data' / 'emulator_health.json'
CONFIRM_SECONDS = 180
REPEAT_SECONDS = 1800


def account_names(bot_dir: Path = Path(r'C:\Program Files\GnBots')) -> dict[str, str]:
    """Читает текущие привязки GnBots; неоднозначные имена не подставляются."""
    try:
        cfg=json.loads((bot_dir/'config.json').read_text(encoding='utf-8-sig'))
        if not isinstance(cfg,dict):return {}
        profile=cfg.get('SelectedProfile','')
        if not isinstance(profile,str) or not re.fullmatch(r'[\w -]+',profile):return {}
        rows=json.loads((bot_dir/'profiles'/f'{profile}.json').read_text(encoding='utf-8-sig'))
        if not isinstance(rows,list):return {}
        bindings={}
        for row in rows:
            if not isinstance(row,dict):continue
            index=str(row.get('InstanceId',''))
            name=row.get('Name')
            if not re.fullmatch(r'\d+',index) or not isinstance(name,str):continue
            name=' '.join(name.split())[:80]
            if name:bindings.setdefault(str(int(index)),set()).add(name)
        return {index:next(iter(names)) for index,names in bindings.items() if len(names)==1}
    except (OSError,ValueError,TypeError):
        return {}


def restart_loop(log_dir, now):
    """Находит повторные перезапуски нескольких аккаунтов без работы в игре."""
    paths = list(Path(log_dir).glob('bot*.txt'))
    if not paths:
        return False
    latest = max(paths, key=lambda path: path.stat().st_mtime)
    with latest.open('rb') as stream:
        stream.seek(max(0, latest.stat().st_size - 1024 * 1024))
        lines = stream.read().decode('utf-8', errors='replace').splitlines()
    restarts, progressed = [], set()
    for line in lines:
        ts = watchdog.parse_line_timestamp_epoch(line)
        if ts is None or not 0 <= now - ts <= 300:
            continue
        match = re.search(r'\|([0-9a-f]{16})\|', line)
        if re.search(r'getMap: Found (world|town)|Check Current Marches:|_dmar:', line):
            if match:
                progressed.add(match[1])
            else:
                return False
        if 'Launch: We will now restart the instance' in line:
            if match:
                restarts.append((ts, match[1]))
    restarts = [(ts, session) for ts, session in restarts if session not in progressed]
    sessions = {session for ts, session in restarts}
    return (len(sessions) >= 3 and len(restarts) >= 6
            and max(ts for ts, session in restarts) - min(ts for ts, session in restarts) >= CONFIRM_SECONDS)


def snapshot():
    windows, headless = {}, set()
    errors = []
    for proc in watchdog.psutil.process_iter(['name', 'cmdline', 'exe', 'create_time']):
        try:
            info = proc.info
            name = (info['name'] or '').lower()
            cmd = ' '.join(info['cmdline'] or [])
            if name == 'dnplayer.exe':
                match = re.search(r'index=(\d+)', cmd)
                if match:
                    windows[int(match[1])] = info
            elif name == 'ld9boxheadless.exe':
                match = re.search(r'--comment\s+leidian(\d+)', cmd)
                if match:
                    headless.add(int(match[1]))
        except (watchdog.psutil.NoSuchProcess, watchdog.psutil.AccessDenied, OSError) as exc:
            errors.append(type(exc).__name__)
    ready, failed = [], {}
    for index, info in windows.items():
        if index not in headless:
            failed[str(index)] = 'нет процесса виртуальной машины'
            continue
        if not info.get('exe'):
            failed[str(index)] = 'не удалось определить путь к ADB'
            continue
        adb = Path(info['exe']).parent / 'adb.exe'
        try:
            result = subprocess.run(
                [str(adb), '-s', f'emulator-{5554 + index * 2}',
                 'shell', 'getprop', 'sys.boot_completed'],
                capture_output=True, text=True, timeout=3,
                creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0),
            )
            if result.returncode == 0 and result.stdout.strip() == '1':
                ready.append(index)
            else:
                failed[str(index)] = 'Android не загрузился / ADB offline'
        except (OSError, subprocess.TimeoutExpired):
            failed[str(index)] = 'Android недоступен через ADB'
    return {'windows': sorted(windows), 'headless': sorted(headless),
            'ready': sorted(ready), 'failed': failed, 'errors': errors,
            'gn_running': watchdog.is_process_running('GnBots.exe')}


def evaluate(sample, previous, now):
    """Подтверждает длительные сбои, в том числе при замене PID окна."""
    state = dict(previous)
    # Лимит отправок относится к экземпляру, а не к текущему набору сбоев.
    # Исчезновение или замена окна не доказывает восстановление Android.
    sent = dict(previous.get('sent_by_key', {}))
    if not sent and previous.get('last_signature') and previous.get('last_sent'):
        sent.update({key:previous['last_sent'] for key in notification_keys(previous['last_signature'])})
    state['sent_by_key']={key:at for key,at in sent.items() if now-at<7*86400}
    active = set(sample['failed'])
    since = {key: value for key, value in previous.get('failed_since', {}).items()
             if key in active}
    for key in active:
        since.setdefault(key, now)
    state['failed_since'] = since
    total_failure = not sample['ready'] and (sample['gn_running'] or sample['windows'])
    state['outage_since'] = previous.get('outage_since', now) if total_failure else None
    if state['outage_since'] is None and total_failure:
        state['outage_since'] = now
    confirmed = sorted(key for key, started in since.items()
                       if now - started >= CONFIRM_SECONDS)
    full = total_failure and now - state['outage_since'] >= CONFIRM_SECONDS
    signature = 'all' if full else ('restart-loop' if sample.get('restart_loop') else ','.join(confirmed))
    keys=notification_keys(signature)
    due=[key for key in keys if key not in sent or now-sent[key]>=REPEAT_SECONDS]
    send=bool(due)
    if send and signature not in ('all','restart-loop'):
        confirmed=[key.removeprefix('vm:') for key in due]
        signature=','.join(confirmed)
    state['notify_keys']=due
    state['ack_keys']=list(dict.fromkeys(due+(['vm:'+key for key in confirmed] if signature=='all' else [])))
    if not signature:
        state['last_signature'] = ''
    state['checked_at'] = now
    state['snapshot'] = sample
    return state, send, signature, confirmed


def notification_keys(signature):
    if signature in ('all','restart-loop'):return ['critical:'+signature]
    return ['vm:'+key for key in signature.split(',') if key.isdigit()]


def mark_delivered(state,signature,now):
    for key in state.pop('ack_keys',[]):state.setdefault('sent_by_key',{})[key]=now
    state.pop('notify_keys',None)
    state['last_signature'],state['last_sent']=signature,now


def alert_text(sample, full=False, confirmed=None):
    title = ('КРИТИЧЕСКИЙ СБОЙ: ни одного загруженного Android' if full else
             'КРИТИЧЕСКИЙ СБОЙ: массовый цикл перезапусков без работы в игре'
             if sample.get('restart_loop') else 'Сбой одного эмулятора LDPlayer'
             if len(confirmed or [])==1 else 'Сбой эмуляторов LDPlayer')
    label=os.getenv('GNLDCHECK_SERVER_LABEL') or platform.node()
    lines = [f'{label} 🚨 {title}',
             f'Окна LDPlayer: {len(sample["windows"])}; VM: {len(sample["headless"])}; '
             f'Android с boot_completed=1: {len(sample["ready"])}.',
             'Сбой подтверждён в течение минимум 3 минут.']
    for key in (confirmed or sorted(sample['failed'])):
        name=sample.get('account_names',{}).get(str(key))
        label=f'{name} (LDPlayer {key})' if name else f'LDPlayer {key}'
        lines.append(f'• {label}: {sample["failed"].get(key, "не загружен")}')
    lines.append('Проверка выполняется каждую минуту. Автоматический ребут этим монитором не выполняется.')
    return '\n'.join(lines)


async def send_verified(cfg, text):
    # Не выводим токен/URL и не учитываем отправку до подтверждения Telegram.
    try:
        async with watchdog.build_bot(cfg) as bot:
            message = await bot.send_message(chat_id=cfg.chat_id, text=text,
                                            message_thread_id=cfg.thread_id)
            print(f'[TELEGRAM] delivered message_id={message.message_id}')
    except Exception as exc:
        print(f'[TELEGRAM] delivery failed: {type(exc).__name__}')
        raise RuntimeError('Telegram delivery failed') from None


async def main(dry_run=False):
    watchdog._load_root_env()
    cfg = watchdog._load_config()
    STATE.parent.mkdir(exist_ok=True)
    if STATE.exists():
        previous = json.loads(STATE.read_text(encoding='utf-8'))
    else:
        previous = {}
    sample = snapshot()
    names=account_names()
    sample['account_names']={key:names[key] for key in sample['failed'] if key in names}
    current = time.time()
    sample['restart_loop'] = restart_loop(cfg.log_dir, current)
    state, send, signature, confirmed = evaluate(sample, previous, current)
    print(json.dumps(sample, ensure_ascii=False))
    if dry_run:
        print(f'[DRY RUN] alert={send}; signature={signature}')
        return
    if send:
        await send_verified(cfg, alert_text(sample, signature == 'all', confirmed))
        mark_delivered(state,signature,current)
    temp = STATE.with_suffix('.tmp')
    temp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding='utf-8')
    os.replace(temp, STATE)


if __name__ == '__main__':
    import argparse
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dry-run',action='store_true')
    parser.add_argument('--server-label',help='Понятное имя сервера, например 208 или F99')
    args=parser.parse_args()
    if args.server_label:os.environ['GNLDCHECK_SERVER_LABEL']=args.server_label
    asyncio.run(main(args.dry_run))
