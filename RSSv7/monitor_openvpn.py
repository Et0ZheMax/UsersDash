import os
import subprocess
import time
import urllib.parse
import urllib.request
import ssl  # для создания контекста без проверки сертификата

# === Настройки ===

TELEGRAM_TOKEN = '7460479135:AAEUcUZdO01AEOVxgA0xlV8ZoLOmZcKw-Uc'
CHAT_ID = '275483461'

# Профили для OpenVPN GUI, в порядке приоритета
GUI_PROFILES = ['xnl.ovpn', 'sde.ovpn']

# Список имён процессов и команды запуска в порядке приоритета
PROCESS_NAMES = [
    'openvpn-gui.exe',     # GUI-клиент
    'openvpn.exe',         # Community CLI
    'OpenVPNConnect.exe',  # Connect client
]

START_COMMANDS = {
    # для GUI — несколько попыток с разными профилями
    'openvpn-gui.exe': [
        # команда без профиля, профили будут подставляться динамически
        r"C:\Program Files\OpenVPN\bin\openvpn-gui.exe",
        '--silent_connection', '1'
    ],
    'openvpn.exe': [
        r"C:\Program Files\OpenVPN\bin\openvpn.exe",
        '--config', r'C:\Program Files\OpenVPN\config\client.ovpn',
        '--verb', '4',
        '--log', r'C:\Program Files\OpenVPN\log\client.log'
    ],
    'OpenVPNConnect.exe': [
        r"C:\Program Files\OpenVPN Connect\OpenVPNConnect.exe"
    ],
}

CHECK_INTERVAL = 60  # секунд между проверками

# === Health-check при старте ===

def health_check():
    """
    Проверяем наличие исполняемых файлов и конфигов.
    Логируем в консоль и отправляем в Telegram, если что-то не найдено.
    """
    missing = []
    print("=== Health-check start ===")

    # проверяем openvpn-gui.exe и оба профиля
    gui_path = START_COMMANDS['openvpn-gui.exe'][0]
    if not os.path.isfile(gui_path):
        missing.append(gui_path)
        print(f"ERROR: GUI exe не найден: {gui_path}")
    config_dir = r"C:\Program Files\OpenVPN\config"
    for profile in GUI_PROFILES:
        cfg = os.path.join(config_dir, profile)
        if not os.path.isfile(cfg):
            missing.append(cfg)
            print(f"ERROR: GUI профиль не найден: {cfg}")

    # проверяем остальные
    for name in ['openvpn.exe', 'OpenVPNConnect.exe']:
        cmd = START_COMMANDS[name][0]
        if not os.path.isfile(cmd):
            missing.append(cmd)
            print(f"ERROR: exe не найден: {cmd}")
        # для community client проверяем config
        if name == 'openvpn.exe':
            cfg = START_COMMANDS[name][START_COMMANDS[name].index('--config') + 1]
            if not os.path.isfile(cfg):
                missing.append(cfg)
                print(f"ERROR: CLI конфиг не найден: {cfg}")

    if missing:
        msg = "❗ Health-check FAILED, отсутствуют файлы:\n" + "\n".join(missing)
        print(msg)
        send_telegram_message(TELEGRAM_TOKEN, CHAT_ID, msg)
        # при желании можно завершить: exit(1)
    else:
        print("✅ Health-check passed. Все файлы на месте.")
    print("=== Health-check end ===\n")

# === Вспомогательные функции ===

def get_running_process():
    """
    Возвращает имя первого запущенного процесса из PROCESS_NAMES, иначе None.
    """
    try:
        out = subprocess.check_output(['tasklist'], stderr=subprocess.DEVNULL)
        text = out.decode('cp866', errors='ignore').lower()
        for name in PROCESS_NAMES:
            if name.lower() in text:
                return name
    except Exception as e:
        print(f"Ошибка при tasklist: {e}")
    return None

def send_telegram_message(token, chat_id, text):
    """
    Отправка уведомления в Telegram, игнорируя SSL-ошибки.
    """
    msg = urllib.parse.quote_plus(text)
    url = f"https://api.telegram.org/bot{token}/sendMessage?chat_id={chat_id}&text={msg}"
    try:
        ctx = ssl._create_unverified_context()
        with urllib.request.urlopen(url, context=ctx) as resp:
            return resp.read()
    except Exception as e:
        print(f"Telegram send failed: {e}")

def restart_process(command):
    """
    Запускает процесс командой command (list). Возвращает True при успехе.
    """
    try:
        subprocess.Popen(command, shell=False)
        return True
    except Exception as e:
        print(f"Restart failed ({command}): {e}")
        return False

# === Основной цикл ===

if __name__ == "__main__":
    # Сначала проверяем здоровье окружения
    health_check()

    while True:
        running = get_running_process()
        if not running:
            alert = "⚠️ OpenVPN не запущен, пытаюсь стартовать клиенты..."
            print(alert)
            send_telegram_message(TELEGRAM_TOKEN, CHAT_ID, alert)

            # 1) Пробуем GUI с разными профилями
            gui_exe = START_COMMANDS['openvpn-gui.exe'][0]
            silent = START_COMMANDS['openvpn-gui.exe'][1:]
            for profile in GUI_PROFILES:
                cmd = [gui_exe, '--connect', profile] + silent
                print(f"Попытка GUI: {profile}")
                if restart_process(cmd):
                    send_telegram_message(TELEGRAM_TOKEN, CHAT_ID, f"✅ openvpn-gui.exe запущен с {profile}")
                    running = 'openvpn-gui.exe'
                    break

            # 2) Если GUI не поднялся, пробуем CLI
            if not running:
                name = 'openvpn.exe'
                cmd = START_COMMANDS[name]
                print("Попытка Community CLI")
                if restart_process(cmd):
                    send_telegram_message(TELEGRAM_TOKEN, CHAT_ID, "✅ openvpn.exe запущен")
                    running = name

            # 3) Если всё ещё нет, пробуем Connect
            if not running:
                name = 'OpenVPNConnect.exe'
                cmd = START_COMMANDS[name]
                print("Попытка Connect client")
                if restart_process(cmd):
                    send_telegram_message(TELEGRAM_TOKEN, CHAT_ID, "✅ OpenVPNConnect.exe запущен")
                    running = name

        # ждём перед следующей проверкой
        time.sleep(CHECK_INTERVAL)
