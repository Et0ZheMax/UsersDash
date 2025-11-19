import psutil
import subprocess
import time
import ctypes

def kill_process(process_name):
    """
    Закрывает все процессы с указанным именем.
    Игнорирует ошибки доступа и случаи, когда процесс уже завершён.
    """
    for proc in psutil.process_iter(['pid', 'name']):
        try:
            if process_name.lower() in proc.info['name'].lower():
                print(f"Закрываю процесс {proc.info['name']} (PID {proc.info['pid']})")
                proc.terminate()
                proc.wait(timeout=3)
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass

def close_console():
    """
    Закрывает текущее консольное окно.
    Использует Windows API для отправки сообщения WM_CLOSE.
    """
    hwnd = ctypes.windll.kernel32.GetConsoleWindow()
    if hwnd:
        ctypes.windll.user32.PostMessageW(hwnd, 0x0010, 0, 0)


def is_process_running(process_name):
    """
    Проверяет, запущен ли процесс с указанным именем.
    """
    for proc in psutil.process_iter(['name']):
        try:
            if process_name.lower() in proc.info['name'].lower():
                return True
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    return False



def start_process(exe_path):
    """
    Запускает процесс по указанному пути.
    """
    subprocess.Popen(exe_path, shell=True)

def main():
    print("Закрываю все процессы LDPlayer.exe...")
    kill_process("dnplayer.exe")

    print("Закрываю все процессы GnBots.exe...")
    kill_process("GnBots.exe")

    print("Закрываю все процессы Ld9BoxHeadless.exe...")
    kill_process("Ld9BoxHeadless.exe")

    print("Закрываю все процессы ld.exe...")
    kill_process("ld.exe")

    print("Жду 5 секунд перед перезапуском...")
    time.sleep(5)

    print("Перезапускаю GnBots...")
    start_process(r"C:\Users\Administrator\Desktop\GnBots.lnk")

    # Ждем, чтобы процесс успел запуститься
    time.sleep(5)

    print("Проверяю, запущен ли процесс GnBots.exe...")
    if is_process_running("GnBots.exe"):
        print("GnBots успешно запущен. Закрываю консольное окно...")
        close_console()
    else:
        print("GnBots не запущен. Проверьте настройки запуска.")

    # Если нужно запускать и Nox напрямую, добавьте вызов, например:
    # print("Запускаю NoxPlayer...")
    # start_process(r"C:\Users\faaaarm\Desktop\Nox.lnk")

if __name__ == "__main__":
    main()
