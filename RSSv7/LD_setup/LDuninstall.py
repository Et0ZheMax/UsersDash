import os
import sys
import time
import subprocess
import ctypes
import traceback
import shutil
import pyautogui
import pyscreeze

# === PyAutoGUI ===
pyautogui.FAILSAFE = True
pyautogui.PAUSE = 0.5

# === Пути, связанные с LDPlayer ===
UNINSTALLER   = r"C:\LDPlayer\LDPlayer9\dnuninst.exe"
INSTALLER     = r"C:\LDPlayer.exe"
TARGET_DIR    = r"C:\LDPlayer\LDPlayer9"
VMS_DIR       = os.path.join(TARGET_DIR, "vms")
BACKUP_VMS    = r"C:\LD_backup\vms"          # фиксированная точка бэкапа

# Относительные координаты поля пути (если придётся менять вручную)
PATH_FIELD_REL = (0.5, 0.4)

# === Локальные шаблоны ===
BASE = os.path.dirname(os.path.abspath(__file__))
os.chdir(BASE)
TPLS = {
    "del":        ("del.png",             20),
    "del_done":   ("del_done.png",        20),
    "window":     ("window.png",          30),
    "settings":   ("settings.png",        20),
    "path":       ("path.png",            10),
    "install":    ("install_button.png",  20),
    "install2":   ("install_button2.png", 20),  # вторая версия
    "done":       ("done.png",           300),
}

# === Вспомогательные функции ===
def debug(msg): print(f"[{time.strftime('%H:%M:%S')}] {msg}")

def is_admin() -> bool:
    try: return ctypes.windll.shell32.IsUserAnAdmin()
    except: return False

def elevate():
    params = " ".join(f'"{a}"' for a in sys.argv)
    ctypes.windll.shell32.ShellExecuteW(None, "runas",
                                        sys.executable, params, None, 1)

def get_template_path(fname: str) -> str:
    for f in os.listdir(BASE):
        if f.lower() == fname.lower():
            return os.path.join(BASE, f)
    raise FileNotFoundError(f"Template not found: {fname}")

def locate_template(name: str):
    raw_name, timeout = TPLS[name]
    path = get_template_path(raw_name)
    debug(f"Searching for template: {os.path.basename(path)} (timeout={timeout}s)")
    start, best_conf = time.time(), 0.0
    while time.time() - start < timeout:
        for gray in (False, True):
            for conf in (0.8, 0.7, 0.6, 0.5):
                try:
                    loc = pyautogui.locateOnScreen(path, confidence=conf, grayscale=gray)
                    if loc:
                        debug(f"Found {os.path.basename(path)} (gray={gray}, conf={conf}): {loc}")
                        return loc
                except pyscreeze.ImageNotFoundException as e:
                    if "highest confidence" in str(e):
                        try:
                            best_conf = max(best_conf,
                                            float(str(e).split("highest confidence = ")[1][:-1]))
                        except: pass
        time.sleep(0.5)
    raise TimeoutError(f"{os.path.basename(path)} not found (max_conf={best_conf:.3f})")

def wait_done():
    raw_name, max_wait = TPLS["done"]
    path = get_template_path(raw_name)
    debug(f"Waiting for finish template: {os.path.basename(path)} (max {max_wait}s)")
    start = time.time()
    while time.time() - start < max_wait:
        try:
            loc = pyautogui.locateOnScreen(path, confidence=0.7)
            if loc:
                debug(f"Finish marker found: {loc}")
                return
        except:
            pass
        time.sleep(3)
    raise TimeoutError(f"Finish template not found within {max_wait}s")

# === Backup / Restore vms ===
def backup_vms_or_exit():
    """Перемещает vms → C:\\LD_backup\\vms, проверяет, что не пусто."""
    if not os.path.exists(VMS_DIR):
        debug("vms folder not found — ничего переносить.")
        return
    if not os.listdir(VMS_DIR):
        sys.exit("vms folder EMPTY — aborting to avoid data loss.")

    os.makedirs(os.path.dirname(BACKUP_VMS), exist_ok=True)
    if os.path.exists(BACKUP_VMS):
        debug("Old backup found – removing it.")
        shutil.rmtree(BACKUP_VMS, ignore_errors=True)

    try:
        shutil.move(VMS_DIR, BACKUP_VMS)
        debug(f"vms moved to {BACKUP_VMS}")
    except Exception as e:
        sys.exit(f"Cannot move vms ({e}) – aborting.")

    if not os.listdir(BACKUP_VMS):
        sys.exit("Backup folder is empty after move – aborting.")
    debug("Backup verification OK.")

def restore_vms():
    """Возвращает vms назад, перезаписывая то, что могло появиться."""
    if not os.path.exists(BACKUP_VMS):
        debug("No vms backup to restore.")
        return
    if os.path.exists(VMS_DIR):
        shutil.rmtree(VMS_DIR, ignore_errors=True)
    try:
        shutil.move(BACKUP_VMS, VMS_DIR)
        debug("vms restored to original location.")
    except Exception as e:
        debug(f"ERROR restoring vms: {e}")
        pyautogui.alert("Установка завершена, но не удалось вернуть vms!\nСмотрите консоль.")

# === Основной сценарий ===
def main():
    debug(f"Working dir: {BASE}")
    backup_vms_or_exit()                     # 0. safeguard

    # 0.1 Закрываем запущенный LDPlayer
    debug("Killing ld.exe processes …")
    subprocess.run(["TASKKILL", "/IM", "ld.exe", "/IM", "dnuninst.exe", "/F, /T"],
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

   # 1) Деинсталляция
    if os.path.exists(UNINSTALLER):
        debug(f"Launching uninstaller: {UNINSTALLER}")
        proc = subprocess.Popen([UNINSTALLER], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        # Снимаем скрин перед нажатием «Удалить»
        ss1 = os.path.join(BASE, "debug_uninstall_window.png")
        pyautogui.screenshot(ss1)
        debug(f"Saved uninstall dialog screenshot -> {ss1}")

        # Клик по кнопке «Удалить»
        try:
            btn = locate_template("del")
            x, y = pyautogui.center(btn)
            debug(f"Clicking Delete at {x},{y}")
            pyautogui.click(x, y)
        except Exception as e:
            debug(f"Failed to locate 'del': {e}")

        # Ждём 10 секунд перед поиском del_done.png
        debug("Waiting 10 seconds before looking for del_done...")
        time.sleep(3)

        # Снимаем скрин после удаления
        ss2 = os.path.join(BASE, "debug_uninstall_done.png")
        pyautogui.screenshot(ss2)
        debug(f"Saved post-delete screenshot -> {ss2}")

        # Клик по «Готово удаления»
        try:
            done = locate_template("del_done")
            x2, y2 = pyautogui.center(done)
            debug(f"Clicking Uninstall Done at {x2},{y2}")
            pyautogui.click(x2, y2)
        except Exception as e:
            debug(f"Failed to locate 'del_done': {e}")

        proc.wait(120)
        debug("Uninstallation complete.")
        time.sleep(1)
    else:
        debug("Uninstaller not found, skipping uninstall step.")

    # 2) Запуск инсталлятора
    if not os.path.exists(INSTALLER):
        raise FileNotFoundError(f"Installer not found: {INSTALLER}")
    debug(f"Launching installer: {INSTALLER}")
    subprocess.Popen([INSTALLER])
    time.sleep(2)

    # 3) Ждём главное окно
    win_loc = locate_template("window")

    # 4) Нажимаем «Настроить»
    try:
        btn = locate_template("settings")
        x, y = pyautogui.center(btn)
        debug(f"Clicking Settings at {x},{y}")
        pyautogui.click(x, y)
    except Exception:
        debug("Settings template not found, sending ENTER")
        pyautogui.press("enter")
    time.sleep(1)

    # 5) Проверка пути установки
    try:
        locate_template("path")
        debug("Install path is correct.")
    except Exception:
        debug("Install path incorrect — entering manually")
        left, top, w, h = win_loc
        fx = int(left + w * PATH_FIELD_REL[0])
        fy = int(top + h * PATH_FIELD_REL[1])
        pyautogui.click(fx, fy)
        time.sleep(0.2)
        pyautogui.hotkey("ctrl", "a")
        pyautogui.press("backspace")
        pyautogui.typewrite(TARGET_DIR)
        time.sleep(0.5)

     # 6) Нажимаем «Установить» (два шаблона подряд)
    inst_btn = None
    for key in ("install", "install2"):          # сначала install_button.png, потом install_button2.png
        try:
            inst_btn = locate_template(key)
            break                                # нашли – выходим из цикла
        except Exception as e:
            debug(f"{key} not found: {e}")

    if not inst_btn:
        raise TimeoutError("Neither install_button nor install_button2 found")

    ix, iy = pyautogui.center(inst_btn)
    debug(f"Clicking Install at {ix},{iy}")
    pyautogui.click(ix, iy)


    # 7) Ожидаем завершения
    wait_done()
    restore_vms()                            # возвращаем vms
    debug("Installation complete — ГОТОВО!")
    pyautogui.alert("ГОТОВО!")

# === Entry point ===
if __name__ == "__main__":
    if not is_admin():
        elevate(); sys.exit()
    try:
        main()
    except Exception:
        debug("ERROR:\n" + traceback.format_exc())
        pyautogui.alert("Ошибка — смотрите консоль")
        sys.exit(1)
