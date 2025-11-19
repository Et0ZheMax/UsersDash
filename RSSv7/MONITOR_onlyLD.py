#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Прозрачная плашка-монитор:
  • считает окна dnplayer.exe
  • показывает статус python-скрипта / окна / произвольных *.exe
  • ЛКМ — перетянуть | ПКМ — меню | F5 — обновить | Q/Esc — закрыть
  • --reset — сбросить сохранённую позицию
"""

###############################################################################
#                                  НАСТРОЙКИ                                  #
###############################################################################
MONITORED_SCRIPT    = "RssCounterWebV7"
SCRIPT_DISPLAY_NAME = "RssV7"

WINDOW_TITLE_MONITOR = ""
WINDOW_DISPLAY_NAME  = "GameWindow"

EXE_MONITOR = [("clo.exe", "CLO")]

FONT_SIZE_PX   = 16          # размер текста
DOT_DIAM_PX    = 15          # диаметр кружка
ROW_MARGIN_PX  = 4           # вертикальный зазор между строками
DOT_SHIFT_PX   = 3         # +n ↓ опускаем, −n ↑ поднимаем

REFRESH_MSEC   = 1000
WINDOW_W, WINDOW_H = 260, 130

###############################################################################
import sys, ctypes, psutil, argparse, platform, importlib.util
from flask import Flask, jsonify, request                    # ← НОВОЕ
import threading                                             # ← НОВОЕ
CONSOLE_TITLE = "MonitorLD"           # ← любое имя, что хотите видеть
if sys.platform == "win32":
    ctypes.windll.kernel32.SetConsoleTitleW(CONSOLE_TITLE)
from pathlib import Path
def _mod_ok(name): return importlib.util.find_spec(name) is not None
if platform.system().lower() != "windows" or not all(_mod_ok(p) for p in ("PyQt6","psutil")):
    sys.exit("Требуется Windows + PyQt6 + psutil")

from PyQt6.QtWidgets import (QApplication, QWidget, QLabel, QVBoxLayout,
                             QHBoxLayout, QMenu)
from PyQt6.QtCore    import Qt, QTimer, QSettings, QPoint
from PyQt6.QtGui     import QFont, QColor, QPainter, QMouseEvent, QGuiApplication

ORG, APP = "F99Tools", "LdCounter"
_INTERESTING = ("dnplayer.exe",)

###############################################################################
# ------------------------------- helpers ----------------------------------- #
def is_script_running(pattern: str) -> bool:
    base, stem = pattern.lower(), Path(pattern).stem.lower()
    keys = {base, stem, f"{stem}.py", f"{stem}.exe", f"{base}.py", f"{base}.exe"}
    for p in psutil.process_iter(['name','exe','cmdline']):
        try:
            blob = " ".join(filter(None, [p.info.get('name',''), p.info.get('exe',''),
                                          " ".join(p.info.get('cmdline') or [])])
                            ).lower().replace("\\","/")
            if any(k in blob for k in keys): return True
        except (psutil.AccessDenied, psutil.ZombieProcess):
            continue
    return False

def is_exe_running(name: str) -> bool:
    nm = name.lower()
    return any((p.info['name'] or '').lower()==nm for p in psutil.process_iter(['name']))

def window_exists(title: str) -> bool:
    if not title or sys.platform!="win32": return False
    found=False; u=ctypes.windll.user32
    CB=ctypes.c_bool; PROC=ctypes.WINFUNCTYPE(CB, ctypes.c_void_p, ctypes.c_void_p)
    def enum(hwnd,_):
        nonlocal found
        if u.IsWindowVisible(hwnd):
            ln=u.GetWindowTextLengthW(hwnd)
            if ln:
                buf=ctypes.create_unicode_buffer(ln+1)
                u.GetWindowTextW(hwnd, buf, ln+1)
                if title.lower() in buf.value.lower():
                    found=True; return False
        return True
    u.EnumWindows(PROC(enum),0); return found

###############################################################################
class Row(QWidget):
    """Одна строка: текст + круг-индикатор."""
    def __init__(self, text: str, ok: bool | None):
        super().__init__()

        h = QHBoxLayout(self)
        h.setContentsMargins(0, ROW_MARGIN_PX, 0, ROW_MARGIN_PX)
        h.setSpacing(8)

        lbl = QLabel(
            text,
            font=QFont("Segoe UI", FONT_SIZE_PX, QFont.Weight.Bold),
            alignment=Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter,
            styleSheet="color:white;",
        )
        lbl.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents)
        h.addWidget(lbl)

        if ok is not None:
            color = "#00ff00" if ok else "#ff0000"

            dot = QLabel(alignment=Qt.AlignmentFlag.AlignCenter)
            dot.setFixedSize(DOT_DIAM_PX, DOT_DIAM_PX)
            dot.setStyleSheet(
                f"background:{color}; border-radius:{DOT_DIAM_PX//2}px;"
            )
            dot.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents)

            # ── НОВОЕ: обёртка с верхним margin ───────────────────────────
            wrap = QWidget()
            wrap.setContentsMargins(0, DOT_SHIFT_PX, 0, 0)   # ← ключ: сдвиг вниз
            v = QVBoxLayout(wrap)
            v.setContentsMargins(0, 0, 0, 0); v.setSpacing(0)
            v.addWidget(dot)
            # ──────────────────────────────────────────────────────────────

            h.addWidget(wrap)



        h.addStretch()


###############################################################################
class Overlay(QWidget):
    def __init__(self, reset=False):
        super().__init__()
        self.setWindowFlags(Qt.WindowType.FramelessWindowHint|
                            Qt.WindowType.WindowStaysOnTopHint|
                            Qt.WindowType.Tool)
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
        self.setFixedSize(WINDOW_W, WINDOW_H)

        self.settings=QSettings(ORG,APP)
        self._restore_pos(reset)

        self.main_lay=QVBoxLayout(self)
        self.main_lay.setContentsMargins(10,10,10,10)
        self.main_lay.setSpacing(0)

        self._rows: list[Row]=[]
        self._drag_prev: QPoint|None=None

        QTimer(self, timeout=self.refresh).start(REFRESH_MSEC)
        self.refresh()

    # --------------------------- ядро обновления -------------------- #
    @staticmethod
    def _cnt(names): return sum(1 for p in psutil.process_iter(['name'])
                                if p.info['name'] in names)

    def refresh(self):
        data=[
            ("Запущено окон: "+str(self._cnt(_INTERESTING)), None),
            (SCRIPT_DISPLAY_NAME or MONITORED_SCRIPT,
             is_script_running(MONITORED_SCRIPT))
        ]
        if WINDOW_TITLE_MONITOR:
            data.append((WINDOW_DISPLAY_NAME or WINDOW_TITLE_MONITOR,
                         window_exists(WINDOW_TITLE_MONITOR)))
        for exe,disp in EXE_MONITOR:
            data.append((disp, is_exe_running(exe)))

        # пересобираем строки
        for r in self._rows: self.main_lay.removeWidget(r); r.deleteLater()
        self._rows=[]
        for txt,ok in data:
            row=Row(txt, ok)
            self.main_lay.addWidget(row)
            self._rows.append(row)

    # --------------------------- фон-плашка ------------------------- #
    def paintEvent(self,_):
        p=QPainter(self); p.setRenderHint(QPainter.RenderHint.Antialiasing)
        p.setBrush(QColor(0,0,0,128)); p.setPen(Qt.PenStyle.NoPen)
        p.drawRoundedRect(self.rect(),15,15)

    # --------------------------- drag-n-drop ------------------------ #
    def mousePressEvent(self,e:QMouseEvent):
        if e.button()==Qt.MouseButton.LeftButton:
            self._drag_prev=e.globalPosition().toPoint()
        elif e.button()==Qt.MouseButton.RightButton:
            self._menu(e.globalPosition().toPoint())
    def mouseMoveEvent(self,e):
        if self._drag_prev:
            d=e.globalPosition().toPoint()-self._drag_prev
            self.move(self.x()+d.x(), self.y()+d.y())
            self._drag_prev=e.globalPosition().toPoint()
    def mouseReleaseEvent(self,_): self._drag_prev=None

    # --------------------------- контекст-меню ---------------------- #
    def _menu(self,pos):
        m=QMenu()
        m.addAction("Сбросить позицию", lambda:self._restore_pos(True))
        m.addSeparator(); m.addAction("Закрыть", self.close)
        m.exec(pos)

    # --------------------------- позиция/сохр ----------------------- #
    def _restore_pos(self,reset):
        if reset:
            self.move(100,100); self.settings.remove("pos"); return
        pos:QPoint=self.settings.value("pos", type=QPoint)
        if pos and not pos.isNull():
            if any(scr.geometry().contains(pos) for scr in QGuiApplication.screens()):
                self.move(pos); return
        self.move(100,100)
    def closeEvent(self,e):
        self.settings.setValue("pos", self.pos()); super().closeEvent(e)
    def keyPressEvent(self,e):
        if e.key()==Qt.Key.Key_F5: self.refresh()
        elif e.key() in (Qt.Key.Key_Q,Qt.Key.Key_Escape): self.close()
        else: super().keyPressEvent(e)


# -------------------------------------------------------------------- HTTP mini-API
app_api = Flask(__name__)

@app_api.route('/api/scriptStatus')
def api_script_status():
    """
    GET /api/scriptStatus?script=RssCounterWebV7  → {"running": true|false}
    Используем is_script_running() – она ловит и exe, и *.py*, и вызовы через python.exe.
    """
    name = request.args.get('script', '')
    return jsonify({"running": is_script_running(name)})



@app_api.route('/api/dnCount')
def api_dn_count():
    """
    GET /api/dnCount
    ➜ {"dnCount": <кол-во-окон>}
    """
    cnt = sum(1 for p in psutil.process_iter(['name'])
              if (p.info.get('name') or '').lower() == 'dnplayer.exe')
    return jsonify({"dnCount": cnt})



###############################################################################
if __name__ == "__main__":
    p = argparse.ArgumentParser(add_help=False)
    p.add_argument("--reset", action="store_true")
    args = p.parse_args()

    app = QApplication(sys.argv)
    ol = Overlay(reset=args.reset)
    ol.show()

    # ───────────────────── HTTP-API запускаем в фоне ─────────────────────
    threading.Thread(
        target=lambda: app_api.run(
            host="0.0.0.0",      # слушаем на всех интерфейсах
            port=5016,           # <— убедитесь, что такой же порт прописан в central_config.json
            threaded=True,
            debug=False
        ),
        daemon=True              # закроется вместе с основным процессом
    ).start()

    sys.exit(app.exec())         # Qt-цикл - блокирующий: ставим ПОСЛЕ Thread-старта
