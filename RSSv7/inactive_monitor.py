#!/usr/bin/env python3
# ░░░  inactive_monitor.py  ░░░
"""
Ищет аккаунты, у которых НЕТ дневного прироста (dayGain==0) И прошло более
THRESH_HOURS часов с момента last_updated. Пишет два JSON и шлёт Telegram.

— Использует ту же БД (resources_web.db), что и RssCounterWeb.
— Аккаунты с "Active": false (в PROFILE) игнорируются.
— Дедупликация уведомлений: шлём только изменения состава списка.
"""

from __future__ import annotations

import ctypes
import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Tuple

import requests

# ─────────────────────────── Константы / настройка ───────────────────────────
BASE_DIR = Path(__file__).resolve().parent
RESOURCES_DB = BASE_DIR / "resources_web.db"
CONFIG_PATH  = BASE_DIR / "config.json"

ALERT_SHORT = BASE_DIR / "inactive15.json"       # лёгкий список для фронта
ALERT_FULL  = BASE_DIR / "inactive_alerts.json"  # подробности для админа
STATE_FILE  = BASE_DIR / "inactive_state.json"   # кого слали в прошлый раз

THRESH_HOURS = int(os.getenv("INACTIVE_HOURS", "15"))
TELEGRAM_TOKEN = os.getenv("TG_TOKEN", "")
TELEGRAM_CHAT  = os.getenv("TG_CHAT", "")
TELEGRAM_MAX_LINES = 50  # не сыпем простыню в ТГ — при необходимости режем

# ─────────────────────────── Утилиты ───────────────────────────
def _ensure_admin() -> None:
    """Windows: перезапустить с правами администратора. На *nix просто предупреждение."""
    try:
        if os.name == "nt":
            if ctypes.windll.shell32.IsUserAnAdmin():
                return
            print("[health-check] Требуются права администратора, перезапуск...")
            params = " ".join(f'"{a}"' for a in sys.argv)
            ctypes.windll.shell32.ShellExecuteW(None, "runas", sys.executable, params, None, 1)
            sys.exit(0)
        else:
            if hasattr(os, "geteuid") and os.geteuid() != 0:
                print("[health-check] Желательно запускать от root (не обязательно).")
    except Exception:
        pass

def _health_check() -> bool:
    """Проверяем файлы и доступ к БД (read-only)."""
    ok = True
    print("─── Health-check ─────────────────────────────────────")
    for p, name in [(RESOURCES_DB, "resources_web.db"), (CONFIG_PATH, "config.json")]:
        if p.is_file():
            print(f"✓ найдено: {name}")
        else:
            print(f"✗ нет файла: {name}")
            ok = False
    if RESOURCES_DB.is_file():
        try:
            sqlite3.connect(f"file:{RESOURCES_DB}?mode=ro", uri=True).close()
            print("✓ SQLite читается (ro)")
        except Exception as e:
            print(f"✗ Ошибка чтения БД: {e}")
            ok = False
    print("──────────────────────────────────────────────────────")
    return ok

def _tz_aware_from_iso(s: str) -> datetime | None:
    """Безопасный парс ISO-строки. Если tz отсутствует — считаем локальную зону."""
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.now().astimezone().tzinfo or timezone.utc)
        return dt
    except Exception:
        return None

def _telegram(text: str) -> None:
    """Отправка сообщения в Telegram. Молчит при ошибке/отсутствии реквизитов."""
    if not (TELEGRAM_TOKEN and TELEGRAM_CHAT and text):
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": TELEGRAM_CHAT, "text": text, "parse_mode": "HTML"}, timeout=15)
    except Exception as e:
        print("[telegram] error:", e, flush=True)

def _load_active_ids_from_profile() -> set | None:
    """Читаем PROFILE_PATH из config.json и набираем Id активных аккаунтов."""
    try:
        cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        prof_path = Path(cfg.get("PROFILE_PATH", ""))
        if not prof_path.is_file():
            return None
        prof = json.loads(prof_path.read_text(encoding="utf-8"))
        return {x.get("Id") for x in prof if x and x.get("Active")}
    except Exception:
        return None

# ─────────────────────────── Работа с БД ───────────────────────────
def _query_resources() -> List[Tuple[str, str, int, int, int, int, str]]:
    """
    SELECT id, nickname, food, wood, stone, gold, last_updated FROM resources
    Возвращаем список кортежей.
    """
    conn = sqlite3.connect(RESOURCES_DB)
    rows = conn.execute(
        "SELECT id, nickname, food, wood, stone, gold, last_updated FROM resources"
    ).fetchall()
    conn.close()
    return rows

def _load_today_baseline() -> Dict[str, Tuple[int, int, int, int]]:
    """baseline за сегодня: id -> (bf, bw, bs, bg). gems не участвуют в dayGain."""
    today = datetime.now().strftime("%Y-%m-%d")
    conn = sqlite3.connect(RESOURCES_DB)
    rows = conn.execute("""
        SELECT id, food, wood, stone, gold
        FROM daily_baseline
        WHERE baseline_date=?
    """, (today,)).fetchall()
    conn.close()
    return {rid: (bf, bw, bs, bg) for (rid, bf, bw, bs, bg) in rows}

# ─────────────────────────── Основная логика ───────────────────────────
def check_inactive_accounts(threshold_hrs: int = THRESH_HOURS) -> List[dict]:
    """
    Ищем аккаунты, у которых dayGain==0 И last_updated старше threshold.
    Пишем файлы и шлём ТГ (с дедупликацией).
    """
    threshold = timedelta(hours=threshold_hrs)
    active_ids = _load_active_ids_from_profile()
    baseline = _load_today_baseline()
    rows = _query_resources()

    # фильтр по активным
    if active_ids is not None:
        rows = [r for r in rows if r[0] in active_ids]

    now = datetime.now(timezone.utc)
    offenders: List[dict] = []

    for acc_id, nick, f, w, s, g, last in rows:
        dt = _tz_aware_from_iso(last)
        if not dt:
            continue

        # считаем dayGain (по food+wood+stone+gold)
        bf, bw, bs, bg = baseline.get(acc_id, (0, 0, 0, 0))
        day_gain = (f - bf) + (w - bw) + (s - bs) + (g - bg)

        if day_gain != 0:
            continue  # есть прирост — не шлём

        if now - dt > threshold:
            offenders.append({
                "id": acc_id,
                "nickname": nick,
                "last": dt.isoformat(),
                "hours": round((now - dt).total_seconds() / 3600, 1),
                "day_gain": 0
            })

    # ── сохраняем json ────────────────────────────────────────────────
    try:
        ALERT_SHORT.write_text(
            json.dumps(
                [{"nickname": o["nickname"], "hours": o["hours"]} for o in offenders],
                ensure_ascii=False,
                indent=2
            ),
            encoding="utf-8"
        )
        ALERT_FULL.write_text(json.dumps(offenders, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        print("[file-write] error:", e)

    # ── дедупликация и Telegram ───────────────────────────────────────
    try:
        prev = set()
        if STATE_FILE.is_file():
            prev = set(json.loads(STATE_FILE.read_text(encoding="utf-8")).get("ids", []))

        cur = set(o["id"] for o in offenders)
        diff_added = [o for o in offenders if o["id"] not in prev]
        diff_removed = [pid for pid in prev if pid not in cur]

        # сохраняем текущее состояние для следующего запуска
        STATE_FILE.write_text(json.dumps({"ids": list(cur)}, ensure_ascii=False, indent=2), encoding="utf-8")

        # шлём только, если что-то изменилось
        if diff_added or diff_removed:
            lines = [f"⚠️ <b>Без прироста > {threshold_hrs} ч</b> (dayGain=0)"]
            if diff_added:
                for o in sorted(diff_added, key=lambda x: x["hours"], reverse=True):
                    ts = _tz_aware_from_iso(o["last"]).astimezone() if o["last"] else None
                    when = ts.strftime("%d.%m %H:%M") if ts else "?"
                    lines.append(f"❗ {o['nickname']} — {when}  ({o['hours']} ч)")
            if diff_removed:
                lines.append("")
                lines.append("✅ Вышли из списка: " + ", ".join(diff_removed))

            # усечение
            if len(lines) > TELEGRAM_MAX_LINES:
                keep = TELEGRAM_MAX_LINES - 2
                cut = len(lines) - keep
                lines = lines[:keep] + [f"… и ещё {cut} строк"]

            _telegram("\n".join(lines))
    except Exception as e:
        print("[notify] error:", e)

    return offenders

# ─────────────────────────── CLI ───────────────────────────
if __name__ == "__main__":
    _ensure_admin()
    if not _health_check():
        sys.exit(1)

    t0 = time.time()
    lst = check_inactive_accounts()
    if lst:
        print("Inactive:", ", ".join(o["nickname"] for o in lst))
    else:
        print("✓ Все активны")
    print(f"Done in {time.time() - t0:.2f}s")
