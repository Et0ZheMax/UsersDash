#!/usr/bin/env python3
# ░░░  inactive_monitor.py  ░░░
"""
Ищет аккаунты, у которых НЕТ дневного прироста (dayGain==0) И прошло более
THRESH_HOURS часов с момента last_updated. Пишет два JSON и шлёт Telegram.

— Использует ту же БД (resources_web.db), что и RssCounterWeb.
— Аккаунты с "Active": false (в PROFILE) игнорируются.
— Старые зависания не скрываются: чем дольше простой, тем важнее алерт.
— Критичные зависания периодически напоминают о себе до восстановления.
"""

from __future__ import annotations

import ctypes
import html
import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Tuple

import requests

# Общая загрузка /.env из корня репозитория (без перезаписи системных env).
def _load_root_env() -> None:
    current_file = Path(__file__).resolve()
    for parent in (current_file.parent, *current_file.parents):
        if (parent / ".git").exists():
            if str(parent) not in sys.path:
                sys.path.insert(0, str(parent))
            break

    from shared.env_loader import load_root_env_file

    load_root_env_file(current_file)

# Загружаем переменные окружения и при импорте из веб-сервера, иначе TG/пороги
# могут оставаться пустыми до ручного запуска скрипта.
_load_root_env()

# ─────────────────────────── Константы / настройка ───────────────────────────
BASE_DIR = Path(__file__).resolve().parent
RESOURCES_DB = BASE_DIR / "resources_web.db"
CONFIG_PATH  = BASE_DIR / "config.json"

ALERT_SHORT = BASE_DIR / "inactive15.json"       # лёгкий список для фронта
ALERT_FULL  = BASE_DIR / "inactive_alerts.json"  # подробности для админа
STATE_FILE  = BASE_DIR / "inactive_state.json"   # кого слали в прошлый раз

TAG_TEXT = "0gain🍽️"

THRESH_HOURS = int(os.getenv("INACTIVE_HOURS", "6"))
CRITICAL_HOURS = int(os.getenv("INACTIVE_CRITICAL_HOURS", "10"))
REMINDER_HOURS = int(os.getenv("INACTIVE_REMINDER_HOURS", "12"))
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

def _telegram(text: str) -> bool:
    """Отправка сообщения в Telegram. Молчит при ошибке/отсутствии реквизитов."""
    if not (TELEGRAM_TOKEN and TELEGRAM_CHAT and text):
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        response = requests.post(
            url,
            json={"chat_id": TELEGRAM_CHAT, "text": text, "parse_mode": "HTML"},
            timeout=15,
        )
        response.raise_for_status()
        return True
    except Exception as e:
        print("[telegram] error:", e, flush=True)
        return False

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

        # Считаем dayGain (по food+wood+stone+gold). Если baseline за сегодня
        # не успел создаться, всё равно не скрываем критичный простой: stale-last_updated
        # важнее точной оценки дневного прироста.
        base_row = baseline.get(acc_id)
        day_gain = None
        if base_row:
            bf, bw, bs, bg = base_row
            day_gain = (f - bf) + (w - bw) + (s - bs) + (g - bg)

        hours_inactive = (now - dt).total_seconds() / 3600
        is_stale = now - dt >= threshold
        zero_gain_or_unknown = day_gain in (0, None)

        if is_stale and zero_gain_or_unknown:
            severity = "critical" if hours_inactive >= CRITICAL_HOURS else "warning"
            offenders.append({
                "id": acc_id,
                "nickname": nick,
                "last": dt.isoformat(),
                "hours": round(hours_inactive, 1),
                "day_gain": day_gain,
                "tag": TAG_TEXT if day_gain == 0 else "stale⏱️",
                "severity": severity,
            })

    # ── сохраняем json ────────────────────────────────────────────────
    try:
        ALERT_SHORT.write_text(
            json.dumps(
                [
                    {
                        "nickname": o["nickname"],
                        "hours": o["hours"],
                        "tag": o.get("tag", TAG_TEXT),
                    }
                    for o in offenders
                ],
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
        previous_accounts: dict[str, dict] = {}
        legacy_ids: set[str] = set()
        if STATE_FILE.is_file():
            previous_state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
            raw_accounts = previous_state.get("accounts", {})
            if isinstance(raw_accounts, dict):
                previous_accounts = {
                    str(acc_id): value
                    for acc_id, value in raw_accounts.items()
                    if isinstance(value, dict)
                }
            legacy_ids = set(previous_state.get("ids", []))

        cur = set(o["id"] for o in offenders)
        prev = set(previous_accounts) or legacy_ids
        current_ts = time.time()
        reminder_seconds = max(1, REMINDER_HOURS) * 3600
        to_notify: List[dict] = []

        for offender in offenders:
            old = previous_accounts.get(offender["id"], {})
            last_alert_at = float(old.get("last_alert_at") or 0)
            severity_changed = old.get("severity") != offender["severity"]
            critical_reminder_due = (
                offender["severity"] == "critical"
                and current_ts - last_alert_at >= reminder_seconds
            )
            if offender["id"] not in prev or severity_changed or critical_reminder_due:
                to_notify.append(offender)

        recovered = [
            previous_accounts[acc_id]
            for acc_id in previous_accounts.keys() - cur
        ]

        notification_sent = False
        if to_notify or recovered:
            critical = [o for o in to_notify if o["severity"] == "critical"]
            warning = [o for o in to_notify if o["severity"] != "critical"]
            lines = []
            if critical:
                lines.append("🚨 <b>КРИТИЧЕСКИ: аккаунты не работают и не собирают ресурсы</b>")
                for o in sorted(critical, key=lambda x: x["hours"], reverse=True):
                    ts = _tz_aware_from_iso(o["last"]).astimezone() if o["last"] else None
                    when = ts.strftime("%d.%m %H:%M") if ts else "?"
                    nickname = html.escape(str(o["nickname"] or o["id"]))
                    lines.append(f"🔴 <b>{nickname}</b> — нет данных с {when} ({o['hours']} ч)")
            if warning:
                if lines:
                    lines.append("")
                lines.append(f"⚠️ <b>Без фарма более {threshold_hrs} ч</b>")
                for o in sorted(warning, key=lambda x: x["hours"], reverse=True):
                    ts = _tz_aware_from_iso(o["last"]).astimezone() if o["last"] else None
                    when = ts.strftime("%d.%m %H:%M") if ts else "?"
                    nickname = html.escape(str(o["nickname"] or o["id"]))
                    lines.append(f"🟠 <b>{nickname}</b> — нет данных с {when} ({o['hours']} ч)")
            if recovered:
                lines.append("")
                names = [html.escape(str(o.get("nickname") or "аккаунт")) for o in recovered]
                lines.append("✅ Работа восстановлена: " + ", ".join(sorted(names)))

            # усечение
            if len(lines) > TELEGRAM_MAX_LINES:
                keep = TELEGRAM_MAX_LINES - 2
                cut = len(lines) - keep
                lines = lines[:keep] + [f"… и ещё {cut} строк"]

            notification_sent = _telegram("\n".join(lines))

        notified_ids = {o["id"] for o in to_notify} if notification_sent else set()
        state_accounts = {}
        for offender in offenders:
            old = previous_accounts.get(offender["id"], {})
            state_accounts[offender["id"]] = {
                "nickname": offender["nickname"],
                "last": offender["last"],
                "severity": offender["severity"],
                "last_alert_at": current_ts if offender["id"] in notified_ids else old.get("last_alert_at", 0),
            }

        # Состояние пишем после отправки: при ошибке Telegram повторим попытку через час.
        STATE_FILE.write_text(
            json.dumps(
                {"version": 2, "ids": sorted(cur), "accounts": state_accounts},
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
    except Exception as e:
        print("[notify] error:", e)

    return offenders

# ─────────────────────────── CLI ───────────────────────────
if __name__ == "__main__":
    _load_root_env()
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
