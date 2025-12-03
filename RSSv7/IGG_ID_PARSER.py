#!/usr/bin/env python3
import os
import re
import json
from datetime import datetime

# --- Параметры ---
LOG_FOLDER       = r"C:\Program Files\GnBots\logs"            # основная папка с логами
EXTRA_LOG_FOLDER = r"C:\Program Files\GnBots\logs"                        # если не нужна — оставьте "" или None
PROFILE_FILE     = r"C:/Program Files/GnBots/profiles/FRESH_NOX.json"
OUTPUT_FILE      = r"C:\LDPlayer\ldChecker\list_ids.json"

# регулярка для поиска "|account_id| … List IDs: … : list_id"
pattern = re.compile(r"\|([0-9a-f]{8,32})\|.*List IDs:.*:\s*(\d+)", re.IGNORECASE)

def load_account_mapping(path: str) -> dict[str, str]:
    """Загружает из FRESH_NOX.json mapping account_id → nickname."""  
    mapping = {}
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    records = data if isinstance(data, list) else [data]
    for rec in records:
        acct_id = rec.get("Id") or rec.get("id")
        name    = rec.get("Name") or rec.get("name")
        if acct_id and name:
            mapping[str(acct_id).lower()] = str(name)
    return mapping  # :contentReference[oaicite:0]{index=0}&#8203;:contentReference[oaicite:1]{index=1}

def scan_file(path: str, id2name: dict, seen: set, results: list):
    """Сканирует один файл и дописывает найденные записи в results."""
    try:
        with open(path, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                if "List IDs" not in line:
                    continue
                m = pattern.search(line)
                if not m:
                    continue
                acct_id = m.group(1).lower()
                list_id = m.group(2)
                key = (acct_id, list_id)
                if key in seen:
                    continue
                seen.add(key)
                results.append({
                    "nickname":   id2name.get(acct_id, acct_id),
                    "account_id": acct_id,
                    "list_id":    list_id
                })
    except Exception as e:
        print(f"Ошибка при чтении {path}: {e}")

def main():
    id2name    = load_account_mapping(PROFILE_FILE)
    today_pref = "bot" + datetime.now().strftime("%Y%m%d")  # e.g. "bot20250420"

    results = []
    seen    = set()

    # 1) Сканируем логи сегодняшнего дня в LOG_FOLDER
    for root, _, files in os.walk(LOG_FOLDER):
        for fname in files:
            if not fname.startswith(today_pref) or not fname.lower().endswith(".txt"):
                continue
            scan_file(os.path.join(root, fname), id2name, seen, results)

    # 2) Если указана EXTRA_LOG_FOLDER, сканируем там все .txt
    if EXTRA_LOG_FOLDER:
        extra_folder = os.path.abspath(EXTRA_LOG_FOLDER)
        if os.path.isdir(extra_folder):
            for root, _, files in os.walk(extra_folder):
                for fname in files:
                    if fname.lower().endswith(".txt"):
                        scan_file(os.path.join(root, fname), id2name, seen, results)
        else:
            print(f"Внимание: папка не найдена — {extra_folder}")

    # 3) Сохраняем результаты
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as out:
        json.dump(results, out, ensure_ascii=False, indent=2)

    print(f"Сохранено {len(results)} записей в {OUTPUT_FILE}")

if __name__ == "__main__":
    main()

