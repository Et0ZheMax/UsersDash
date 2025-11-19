import os
import sqlite3

# Путь к твоей базе. Если в конфиге стоит sqlite:///app.db,
# то файл лежит рядом с app.py и называется app.db
DB_PATH = os.path.join(os.path.dirname(__file__), "app.db")

def main():
    print(f"[MIGRATION] Using DB: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # смотрим список колонок в farm_data
    cur.execute("PRAGMA table_info(farm_data)")
    cols = [row[1] for row in cur.fetchall()]
    print(f"[MIGRATION] farm_data columns before: {cols}")

    if "telegram_tag" not in cols:
        print("[MIGRATION] Adding column telegram_tag to farm_data...")
        cur.execute("ALTER TABLE farm_data ADD COLUMN telegram_tag VARCHAR(64)")
        conn.commit()
        print("[MIGRATION] Column telegram_tag added.")
    else:
        print("[MIGRATION] Column telegram_tag already exists, nothing to do.")

    cur.execute("PRAGMA table_info(farm_data)")
    cols_after = [row[1] for row in cur.fetchall()]
    print(f"[MIGRATION] farm_data columns after: {cols_after}")

    conn.close()

if __name__ == "__main__":
    main()
