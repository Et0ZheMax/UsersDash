"""Minimalistic GUI tool to view / edit Email, Password, IGG ID for every account
in a GnBots profile JSON (FRESH_NOX.json‑like).

Key points
==========
* **Only edited rows are rewritten** – untouched accounts stay byte‑for‑byte identical.
* A *.bak* backup of the original JSON is created once.
* Extra: **Export** current table to **CSV** or **Excel** (.xlsx).
  *Exports never modify the JSON file.*

Usage
-----
1. **Open config** – pick a JSON profile.
2. Table appears: *Account | Email | Password | IGG ID*.
3. Edit as needed.
4. **Save** – updates only changed rows.
5. **Export ▶ CSV / Excel** – choose where to write a snapshot of the visible data.

Dependencies
------------
* Python ≥3.8   (built‑in `tkinter`, `csv`)
* For .xlsx export: `openpyxl` (`pip install openpyxl`) – if missing, button is disabled.
"""

import json, csv, sys
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from pathlib import Path
from copy import deepcopy
from hashlib import md5

try:
    import openpyxl  # type: ignore
    XLSX_ENABLED = True
except ImportError:
    XLSX_ENABLED = False

MENU_TEMPLATE = {
    "ScriptId": "appmenu",
    "OrderId": 1,
    "Config": {"Shuffle": False, "Slot": "igg", "Custom": "", "Email": "", "Password": ""},
    "Id": 1, "IsActive": True, "IsCopy": False,
    "ScheduleData": {"Active": False, "Last": "0001-01-01T00:00:00", "Daily": False, "Hourly": False, "Weekly": False},
    "ScheduleRules": []
}

COLS = ("email", "password", "igg")
HDRS = ("Account", "Email", "Password", "IGG ID")

class AccountRow:
    def __init__(self, master: tk.Frame, row: int, acc: dict):
        self.acc = acc
        self.vars = {c: tk.StringVar() for c in COLS}
        self.make_widgets(master, row)
        self.orig_hash = self.hash_menu()

    # ----------------- helpers -----------------
    def extract_menu(self):
        try:
            md = json.loads(self.acc.get("MenuData", "{}"))
            cfg = md.get("Config", {})
            return cfg.get("Email", ""), cfg.get("Password", ""), cfg.get("Custom", "")
        except Exception:
            return "", "", ""

    def hash_menu(self):
        return md5(self.acc.get("MenuData", "").encode()).hexdigest()

    # ----------------- GUI -----------------
    def make_widgets(self, master, row):
        email, pwd, igg = self.extract_menu()
        self.vars["email"].set(email)
        self.vars["password"].set(pwd)
        self.vars["igg"].set(igg)
        tk.Label(master, text=self.acc.get("Name", f"Acc {row}"), anchor="w") \
            .grid(row=row, column=0, sticky="nsew", padx=2, pady=1)
        for col, key in enumerate(COLS, start=1):
            ttk.Entry(master, textvariable=self.vars[key], width=24) \
                .grid(row=row, column=col, sticky="nsew", padx=2, pady=1)

    # ----------------- public -----------------
    def changed(self):
        tpl = deepcopy(MENU_TEMPLATE)
        tpl["Config"].update({
            "Email": self.vars["email"].get().strip(),
            "Password": self.vars["password"].get().strip(),
            "Custom": self.vars["igg"].get().strip(),
        })
        return md5(json.dumps(tpl, separators=(',', ':'), ensure_ascii=False).encode()).hexdigest() != self.orig_hash

    def apply(self):
        if not self.changed():
            return False
        tpl = deepcopy(MENU_TEMPLATE)
        tpl["Config"].update({
            "Email": self.vars["email"].get().strip(),
            "Password": self.vars["password"].get().strip(),
            "Custom": self.vars["igg"].get().strip(),
        })
        self.acc["MenuData"] = json.dumps(tpl, separators=(',', ':'), ensure_ascii=False)
        self.orig_hash = self.hash_menu()
        return True

    def snapshot(self):
        return (
            self.acc.get("Name", ""),
            self.vars["email"].get(),
            self.vars["password"].get(),
            self.vars["igg"].get(),
        )

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Account editor – Email / Password / IGG")
        self.geometry("840x540")
        self.resizable(True, True)
        # ---- top bar ----
        bar = ttk.Frame(self); bar.pack(fill=tk.X, padx=6, pady=4)
        ttk.Button(bar, text="Open config", command=self.open_file).pack(side=tk.LEFT, padx=2)
        ttk.Button(bar, text="Save", command=self.save_file).pack(side=tk.LEFT, padx=2)
        export_menu = tk.Menubutton(bar, text="Export ▾", relief=tk.RAISED)
        m = tk.Menu(export_menu, tearoff=0)
        m.add_command(label="CSV", command=self.export_csv)
        if XLSX_ENABLED:
            m.add_command(label="Excel (.xlsx)", command=self.export_xlsx)
        export_menu.config(menu=m)
        export_menu.pack(side=tk.LEFT, padx=2)
        self.file_lbl = ttk.Label(bar, text="<no file>"); self.file_lbl.pack(side=tk.LEFT, padx=10)
        # ---- table ----
        outer = ttk.Frame(self); outer.pack(fill=tk.BOTH, expand=True, padx=6, pady=4)
        canvas = tk.Canvas(outer); vsb = ttk.Scrollbar(outer, orient="vertical", command=canvas.yview)
        canvas.configure(yscrollcommand=vsb.set); vsb.pack(side=tk.RIGHT, fill=tk.Y); canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.tbl = ttk.Frame(canvas); canvas.create_window((0, 0), window=self.tbl, anchor="nw")
        self.tbl.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        for col, h in enumerate(HDRS):
            ttk.Label(self.tbl, text=h, font=("Segoe UI", 10, "bold")) \
                .grid(row=0, column=col, sticky="nsew", padx=2, pady=2)
            self.tbl.columnconfigure(col, weight=1)
        # ---- data ----
        self.rows, self.data, self.file_path = [], [], None

    # --------------- file ops ---------------
    def open_file(self):
        p = filedialog.askopenfilename(title="Choose profile JSON", filetypes=[("JSON", "*.json"), ("All", "*.*")])
        if not p: return
        self.file_path = Path(p); self.file_lbl.config(text=self.file_path.name)
        try:
            with open(self.file_path, "r", encoding="utf-8") as f:
                self.data = json.load(f)
                if not isinstance(self.data, list): raise ValueError("Expected JSON array")
        except Exception as e:
            messagebox.showerror("Error", str(e)); return
        self.populate()

    def save_file(self):
        if not self.file_path: return
        changed = sum(r.apply() for r in self.rows)
        if not changed:
            messagebox.showinfo("No changes", "Nothing modified."); return
        bak = self.file_path.with_suffix(".bak")
        if not bak.exists():
            try: bak.write_bytes(self.file_path.read_bytes())
            except Exception: pass
        try:
            with open(self.file_path, "w", encoding="utf-8") as f:
                json.dump(self.data, f, ensure_ascii=False, indent=2)
            messagebox.showinfo("Saved", f"Updated {changed} account(s)")
        except Exception as e:
            messagebox.showerror("Error", str(e))

    # --------------- table ---------------
    def populate(self):
        for w in self.tbl.winfo_children()[len(HDRS):]: w.destroy()
        self.rows.clear()
        for i, acc in enumerate(self.data, 1):
            self.rows.append(AccountRow(self.tbl, i, acc))

    # --------------- export ---------------
    def snapshot(self):
        return [r.snapshot() for r in self.rows]

    def export_csv(self):
        fn = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV", "*.csv")])
        if not fn: return
        try:
            with open(fn, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f); w.writerow(HDRS); w.writerows(self.snapshot())
            messagebox.showinfo("Exported", "CSV saved")
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def export_xlsx(self):
        if not XLSX_ENABLED: return
        fn = filedialog.asksaveasfilename(defaultextension=".xlsx", filetypes=[("Excel", "*.xlsx")])
        if not fn: return
        try:
            wb = openpyxl.Workbook(); ws = wb.active; ws.title = "Accounts"
            ws.append(HDRS)
            for row in self.snapshot():
                ws.append(row)
            for col in range(1, len(HDRS)+1):
                ws.column_dimensions[openpyxl.utils.get_column_letter(col)].width = 25
            wb.save(fn)
            messagebox.showinfo("Exported", "Excel file saved")
        except Exception as e:
            messagebox.showerror("Error", str(e))
if __name__ == "__main__":
    App().mainloop()
