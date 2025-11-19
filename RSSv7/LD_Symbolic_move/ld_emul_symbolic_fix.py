import os
import re
import json
import shutil
import subprocess
import threading
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox, filedialog
from typing import List, Dict, Optional

# ---------- DEFAULT PATHS (editable via GUI) ----------
LD_PATH  = r"C:\LDPlayer\LDPlayer9"
SRC_ROOT = r"E:\vmss"
CFG_ROOT = r"E:\vmss\config"
DST_ROOT = os.path.join(LD_PATH, "vms")
# ------------------------------------------------------

CHUNK = 2 * 1024 * 1024  # 2 MB

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("LDPlayer Manager")
        self.geometry("960x620")

        self.instances: List[Dict[str, str]] = []
        self.current: Optional[int] = None

        self._build_ui()
        self._load_instances()

    # ---------- UI ---------------------------------------------------------
    def _build_ui(self):
        paned = tk.PanedWindow(self, sashrelief="raised")
        paned.pack(fill="both", expand=True)

        # Log + progress
        left = tk.Frame(paned)
        self.log = scrolledtext.ScrolledText(left, state="disabled",
                                             wrap="word", height=22)
        self.log.pack(fill="both", expand=True, padx=5, pady=(5, 0))

        self.pbar = ttk.Progressbar(left, orient="horizontal",
                                    mode="determinate", maximum=100)
        self.pbar.pack(fill="x", padx=5, pady=(0, 1))

        self.plabel = tk.Label(left, text="0.0 %")
        self.plabel.pack(pady=(0, 6))
        paned.add(left)

        # Tree
        right = tk.Frame(paned)
        self.tree = ttk.Treeview(right,
                                 columns=("Account", "Status"),
                                 show="headings", height=22)
        self.tree.heading("Account", text="Account")
        self.tree.heading("Status",  text="Status")
        self.tree.pack(fill="both", expand=True, padx=5, pady=5)
        # клик по строке — запускаем именно этот аккаунт
        self.tree.bind("<Double-1>", self._on_tree_click)
        paned.add(right)

        # Button bar
        bar = tk.Frame(self); bar.pack(fill="x", pady=4)
        tk.Button(bar, text="⚙", width=3,
                  command=self.open_settings).pack(side="left", padx=4)

        self.start_btn = tk.Button(bar, text="Start Next",
                                   command=self.process_next)
        self.start_btn.pack(side="left", padx=6)

        self.auto_var = tk.BooleanVar(self, value=False)
        tk.Checkbutton(bar, text="Auto Next",
                       variable=self.auto_var).pack(side="left")

        self.done_btn = tk.Button(bar, text="Эмуль загружен",
                                  state="disabled",
                                  command=self.on_ready)
        self.done_btn.pack(side="right", padx=6)

    # ---------- logging / progress ----------------------------------------
    def log_msg(self, txt: str):
        self.log.config(state="normal")
        self.log.insert("end", txt + "\n")
        self.log.see("end")
        self.log.config(state="disabled")

    def progress_set(self, pct: float):
        self.pbar["value"] = pct
        self.plabel.config(text=f"{pct:5.1f} %")
        self.update_idletasks()

    # ---------- settings dialog -------------------------------------------
    def open_settings(self):
        dlg = tk.Toplevel(self); dlg.title("Settings")

        paths = {"LD_PATH": LD_PATH,
                 "SRC_ROOT": SRC_ROOT,
                 "DST_ROOT": DST_ROOT}
        entries = {}

        def choose(entry: tk.Entry):
            path = filedialog.askdirectory(initialdir=entry.get())
            if path:
                entry.delete(0, "end"); entry.insert(0, path)

        for i, (k, v) in enumerate(paths.items()):
            tk.Label(dlg, text=k).grid(row=i, column=0, sticky="e")
            e = tk.Entry(dlg, width=60); e.insert(0, v)
            e.grid(row=i, column=1, padx=4, pady=2)
            tk.Button(dlg, text="…",
                      command=lambda ent=e: choose(ent)
                     ).grid(row=i, column=2)
            entries[k] = e

        def save():
            global LD_PATH, SRC_ROOT, DST_ROOT, CFG_ROOT
            LD_PATH  = entries["LD_PATH" ].get()
            SRC_ROOT = entries["SRC_ROOT"].get()
            DST_ROOT = entries["DST_ROOT"].get()
            CFG_ROOT = os.path.join(SRC_ROOT, "config")
            dlg.destroy()
            self._reload_instances()

        tk.Button(dlg, text="Save",
                  command=save).grid(row=3, column=0, columnspan=3, pady=6)

    # ---------- list -------------------------------------------------------
    def _reload_instances(self):
        for iid in self.tree.get_children():
            self.tree.delete(iid)
        self.instances.clear()
        self._load_instances()
        self.progress_set(0)

    def _load_instances(self):
        for folder in sorted(os.listdir(SRC_ROOT)):
            if not re.fullmatch(r"leidian\d+", folder):
                continue
            phys = os.path.join(SRC_ROOT, folder)
            if (not os.path.isdir(phys)
                    or not os.path.exists(os.path.join(phys, "data.vmdk"))):
                continue

            num = int(folder[7:])          # 47 из leidian47
            cfg = os.path.join(CFG_ROOT, f"{folder}.config")
            base = f"leidian{num}"
            if os.path.exists(cfg):
                try:
                    data = json.load(open(cfg, encoding="utf-8"))
                    base = data.get("statusSettings.playerName", base)
                except Exception:
                    pass
            name = f"{base}_{num}"

            status = ("linked"
                      if os.path.lexists(os.path.join(DST_ROOT, folder))
                      else "pending")
            self.instances.append({"folder": folder,
                                   "name":   name,
                                   "status": status})

        self.instances.sort(key=lambda x: x["status"] != "pending")
        for inst in self.instances:
            tag = "green" if inst["status"] == "linked" else "red"
            self.tree.insert("",
                             "end",
                             iid=inst["folder"],
                             values=(inst["name"], inst["status"]),
                             tags=(tag,))
        self.tree.tag_configure("green", background="#d8ffd8")
        self.tree.tag_configure("red",   background="#ffd8d8")

    # ---------- start cycle -----------------------------------------------
    def _start_instance(self, folder: str):
        if self.start_btn["state"] == "disabled":
            return
        # find this pending
        target = next((i for i in self.instances
                       if i["folder"] == folder and i["status"] == "pending"),
                      None)
        if not target:
            return
        self.current = self.instances.index(target)
        self.start_btn.config(state="disabled")
        threading.Thread(target=self._move_and_launch,
                         args=(folder,),
                         daemon=True).start()

    def process_next(self):
        pending = next((i for i in self.instances
                        if i["status"] == "pending"),
                       None)
        if not pending:
            messagebox.showinfo("Done", "No pending instances")
            return
        self._start_instance(pending["folder"])

    def _on_tree_click(self, event):
        iid = self.tree.identify_row(event.y)
        if iid:
            self._start_instance(iid)

    # ---------- move → launch ---------------------------------------------
    def _move_and_launch(self, folder: str):
        src = os.path.join(SRC_ROOT, folder)
        dst = os.path.join(DST_ROOT, folder)
        idx = int(folder[7:])

        # already linked?
        if os.path.lexists(dst):
            self._after_linked(skip=True)
            return

        inst = self.instances[self.current]
        self.log_msg(f"=== START {inst['name']} (id {idx}) ===")

        # -------- перемещение с прогрессом ----------
        total = sum(os.path.getsize(os.path.join(r, f))
                    for r, _, fs in os.walk(src) for f in fs)
        transferred = 0
        os.makedirs(dst, exist_ok=True)
        self.progress_set(0)

        for root, _, files in os.walk(src):
            for f in files:
                sp = os.path.join(root, f)
                rp = os.path.relpath(sp, src)
                dp = os.path.join(dst, rp)
                os.makedirs(os.path.dirname(dp), exist_ok=True)

                with open(sp, "rb") as rf, open(dp, "wb") as wf:
                    while True:
                        buf = rf.read(CHUNK)
                        if not buf:
                            break
                        wf.write(buf)
                        transferred += len(buf)
                        self.progress_set(transferred / total * 100)

        shutil.rmtree(src)
        self.log_msg("Move complete")

        subprocess.Popen([os.path.join(LD_PATH, "ldconsole.exe"),
                          "launch", "--index", str(idx)],
                         stdout=subprocess.DEVNULL,
                         stderr=subprocess.DEVNULL)
        self.log_msg("Waiting for «Эмуль загружен»")
        self.done_btn.config(state="normal")

    # ---------- after user click ------------------------------------------
    def on_ready(self):
        inst = self.instances[self.current]
        folder = inst["folder"]
        dst = os.path.join(DST_ROOT, folder)
        src = os.path.join(SRC_ROOT, folder)
        idx = int(folder[7:])

        self.done_btn.config(state="disabled")
        self.log_msg("Stopping emulator…")
        subprocess.run([os.path.join(LD_PATH, "ldconsole.exe"),
                        "quit", "--index", str(idx)],
                       stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL)

        self.log_msg("Moving back to E:")
        shutil.move(dst, src)

        self.log_msg("Creating junction")
        subprocess.run(f'cmd /c mklink /J "{dst}" "{src}"',
                       shell=True,
                       stdout=subprocess.DEVNULL)

        self._after_linked()

    # ---------- wrap-up ----------------------------------------------------
    def _after_linked(self, skip: bool = False):
        inst = self.instances[self.current]
        inst["status"] = "linked"
        self.tree.item(inst["folder"],
                       values=(inst["name"], "linked"),
                       tags=("green",))
        if not skip:
            self.log_msg(f"{inst['folder']} done\n")

        self.progress_set(0)
        self.start_btn.config(state="normal")
        if self.auto_var.get():
            self.process_next()


if __name__ == "__main__":
    App().mainloop()
