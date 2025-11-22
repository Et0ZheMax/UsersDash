import os
import json
import re
import stat
import sqlite3
import psutil
import subprocess
import shlex
import time
import shutil
import ctypes
import socket
import typing as t
import paramiko
import requests
from io import BytesIO
from PIL import ImageGrab
import base64
import pythoncom
import wmi
import sys
import csv
import threading
import inactive_monitor   
from icmplib import ping as icmp_ping 
from pathlib import Path
from flask import jsonify, request
from datetime import datetime, timezone, date, timedelta 
from flask import Flask, jsonify, render_template, request
from flask_cors import CORS

# Установка всего: python -m pip install -U psutil paramiko requests Pillow pywin32 WMI icmplib Flask Flask-Cors

# Пример: задаём своему скрипту заголовок «MyUniqueScript»
title = "RssV7_F99"
if sys.platform == "win32":
    ctypes.windll.kernel32.SetConsoleTitleW(title)

# -------------------------------------------------
# Функции для запуска с правами администратора
# -------------------------------------------------
def is_admin() -> bool:
    try:
        return ctypes.windll.shell32.IsUserAnAdmin()
    except Exception:
        return False

if not is_admin():
    # Перезапуск под админом
    script = os.path.abspath(sys.argv[0])
    params = " ".join([f'"{script}"'] + [f'"{arg}"' for arg in sys.argv[1:]])
    ctypes.windll.shell32.ShellExecuteW(None, "runas", sys.executable, params, None, 1)
    sys.exit(0)


DEBUG = True  # или False, если не нужен режим отладки

# -------------------------------------------------
# Функции health_check
# -------------------------------------------------

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

CONFIG_PATH = os.path.join(BASE_DIR, "config.json")

def _write_default_config(path):
    default = {
        "LOGS_DIR": r"C:\Program Files (x86)\GnBots\logs",
        "PROFILE_PATH": r"C:\Program Files (x86)\GnBots\config\profiles.json",
        "SRC_VMS": r"D:\Backups\VMs",
        "DST_VMS": r"D:\Prod\VMs",
        "GNBOTS_SHORTCUT": r"C:\Program Files (x86)\GnBots\GnBots.lnk",
        "TELEGRAM_TOKEN": "",
        "TELEGRAM_CHAT_ID": ""
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(default, f, ensure_ascii=False, indent=2)

# 1) Загружаем/создаём конфиг
if not os.path.isfile(CONFIG_PATH):
    print("[CONFIG] config.json not found, creating default…")
    _write_default_config(CONFIG_PATH)

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    CONFIG = json.load(f)

# 2) Единый источник путей из конфига (убираем дубли: LOG_DIR != LOGS_DIR)
LOGS_DIR        = CONFIG.get("LOGS_DIR", r"C:\Program Files (x86)\GnBots\logs")
PROFILE_PATH    = CONFIG.get("PROFILE_PATH", "")
SRC_VMS         = CONFIG.get("SRC_VMS", "")
DST_VMS         = CONFIG.get("DST_VMS", "")
GNBOTS_SHORTCUT = CONFIG.get("GNBOTS_SHORTCUT", "")

# 3) БД
RESOURCES_DB = os.path.join(BASE_DIR, "resources_web.db")
LOGS_DB      = os.path.join(BASE_DIR, "logs_cache.db")

# 4) Телега — из ENV имеет приоритет, затем config.json
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN", CONFIG.get("TELEGRAM_TOKEN", ""))
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", CONFIG.get("TELEGRAM_CHAT_ID", ""))

# 5) Health-check (создаст папки БД, проверит конфиг)
def health_check():
    errs = []
    if not os.path.isfile(CONFIG_PATH):
        errs.append(f"CONFIG not found: {CONFIG_PATH}")
    for db in (RESOURCES_DB, LOGS_DB):
        d = os.path.dirname(db)
        try:
            os.makedirs(d, exist_ok=True)
        except Exception as e:
            errs.append(f"Cannot create dir {d}: {e}")
    if not LOGS_DIR or not os.path.isdir(os.path.dirname(LOGS_DIR)):
        # Мягкая проверка, путь к логам может быть на другом диске
        print(f"[HEALTH-CHECK] LOGS_DIR='{LOGS_DIR}' (exists: {os.path.isdir(LOGS_DIR)})")
    if errs:
        for e in errs: print("[HEALTH-CHECK ERROR]", e)
        sys.exit(1)
    print("[HEALTH-CHECK] OK.")

health_check()


# ──────────── Ш А Б Л О Н Ы ────────────
TEMPLATES = {
    "650": r"""[{"ScriptId":"vikingbot.base.gathervip","Uid":"vikingbot.base.gathervip_3","OrderId":6,"Config":{"LevelStartAt":{"value":"3","options":["1","2","3","4","5","6"]},"Monster":false,"Niflung":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Divine":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Farm":{"value":"3","options":["off","1","2","3","4","5","6","7","8","9"]},"Sawmill":{"value":"3","options":["off","1","2","3","4","5","6","7","8","9"]},"Quarry":{"value":"3","options":["off","1","2","3","4","5","6","7","8","9"]},"Gold":{"value":"3","options":["off","1","2","3","4","5","6","7","8","9"]},"RallyTime":{"value":"5min","options":["5min","10min","30min","8hours"]},"reduceLevel":false,"marches":"5","farmLowestResource":false},"Id":6,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.gathervip","Uid":"vikingbot.base.gathervip_4","OrderId":6,"Config":{"LevelStartAt":{"value":"3","options":["1","2","3","4","5","6"]},"Monster":false,"Niflung":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Divine":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Farm":{"value":"3","options":["off","1","2","3","4","5","6","7","8","9"]},"Sawmill":{"value":"3","options":["off","1","2","3","4","5","6","7","8","9"]},"Quarry":{"value":"3","options":["off","1","2","3","4","5","6","7","8","9"]},"Gold":{"value":"3","options":["off","1","2","3","4","5","6","7","8","9"]},"RallyTime":{"value":"5min","options":["5min","10min","30min","8hours"]},"reduceLevel":false,"marches":"5","farmLowestResource":false},"Id":6,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.buffs","Uid":"vikingbot.base.buffs","OrderId":2,"Config":{"Attack":{"value":"Off","options":["Off","Any"]},"Defense":{"value":"Off","options":["Off","Any"]},"Gather":{"value":"Any","options":["Off","Any"]},"Workers":{"value":"Off","options":["Off","Any"]},"Deception":{"value":"Off","options":["Off","Any"]},"Trade":{"value":"Off","options":["Off","Any"]},"Patrol":{"value":"Off","options":["Off","Any"]},"useGems":false},"Id":2,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|12:00 AM|1:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.buffs","Uid":"vikingbot.base.buffs_1","OrderId":6,"Config":{"Attack":{"value":"Off","options":["Off","Any"]},"Defense":{"value":"Off","options":["Off","Any"]},"Gather":{"value":"Any","options":["Off","Any"]},"Workers":{"value":"Off","options":["Off","Any"]},"Deception":{"value":"Off","options":["Off","Any"]},"Trade":{"value":"Off","options":["Off","Any"]},"Patrol":{"value":"Off","options":["Off","Any"]},"useGems":false},"Id":6,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|8:00 AM|9:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.buffs","Uid":"vikingbot.base.buffs_2","OrderId":6,"Config":{"Attack":{"value":"Off","options":["Off","Any"]},"Defense":{"value":"Off","options":["Off","Any"]},"Gather":{"value":"Any","options":["Off","Any"]},"Workers":{"value":"Off","options":["Off","Any"]},"Deception":{"value":"Off","options":["Off","Any"]},"Trade":{"value":"Off","options":["Off","Any"]},"Patrol":{"value":"Off","options":["Off","Any"]},"useGems":false},"Id":6,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|4:00 PM|5:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.gathervip","Uid":"vikingbot.base.gathervip_1","OrderId":3,"Config":{"LevelStartAt":{"value":"3","options":["1","2","3","4","5","6"]},"Monster":false,"Niflung":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Divine":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Farm":{"value":"3","options":["off","1","2","3","4","5","6","7","8","9"]},"Sawmill":{"value":"3","options":["off","1","2","3","4","5","6","7","8","9"]},"Quarry":{"value":"3","options":["off","1","2","3","4","5","6","7","8","9"]},"Gold":{"value":"3","options":["off","1","2","3","4","5","6","7","8","9"]},"RallyTime":{"value":"5min","options":["5min","10min","30min","8hours"]},"reduceLevel":false,"marches":"5","farmLowestResource":false},"Id":3,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]}]""",

    "PREM": r"""[{"ScriptId":"vikingbot.base.gathervip","Uid":"vikingbot.base.gathervip_2","OrderId":0,"Config":{"LevelStartAt":{"value":"6","options":["1","2","3","4","5","6"]},"Monster":false,"Niflung":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Divine":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Farm":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Sawmill":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Quarry":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Gold":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"RallyTime":{"value":"10min","options":["5min","10min","30min","8hours"]},"reduceLevel":false,"marches":"5","farmLowestResource":false},"Id":0,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.gathervip","Uid":"vikingbot.base.gathervip_2","OrderId":1,"Config":{"LevelStartAt":{"value":"6","options":["1","2","3","4","5","6"]},"Monster":false,"Niflung":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Divine":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Farm":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Sawmill":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Quarry":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Gold":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"RallyTime":{"value":"10min","options":["5min","10min","30min","8hours"]},"reduceLevel":false,"marches":"5","farmLowestResource":false},"Id":1,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.dailies","Uid":"vikingbot.base.dailies_2","OrderId":2,"Config":{"quest":true,"recruit":true,"vip":true,"worker":{"value":"Off","options":["Off","Common","Rare","Legend","All"]},"gems":true,"errands":{"value":"10","options":["Off","5","10","15","20"]},"specialFarmer":true,"skipVoyageLushLand":true,"events":false,"collectCrystals":true},"Id":2,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|3:00 AM|5:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.dailies","Uid":"vikingbot.base.dailies_2","OrderId":3,"Config":{"quest":true,"recruit":true,"vip":true,"worker":{"value":"Off","options":["Off","Common","Rare","Legend","All"]},"gems":true,"errands":{"value":"10","options":["Off","5","10","15","20"]},"specialFarmer":true,"skipVoyageLushLand":true,"events":false,"collectCrystals":true},"Id":3,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|12:00 PM|2:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.dailies","Uid":"vikingbot.base.dailies_2","OrderId":4,"Config":{"quest":true,"recruit":true,"vip":true,"worker":{"value":"Off","options":["Off","Common","Rare","Legend","All"]},"gems":true,"errands":{"value":"10","options":["Off","5","10","15","20"]},"specialFarmer":true,"skipVoyageLushLand":true,"events":false,"collectCrystals":true},"Id":4,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|6:00 PM|8:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.alliancedonation","Uid":"vikingbot.base.alliancedonation","OrderId":5,"Config":{"allianceGift":true,"allianceDonation":{"value":"Recommended","options":["Recommended","Development","Territory","War","Skills","Off"]}},"Id":5,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|12:00 AM|3:00 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.alliancedonation","Uid":"vikingbot.base.alliancedonation_2","OrderId":6,"Config":{"allianceGift":true,"allianceDonation":{"value":"Recommended","options":["Recommended","Development","Territory","War","Skills","Off"]}},"Id":6,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|10:00 AM|1:00 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.alliancedonation","Uid":"vikingbot.base.alliancedonation_1","OrderId":7,"Config":{"allianceGift":true,"allianceDonation":{"value":"Recommended","options":["Recommended","Development","Territory","War","Skills","Off"]}},"Id":7,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|7:00 PM|9:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.buffs","Uid":"vikingbot.base.buffs","OrderId":8,"Config":{"Attack":{"value":"Off","options":["Off","Any"]},"Defense":{"value":"Off","options":["Off","Any"]},"Gather":{"value":"Any","options":["Off","Any"]},"Workers":{"value":"Off","options":["Off","Any"]},"Deception":{"value":"Off","options":["Off","Any"]},"Trade":{"value":"Off","options":["Off","Any"]},"Patrol":{"value":"Off","options":["Off","Any"]},"useGems":false},"Id":8,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|12:00 AM|2:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.buffs","Uid":"vikingbot.base.buffs_1","OrderId":9,"Config":{"Attack":{"value":"Off","options":["Off","Any"]},"Defense":{"value":"Off","options":["Off","Any"]},"Gather":{"value":"Any","options":["Off","Any"]},"Workers":{"value":"Off","options":["Off","Any"]},"Deception":{"value":"Off","options":["Off","Any"]},"Trade":{"value":"Off","options":["Off","Any"]},"Patrol":{"value":"Off","options":["Off","Any"]},"useGems":false},"Id":9,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|8:00 AM|10:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.buffs","Uid":"vikingbot.base.buffs_2","OrderId":10,"Config":{"Attack":{"value":"Off","options":["Off","Any"]},"Defense":{"value":"Off","options":["Off","Any"]},"Gather":{"value":"Any","options":["Off","Any"]},"Workers":{"value":"Off","options":["Off","Any"]},"Deception":{"value":"Off","options":["Off","Any"]},"Trade":{"value":"Off","options":["Off","Any"]},"Patrol":{"value":"Off","options":["Off","Any"]},"useGems":false},"Id":10,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|4:00 PM|6:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.recruitment","Uid":"vikingbot.base.recruitment","OrderId":11,"Config":{"Infantry":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"Archer":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"Pikemen":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"Porter":{"value":"1","options":["Off","Auto","1","2","3","4","5","6","7"]},"Amount":{"value":"100%","options":["100%","75%","50%","25%"]},"UpgradeInfantry":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradeArcher":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradePikemen":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradePorter":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"useResources":true,"useSpeedUps":false},"Id":11,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|12:00 AM|2:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.recruitment","Uid":"vikingbot.base.recruitment_1","OrderId":12,"Config":{"Infantry":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"Archer":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"Pikemen":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"Porter":{"value":"1","options":["Off","Auto","1","2","3","4","5","6","7"]},"Amount":{"value":"100%","options":["100%","75%","50%","25%"]},"UpgradeInfantry":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradeArcher":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradePikemen":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradePorter":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"useResources":true,"useSpeedUps":false},"Id":12,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|8:00 AM|10:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.recruitment","Uid":"vikingbot.base.recruitment_2","OrderId":13,"Config":{"Infantry":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"Archer":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"Pikemen":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"Porter":{"value":"1","options":["Off","Auto","1","2","3","4","5","6","7"]},"Amount":{"value":"100%","options":["100%","75%","50%","25%"]},"UpgradeInfantry":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradeArcher":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradePikemen":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradePorter":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"useResources":true,"useSpeedUps":false},"Id":13,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|4:00 PM|6:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.upgrade","Uid":"vikingbot.base.upgrade","OrderId":14,"Config":{"Upgrade":{"value":"MainHall","options":["MainHall","Specfic","Villages"]},"EagleNest":false,"Warehouse":false,"HallofValor":false,"TribeHall":true,"DivinationShack":false,"Academy":false,"Watchtower":false,"Infirmary":false,"Infantry":false,"Archer":false,"Porter":false,"Pikemen":false,"SquadBase":false,"VillageHall":false,"Workshop":false,"Prison":false,"DefenderCamp":false,"SuppyHub":false,"Market":false,"useSpeedUps":false,"useResources":true},"Id":14,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|12:00 AM|2:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.upgrade","Uid":"vikingbot.base.upgrade_2","OrderId":15,"Config":{"Upgrade":{"value":"MainHall","options":["MainHall","Specfic","Villages"]},"EagleNest":false,"Warehouse":false,"HallofValor":false,"TribeHall":true,"DivinationShack":false,"Academy":false,"Watchtower":false,"Infirmary":false,"Infantry":false,"Archer":false,"Porter":false,"Pikemen":false,"SquadBase":false,"VillageHall":false,"Workshop":false,"Prison":false,"DefenderCamp":false,"SuppyHub":false,"Market":false,"useSpeedUps":false,"useResources":true},"Id":15,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|8:00 AM|10:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.upgrade","Uid":"vikingbot.base.upgrade_2","OrderId":16,"Config":{"Upgrade":{"value":"MainHall","options":["MainHall","Specfic","Villages"]},"EagleNest":false,"Warehouse":false,"HallofValor":false,"TribeHall":true,"DivinationShack":false,"Academy":false,"Watchtower":false,"Infirmary":false,"Infantry":false,"Archer":false,"Porter":false,"Pikemen":false,"SquadBase":false,"VillageHall":false,"Workshop":false,"Prison":false,"DefenderCamp":false,"SuppyHub":false,"Market":false,"useSpeedUps":false,"useResources":true},"Id":16,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|4:00 PM|6:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.research","Uid":"vikingbot.base.research","OrderId":17,"Config":{"research":{"value":"Economy","options":["Economy","Military"]},"upgrade":true,"useResources":true,"useSpeedUps":false},"Id":17,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|12:00 AM|2:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.research","Uid":"vikingbot.base.research_1","OrderId":18,"Config":{"research":{"value":"Economy","options":["Economy","Military"]},"upgrade":true,"useResources":true,"useSpeedUps":false},"Id":18,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|8:00 AM|10:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.research","Uid":"vikingbot.base.research_2","OrderId":19,"Config":{"research":{"value":"Economy","options":["Economy","Military"]},"upgrade":true,"useResources":true,"useSpeedUps":false},"Id":19,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|4:00 PM|6:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.divinationshack","Uid":"vikingbot.base.divinationshack","OrderId":20,"Config":{"SpeedUp":false,"Food":false,"Stones":false,"Gold":true,"Gems":false,"Lumber":false,"ConstructionSpeed":false,"TrainExpansion":false,"ForgingSpeed":false,"ResearchSpeed":false,"TrainingSpeed":false,"ForgingConsumption":false,"HealingSpeed":false,"TrainingConsumption":false,"HealingConsumption":false},"Id":20,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|12:00 AM|2:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.divinationshack","Uid":"vikingbot.base.divinationshack_1","OrderId":21,"Config":{"SpeedUp":false,"Food":false,"Stones":false,"Gold":true,"Gems":false,"Lumber":false,"ConstructionSpeed":false,"TrainExpansion":false,"ForgingSpeed":false,"ResearchSpeed":false,"TrainingSpeed":false,"ForgingConsumption":false,"HealingSpeed":false,"TrainingConsumption":false,"HealingConsumption":false},"Id":21,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|8:00 AM|10:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.divinationshack","Uid":"vikingbot.base.divinationshack_2","OrderId":22,"Config":{"SpeedUp":false,"Food":false,"Stones":false,"Gold":true,"Gems":false,"Lumber":false,"ConstructionSpeed":false,"TrainExpansion":false,"ForgingSpeed":false,"ResearchSpeed":false,"TrainingSpeed":false,"ForgingConsumption":false,"HealingSpeed":false,"TrainingConsumption":false,"HealingConsumption":false},"Id":22,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|4:00 PM|6:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.mail","Uid":"vikingbot.base.mail","OrderId":23,"Config":{"skip":0},"Id":23,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|3:00 AM|5:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.mail","Uid":"vikingbot.base.mail_1","OrderId":24,"Config":{"skip":0},"Id":24,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|6:00 PM|8:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.dragoncave","Uid":"vikingbot.base.dragoncave","OrderId":25,"Config":{"Resources":true,"Speedups":false,"Buffs":false,"Equipment":false,"Mounts":false,"Others":false,"ResourcesUseGold":true,"Gray":false,"Green":false,"Blue":true,"Purple":true},"Id":25,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|3:00 AM|5:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.exploration","Uid":"vikingbot.base.exploration","OrderId":26,"Config":{"AtheronSnowfields":true,"NovaForest":true,"DanaPlains":true,"MtKhajag":true,"AsltaRange":true,"Dornfjord":true,"GertlandIsland":true,"highestMission":true,"lowestMission":false,"fastestMission":false},"Id":26,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.commission","Uid":"vikingbot.base.commission","OrderId":27,"Config":{"skip":0},"Id":27,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.gathervip","Uid":"vikingbot.base.gathervip_2","OrderId":28,"Config":{"LevelStartAt":{"value":"6","options":["1","2","3","4","5","6"]},"Monster":false,"Niflung":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Divine":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Farm":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Sawmill":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Quarry":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Gold":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"RallyTime":{"value":"10min","options":["5min","10min","30min","8hours"]},"reduceLevel":false,"marches":"5","farmLowestResource":false},"Id":28,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]}]""",

    "1100": r"""[{"ScriptId":"vikingbot.base.gathervip","Uid":"vikingbot.base.gathervip_2","OrderId":0,"Config":{"LevelStartAt":{"value":"6","options":["1","2","3","4","5","6"]},"Monster":false,"Niflung":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Divine":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Farm":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Sawmill":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Quarry":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Gold":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"RallyTime":{"value":"10min","options":["5min","10min","30min","8hours"]},"reduceLevel":false,"marches":"5","farmLowestResource":false},"Id":0,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.gathervip","Uid":"vikingbot.base.gathervip_2","OrderId":1,"Config":{"LevelStartAt":{"value":"6","options":["1","2","3","4","5","6"]},"Monster":false,"Niflung":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Divine":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Farm":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Sawmill":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Quarry":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Gold":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"RallyTime":{"value":"10min","options":["5min","10min","30min","8hours"]},"reduceLevel":false,"marches":"5","farmLowestResource":false},"Id":1,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.dailies","Uid":"vikingbot.base.dailies_2","OrderId":2,"Config":{"quest":true,"recruit":true,"vip":true,"worker":{"value":"Off","options":["Off","Common","Rare","Legend","All"]},"gems":true,"errands":{"value":"10","options":["Off","5","10","15","20"]},"specialFarmer":true,"skipVoyageLushLand":true,"events":false,"collectCrystals":true},"Id":2,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|3:00 AM|5:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.dailies","Uid":"vikingbot.base.dailies_2","OrderId":3,"Config":{"quest":true,"recruit":true,"vip":true,"worker":{"value":"Off","options":["Off","Common","Rare","Legend","All"]},"gems":true,"errands":{"value":"10","options":["Off","5","10","15","20"]},"specialFarmer":true,"skipVoyageLushLand":true,"events":false,"collectCrystals":true},"Id":3,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|12:00 PM|2:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.dailies","Uid":"vikingbot.base.dailies_2","OrderId":4,"Config":{"quest":true,"recruit":true,"vip":true,"worker":{"value":"Off","options":["Off","Common","Rare","Legend","All"]},"gems":true,"errands":{"value":"10","options":["Off","5","10","15","20"]},"specialFarmer":true,"skipVoyageLushLand":true,"events":false,"collectCrystals":true},"Id":4,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|6:00 PM|8:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.alliancedonation","Uid":"vikingbot.base.alliancedonation","OrderId":5,"Config":{"allianceGift":true,"allianceDonation":{"value":"Recommended","options":["Recommended","Development","Territory","War","Skills","Off"]}},"Id":5,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|12:00 AM|3:00 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.alliancedonation","Uid":"vikingbot.base.alliancedonation_2","OrderId":6,"Config":{"allianceGift":true,"allianceDonation":{"value":"Recommended","options":["Recommended","Development","Territory","War","Skills","Off"]}},"Id":6,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|10:00 AM|1:00 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.alliancedonation","Uid":"vikingbot.base.alliancedonation_1","OrderId":7,"Config":{"allianceGift":true,"allianceDonation":{"value":"Recommended","options":["Recommended","Development","Territory","War","Skills","Off"]}},"Id":7,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|7:00 PM|9:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.buffs","Uid":"vikingbot.base.buffs","OrderId":8,"Config":{"Attack":{"value":"Off","options":["Off","Any"]},"Defense":{"value":"Off","options":["Off","Any"]},"Gather":{"value":"Any","options":["Off","Any"]},"Workers":{"value":"Off","options":["Off","Any"]},"Deception":{"value":"Off","options":["Off","Any"]},"Trade":{"value":"Off","options":["Off","Any"]},"Patrol":{"value":"Off","options":["Off","Any"]},"useGems":false},"Id":8,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|12:00 AM|2:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.buffs","Uid":"vikingbot.base.buffs_1","OrderId":9,"Config":{"Attack":{"value":"Off","options":["Off","Any"]},"Defense":{"value":"Off","options":["Off","Any"]},"Gather":{"value":"Any","options":["Off","Any"]},"Workers":{"value":"Off","options":["Off","Any"]},"Deception":{"value":"Off","options":["Off","Any"]},"Trade":{"value":"Off","options":["Off","Any"]},"Patrol":{"value":"Off","options":["Off","Any"]},"useGems":false},"Id":9,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|8:00 AM|10:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.buffs","Uid":"vikingbot.base.buffs_2","OrderId":10,"Config":{"Attack":{"value":"Off","options":["Off","Any"]},"Defense":{"value":"Off","options":["Off","Any"]},"Gather":{"value":"Any","options":["Off","Any"]},"Workers":{"value":"Off","options":["Off","Any"]},"Deception":{"value":"Off","options":["Off","Any"]},"Trade":{"value":"Off","options":["Off","Any"]},"Patrol":{"value":"Off","options":["Off","Any"]},"useGems":false},"Id":10,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|4:00 PM|6:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.mail","Uid":"vikingbot.base.mail","OrderId":23,"Config":{"skip":0},"Id":23,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|3:00 AM|5:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.mail","Uid":"vikingbot.base.mail_1","OrderId":24,"Config":{"skip":0},"Id":24,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|6:00 PM|8:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.gathervip","Uid":"vikingbot.base.gathervip_2","OrderId":28,"Config":{"LevelStartAt":{"value":"6","options":["1","2","3","4","5","6"]},"Monster":false,"Niflung":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Divine":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Farm":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Sawmill":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Quarry":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Gold":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"RallyTime":{"value":"10min","options":["5min","10min","30min","8hours"]},"reduceLevel":false,"marches":"5","farmLowestResource":false},"Id":28,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]}]""",

    "TRAIN": r"""[{"ScriptId":"vikingbot.base.stagingpost","Uid":"vikingbot.base.stagingpost","OrderId":0,"Config":{"redMission":false,"marches":"10","ignoreSuicide":false},"Id":0,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.dailies","Uid":"vikingbot.base.dailies_2","OrderId":1,"Config":{"quest":true,"recruit":true,"vip":true,"worker":{"value":"Off","options":["Off","Common","Rare","Legend","All"]},"gems":true,"errands":{"value":"10","options":["Off","5","10","15","20"]},"specialFarmer":true,"skipVoyageLushLand":true,"events":false,"collectCrystals":true},"Id":1,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|3:00 AM|5:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.dailies","Uid":"vikingbot.base.dailies_2","OrderId":2,"Config":{"quest":true,"recruit":true,"vip":true,"worker":{"value":"Off","options":["Off","Common","Rare","Legend","All"]},"gems":true,"errands":{"value":"10","options":["Off","5","10","15","20"]},"specialFarmer":true,"skipVoyageLushLand":true,"events":false,"collectCrystals":true},"Id":2,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|12:00 PM|2:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.dailies","Uid":"vikingbot.base.dailies_3","OrderId":3,"Config":{"quest":true,"recruit":true,"vip":true,"worker":{"value":"Off","options":["Off","Common","Rare","Legend","All"]},"gems":true,"errands":{"value":"10","options":["Off","5","10","15","20"]},"specialFarmer":true,"skipVoyageLushLand":true,"events":false,"collectCrystals":true},"Id":3,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|3:00 AM|5:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.dailies","Uid":"vikingbot.base.dailies_2","OrderId":4,"Config":{"quest":false,"recruit":true,"vip":false,"worker":{"value":"Off","options":["Off","Common","Rare","Legend","All"]},"gems":false,"errands":{"value":"Off","options":["Off","5","10","15","20"]},"specialFarmer":false,"skipVoyageLushLand":false,"events":false,"collectCrystals":false},"Id":4,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.alliancedonation","Uid":"vikingbot.base.alliancedonation","OrderId":5,"Config":{"allianceGift":true,"allianceDonation":{"value":"Off","options":["Recommended","Development","Territory","War","Skills","Off"]}},"Id":5,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|12:00 AM|3:00 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.alliancedonation","Uid":"vikingbot.base.alliancedonation_2","OrderId":6,"Config":{"allianceGift":true,"allianceDonation":{"value":"Off","options":["Recommended","Development","Territory","War","Skills","Off"]}},"Id":6,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|10:00 AM|1:00 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.alliancedonation","Uid":"vikingbot.base.alliancedonation_1","OrderId":7,"Config":{"allianceGift":true,"allianceDonation":{"value":"Off","options":["Recommended","Development","Territory","War","Skills","Off"]}},"Id":7,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|7:00 PM|9:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.build","Uid":"vikingbot.base.build","OrderId":8,"Config":{"skip":0},"Id":8,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.upgrade","Uid":"vikingbot.base.upgrade","OrderId":9,"Config":{"Upgrade":{"value":"MainHall","options":["MainHall","Specfic","Villages"]},"EagleNest":false,"Warehouse":false,"HallofValor":false,"TribeHall":true,"DivinationShack":false,"Academy":false,"Watchtower":false,"Infirmary":false,"Infantry":false,"Archer":false,"Porter":true,"Pikemen":false,"SquadBase":false,"VillageHall":false,"Workshop":false,"Prison":false,"DefenderCamp":false,"SuppyHub":false,"Market":false,"useSpeedUps":true,"useResources":true},"Id":9,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.upgrade","Uid":"vikingbot.base.upgrade_1","OrderId":24,"Config":{"Upgrade":{"value":"MainHall","options":["MainHall","Specfic","Villages"]},"EagleNest":false,"Warehouse":false,"HallofValor":false,"TribeHall":true,"DivinationShack":false,"Academy":false,"Watchtower":false,"Infirmary":false,"Infantry":false,"Archer":false,"Porter":true,"Pikemen":false,"SquadBase":false,"VillageHall":false,"Workshop":false,"Prison":false,"DefenderCamp":false,"SuppyHub":false,"Market":false,"useSpeedUps":false,"useResources":true},"Id":24,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.recruitment","Uid":"vikingbot.base.recruitment","OrderId":10,"Config":{"Infantry":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"Archer":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"Pikemen":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"Porter":{"value":"1","options":["Off","Auto","1","2","3","4","5","6","7"]},"Amount":{"value":"100%","options":["100%","75%","50%","25%"]},"UpgradeInfantry":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradeArcher":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradePikemen":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradePorter":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"useResources":true,"useSpeedUps":true},"Id":10,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.recruitment","Uid":"vikingbot.base.recruitment_1","OrderId":24,"Config":{"Infantry":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"Archer":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"Pikemen":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"Porter":{"value":"1","options":["Off","Auto","1","2","3","4","5","6","7"]},"Amount":{"value":"100%","options":["100%","75%","50%","25%"]},"UpgradeInfantry":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradeArcher":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradePikemen":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradePorter":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"useResources":true,"useSpeedUps":false},"Id":24,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.research","Uid":"vikingbot.base.research","OrderId":11,"Config":{"research":{"value":"Economy","options":["Economy","Military"]},"upgrade":true,"useResources":true,"useSpeedUps":true},"Id":11,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.research","Uid":"vikingbot.base.research_1","OrderId":24,"Config":{"research":{"value":"Economy","options":["Economy","Military"]},"upgrade":true,"useResources":true,"useSpeedUps":false},"Id":24,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.divinationshack","Uid":"vikingbot.base.divinationshack","OrderId":12,"Config":{"SpeedUp":true,"Food":false,"Stones":false,"Gold":false,"Gems":false,"Lumber":false,"ConstructionSpeed":false,"TrainExpansion":false,"ForgingSpeed":false,"ResearchSpeed":false,"TrainingSpeed":false,"ForgingConsumption":false,"HealingSpeed":false,"TrainingConsumption":false,"HealingConsumption":false},"Id":12,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|12:00 AM|2:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.divinationshack","Uid":"vikingbot.base.divinationshack_1","OrderId":13,"Config":{"SpeedUp":true,"Food":false,"Stones":false,"Gold":false,"Gems":false,"Lumber":false,"ConstructionSpeed":false,"TrainExpansion":false,"ForgingSpeed":false,"ResearchSpeed":false,"TrainingSpeed":false,"ForgingConsumption":false,"HealingSpeed":false,"TrainingConsumption":false,"HealingConsumption":false},"Id":13,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|8:00 AM|10:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.divinationshack","Uid":"vikingbot.base.divinationshack_2","OrderId":14,"Config":{"SpeedUp":true,"Food":false,"Stones":false,"Gold":false,"Gems":false,"Lumber":false,"ConstructionSpeed":false,"TrainExpansion":false,"ForgingSpeed":false,"ResearchSpeed":false,"TrainingSpeed":false,"ForgingConsumption":false,"HealingSpeed":false,"TrainingConsumption":false,"HealingConsumption":false},"Id":14,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|4:00 PM|6:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.mail","Uid":"vikingbot.base.mail","OrderId":15,"Config":{"skip":0},"Id":15,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|3:00 AM|5:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.mail","Uid":"vikingbot.base.mail_1","OrderId":16,"Config":{"skip":0},"Id":16,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|6:00 PM|8:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.eaglenest","Uid":"vikingbot.base.eaglenest","OrderId":17,"Config":{"skip":0},"Id":17,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.villages","Uid":"vikingbot.base.villages","OrderId":18,"Config":{"skip":0,"marches":"15"},"Id":18,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.exploration","Uid":"vikingbot.base.exploration","OrderId":19,"Config":{"AtheronSnowfields":true,"NovaForest":true,"DanaPlains":true,"MtKhajag":true,"AsltaRange":true,"Dornfjord":true,"GertlandIsland":true,"highestMission":true,"lowestMission":false,"fastestMission":false},"Id":19,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.commission","Uid":"vikingbot.base.commission","OrderId":20,"Config":{"skip":0},"Id":20,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.heal","Uid":"vikingbot.base.heal","OrderId":21,"Config":{"skip":0,"useResources":true},"Id":21,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]}]""",

    "TRAIN2": r"""[{"ScriptId":"vikingbot.base.stagingpost","Uid":"vikingbot.base.stagingpost","OrderId":0,"Config":{"redMission":false,"marches":"10","ignoreSuicide":false},"Id":0,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.dailies","Uid":"vikingbot.base.dailies_2","OrderId":1,"Config":{"quest":true,"recruit":true,"vip":true,"worker":{"value":"Off","options":["Off","Common","Rare","Legend","All"]},"gems":true,"errands":{"value":"10","options":["Off","5","10","15","20"]},"specialFarmer":true,"skipVoyageLushLand":true,"events":false,"collectCrystals":true},"Id":1,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|3:00 AM|5:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.dailies","Uid":"vikingbot.base.dailies_2","OrderId":2,"Config":{"quest":true,"recruit":true,"vip":true,"worker":{"value":"Off","options":["Off","Common","Rare","Legend","All"]},"gems":true,"errands":{"value":"10","options":["Off","5","10","15","20"]},"specialFarmer":true,"skipVoyageLushLand":true,"events":false,"collectCrystals":true},"Id":2,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|12:00 PM|2:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.dailies","Uid":"vikingbot.base.dailies_2","OrderId":3,"Config":{"quest":true,"recruit":true,"vip":true,"worker":{"value":"Off","options":["Off","Common","Rare","Legend","All"]},"gems":true,"errands":{"value":"10","options":["Off","5","10","15","20"]},"specialFarmer":true,"skipVoyageLushLand":true,"events":false,"collectCrystals":true},"Id":3,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|6:00 PM|8:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.dailies","Uid":"vikingbot.base.dailies_3","OrderId":4,"Config":{"quest":false,"recruit":true,"vip":false,"worker":{"value":"Off","options":["Off","Common","Rare","Legend","All"]},"gems":false,"errands":{"value":"Off","options":["Off","5","10","15","20"]},"specialFarmer":false,"skipVoyageLushLand":false,"events":false,"collectCrystals":false},"Id":4,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.alliancedonation","Uid":"vikingbot.base.alliancedonation","OrderId":5,"Config":{"allianceGift":true,"allianceDonation":{"value":"Recommended","options":["Recommended","Development","Territory","War","Skills","Off"]}},"Id":5,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|12:00 AM|3:00 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.alliancedonation","Uid":"vikingbot.base.alliancedonation_2","OrderId":6,"Config":{"allianceGift":true,"allianceDonation":{"value":"Recommended","options":["Recommended","Development","Territory","War","Skills","Off"]}},"Id":6,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|10:00 AM|1:00 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.alliancedonation","Uid":"vikingbot.base.alliancedonation_1","OrderId":7,"Config":{"allianceGift":true,"allianceDonation":{"value":"Recommended","options":["Recommended","Development","Territory","War","Skills","Off"]}},"Id":7,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|7:00 PM|9:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.build","Uid":"vikingbot.base.build","OrderId":8,"Config":{"skip":0},"Id":8,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.buffs","Uid":"vikingbot.base.buffs","OrderId":9,"Config":{"Attack":{"value":"Off","options":["Off","Any"]},"Defense":{"value":"Off","options":["Off","Any"]},"Gather":{"value":"Any","options":["Off","Any"]},"Workers":{"value":"Off","options":["Off","Any"]},"Deception":{"value":"Off","options":["Off","Any"]},"Trade":{"value":"Off","options":["Off","Any"]},"Patrol":{"value":"Off","options":["Off","Any"]},"useGems":false},"Id":9,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|12:00 AM|2:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.buffs","Uid":"vikingbot.base.buffs_1","OrderId":10,"Config":{"Attack":{"value":"Off","options":["Off","Any"]},"Defense":{"value":"Off","options":["Off","Any"]},"Gather":{"value":"Any","options":["Off","Any"]},"Workers":{"value":"Off","options":["Off","Any"]},"Deception":{"value":"Off","options":["Off","Any"]},"Trade":{"value":"Off","options":["Off","Any"]},"Patrol":{"value":"Off","options":["Off","Any"]},"useGems":false},"Id":10,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|8:00 AM|10:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.buffs","Uid":"vikingbot.base.buffs_2","OrderId":11,"Config":{"Attack":{"value":"Off","options":["Off","Any"]},"Defense":{"value":"Off","options":["Off","Any"]},"Gather":{"value":"Any","options":["Off","Any"]},"Workers":{"value":"Off","options":["Off","Any"]},"Deception":{"value":"Off","options":["Off","Any"]},"Trade":{"value":"Off","options":["Off","Any"]},"Patrol":{"value":"Off","options":["Off","Any"]},"useGems":false},"Id":11,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|4:00 PM|6:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.upgrade","Uid":"vikingbot.base.upgrade_1","OrderId":26,"Config":{"Upgrade":{"value":"MainHall","options":["MainHall","Specfic","Villages"]},"EagleNest":false,"Warehouse":false,"HallofValor":false,"TribeHall":true,"DivinationShack":false,"Academy":false,"Watchtower":false,"Infirmary":false,"Infantry":false,"Archer":false,"Porter":false,"Pikemen":false,"SquadBase":false,"VillageHall":false,"Workshop":false,"Prison":false,"DefenderCamp":false,"SuppyHub":false,"Market":false,"useSpeedUps":true,"useResources":true},"Id":26,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.upgrade","Uid":"vikingbot.base.upgrade","OrderId":13,"Config":{"Upgrade":{"value":"MainHall","options":["MainHall","Specfic","Villages"]},"EagleNest":false,"Warehouse":false,"HallofValor":false,"TribeHall":true,"DivinationShack":false,"Academy":false,"Watchtower":false,"Infirmary":false,"Infantry":false,"Archer":false,"Porter":false,"Pikemen":false,"SquadBase":false,"VillageHall":false,"Workshop":false,"Prison":false,"DefenderCamp":false,"SuppyHub":false,"Market":false,"useSpeedUps":false,"useResources":true},"Id":13,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.recruitment","Uid":"vikingbot.base.recruitment_1","OrderId":26,"Config":{"Infantry":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"Archer":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"Pikemen":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"Porter":{"value":"1","options":["Off","Auto","1","2","3","4","5","6","7"]},"Amount":{"value":"100%","options":["100%","75%","50%","25%"]},"UpgradeInfantry":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradeArcher":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradePikemen":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradePorter":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"useResources":true,"useSpeedUps":true},"Id":26,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.recruitment","Uid":"vikingbot.base.recruitment","OrderId":12,"Config":{"Infantry":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"Archer":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"Pikemen":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"Porter":{"value":"1","options":["Off","Auto","1","2","3","4","5","6","7"]},"Amount":{"value":"100%","options":["100%","75%","50%","25%"]},"UpgradeInfantry":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradeArcher":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradePikemen":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradePorter":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"useResources":true,"useSpeedUps":false},"Id":12,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.research","Uid":"vikingbot.base.research","OrderId":14,"Config":{"research":{"value":"Economy","options":["Economy","Military"]},"upgrade":true,"useResources":true,"useSpeedUps":true},"Id":14,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.research","Uid":"vikingbot.base.research_1","OrderId":26,"Config":{"research":{"value":"Economy","options":["Economy","Military"]},"upgrade":true,"useResources":true,"useSpeedUps":false},"Id":26,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.divinationshack","Uid":"vikingbot.base.divinationshack","OrderId":15,"Config":{"SpeedUp":true,"Food":false,"Stones":false,"Gold":false,"Gems":false,"Lumber":false,"ConstructionSpeed":false,"TrainExpansion":false,"ForgingSpeed":false,"ResearchSpeed":false,"TrainingSpeed":false,"ForgingConsumption":false,"HealingSpeed":false,"TrainingConsumption":false,"HealingConsumption":false},"Id":15,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|12:00 AM|2:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.divinationshack","Uid":"vikingbot.base.divinationshack_1","OrderId":16,"Config":{"SpeedUp":true,"Food":false,"Stones":false,"Gold":false,"Gems":false,"Lumber":false,"ConstructionSpeed":false,"TrainExpansion":false,"ForgingSpeed":false,"ResearchSpeed":false,"TrainingSpeed":false,"ForgingConsumption":false,"HealingSpeed":false,"TrainingConsumption":false,"HealingConsumption":false},"Id":16,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|8:00 AM|10:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.divinationshack","Uid":"vikingbot.base.divinationshack_2","OrderId":17,"Config":{"SpeedUp":true,"Food":false,"Stones":false,"Gold":false,"Gems":false,"Lumber":false,"ConstructionSpeed":false,"TrainExpansion":false,"ForgingSpeed":false,"ResearchSpeed":false,"TrainingSpeed":false,"ForgingConsumption":false,"HealingSpeed":false,"TrainingConsumption":false,"HealingConsumption":false},"Id":17,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|4:00 PM|6:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.mail","Uid":"vikingbot.base.mail","OrderId":18,"Config":{"skip":0},"Id":18,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|3:00 AM|5:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.mail","Uid":"vikingbot.base.mail_1","OrderId":19,"Config":{"skip":0},"Id":19,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|6:00 PM|8:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.exploration","Uid":"vikingbot.base.exploration","OrderId":20,"Config":{"AtheronSnowfields":true,"NovaForest":true,"DanaPlains":true,"MtKhajag":true,"AsltaRange":true,"Dornfjord":true,"GertlandIsland":true,"highestMission":true,"lowestMission":false,"fastestMission":false},"Id":20,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.commission","Uid":"vikingbot.base.commission","OrderId":21,"Config":{"skip":0},"Id":21,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.dragoncave","Uid":"vikingbot.base.dragoncave","OrderId":22,"Config":{"Resources":true,"Speedups":true,"Buffs":false,"Equipment":false,"Mounts":false,"Others":false,"ResourcesUseGold":true,"Gray":false,"Green":false,"Blue":true,"Purple":true},"Id":22,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|3:00 AM|5:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.gathervip","Uid":"vikingbot.base.gathervip_2","OrderId":23,"Config":{"LevelStartAt":{"value":"6","options":["1","2","3","4","5","6"]},"Monster":false,"Niflung":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Divine":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Farm":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Sawmill":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Quarry":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Gold":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"RallyTime":{"value":"10min","options":["5min","10min","30min","8hours"]},"reduceLevel":false,"marches":"7","farmLowestResource":true},"Id":23,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]}]""",
   
}
# ──────────────────────────────────────────────────────────────────────


app = Flask(__name__, template_folder="templates")
CORS(app)

LAST_UPDATE_TIME = None

LOG_PATTERN = re.compile(
    r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} \+\d{2}:\d{2}) "
    r"\[DBG\] DEBUG\|(.*?)\|CityResourcesAmount:\{Food:(\d+), Wood:(\d+), Stone:(\d+), Gold:(\d+), Gems:(\d+)"
)

############################################
# Список серверов (с URL) для виджета
############################################

SERVERS = [
    {
      "name": "208",
      "ip": "185.186.143.208",
      "user": "Administrator",
      "password": "01091945kottT!",
      "url": "https://hotly-large-coral.cloudpub.ru/",
      "start_path": r"C:\Users\Administrator\Desktop\GnBots.lnk",  # WMI     
    },
    {
      "name": "F99",
      "ip": "192.168.31.234",
      "user": "Administrator",
      "password": "01091945kottT!",
      "url": "https://tastelessly-quickened-chub.cloudpub.ru/",
      "start_path": r"C:\Users\Administrator\Desktop\GnBots.lnk",  # SSH
    },
    {
      "name": "R9",
      "ip": "192.168.31.46",
      "user": "administrator",
      "password": "01091945kottT!",
      "url": "https://creakily-big-spaniel.cloudpub.ru/",
      "start_path": r"C:\Users\administrator\Desktop\GnBots.lnk",  # SSH
    },
    {
      "name": "RSS",
      "ip": "192.168.31.9",
      "user": "Administrator",
      "password": "01091945koT",
      "url": "https://fiendishly-awake-stickleback.cloudpub.ru/",
      "start_path": r"C:\Users\Administrator\Desktop\GnBots.lnk",  # SSH
    }
]


##############################
# Helper для BD
##############################
# === DB helper: единая точка открытия соединений SQLite ===
def open_db(path, *args, **kwargs):
    """
    Открывает SQLite с безопасными/быстрыми PRAGMA.
    Принимает любые args/kwargs как у sqlite3.connect (например, check_same_thread=False),
    чтобы не падать, если где-то остались старые вызовы.
    """
    import sqlite3

    # Значения по умолчанию (можно переопределить через kwargs в вызовах)
    kwargs.setdefault("check_same_thread", False)   # для многопоточного Flask
    kwargs.setdefault("timeout", 30.0)              # дольше ждём блокировки
    # row_factory удобно сразу настроить для dict-подобного доступа
    row_factory = kwargs.pop("row_factory", sqlite3.Row)

    # ВАЖНО: здесь должен быть именно sqlite3.connect, НE open_db!
    con = sqlite3.connect(path, *args, **kwargs)
    con.row_factory = row_factory

    # Настройка PRAGMA в одном месте
    try:
        with con:  # автокоммит PRAGMA
            con.execute("PRAGMA journal_mode=WAL;")
            con.execute("PRAGMA synchronous=NORMAL;")
            con.execute("PRAGMA temp_store=MEMORY;")
            con.execute("PRAGMA foreign_keys=ON;")
    except Exception as e:
        # WAL может не примениться на read-only путях — не критично
        print(f"[DB] PRAGMA setup warning for {path}: {e}")

    return con



##############################
# Утилиты для процессов / STOP
##############################

def kill_process(name: str, soft_timeout: int = 5, hard_timeout: int = 5) -> list[int]:
    """
    Пытается завершить все процессы с указанным именем:
      1) мягкий terminate() + ожидание soft_timeout секунд
      2) жесткий kill() + ожидание hard_timeout секунд
      3) taskkill /F /T на отмёт оставшиеся
    Возвращает список PID-ов, к‑т были задействованы.
    """
    killed_pids = []
    for proc in psutil.process_iter(['pid', 'name']):
        if proc.info['name'] and proc.info['name'].lower() == name.lower():
            pid = proc.info['pid']
            try:
                proc.terminate()
                proc.wait(timeout=soft_timeout)
            except psutil.TimeoutExpired:
                try:
                    proc.kill()
                    proc.wait(timeout=hard_timeout)
                except psutil.TimeoutExpired:
                    # последнее средство
                    subprocess.run(
                        ['taskkill', '/F', '/T', '/PID', str(pid)],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL
                    )
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                # процесс уже ушёл или нет прав
                pass
            killed_pids.append(pid)
    return killed_pids

def is_process_running(process_name):
    for proc in psutil.process_iter(['name']):
        try:
            if process_name.lower() in proc.info['name'].lower():
                return True
        except:
            pass
    return False

def start_process(exe_path):
    subprocess.Popen(exe_path, shell=True)

def do_stop_logic() -> list[str]:
    """
    Останавливает несколько процессов и собирает логи:
      — GnBots.exe
      — dnplayer.exe
      — Ld9BoxHeadless.exe
    Возвращает список сообщений о результатах.
    """
    logs: list[str] = []
    for name in ("GnBots.exe", "dnplayer.exe", "Ld9BoxHeadless.exe"):
        logs.append(f"⏹ Останавливаем {name}...")
        # kill_process возвращает список PID‑ов, которые были убиты
        try:
            killed = kill_process(name, soft_timeout=10, hard_timeout=5)
            if killed:
                logs.append(f"✅ Процессы {name} завершены: {', '.join(map(str, killed))}")
            else:
                logs.append(f"ℹ Процесс {name} не найден или уже завершён.")
        except Exception as e:
            logs.append(f"❗ Ошибка при попытке остановить {name}: {e}")
        
        # короткая пауза перед проверкой
        time.sleep(2)
        if is_process_running(name):
            logs.append(f"❗ {name} всё ещё работает (возможно нет прав)?")

    logs.append("✔ Stop завершён.")
    return logs

def do_reboot_logic():
    logs= do_stop_logic()
    logs.append("Запускаем GnBots.exe -start")
    try:
        start_process(r"C:\Users\administrator\Desktop\GnBots.lnk")
    except Exception as e:
        logs.append("Ошибка запуска GnBots.exe: "+str(e))
    logs.append("Reboot завершён.")
    return logs

############################
# Удалённый STOP/REBOOT (SSH / WMI)
############################

def stop_remote_ssh(server):
    logs=[]
    try:
        logs.append(f"STOP {server['name']} via SSH...")
        ssh= paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(server["ip"], username=server["user"], password=server["password"], timeout=5)
        for proc in ["GnBots.exe","dnplayer.exe"]:
            cmd= f"taskkill /F /IM {proc}"
            stdin, stdout, stderr= ssh.exec_command(cmd)
            out= stdout.read().decode("cp866","ignore")
            err= stderr.read().decode("cp866","ignore")
            logs.append(f"{cmd} => {out.strip()} {err.strip()}")
        ssh.close()
    except Exception as e:
        logs.append(f"Ошибка stop_remote_ssh: {e}")
    return logs

def start_remote_ssh(server):
    logs=[]
    try:
        logs.append(f"START {server['name']} via SSH => {server['start_path']}")
        ssh= paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(server["ip"], username=server["user"], password=server["password"], timeout=5)
        cmd= f'start "" "{server["start_path"]}"'
        ssh.exec_command(cmd)
        logs.append("GnBots запущен.")
        ssh.close()
    except Exception as e:
        logs.append(f"Ошибка start_remote_ssh: {e}")
    return logs

def reboot_remote_ssh(server):
    logs= stop_remote_ssh(server)
    time.sleep(3)
    logs += start_remote_ssh(server)
    return logs

# Для 208 — WMI STOP/REBOOT
import pythoncom
import wmi

def stop_remote_wmi(server):
    logs=[]
    logs.append(f"STOP {server['name']} via WMI...")

    pythoncom.CoInitialize()
    try:
        conn = wmi.WMI(server["ip"], user=server["user"], password=server["password"])
        # Останавливаем GnBots / dnplayer
        for p in conn.Win32_Process(Name='GnBots.exe'):
            logs.append(f"Killing GnBots.exe pid={p.ProcessId}")
            p.Terminate()
        for p in conn.Win32_Process(Name='dnplayer.exe'):
            logs.append(f"Killing dnplayer.exe pid={p.ProcessId}")
            p.Terminate()
    except Exception as e:
        logs.append("Ошибка stop_remote_wmi: "+ str(e))
    finally:
        pythoncom.CoUninitialize()
    return logs

def start_remote_wmi(server):
    logs=[]
    logs.append(f"START {server['name']} via WMI => {server['start_path']}")

    pythoncom.CoInitialize()
    try:
        conn= wmi.WMI(server["ip"], user=server["user"], password=server["password"])
        cmd= server["start_path"]
        res= conn.Win32_Process.Create(CommandLine=cmd)
        logs.append(f"Create => {res}")
    except Exception as e:
        logs.append("Ошибка start_remote_wmi: "+ str(e))
    finally:
        pythoncom.CoUninitialize()
    return logs

def reboot_remote_wmi(server):
    logs= stop_remote_wmi(server)
    time.sleep(3)
    logs += start_remote_wmi(server)
    return logs

def stop_remote(server):
    """Определяем, ssh или wmi."""
    if server["name"]=="208":
        return stop_remote_wmi(server)
    else:
        return stop_remote_ssh(server)

def start_remote(server):
    if server["name"]=="208":
        return start_remote_wmi(server)
    else:
        return start_remote_ssh(server)

def reboot_remote(server):
    if server["name"]=="208":
        return reboot_remote_wmi(server)
    else:
        return reboot_remote_ssh(server)

##############################
# ИНИЦИАЛИЗАЦИЯ БАЗ
##############################

def init_resources_db():
    conn = open_db(RESOURCES_DB)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS resources (
            id TEXT PRIMARY KEY,
            nickname TEXT,
            food INTEGER,
            wood INTEGER,
            stone INTEGER,
            gold INTEGER,
            gems INTEGER,
            last_updated TEXT
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS daily_baseline (
            id TEXT,
            nickname TEXT,
            food INTEGER,
            wood INTEGER,
            stone INTEGER,
            gold INTEGER,
            gems INTEGER,
            baseline_date TEXT,
            PRIMARY KEY(id, baseline_date)
        )
    """)
    conn.commit()
    conn.close()

def init_expenses():
    conn = open_db(RESOURCES_DB)
    conn.execute("""CREATE TABLE IF NOT EXISTS expenses(
         id INTEGER PRIMARY KEY AUTOINCREMENT,
         amount INTEGER NOT NULL,
         dt TEXT NOT NULL
    )""")
    conn.commit(); conn.close()


# === ПОСЛЕ СТРОКИ: def init_logs_db(): ===
def init_logs_db():
    """
    Инициализация БД логов:
      - files_offset: смещения прочитанного для больших логов
      - cached_logs: кэшированные строки (с индексами)
      - resource_snapshots: снапшоты ресурсов с индексами
      - дедупликация по (acc_id, dt) и создание UNIQUE-индекса
    """
    conn = open_db(LOGS_DB)
    try:
        c = conn.cursor()

        # 1) Таблица смещений
        c.execute("""
            CREATE TABLE IF NOT EXISTS files_offset (
                filename TEXT PRIMARY KEY,
                last_pos INTEGER NOT NULL
            )
        """)

        # 2) Кэш логов по аккаунтам
        c.execute("""
            CREATE TABLE IF NOT EXISTS cached_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                acc_id TEXT,
                nickname TEXT,
                dt TEXT,         -- 'YYYY-MM-DD HH:MM:SS.mmm +HH:MM'
                raw_line TEXT
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_cached_logs_acc ON cached_logs(acc_id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_cached_logs_id ON cached_logs(id DESC)")

        # 3) Снапшоты ресурсов
        c.execute("""
            CREATE TABLE IF NOT EXISTS resource_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                acc_id TEXT NOT NULL,
                dt TEXT NOT NULL,   -- 'YYYY-MM-DD HH:MM:SS.mmm +HH:MM'
                food INTEGER,
                wood INTEGER,
                stone INTEGER,
                gold INTEGER
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_rs_acc_dt ON resource_snapshots(acc_id, dt)")

        # 4) Дедупликация по (acc_id, dt) перед созданием уникального индекса
        #    Оставляем запись с максимальным id
        try:
            c.execute("""
                DELETE FROM resource_snapshots
                WHERE id NOT IN (
                    SELECT MAX(id) FROM resource_snapshots
                    GROUP BY acc_id, dt
                )
            """)
        except Exception as e:
            print("[init_logs_db] dedup resource_snapshots warn:", e)

        # 5) Уникальный индекс по (acc_id, dt), чтобы не плодить дублей при перечтении логов
        try:
            c.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_rs_acc_dt ON resource_snapshots(acc_id, dt)")
        except Exception as e:
            # Если здесь ошибка, значит в таблице всё ещё остались дубликаты.
            # Можно повторно попробовать более агрессивную чистку, но, как правило, блока выше хватает.
            print("[init_logs_db] create unique index warn:", e)

        conn.commit()
        print("[init_logs_db] OK: schema ensured, indexes present")
    except Exception as e:
        print("[init_logs_db] ERROR:", e)
        # не забываем, чтобы не оставить транзакцию открытой
        try: conn.rollback()
        except: pass
        raise
    finally:
        try: conn.close()
        except: pass


# ───── после init_logs_db() добавьте ─────
def init_accounts_db():
    conn = open_db(RESOURCES_DB)
    c    = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS account_meta(
          id          TEXT PRIMARY KEY,   -- ID = тот же, что в profile.json
          email       TEXT,
          passwd      TEXT,
          igg         TEXT,
          pay_until   TEXT,   -- YYYY-MM-DD
          tariff_rub  INTEGER DEFAULT 0,
          server      TEXT,
          tg_tag      TEXT
        )
    """)

    # Таблица могла быть создана в старой версии без новых колонок.
    existing_cols = {
        row[1] for row in c.execute("PRAGMA table_info(account_meta)").fetchall()
    }
    if "server" not in existing_cols:
        c.execute("ALTER TABLE account_meta ADD COLUMN server TEXT")
    if "tg_tag" not in existing_cols:
        c.execute("ALTER TABLE account_meta ADD COLUMN tg_tag TEXT")
    conn.commit(); conn.close()


##############################
# Работа аккаунтами -данные\пароли
##############################
# ───────────────────  SYNC account_meta  ───────────────────
def sync_account_meta():
    """Держит account_meta = текущее множество активных Id."""
    profiles, ok = load_profiles(return_status=True)
    if not ok and not profiles:
        print("PROFILE read failed — skip sync_account_meta")
        return

    active_ids = {p["Id"] for p in profiles}

    conn = open_db(RESOURCES_DB)
    c    = conn.cursor()

    # ── удалить лишние ──
    if active_ids:
        marks = ",".join("?"*len(active_ids))
        c.execute(f"DELETE FROM account_meta WHERE id NOT IN ({marks})",
                  tuple(active_ids))
    else:                         # если активных вообще нет
        c.execute("DELETE FROM account_meta")

    # ── вставить недостающие ──
    if active_ids:
        placeholders = ",".join("(?, '', '', '', '', NULL, '', '')"
                                for _ in active_ids)
        c.execute(f"""
            INSERT OR IGNORE INTO account_meta
            (id,email,passwd,igg,pay_until,tariff_rub,server,tg_tag)
            VALUES {placeholders}
        """, tuple(active_ids))

    conn.commit(); conn.close()



##############################
# Работа с профилями
##############################

PROFILE_CACHE: list[dict[str, t.Any]] | None = None


def load_profiles(*, return_status: bool = False):
    """
    Возвращает список активных аккаунтов из PROFILE_PATH.

    :param return_status: True → вернуть (profiles, ok),
                          False → вернуть только profiles.
    """

    def _result(profiles: list[dict[str, t.Any]], ok: bool):
        return (profiles, ok) if return_status else profiles

    global PROFILE_CACHE

    if not os.path.exists(PROFILE_PATH):
        print(f"PROFILE not found: {PROFILE_PATH}")
        return _result(PROFILE_CACHE or [], False)
    try:
        with open(PROFILE_PATH, "r", encoding="utf-8") as f:
            raw = f.read().strip()
            if not raw:
                print(f"PROFILE is empty: {PROFILE_PATH}")
                return _result(PROFILE_CACHE or [], False)
            data = json.loads(raw)
    except json.JSONDecodeError as exc:
        print(f"PROFILE has invalid JSON: {exc}")
        return _result(PROFILE_CACHE or [], False)

    if not isinstance(data, list):
        print(f"PROFILE data is not a list: type={type(data)}")
        return _result(PROFILE_CACHE or [], False)

    active_profiles = [acc for acc in data if acc.get("Active")]
    PROFILE_CACHE = active_profiles
    return _result(active_profiles, True)

# вверху, рядом с load_profiles()
def load_active_names():
    """возвращает [(Id, Name)] активных аккаунтов"""
    profiles, _ = load_profiles(return_status=True)
    return [(a["Id"], a.get("Name","")) for a in profiles]



def ensure_active_in_db(active_accounts):
    conn= open_db(RESOURCES_DB)
    c=conn.cursor()
    for acc in active_accounts:
        c.execute("""
            INSERT OR IGNORE INTO resources
            (id, nickname, food, wood, stone, gold, gems, last_updated)
            VALUES(?, ?, 0,0,0,0,0, '1970-01-01T00:00:00')
        """,(acc["Id"], acc["Name"]))
    conn.commit()
    conn.close()

##############################
# Парсинг логов
##############################

def parse_logs():
    global LAST_UPDATE_TIME
    acts= load_profiles()
    if not acts:
        print("Нет активных аккаунтов => parse_logs skip")
        return
    ensure_active_in_db(acts)
    acc_map= {a["Id"]: a["Name"] for a in acts}

    do_resources_update(acc_map)
    LAST_UPDATE_TIME= datetime.now(timezone.utc)
    print("parse_logs done. LAST_UPDATE_TIME =", LAST_UPDATE_TIME.isoformat())

def do_resources_update(acc_map):
    conn_res = open_db(RESOURCES_DB)
    c_res= conn_res.cursor()

    conn_log= open_db(LOGS_DB)
    c_log= conn_log.cursor()

    offsets={}
    off_rows= c_log.execute("SELECT filename,last_pos FROM files_offset").fetchall()
    for (fn,ps) in off_rows:
        offsets[fn]= ps

    dt_now_str= datetime.now().strftime("%Y%m%d")

    if not os.path.exists(LOGS_DIR):
        print("LOGS_DIR not found:", LOGS_DIR)
        conn_res.close()
        conn_log.close()
        return

    for fname in os.listdir(LOGS_DIR):
        # ищем botYYYYmmdd*.txt
        if fname.startswith("bot"+ dt_now_str) and fname.endswith(".txt"):
            fullp= os.path.join(LOGS_DIR, fname)
            prev_pos= offsets.get(fname, 0)
            try:
                with open(fullp,"rb") as f:
                    f.seek(prev_pos,0)
                    while True:
                        line_bytes= f.readline()
                        if not line_bytes:
                            break
                        new_pos= f.tell()
                        line_str= line_bytes.decode("utf-8", "replace").rstrip("\r\n")

                        mm= LOG_PATTERN.search(line_str)
                        if mm:
                            ts_str, log_id, fd,wd,st,gd,gm= mm.groups()
                            if log_id in acc_map:
                                dt= datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S.%f %z")
                                iso_ts= dt.isoformat()
                                c_res.execute("""
                                  INSERT INTO resources(id,nickname,food,wood,stone,gold,gems,last_updated)
                                  VALUES(?,?,?,?,?,?,?,?)
                                  ON CONFLICT(id) DO UPDATE SET
                                    nickname=excluded.nickname,
                                    food=excluded.food,
                                    wood=excluded.wood,
                                    stone=excluded.stone,
                                    gold=excluded.gold,
                                    gems=excluded.gems,
                                    last_updated=excluded.last_updated
                                  WHERE excluded.last_updated>resources.last_updated
                                """,(log_id, acc_map[log_id],
                                     int(fd), int(wd), int(st), int(gd), int(gm), iso_ts))

                                local_date= dt.astimezone().strftime("%Y-%m-%d")
                                if local_date == datetime.now().strftime("%Y-%m-%d"):
                                    row_ex= c_res.execute("""
                                      SELECT 1 FROM daily_baseline
                                      WHERE id=? AND baseline_date=?
                                    """,(log_id, local_date)).fetchone()
                                    if not row_ex:
                                        c_res.execute("""
                                          INSERT INTO daily_baseline
                                          (id,nickname,food,wood,stone,gold,gems,baseline_date)
                                          VALUES(?,?,?,?,?,?,?,?)
                                        """,(log_id, acc_map[log_id],
                                             int(fd), int(wd), int(st), int(gd), int(gm),
                                             local_date))

                        # кэшируем                    
                        # кэшируем + снапшоты ресурсов
                        # кэшируем + снапшоты ресурсов (строгое сопоставление |ACC_ID|)
                        for acid, nick in acc_map.items():
                            # важно: матчим по "|<ID>|", чтобы исключить пересечения префиксов (Alex8914_1 vs Alex8914_14)
                            if f"|{acid}|" not in line_str:
                                continue

                            m = _DT_RE.match(line_str)  # ^YYYY-MM-DD ... +ZZ:ZZ
                            if not m:
                                continue
                            dt_part = m.group(1)

                            c_log.execute("""
                            INSERT INTO cached_logs(acc_id, nickname, dt, raw_line)
                            VALUES(?,?,?,?)
                            """, (acid, nick, dt_part, line_str))

                            if "CityResourcesAmount:" in line_str:
                                try:
                                    import re
                                    def _extract(name: str, s: str) -> int | None:
                                        m2 = re.search(rf"{name}\s*:\s*(\d+)", s)
                                        return int(m2.group(1)) if m2 else None

                                    food  = _extract("Food",  line_str)
                                    wood  = _extract("Wood",  line_str)
                                    stone = _extract("Stone", line_str)
                                    gold  = _extract("Gold",  line_str)

                                    # вставляем снапшот с защитой от дублей по (acc_id, dt)
                                    c_log.execute("""
                                    INSERT OR REPLACE INTO resource_snapshots(acc_id, dt, food, wood, stone, gold)
                                    VALUES(?,?,?,?,?,?)
                                    """, (acid, dt_part, food, wood, stone, gold))
                                except Exception as e:
                                    print("[parse CityResourcesAmount] skip:", e)




                        offsets[fname] = new_pos
            except Exception as e:
                print("Error reading file:", fullp, e)

    for ff in offsets:
        c_log.execute("""
          INSERT OR REPLACE INTO files_offset(filename,last_pos)
          VALUES(?,?)
        """, (ff, offsets[ff]))

    conn_res.commit()
    conn_log.commit()
    conn_res.close()
    conn_log.close()

##############################
# Helpers
##############################

# ─── robust scheduled-task checker ───────────────────────────────────────────
import subprocess

def _task_enabled(task_name: str) -> bool | None:
    """
    Возвращает:
        True   – задача существует и включена
        False  – задача существует и отключена
        None   – задача не найдена / schtasks вернул ошибку
    """
    try:
        raw = subprocess.check_output(
            ["schtasks", "/Query", "/TN", task_name, "/FO", "LIST", "/V"],
            stderr=subprocess.STDOUT, timeout=5
        )
    except subprocess.CalledProcessError:
        # задача не существует
        return None

    # пытаемся декодировать вывод
    for enc in ("cp866", "cp437", "utf-8"):
        try:
            text = raw.decode(enc, errors="ignore").lower()
            break
        except UnicodeDecodeError:
            continue
    else:
        return None

    state_val = None
    for line in text.splitlines():
        if ":" not in line:
            continue
        left, right = [p.strip() for p in line.split(":", 1)]
        left = left.lower(); right = right.lower()

        # английские локали
        if left.startswith("scheduled task state") or left.startswith("enabled"):
            state_val = right
            break
        # русские локали
        if ("состояние задачи" in left) or ("запланированной задачи" in left):
            state_val = right
            break

    # общий fallback — ищем ключевые слова в сыром тексте
    if state_val is None:
        if any(w in text for w in ("enabled", "включено", "да", "yes")):
            state_val = "enabled"
        elif any(w in text for w in ("disabled", "отключено", "нет", "no")):
            state_val = "disabled"

    # нормализуем к булю / None
    if state_val in ("enabled", "yes", "true", "да", "1"):
        return True
    if state_val in ("disabled", "no", "false", "нет", "0"):
        return False
    return None
# ─────────────────────────────────────────────────────────────────────────────


def shorten_number(num):
    if num == 0:
        return "0"
    sign = ""
    if num < 0:
        sign = "-"
        num = abs(num)
    if num < 1000:
        return f"{sign}{num}"
    elif num < 1_000_000:
        return f"{sign}{num // 1000}k"
    elif num < 1_000_000_000:
        return f"{sign}{num // 1_000_000}m"
    else:
        b = num / 1_000_000_000
        return f"{sign}{b:.1f}b"

def transformLogLine(dt_part, line_str):
    try:
        dt= datetime.strptime(dt_part,"%Y-%m-%d %H:%M:%S.%f")
        short= dt.strftime("%d-%m %H:%M")
    except:
        short= dt_part
    rest= line_str[28:].strip()
    return f"{short} {rest}"


##############################
# BACKUP
##############################

# ────────────────────────── настройки ──────────────────────────
SERVER = "F99"                                       # имя сервера
BACKUP_CONFIG_SRC      = r"C:\LDPlayer\LDPlayer9\vms\config"
BACKUP_CONFIG_DST_ROOT = r"C:\LD_backup\configs"
BACKUP_ACCS_DST_ROOT   = r"C:\LD_backup\accs_data"
BACKUP_PROFILES_DST_ROOT = r"C:\LD_backup\bot_acc_configs"
FIX_BACKUP_ROOT = r"C:\LD_backup\fix_backup"   # ← NEW


# ────────────────────────── вспомогалки ───────────────────────
def _ensure_dir(path: str):
    """Создаёт каталог *path* вместе со всеми промежуточными."""
    os.makedirs(path, exist_ok=True)


def _try_copy_file(src: str, dst: str) -> bool:
    """Пытается скопировать файл и возвращает успех/неуспех."""
    try:
        shutil.copy2(src, dst)
        return True
    except Exception as e:
        print(f"[BACKUP] error copying {src} → {dst}: {e}", flush=True)
        return False


# ────────────────────── новый хелпер ────────────────────────
def _retry_failed_configs(failed_files: list[str], dst_folder: str) -> None:
    """
    Повторно пытается скопировать неудачные файлы в папку dst_folder:
      - до 3 попыток с интервалом 10 минут
      - только для текущей даты
    """
    def _worker():
        today = datetime.now().strftime("%d__%m__%Y")
        attempts = 0
        max_attempts = 3
        interval = 10 * 60  # 10 минут
        while failed_files and attempts < max_attempts and datetime.now().strftime("%d__%m__%Y") == today:
            time.sleep(interval)
            for fname in failed_files.copy():
                src = os.path.join(BACKUP_CONFIG_SRC, fname)
                if os.path.isfile(src):
                    try:
                        shutil.copy2(src, dst_folder)
                        failed_files.remove(fname)
                        print(f"[BACKUP RETRY] Success: {fname}")
                    except Exception as e:
                        print(f"[BACKUP RETRY] Error copying {fname}: {e}", flush=True)
            attempts += 1
        if failed_files:
            print(f"[BACKUP RETRY] After {max_attempts} attempts failed: {failed_files}")
    threading.Thread(target=_worker, daemon=True).start()

# ────────────────────────── BACKUP CONFIGS ────────────────────
def backup_configs() -> None:
    r"""
    Копирует все файлы из …\\vms\\config в
      C:\\LD_backup\\configs\\<ДД__ММ__ГГГГ>\\
    При ошибках копирования — планирует повторные попытки.
    """
    dst = os.path.join(BACKUP_CONFIG_DST_ROOT,
                       datetime.now().strftime("%d__%m__%Y"))
    _ensure_dir(dst)
    failed = []
    for fname in os.listdir(BACKUP_CONFIG_SRC):
        src = os.path.join(BACKUP_CONFIG_SRC, fname)
        if os.path.isfile(src):
            try:
                shutil.copy2(src, dst)
            except Exception as e:
                failed.append(fname)
                print(f"[BACKUP] error copying {fname}: {e}", flush=True)
    if failed:
        print(f"[BACKUP] Scheduling retries for: {failed}", flush=True)
        _retry_failed_configs(failed, dst)
    else:
        print(f"[BACKUP] configs  →  {dst}", flush=True)
# ───────────────────────────────────────────────────────────────

# ───── вставьте рядом с backup_configs() ─────
def emergency_replace_configs(backup_dir_override: str | None = None) -> list[str]:
    """
    Полностью заменяет все *.config в DST_VMS\\config
    на файлы из выбранного источника.
    Перед заменой делает резервную копию рабочей папки
    в  C:\\LD_backup\\fix_backup\\ДД__ММ__ГГГГ[_HH-MM-SS]\\
    """
    logs: list[str] = []
    today_stamp = datetime.now().strftime("%d__%m__%Y_%H-%M-%S")

   # ── 0) пути ────────────────────────────────────────────────
    if backup_dir_override:             # выбранная папка из «Пути»
        src_dir = os.path.join(BACKUP_CONFIG_DST_ROOT, backup_dir_override)
    else:                               # дефолт → как в обычном FIX
        src_dir = os.path.join(SRC_VMS, "config")

    dst_dir    = os.path.join(DST_VMS, "config")
    backup_dst = os.path.join(FIX_BACKUP_ROOT, today_stamp)

    # защита от «копируем сами в себя»
    if os.path.normcase(src_dir) == os.path.normcase(dst_dir):
        logs.append("ℹ Источник и приёмник совпадают — замена не требуется.")
        return logs

    # ── 1) валидация ───────────────────────────────────────────
    if not os.path.isdir(src_dir):
        logs.append(f"❌ Источник не найден: {src_dir}")
        return logs
    if not os.listdir(src_dir):
        logs.append(f"❌ Источник пустой: {src_dir}")
        return logs

    _ensure_dir(FIX_BACKUP_ROOT)

    # ── 2) бэкап текущих config’ов ─────────────────────────────
    try:
        if os.path.isdir(dst_dir):
            shutil.copytree(dst_dir, backup_dst, dirs_exist_ok=False)
            logs.append(f"🗄 Backup ⇒ {backup_dst}")
        else:
            logs.append("ℹ Рабочая папка config отсутствует — бэкап пропущен.")
    except Exception as e:
        logs.append(f"❗ Ошибка бэкапа: {e}")
        return logs          # стопаем, чтобы не потерять оригиналы

    # ── 3) копируем новые config’ы ─────────────────────────────
    copied = 0
    try:
        _ensure_dir(dst_dir)
        for fname in os.listdir(src_dir):
            if not fname.lower().endswith(".config"):
                continue
            src_f = os.path.join(src_dir, fname)
            dst_f = os.path.join(dst_dir, fname)
            try:
                if os.path.exists(dst_f):
                    os.chmod(dst_f, stat.S_IWRITE)
                shutil.copy2(src_f, dst_f)
                copied += 1
            except Exception as e:
                logs.append(f"⚠ {fname}: {e}")
        logs.append(f"✅ Скопировано {copied} файлов из {src_dir}")
    except Exception as e:
        logs.append(f"❗ Ошибка копирования: {e}")

    return logs


# ───────────────────── сбор данных аккаунтов ──────────────────
def _collect_accounts_rows():
    """
    Возвращает [(Name, Email, Pass, IGG, Pay-until, Tariff)] для активных.
    """
    active = {p["Id"]: p.get("Name", "") for p in load_profiles()}
    conn   = open_db(RESOURCES_DB)
    meta   = {r[0]: r[1:] for r in
              conn.execute("SELECT id,email,passwd,igg,pay_until,tariff_rub "
                           "FROM account_meta")}
    conn.close()

    rows = []
    for acc_id, name in active.items():
        email, passwd, igg, pu, tariff = meta.get(acc_id, ("", "", "", "", 0))
        if pu:
            try:
                pu = datetime.strptime(pu, "%Y-%m-%d").strftime("%d.%m.%y")
            except ValueError:
                pass
        rows.append((name, email, passwd, igg, pu, tariff))
    return rows

# ───────────────────── BACKUP ACCOUNTS.CSV ────────────────────
def backup_accounts_csv() -> None:
    r"""
    Формирует CSV «Имя;E-mail;Пароль;IGG;Оплата;Тариф» и сохраняет в
      C:\\LD_backup\\accs_data\\<SERVER>_<ДД__ММ__ГГГГ>\\accounts.csv
    """
    subdir   = f"{SERVER}_{datetime.now().strftime('%d__%m__%Y')}"
    dst_dir  = os.path.join(BACKUP_ACCS_DST_ROOT, subdir)
    _ensure_dir(dst_dir)
    csv_path = os.path.join(dst_dir, "accounts.csv")

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=';')
        w.writerow(["Имя", "E-mail", "Пароль", "IGG", "Оплата", "Тариф"])
        for row in _collect_accounts_rows():
            w.writerow(row)
    print(f"[BACKUP] accounts.csv  →  {csv_path}")


def backup_profiles_json() -> None:
    r"""
    Делает копию файла профилей из PROFILE_PATH в
      C:\\LD_backup\\bot_acc_configs\\<ДД__ММ__ГГГГ>\\profiles.json
    Создаёт папку, если её нет, и тихо пропускает, если исходный файл не найден.
    """
    if not PROFILE_PATH:
        print("[BACKUP] PROFILE_PATH не задан — пропускаем backup_profiles_json()", flush=True)
        return
    if not os.path.isfile(PROFILE_PATH):
        print(f"[BACKUP] profiles.json не найден: {PROFILE_PATH}", flush=True)
        return

    today_stamp = datetime.now().strftime("%d__%m__%Y")
    dst_dir = os.path.join(BACKUP_PROFILES_DST_ROOT, today_stamp)
    _ensure_dir(dst_dir)

    dst = os.path.join(dst_dir, os.path.basename(PROFILE_PATH) or "profiles.json")
    if _try_copy_file(PROFILE_PATH, dst):
        print(f"[BACKUP] profiles.json  →  {dst}", flush=True)

# ─────────────────── проверка «уже есть ли за сегодня» ───────────────────
def ensure_today_backups() -> None:
    """При старте приложения проверяет, есть ли бэкап за сегодня; если нет — делает."""
    today_stamp = datetime.now().strftime("%d__%m__%Y")
    cfg_dir  = os.path.join(BACKUP_CONFIG_DST_ROOT,  today_stamp)
    acc_dir  = os.path.join(BACKUP_ACCS_DST_ROOT, f"{SERVER}_{today_stamp}")
    profiles_path = os.path.join(BACKUP_PROFILES_DST_ROOT,
                                 today_stamp,
                                 os.path.basename(PROFILE_PATH) or "profiles.json")
    cfg_ok   = os.path.isdir(cfg_dir) and os.listdir(cfg_dir)
    acc_ok   = os.path.isfile(os.path.join(acc_dir, "accounts.csv"))
    profiles_ok = os.path.isfile(profiles_path)
    if not cfg_ok:
        backup_configs()
    if not acc_ok:
        backup_accounts_csv()
    if not profiles_ok:
        backup_profiles_json()

# ─────────────────── ежедневное расписание 00:00 ─────────────────────────
def _schedule_daily_backups() -> None:
    """Фоновый поток, который каждый день ровно в 00:00 делает два бэкапа."""
    def _worker():
        while True:
            now = datetime.now()
            next_run = datetime.combine(now.date() + timedelta(days=1),
                                        datetime.min.time())
            time.sleep(max(0, (next_run - now).total_seconds()))
            try:
                backup_configs()
                backup_accounts_csv()
                backup_profiles_json()
            except Exception as e:
                print("[BACKUP] error:", e, flush=True)
    threading.Thread(target=_worker, daemon=True).start()

# === BACKUP END ===

def _schedule_pay_notifications() -> None:
    """
    Фоновый поток, который каждый день в 09:00 и 18:00 отправляет
    сводку по оплатам в Telegram (если есть что сообщить).
    """
    def _worker():
        while True:
            now = datetime.now()

            # вычисляем ближайший «круглый» запуск
            today9  = now.replace(hour=9,  minute=0, second=0, microsecond=0)
            today18 = now.replace(hour=18, minute=0, second=0, microsecond=0)
            candidates = [t for t in (today9, today18) if t > now]

            if candidates:                       # сегодня ещё будет рассылка
                next_run = min(candidates)
            else:                                # следующий день, 09:00
                next_run = (today9 + timedelta(days=1))

            time.sleep(max(0, (next_run - now).total_seconds()))

            try:
                msg = _compose_pay_alert_message()
                if msg:                    
                    # Сообщение для Telegram. Параметр add_fix_link игнорируется внутри _send_telegram.
                    _send_telegram(msg, add_fix_link=False)

            except Exception as e:
                print(f"[TG-scheduler] error: {e}", flush=True)

                # спим минуту, чтобы не уйти в быстрый бесконечный цикл
                time.sleep(60)

    threading.Thread(target=_worker, daemon=True).start()

# ────────────────── INACTIVE-CHECK scheduler ──────────────────
def _schedule_inactive_checker(interval_min: int = 60):
    """
    Проверка «без прироста >15 ч» каждые *interval_min* минут
    (по умолчанию – раз в час).  Работает в отдельном демоне-треде.
    """
    import threading, time
    def _worker():
        while True:
            try:
                inactive_monitor.check_inactive_accounts()
            except Exception as e:
                print("[inactive-checker]", e, flush=True)
            time.sleep(interval_min * 60)
    threading.Thread(target=_worker, daemon=True).start()


##############################
# FIX
##############################

def remove_readonly(folder_path):
    if os.path.exists(folder_path):
        for root, dirs, files in os.walk(folder_path):
            for d in dirs:
                dir_path = os.path.join(root, d)
                try:
                    file_stat = os.stat(dir_path)
                    os.chmod(dir_path, file_stat.st_mode | stat.S_IWRITE)
                except Exception as e:
                    print(f"Ошибка при изменении атрибутов {dir_path}: {e}")
            for f in files:
                file_path = os.path.join(root, f)
                try:
                    file_stat = os.stat(file_path)
                    os.chmod(file_path, file_stat.st_mode | stat.S_IWRITE)
                except Exception as e:
                    print(f"Ошибка при изменении атрибутов {file_path}: {e}")
    else:
        print(f"Папка не найдена: {folder_path}")

def do_fix_logic(acc_id: str,
                 *, only_config: bool = False,
                 cfg_src_override: str | None = None) -> list[str]:
    """
    Выполняет «FIX» для указанного аккаунта.

    :param acc_id:   ID аккаунта (GUID).
    :param only_config: True → копируется только файл leidianXX.config;
                        False → полный Fix (папка эмулятора + config).
    :return: список строк‑логов.
    """
    logs: list[str] = []
    logs.append(f"─── FIX start (only_config={only_config}) — acc_id={acc_id}")

    # ───────────────────── поиск InstanceId в профиле ─────────────────────
    profiles = load_profiles()
    inst_id, nickname = None, "???"
    for p in profiles:
        if p.get("Id") == acc_id:
            inst_id  = p.get("InstanceId")
            nickname = p.get("Name", "???")
            break

    if inst_id is None:
        logs.append("❗ Аккаунт не найден в JSON‑профиле.")
        return logs

    logs.append(f"Аккаунт {nickname}, InstanceId={inst_id}")

    # ───────────────────── копирование CONFIG ─────────────────────────────
    src_cfg_dir = cfg_src_override if cfg_src_override \
                else os.path.join(SRC_VMS, "config")
    src_cfg     = os.path.join(src_cfg_dir, f"leidian{inst_id}.config")
    dst_cfg = os.path.join(DST_VMS, "config", f"leidian{inst_id}.config")
    logs.append(f"Копируем config\n  {src_cfg}\n  → {dst_cfg}")

    # NEW — проверка совпадения ника при использовании бэкапа
    if cfg_src_override:
        try:
            with open(src_cfg, "rb") as fh:
                buf = fh.read()
            if nickname.encode("utf-8", "ignore") not in buf:
                logs.append(f"❌ Конфликт: в бэкапе нет ника «{nickname}»")
                return logs
        except Exception as e:
            logs.append(f"⚠ Не удалось проверить ник в конфиге: {e}")
            return logs


    try:
        if os.path.exists(dst_cfg):
            os.chmod(dst_cfg, stat.S_IWRITE)        # снимаем Read‑only
            os.remove(dst_cfg)
        shutil.copy2(src_cfg, dst_cfg)
        logs.append("✅ Config скопирован.")
    except Exception as e:
        logs.append(f"❗ Ошибка копирования config: {e}")

    # ───────────────────── если нужен только config – выходим ─────────────
    if only_config:
        logs.append("FIX (config‑only) завершён.")
        return logs

    # ───────────────────── СТОП процессов GnBots/dnplayer ────────────────
    for proc_name in ("GnBots.exe", "dnplayer.exe", "Ld9BoxHeadless.exe"):
        logs.append(f"⏹ Закрываю {proc_name}…")
        kill_process(proc_name)
        time.sleep(2)
        if is_process_running(proc_name):
            logs.append(f"⚠ {proc_name} всё ещё запущен.")

    # ───────────────────── копирование папки leidianXX ────────────────────
    src_dir = os.path.join(SRC_VMS, f"leidian{inst_id}")
    dst_dir = os.path.join(DST_VMS, f"leidian{inst_id}")
    logs.append(f"Копируем папку\n  {src_dir}\n  → {dst_dir}")

    try:
        real_dst = os.path.realpath(dst_dir) if os.path.islink(dst_dir) else dst_dir
        if os.path.exists(real_dst):
            shutil.rmtree(real_dst)
        shutil.copytree(src_dir, real_dst)
        logs.append("✅ Папка скопирована.")
    except Exception as e:
        logs.append(f"❗ Ошибка копирования папки: {e}")

    # ───────────────────── запуск GnBots ──────────────────────────────────
    logs.append("▶ Запускаю GnBots.exe -start")
    try:
        start_process(GNBOTS_SHORTCUT)       # путь берётся из config.json
    except Exception as e:
        logs.append(f"❗ Ошибка запуска GnBots: {e}")

    logs.append("FIX (full) завершён.")
    return logs




###########################################
# SERVER STATUS (детальная)
###########################################

def check_all_servers():
    """
    Возвращаем словарь:
    {
      "208": {
        "pingOk": bool,
        "sshOk": None/False/True,
        "wmiOk": None/False/True,
        "gnOk": bool,
        "dnOk": bool,
        "url": "...",
      },
      ...
    }
    """
    results={}
    for srv in SERVERS:
        detail = check_server_details(srv)
        results[srv["name"]] = detail
    return results



def check_server_details(server):
    ip = server["ip"]
    out = {
        "pingOk": False, "sshOk": None, "wmiOk": None,
        "gnOk": False,  "dnOk": False,  "dnCount": None,
        "cpu": None,    "ram": None,    "url": server["url"]
    }

    out["pingOk"] = ping_server(ip)

    # ───── ветка 208 / WMI ─────
    if server["name"] == "208":
        if out["pingOk"]:
            gn_ok, dn_ok, wmi_ok = check_processes_wmi_identical(server)
            out.update({"wmiOk": wmi_ok, "gnOk": gn_ok, "dnOk": dn_ok})
            if wmi_ok:
                out["dnCount"] = count_dnplayer_remote_wmi(server)   # ← вместо local
                out["cpu"]     = psutil.cpu_percent(interval=0.5)
                out["ram"]     = psutil.virtual_memory().percent

    # ───── все остальные / SSH ─────
    else:
        if out["pingOk"]:
            out["sshOk"] = check_ssh_port(ip)
            if out["sshOk"]:
                gn_ok, dn_ok = check_processes_ssh(server)
                out.update({"gnOk": gn_ok, "dnOk": dn_ok})
                out["dnCount"] = count_dnplayer_remote_ssh(server)

    return out



# в самом верху, после import’ов

def count_dnplayer_local():
    """Считает окна dnplayer.exe на локальной машине."""
    return sum(
        1
        for p in psutil.process_iter(['name'])
        if 'dnplayer.exe' in (p.info['name'] or '').lower()
    )

def count_dnplayer_remote_ssh(server):
    """Считает окна dnplayer.exe через SSH на удалённом сервере."""
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(
            server['ip'],
            username=server['user'],
            password=server['password'],
            timeout=5
        )
        stdin, stdout, _ = ssh.exec_command(
            'tasklist /FI "IMAGENAME eq dnplayer.exe"'
        )
        data = stdout.read().decode('cp866', 'ignore')
        ssh.close()
        return data.lower().count('dnplayer.exe')
    except Exception:
        return None


# ───────── helpers ─────────
def count_dnplayer_remote_wmi(server: dict) -> int | None:
    """
    Возвращает количество процессов dnplayer.exe на удалённом сервере по WMI.
    Если подключиться не удалось – None.
    """
    import pythoncom, wmi
    pythoncom.CoInitialize()
    try:
        conn = wmi.WMI(server["ip"],
                       user=server["user"],
                       password=server["password"])
        return sum(1 for _ in conn.Win32_Process(Name='dnplayer.exe'))
    except Exception:
        return None
    finally:
        pythoncom.CoUninitialize()


def check_processes_wmi_identical(server):
    """
    Абсолютно та же логика, что в vrServerStats.py => check_processes_wmi
    Возвращает (gn_ok, dn_ok, wmi_ok).
    """
    import wmi
    try:
        if DEBUG:
            print(f"[WMI] Подключаемся к {server['name']} ({server['ip']})...")
        conn = wmi.WMI(server["ip"], user=server["user"], password=server["password"])
        if DEBUG:
            print(f"[WMI] Успешно! Смотрим процессы Win32_Process...")
        processes = [p.Name for p in conn.Win32_Process()]
        if DEBUG:
            print(f"[WMI] {server['name']} => Количество процессов: {len(processes)} (вывожу до 10: {processes[:10]})")
        gn_ok = ("GnBots.exe" in processes)
        dn_ok = ("dnplayer.exe" in processes)
        return gn_ok, dn_ok, True
    except Exception as e:
        if DEBUG:
            print(f"[WMI] Ошибка при подключении/чтении {server['name']}: {e}")
        return (False, False, False)

def check_processes_ssh(server):
    """
    (gnOk, dnOk) через SSH 'tasklist'
    """
    import paramiko
    gn, dn = False, False
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(server["ip"], username=server["user"], password=server["password"], timeout=5)
        stdin, stdout, stderr = ssh.exec_command("tasklist")
        data = stdout.read().decode("cp866","ignore")
        ssh.close()
        gn = ("GnBots.exe" in data)
        dn = ("dnplayer.exe" in data)
    except Exception as e:
        if DEBUG:
            print(f"[SSH] Ошибка: {e}")
    return gn, dn

def ping_server(ip: str, *, timeout: float = 0.6) -> bool:          # ← NEW
    """
    Один ICMP-эхо-запрос через icmplib (~70 мс).
    Без прав raw-socket откатываемся к штатному ping.
    """
    try:
        reply = icmp_ping(ip, count=1, timeout=timeout, privileged=False)
        return reply.is_alive
    except Exception:
        param = "-n 1" if os.name=="nt" else "-c 1"
        cmd   = f"ping {param} {ip}"
        rc    = os.system(cmd + (" > nul 2>&1" if os.name=="nt" else " > /dev/null 2>&1"))
        return rc == 0

def check_ssh_port(ip):
    s= socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(2)
    try:
        r= s.connect_ex((ip,22))
    finally:
        s.close()
    return (r==0)

def check_processes_wmi_identical(server):
    # Выполняем COM-инициализацию в начале
    pythoncom.CoInitialize()

    try:
        if DEBUG:
            print(f"[WMI] Подключаемся к {server['name']} ({server['ip']})...")

        conn = wmi.WMI(server["ip"], user=server["user"], password=server["password"])
        processes = [p.Name for p in conn.Win32_Process()]

        if DEBUG:
            print(f"[WMI] {server['name']} => {len(processes)} процессов (первые 10: {processes[:10]})")

        gn_ok = ("GnBots.exe" in processes)
        dn_ok = ("dnplayer.exe" in processes)
        return gn_ok, dn_ok, True

    except Exception as e:
        if DEBUG:
            print(f"[WMI] Ошибка при подключении/чтении {server['name']}: {e}")
        return (False, False, False)

    finally:
        # Завершаем COM
        pythoncom.CoUninitialize()

def check_gn_dn_wmi(server):
    """
    Возвращает (gnOk, dnOk)
    """
    try:
        c= wmi.WMI(server["ip"], user=server["user"], password=server["password"])
        procs = [p.Name for p in c.Win32_Process()]
        gn= ("GnBots.exe" in procs)
        dn= ("dnplayer.exe" in procs)
        return (gn, dn)
    except:
        return (False, False)

def check_gn_dn_ssh(server):
    """
    Возвращает (gnOk, dnOk)
    """
    try:
        ssh= paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(server["ip"], username=server["user"], password=server["password"], timeout=5)
        stdin, stdout, stderr= ssh.exec_command("tasklist")
        data= stdout.read().decode("cp866","ignore")
        ssh.close()
        gn= ("GnBots.exe" in data)
        dn= ("dnplayer.exe" in data)
        return (gn, dn)
    except:
        return (False, False)


@app.route("/api/serverStatus")
def api_serverStatus():
    results = {}
    for srv in SERVERS:
        results[srv["name"]] = check_server_details(srv)
    return jsonify(results)

# ───── Вставьте после @app.route("/api/serverStatus") ─────

def _query_task_enabled(task_name: str) -> bool | None:
    """True/False если задача существует, None если schtasks вернул ошибку."""
    import subprocess, shlex
    try:
        out = subprocess.check_output(
            ["schtasks", "/Query", "/TN", task_name, "/FO", "LIST", "/V"],
            stderr=subprocess.STDOUT, timeout=3
        ).decode("cp866", "ignore").lower()
        return ("enabled" in out) or ("включена" in out) or ("yes" in out)
    except subprocess.CalledProcessError:
        return None                # задачи нет → None


# ───── crashed.json alias ─────
@app.route("/api/crashed")
def api_crashed_alias():
    return api_crashed_emu()       # просто проксируем


# ───────────────────  DATE PARSER  ───────────────────
def _parse_any_date(s: str) -> date | None:
    """Принимает '2025-06-21', '21.06.2025' или '21.06.25' — возвращает date."""
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d.%m.%y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    return None

# ─────────────────────  PAY-ALERTS  ─────────────────────
def _get_pay_alerts():
    """
    Возвращает список словарей для виджета «Оплата»:
      {id, name, pay, tariff, status, missing[]}

    status:
      overdue  – дата оплаты прошла или сегодня
      soon     – дата через 1-2 дня
      missing  – нет даты оплаты или других ключевых полей
                 (Email / Password / IGG / Tariff)
      ok       – скрываем из виджета
    """
    today = date.today()
    res   = []

    sync_account_meta()                      # актуализируем account_meta
    name_map = dict(load_active_names())     # показываем только активные

    conn = open_db(RESOURCES_DB)
    c    = conn.cursor()
    rows = c.execute("""
        SELECT id, email, passwd, igg, pay_until, tariff_rub
        FROM account_meta
    """).fetchall()
    conn.close()

    for acc_id, email, passwd, igg, pu, tariff in rows:
        if acc_id not in name_map:           # выключенные аккаунты пропускаем
            continue

        missing = []
        if not email:   missing.append("Email")
        if not passwd:  missing.append("Password")
        if not igg:     missing.append("IGG")
        if not tariff:  missing.append("Tariff")

        paystr = ""
        status = "ok"

        # ─── Проверяем дату оплаты ───
        if pu:                                # дата в БД присутствует
            d = _parse_any_date(pu)
            if d:
                paystr = d.strftime("%d.%m")
                delta  = (d - today).days
                if   delta <= 0: status = "overdue"
                elif delta <= 2: status = "soon"
            else:                             # не смогли распарсить
                missing.append("PayDate")
                status = "missing"
        else:                                 # даты вообще нет
            missing.append("PayDate")
            status = "missing"

        # Если другие поля пустые, но статус пока OK → делаем missing
        if status == "ok" and missing:
            status = "missing"

        if status != "ok":                    # только проблемные попадают в виджет
            res.append({
                "id"     : acc_id,
                "name"   : name_map[acc_id],
                "pay"    : paystr,
                "tariff" : tariff or 0,
                "status" : status,
                "missing": missing
            })

    # сортировка: просроченные → скоро → отсутствие данных
    order = {"overdue": 0, "soon": 1, "missing": 2}
    res.sort(key=lambda r: order.get(r["status"], 3))
    return res


# ▶▶▶ Telegram helpers ◀◀◀
# Универсальная отправка в Telegram.
# Параметр add_fix_link сохранён для обратной совместимости, но игнорируется.
def _send_telegram(text: str, add_fix_link: bool | None = None) -> None:
    # Если токены/чат не заданы — не падаем, а аккуратно сообщаем в консоль
    if not (TELEGRAM_TOKEN and TELEGRAM_CHAT_ID):
        print("[TG] skipped: TELEGRAM_TOKEN/CHAT_ID not set", flush=True)
        return
    try:
        url  = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        resp = requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text"   : text,
            "parse_mode": "HTML",
        }, timeout=15)
        if resp.status_code != 200:
            print(f"[TG] HTTP {resp.status_code}: {resp.text}", flush=True)
    except Exception as e:
        print(f"[TG] exception: {e}", flush=True)



def _compose_pay_alert_message() -> str | None:
    """
    Формирует текст для Telegram на основе _get_pay_alerts().
    Возвращает None, если актуальных уведомлений нет.
    """
    alerts = _get_pay_alerts()
    if not alerts:                      # всё в порядке — ничего не шлём
        return None

    lines = [f"<b>Сервер {SERVER}: оплата</b>"]
    for rec in alerts:
        # Иконка-префикс по статусу
        if rec["status"] == "overdue":
            icon = "❗"
        elif rec["status"] == "soon":
            icon = "⚠️"
        else:                           # missing
            icon = "❔"

        line  = f"{icon} {rec['name']}"
        if rec["pay"]:
            line += f" — до {rec['pay']}"
        if rec["tariff"]:
            line += f" ({rec['tariff']} ₽)"
        if rec["missing"]:
            line += f" — N/A {', '.join(rec['missing'])}"
        lines.append(line)

    return "\n".join(lines)
# ▲▲▲ Telegram helpers end ▲▲▲


# 
###########################################
# Flask endpoints
###########################################

@app.route("/")
def index_page():
    return render_template("index.html")

@app.route("/logs")
def logs_page():
    return render_template("logs.html")

@app.route("/fix")
def fix_page():
    return render_template("fix.html")

@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    parse_logs()
    global LAST_UPDATE_TIME
    LAST_UPDATE_TIME= datetime.now(timezone.utc)
    return {"status":"ok","last_update": LAST_UPDATE_TIME.isoformat()}

# ───── NEW: отдаём inactive15.json ─────
@app.route("/api/inactive15")
def api_inactive15():
    """
    Возвращает [{"nickname":"Alex898","hours":17.4}, …]
    или [] если файл ещё не создан.
    """
    path = Path(__file__).with_name("inactive15.json")
    if not path.is_file():
        return jsonify([])
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        # фильтруем дубликаты / пустые
        uniq = {d["nickname"]: d for d in data if d.get("nickname")}
        # сортируем по убыванию часов
        ordered = sorted(uniq.values(), key=lambda d: d["hours"], reverse=True)
        return jsonify(ordered)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
# ──────────────────────────────────────────────────────────────────



@app.route("/api/screenshot", methods=["GET"])
def api_screenshot():
    """
    Снимает скриншот экрана этого сервера и возвращает
    его в base64-представлении в JSON.
    """
    try:
        img = ImageGrab.grab()               # локальный скрин
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    buf = BytesIO()
    img.save(buf, format="PNG")
    b64 = base64.b64encode(buf.getvalue()).decode()
    return jsonify({"data": f"data:image/png;base64,{b64}"})

@app.route("/api/accounts_profile", methods=["PATCH"])
def api_accounts_profile():
    """
    Получает список {id,email,passwd,igg} и
    обновляет FRESH_NOX.json (только эти поля).
    """
    payload = request.json or []
    rec_map = {p["id"]: p for p in payload}

    if not os.path.exists(PROFILE_PATH):
        return jsonify({"err":"profile not found"}),404

    with open(PROFILE_PATH, "r", encoding="utf-8") as f:
        prof = json.load(f)

    for acc in prof:
        rid = acc.get("Id")
        if rid not in rec_map:      # не меняем
            continue
        upd = rec_map[rid]

        # --- правим MenuData.Config ---
        try:
            md = json.loads(acc.get("MenuData","{}"))
        except Exception:
            md = {}
        cfg = md.setdefault("Config", {})
        cfg["Email"]    = upd.get("email","")
        cfg["Password"] = upd.get("passwd","")
        cfg["Custom"]   = upd.get("igg","")          # IGG
        acc["MenuData"] = json.dumps(md, ensure_ascii=False)

    with open(PROFILE_PATH, "w", encoding="utf-8") as f:
        json.dump(prof, f, ensure_ascii=False, indent=2)

    sync_account_meta()      # ← NEW
    return jsonify({"status":"ok"})

@app.route("/api/log_slice")
def log_slice():
    """Отдаёт «хвост» из n строк последнего лога за сегодня.
       /api/log_slice?lines=50   (параметр необязателен)"""
    try:
        n = int(request.args.get("lines", 50))
    except (TypeError, ValueError):
        n = 50

    today = date.today().strftime("%Y-%m-%d")
    files = sorted(Path(LOGS_DIR).glob(f"*{today}*.log"))

    if not files:
        return jsonify({"lines": []})

    tail = Path(files[-1]).read_text(errors="ignore").splitlines()[-n:]
    return jsonify({"lines": tail})

# ───── применить шаблон к аккаунту ─────
# ───── применить шаблон к аккаунту ─────
@app.route("/api/manage/account/<acc_id>/apply_template", methods=["POST"])
def api_apply_template(acc_id):
    """
    POST body: {"template": "650" | "1100" | ...}
    Действие: заменяет поле Data у аккаунта выбранным шаблоном.
    Возврат: 200 {"status":"ok","acc_id":..., "template":...} или ошибка.
    """
    try:
        payload = request.get_json(silent=True) or {}
        tmpl_name = (payload.get("template") or "").strip()

        # 1) проверяем шаблон
        if tmpl_name not in TEMPLATES:
            return jsonify({"error": "template not found"}), 404

        # 2) читаем профиль
        if not os.path.exists(PROFILE_PATH):
            return jsonify({"error": "profile not found"}), 404
        with open(PROFILE_PATH, "r", encoding="utf-8") as f:
            prof = json.load(f)

        # 3) находим аккаунт
        acc = next((a for a in prof if a.get("Id") == acc_id), None)
        if not acc:
            return jsonify({"error": "acc not found"}), 404

        # 4) пишем шаблон в Data без лишних пробелов внутри JSON
        try:
            # если шаблон хранится как строка JSON → распарсить и сериализовать компактно
            acc["Data"] = json.dumps(
                json.loads(TEMPLATES[tmpl_name]),
                ensure_ascii=False,
                separators=(",", ":")  # компактно: без пробелов после ':' и вокруг ','
            )
        except Exception:
            # если шаблон вдруг невалидный JSON — сохраняем как есть
            acc["Data"] = TEMPLATES[tmpl_name]

        # 5) сохраняем профиль на диск
        with open(PROFILE_PATH, "w", encoding="utf-8") as f:
            json.dump(prof, f, ensure_ascii=False, indent=2)

        # 6) (опционально) синхронизируем быструю мету, если используешь её в интерфейсе
        try:
            if "sync_account_meta" in globals():
                sync_account_meta()
        except Exception:
            app.logger.exception("sync_account_meta failed (non-critical)")

        # 7) ОБЯЗАТЕЛЬНО возвращаем ответ клиенту
        return jsonify({"status": "ok", "acc_id": acc_id, "template": tmpl_name})

    except Exception as e:
        # Лог + понятный ответ фронту (т.е. тост станет красным)
        app.logger.exception("api_apply_template failed")
        return jsonify({"error": "internal", "details": str(e)}), 500


@app.route("/api/accounts_meta_full")
def api_accounts_meta_full():
    try:
        # ids фильтр
        ids = set(filter(None, request.args.get("ids", "").split(","))) or None

        # 1) читаем профиль (только активные)
        profile = []
        if os.path.exists(PROFILE_PATH):
            with open(PROFILE_PATH, "r", encoding="utf-8") as f:
                for a in json.load(f):
                    if not a.get("Active"):
                        continue
                    if ids and a.get("Id") not in ids:
                        continue

                    # --- E-mail / Pass / IGG из профиля ---
                    email = passwd = igg = ""
                    try:
                        md = json.loads(a.get("MenuData", "{}"))
                        cfg = md.get("Config", {})
                        email  = cfg.get("Email", "") or ""
                        passwd = cfg.get("Password", "") or ""
                        igg    = cfg.get("Custom", "") or ""
                    except Exception:
                        pass

                    profile.append({
                        "id": a.get("Id"),
                        "name": a.get("Name", ""),
                        "email": email,
                        "passwd": passwd,
                        "igg": igg,
                        "server": SERVER,
                    })

        # 2) account_meta: берём ВСЕ поля, чтобы был фолбэк для учёток
        conn = open_db(RESOURCES_DB)
        c = conn.cursor()
        meta = {
            r[0]: {
                "email":      r[1] or "",
                "passwd":     r[2] or "",
                "igg":        r[3] or "",
                "pay_until":  r[4] or "",
                "tariff_rub": r[5] or 0,
                "server":     r[6] or "",
                "tg_tag":     r[7] or "",
            }
            for r in c.execute("""
                SELECT id, email, passwd, igg, pay_until, tariff_rub, server, tg_tag
                FROM account_meta
            """)
        }
        conn.close()

        # 3) объединяем с фолбэком: пустые поля из профиля → подставляем из БД
        out = []
        for p in profile:
            m = meta.get(p["id"], {})
            merged = {
                "id": p.get("id"),
                "name": p.get("name", ""),
                "email": p.get("email") or m.get("email", ""),
                "passwd": p.get("passwd") or m.get("passwd", ""),
                "igg": p.get("igg") or m.get("igg", ""),
                "pay_until": m.get("pay_until", ""),
                "tariff_rub": m.get("tariff_rub", 0) or 0,
                "server": p.get("server") or m.get("server") or SERVER,
                "tg_tag": m.get("tg_tag", ""),
            }
            out.append(merged)

        return jsonify({
            "ok": True,
            "server": SERVER,
            "count": len(out),
            "items": out,
        })
    except Exception as exc:
        app.logger.exception("api_accounts_meta_full failed")
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/payalert")
def api_payalert():
    return jsonify(_get_pay_alerts())


@app.route("/api/payalert/extend/<acc_id>", methods=["POST"])
def api_payalert_extend(acc_id):
    conn = open_db(RESOURCES_DB)
    c    = conn.cursor()
    row  = c.execute(
            "SELECT pay_until FROM account_meta WHERE id=?",
            (acc_id,)).fetchone()
    today = date.today()
    if row and row[0]:
        try:
            cur = datetime.strptime(row[0], "%Y-%m-%d").date()
        except ValueError:
            cur = today
    else:
        cur = today

    new = cur + timedelta(days=30)
    c.execute("UPDATE account_meta SET pay_until=? WHERE id=?",
              (new.strftime("%Y-%m-%d"), acc_id))
    conn.commit(); conn.close()
    return jsonify({"status":"ok","new_date":new.strftime("%Y-%m-%d")})


@app.route("/api/accounts_meta", methods=["GET"])
def api_acc_meta_get():
    conn = open_db(RESOURCES_DB)
    rows = conn.execute("SELECT * FROM account_meta").fetchall()
    conn.close()
    return jsonify([{k[0]:row[idx] for idx,k in enumerate(conn.execute('PRAGMA table_info(account_meta)'))} for row in rows])

@app.route("/api/accounts_meta", methods=["PUT"])
def api_acc_meta_put():
    payload = request.json         # [{id,email,passwd,igg,pay_until,tariff_rub}, ...]
    if not isinstance(payload,list): return jsonify({"err":"bad"}),400
    conn = open_db(RESOURCES_DB); c = conn.cursor()
    for rec in payload:
        if "id" not in rec: continue
        cols = ["email","passwd","igg","pay_until","tariff_rub"]
        vals = [rec.get(k) for k in cols]
        c.execute(f"""
           INSERT INTO account_meta(id,{','.join(cols)})
           VALUES(?,?,?,?,?,?)
           ON CONFLICT(id) DO UPDATE SET
             {', '.join([f'{k}=excluded.{k}' for k in cols])}
        """, [rec["id"],*vals])
    conn.commit(); conn.close()
    return jsonify({"status":"ok"})

@app.route("/api/income")
def api_income():
    today = date.today()

    # последний день текущего месяца
    month_end = (today.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)

    conn = open_db(RESOURCES_DB)
    c    = conn.cursor()

    rows = c.execute("SELECT pay_until, tariff_rub FROM account_meta").fetchall()

    # есть ли таблица expenses → рассчитываем общую сумму расходов
    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='expenses'")
    if c.fetchone():
        exp = c.execute("SELECT COALESCE(SUM(amount),0) FROM expenses").fetchone()[0] or 0
    else:
        exp = 0

    conn.close()

    # общий доход (с учётом возможных NULL)
    total = sum((r[1] or 0) for r in rows)

    # доход, ещё оставшийся до конца текущего месяца
    left = 0
    for pay_until, tariff in rows:
        if not tariff:               # тариф может быть NULL
            continue
        try:
            if pay_until:
                pu_date = datetime.strptime(pay_until, "%Y-%m-%d").date()
                if today <= pu_date <= month_end:
                    left += tariff
        except ValueError:
            # неверный формат даты — игнорируем
            pass

    # вычитаем расходы из обеих сумм
    return jsonify({"total": total - exp, "left": left})

@app.route("/api/expenses", methods=["GET","POST","PUT"])
def api_expenses():
    conn = open_db(RESOURCES_DB)

    # гарантируем, что таблица есть ──────────────▼
    conn.execute("""CREATE TABLE IF NOT EXISTS expenses(
                      id INTEGER PRIMARY KEY AUTOINCREMENT,
                      amount INTEGER NOT NULL,
                      dt TEXT NOT NULL)""")

    if request.method == "POST":
        amt = int(request.json.get("amount",0))
        if amt <= 0:
            return jsonify({"err":"amount"}), 400
        conn.execute("INSERT INTO expenses(amount,dt) VALUES(?,?)",
                     (amt, datetime.now().isoformat()))
        conn.commit()

    elif request.method == "PUT":           # «записать новое значение»
        amt = int(request.json.get("amount",0))
        if amt < 0:
            return jsonify({"err":"negative"}), 400
        conn.execute("DELETE FROM expenses")
        if amt:
            conn.execute("INSERT INTO expenses(amount,dt) VALUES(?,?)",
                         (amt, datetime.now().isoformat()))
        conn.commit()

    total = conn.execute(
        "SELECT COALESCE(SUM(amount),0) FROM expenses").fetchone()[0]
    conn.close()
    return jsonify({"total": total})

@app.route("/api/taskState")
def api_task_state():
    tn  = request.args.get("name","")
    val = _task_enabled(tn)
    return jsonify({"name": tn, "enabled": val})



# === ДО ВСТАВКИ НАЙДИ БЛОК С ДРУГИМИ @app.route("/api/…") И ВСТАВЬ РЯДОМ ===
from datetime import datetime, timedelta, timezone
import re

# Парсим dt из cached_logs (формат: 'YYYY-MM-DD HH:MM:SS.mmm +HH:MM')
_DT_RE = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} [\+\-]\d{2}:\d{2})")
def _parse_dt_iso_with_tz(dt_str: str) -> datetime | None:
    try:
        # Совпадает с тем, что записываем в cached_logs (первые 23+6 символов)
        return datetime.strptime(dt_str.strip(), "%Y-%m-%d %H:%M:%S.%f %z")
    except Exception:
        return None

def _format_hms(total_seconds: int) -> str:
    if total_seconds < 0: total_seconds = 0
    h = total_seconds // 3600
    m = (total_seconds % 3600) // 60
    s = total_seconds % 60
    return f"{h:02d}:{m:02d}:{s:02d}"

def _housekeep_cached_logs(conn, keep_days: int = 14):
    """
    Мягкая чистка: удаляем логи старше N дней.
    Хранить дольше смысла нет — "время круга" считаем в окне (по умолчанию 24ч).
    """
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=keep_days)).astimezone()
        # dt в базе в текстовом виде, но ISO-морфный и лексикографически сравним.
        # Преобразуем к тому же формату, что сохраняем в cached_logs.dt:
        #   "YYYY-MM-DD HH:MM:SS.mmm +03:00"
        cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + cutoff.strftime(" %z")
        conn.execute("DELETE FROM cached_logs WHERE dt < ?", (cutoff_str,))
        conn.commit()
    except Exception as e:
        print("[housekeep_cached_logs] warn:", e)

def _compute_cycle_stats(window_hours: int = 24,
                         min_gap_minutes: int = 5,
                         max_gap_hours: int = 3) -> dict:
    """
    Считает среднее время круга:
      • берём по каждому активному аккаунту точки 'Account Done'
      • сортируем по времени, берём соседние интервалы
      • игнорируем интервалы < min_gap_minutes (глюки) и > max_gap_hours (аномалии/простои)
      • усредняем по всем валидным интервалам (взвешенно по числу интервалов)
    Возвращает JSON-словарь для фронта.
    """
    acts = load_profiles()
    active_ids = [(a["Id"], a.get("Name","")) for a in acts]
    if not active_ids:
        return {
            "avg_cycle_seconds": None,
            "avg_cycle_hms": "—",
            "accounts_used": 0,
            "intervals_used": 0,
            "window_hours": window_hours,
            "min_gap_minutes": min_gap_minutes,
            "max_gap_hours": max_gap_hours,
            "per_account": []
        }

    now_utc = datetime.now(timezone.utc)
    window_start = (now_utc - timedelta(hours=window_hours)).astimezone()
    window_str = window_start.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + window_start.strftime(" %z")

    conn = open_db(LOGS_DB)
    c = conn.cursor()

    # Раз в час делаем чистку старых записей (нечасто, дёшево)
    try:
        if datetime.now().minute == 0:
            _housekeep_cached_logs(conn, keep_days=14)
    except Exception as e:
        print("[cycle_stats] housekeeping skipped:", e)

    total_secs = 0
    total_intervals = 0
    per_acc = []

    min_gap = timedelta(minutes=min_gap_minutes)
    max_gap = timedelta(hours=max_gap_hours)

    for acc_id, nickname in active_ids:
        # Берём ТОЛЬКО сегодня/нужное окно, и только строки с "Account Done"
        rows = c.execute("""
          SELECT dt, raw_line
          FROM cached_logs
          WHERE acc_id=? AND dt >= ? AND raw_line LIKE '%Account Done%'
          ORDER BY id ASC
        """, (acc_id, window_str)).fetchall()

        times = []
        for dt_text, raw in rows:
            # подстрахуемся: проверим префикс даты точно в raw_line
            # но лучше парсить dt из столбца dt (он уже с таймзоной)
            d = _parse_dt_iso_with_tz(dt_text)
            if d is not None:
                times.append(d)

        if len(times) < 2:
            continue

        # находим интервалы между соседними "Account Done"
        good = []
        prev = times[0]
        for cur in times[1:]:
            delta = cur - prev
            prev = cur
            if delta < min_gap or delta > max_gap:
                # игнорим глюки (<5 мин) и нехарактерные длинные простои
                continue
            good.append(delta.total_seconds())

        if not good:
            continue

        acc_avg = sum(good) / len(good)
        per_acc.append({
            "id": acc_id,
            "nickname": nickname,
            "intervals": len(good),
            "avg_seconds": int(acc_avg),
            "avg_hms": _format_hms(int(acc_avg))
        })
        total_secs += sum(good)
        total_intervals += len(good)

    if total_intervals == 0:
        return {
            "avg_cycle_seconds": None,
            "avg_cycle_hms": "—",
            "accounts_used": 0,
            "intervals_used": 0,
            "window_hours": window_hours,
            "min_gap_minutes": min_gap_minutes,
            "max_gap_hours": max_gap_hours,
            "per_account": []
        }

    global_avg = int(total_secs / total_intervals)
    return {
        "avg_cycle_seconds": global_avg,
        "avg_cycle_hms": _format_hms(global_avg),
        "accounts_used": len(per_acc),
        "intervals_used": total_intervals,
        "window_hours": window_hours,
        "min_gap_minutes": min_gap_minutes,
        "max_gap_hours": max_gap_hours,
        "per_account": per_acc
    }

@app.route("/api/cycle_time")
def api_cycle_time():
    """
    Возвращает оценку "времени круга" по cached_logs:
      { avg_cycle_seconds, avg_cycle_hms, accounts_used, intervals_used, window_hours, ... }
    Параметры (query):
      window_hours=24  — окно анализа
      min_gap_minutes=5 — игнорировать интервалы меньше (защита от глюков)
      max_gap_hours=6   — отсекать слишком длинные простои
    """
    try:
        wh = int(request.args.get("window_hours", 24))
    except:
        wh = 24
    try:
        mg = int(request.args.get("min_gap_minutes", 5))
    except:
        mg = 5
    try:
        mx = int(request.args.get("max_gap_hours", 6))
    except:
        mx = 6

    stats = _compute_cycle_stats(window_hours=wh, min_gap_minutes=mg, max_gap_hours=mx)
    return jsonify(stats)
# ВРемя круга ВСЁ

import math
from collections import defaultdict

def _utcnow_local() -> datetime:
    return datetime.now(timezone.utc).astimezone()

def _range_start_end(range_key: str) -> tuple[str, str]:
    """
    Возвращаем (start_iso, end_iso) для окна 'day'|'week'|'month'
    в формате 'YYYY-MM-DD HH:MM:SS.mmm +HH:MM' (лексикографически сравним).
    """
    now = _utcnow_local()
    if range_key == "day":
        start = now - timedelta(days=1)
    elif range_key == "week":
        start = now - timedelta(days=7)
    else:
        start = now - timedelta(days=30)
    start_s = start.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + start.strftime(" %z")
    end_s   = now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + now.strftime(" %z")
    return start_s, end_s

def _estimate_account_cycle(acc_id: str,
                            min_gap_minutes: int = 5,
                            max_gap_hours: int = 6,
                            window_hours: int = 48) -> tuple[int|None, datetime|None]:
    """
    Оцениваем средний цикл аккаунта (сек) и ищем последний Account Done.
    """
    conn = open_db(LOGS_DB)
    c = conn.cursor()
    window_start = (_utcnow_local() - timedelta(hours=window_hours)).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + _utcnow_local().strftime(" %z")
    rows = c.execute("""
      SELECT dt
      FROM cached_logs
      WHERE acc_id = ? AND dt >= ? AND raw_line LIKE '%Account Done%'
      ORDER BY id ASC
    """, (acc_id, window_start)).fetchall()

    times = []
    last_done = None
    for (dt_text,) in rows:
        d = _parse_dt_iso_with_tz(dt_text)
        if d:
            times.append(d)
            last_done = d

    if len(times) < 2:
        return (None, last_done)

    min_gap = timedelta(minutes=min_gap_minutes)
    max_gap = timedelta(hours=max_gap_hours)
    good = []
    p = times[0]
    for cur in times[1:]:
        delta = cur - p
        p = cur
        if delta < min_gap or delta > max_gap:
            continue
        good.append(delta.total_seconds())

    if not good:
        return (None, last_done)

    return (int(sum(good)/len(good)), last_done)

def _group_key(ts: datetime, range_key: str) -> str:
    if range_key == "day":
        return ts.strftime("%Y-%m-%d %H:00")  # почасовые бины
    else:
        return ts.strftime("%Y-%m-%d")        # по дням
# Конец НОВЫХ ПОМОЩНИКОВ


@app.route("/api/account_stats")
def api_account_stats():
    """
    Агрегаты/график по аккаунту с учётом локальной зоны пользователя.

    Параметры:
      acc_id=<id>            (обяз.)
      range=day|week|month   (по умолчанию day)
      mode=normal|losses     (по умолчанию normal)
      tz_offset=<минуты>     (минуты смещения от UTC по данным браузера, как в JS: new Date().getTimezoneOffset(); для МСК будет -180)

    Определения:
      - "Сутки": от локальной полуночи пользователя до "сейчас".
      - "Неделя": сумма net-приростов по каждому календарному дню за последние 7 локальных дней.
      - "Месяц": сумма net-приростов по дням за последние 30 локальных дней.
      - "Обычный": положительные дельты (max(delta,0)); "Минусы": |отрицательные дельты|.
      - Точки графика: "Сутки" — по часам, "Неделя/Месяц" — по дням.
    """
    acc_id = request.args.get("acc_id", "").strip()
    if not acc_id:
        return jsonify({"error": "acc_id required"}), 400

    range_key = (request.args.get("range", "day") or "day").lower()
    if range_key not in ("day", "week", "month"):
        range_key = "day"

    mode = (request.args.get("mode", "normal") or "normal").lower()
    if mode not in ("normal", "losses"):
        mode = "normal"

    # смещение зоны пользователя (минуты)
    try:
        tz_off_min = int(request.args.get("tz_offset", "0"))
    except Exception:
        tz_off_min = 0

    # хелперы преобразования времени
    from datetime import timezone
    user_tz = timezone(timedelta(minutes=-tz_off_min))  # в JS offset отрицательный для восточных зон; приводим к tzinfo

    def to_dt(dt_text: str) -> datetime | None:
        d = _parse_dt_iso_with_tz(dt_text)
        return d

    def to_user_local(d: datetime) -> datetime:
        # d — aware (с TZ из лога), приводим к зоне пользователя
        return d.astimezone(user_tz)

    # Вытаскиваем снапшоты за последние 35 суток (чтобы хватило для month)
    conn = open_db(LOGS_DB)
    c = conn.cursor()
    cutoff = (datetime.now(timezone.utc).astimezone() - timedelta(days=35))
    cutoff_s = cutoff.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + cutoff.strftime(" %z")
    rows = c.execute("""
      SELECT dt, food, wood, stone, gold
      FROM resource_snapshots
      WHERE acc_id = ? AND dt >= ?
      ORDER BY id ASC
    """, (acc_id, cutoff_s)).fetchall()

    # Преобразуем и упорядочим
    snaps = []
    for dt_text, f, w, s, g in rows:
        d = to_dt(dt_text)
        if not d:
            continue
        snaps.append((d, int(f or 0), int(w or 0), int(s or 0), int(g or 0)))
    if len(snaps) < 2:
        # попробуем вернуть хотя бы метаданные + пустые ряды
        avg_sec, last_done = _estimate_account_cycle(acc_id)
        return jsonify({
            "acc_id": acc_id,
            "nickname": next((a.get("Name","") for a in load_profiles() if a["Id"]==acc_id), None),
            "range": range_key,
            "mode": mode,
            "points": [],
            "totals": {"day":{"food":0,"wood":0,"stone":0,"gold":0},
                       "week":{"food":0,"wood":0,"stone":0,"gold":0},
                       "month":{"food":0,"wood":0,"stone":0,"gold":0}},
            "forecast_month": {"food":0,"wood":0,"stone":0,"gold":0,"ok": False},
            "last_done_iso": last_done.strftime("%Y-%m-%dT%H:%M:%S%z") if last_done else None,
            "next_eta_iso": (last_done + timedelta(seconds=avg_sec)).strftime("%Y-%m-%dT%H:%M:%S%z") if last_done and avg_sec else None,
            "next_eta_seconds": avg_sec,
            "cycle_avg_seconds": avg_sec,
            "available_days": 0
        })

    # Строим дельты между соседними снапшотами
    deltas = []  # (ts_user_local, dF, dW, dS, dG)
    prev = snaps[0]
    for cur in snaps[1:]:
        (dp, fp, wp, sp, gp) = prev
        (dc, fc, wc, sc, gc) = cur
        dd = dc  # время текущей точки
        # дельта как изменение остатков между соседними снапшотами
        dF, dW, dS, dG = (fc - fp), (wc - wp), (sc - sp), (gc - gp)
        # в нужном режиме
        if mode == "normal":
            dF = max(dF, 0); dW = max(dW, 0); dS = max(dS, 0); dG = max(dG, 0)
        else:
            dF = abs(min(dF, 0)); dW = abs(min(dW, 0)); dS = abs(min(dS, 0)); dG = abs(min(dG, 0))
        deltas.append((to_user_local(dd), dF, dW, dS, dG))
        prev = cur

    # Границы периодов (локальные для пользователя)
    now_u = datetime.now(user_tz)
    # полуночь сегодняшняя
    today_start = now_u.replace(hour=0, minute=0, second=0, microsecond=0)
    # массив стартов для 7 и 30 суток назад
    week_starts = [ (today_start - timedelta(days=i)) for i in range(7, -1, -1) ]  # 8 меток от -7 до 0
    month_starts = [ (today_start - timedelta(days=i)) for i in range(30, -1, -1) ]  # 31 метка

    # Агрегируем по бинам:
    #  - день: каждый bin = 1 час от today_start
    #  - неделя/месяц: bin = календарный день
    from collections import defaultdict
    day_bins = defaultdict(lambda: [0,0,0,0])
    week_bins = defaultdict(lambda: [0,0,0,0])
    month_bins = defaultdict(lambda: [0,0,0,0])

    for ts, f, w, s, g in deltas:
        # сутки (с полуночи по часам)
        if ts >= today_start and ts <= now_u:
            key_h = ts.replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:00")
            day_bins[key_h][0]+=f; day_bins[key_h][1]+=w; day_bins[key_h][2]+=s; day_bins[key_h][3]+=g
        # неделя (календарные дни)
        if ts >= (today_start - timedelta(days=7)) and ts <= now_u:
            key_d = ts.replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%d")
            week_bins[key_d][0]+=f; week_bins[key_d][1]+=w; week_bins[key_d][2]+=s; week_bins[key_d][3]+=g
        # месяц (календарные дни)
        if ts >= (today_start - timedelta(days=30)) and ts <= now_u:
            key_m = ts.replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%d")
            month_bins[key_m][0]+=f; month_bins[key_m][1]+=w; month_bins[key_m][2]+=s; month_bins[key_m][3]+=g

    # Точки для графика под выбранный диапазон
    points = []
    if range_key == "day":
        keys_sorted = sorted(day_bins.keys())
        for k in keys_sorted:
            F,W,S,G = day_bins[k]
            points.append({"ts": k, "food":F, "wood":W, "stone":S, "gold":G})
    elif range_key == "week":
        keys_sorted = sorted(week_bins.keys())
        for k in keys_sorted:
            F,W,S,G = week_bins[k]
            points.append({"ts": k, "food":F, "wood":W, "stone":S, "gold":G})
    else:
        keys_sorted = sorted(month_bins.keys())
        for k in keys_sorted:
            F,W,S,G = month_bins[k]
            points.append({"ts": k, "food":F, "wood":W, "stone":S, "gold":G})

    # TOTАLS:
    #  - day: просто сумма day_bins
    #  - week: сумма по каждому дню недели (уже бины)
    #  - month: сумма по каждому дню месяца (уже бины)
    def totals_of(bins_dict):
        t = [0,0,0,0]
        for v in bins_dict.values():
            t[0]+=v[0]; t[1]+=v[1]; t[2]+=v[2]; t[3]+=v[3]
        return {"food":t[0], "wood":t[1], "stone":t[2], "gold":t[3]}

    totals_day   = totals_of(day_bins)
    totals_week  = totals_of(week_bins)
    totals_month = totals_of(month_bins)

    # FORECAST: если есть >= 5-7 дней покрытия — пропорция на 30 дней
    covered_days = len(week_bins.keys())
    forecast_ok = covered_days >= 5
    k = 30/7
    forecast = {"food":0,"wood":0,"stone":0,"gold":0,"ok": False}
    if forecast_ok:
        forecast = {
            "food":  int(totals_week["food"]  * k),
            "wood":  int(totals_week["wood"]  * k),
            "stone": int(totals_week["stone"] * k),
            "gold":  int(totals_week["gold"]  * k),
            "ok": True
        }

    # последний Account Done и ETA следующего (как было)
    avg_sec, last_done = _estimate_account_cycle(acc_id)
    next_eta_iso = None
    if last_done and avg_sec:
        next_eta_iso = (last_done + timedelta(seconds=avg_sec)).strftime("%Y-%m-%dT%H:%M:%S%z")

    # available_days — span по снапшотам (UTC-независимо)
    try:
        d0 = snaps[0][0]; d1 = snaps[-1][0]
        available_days = max(0, int((d1 - d0).total_seconds() // 86400))
    except:
        available_days = 0

    nickname = None
    try:
        for a in load_profiles():
            if a["Id"] == acc_id:
                nickname = a.get("Name","")
                break
    except:
        pass

    return jsonify({
        "acc_id": acc_id,
        "nickname": nickname,
        "range": range_key,
        "mode": mode,
        "points": points,
        "totals": {
            "day":   totals_day,
            "week":  totals_week,
            "month": totals_month
        },
        "forecast_month": forecast,
        "last_done_iso": last_done.strftime("%Y-%m-%dT%H:%M:%S%z") if last_done else None,
        "next_eta_iso": next_eta_iso,
        "next_eta_seconds": avg_sec,
        "cycle_avg_seconds": avg_sec,
        "available_days": available_days
    })

# 1.4. API: агрегаты и ряд для графика
# После твоего /api/cycle_time добавь новый маршрут(они выше)

@app.route("/manage")
def manage_page():
    """Отдаём manage.html"""
    return render_template("manage.html")

@app.route("/api/manage/accounts", methods=["GET"])
def api_manage_accounts():
    """Возвращаем список всех аккаунтов из PROFILE_PATH"""
    if not os.path.exists(PROFILE_PATH):
        return jsonify([])
    with open(PROFILE_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    # data — массив: [{ "Name":"...", "Id":"...", "Active":..., ...}, ...]
    # Возвращаем как есть
    return jsonify(data)


def _parse_json_field(raw_value, default):
    """
    Аккуратно парсим JSON из строки, возвращаем default при ошибке.
    Если значение уже dict/list — возвращаем как есть.
    """
    if isinstance(raw_value, (dict, list)):
        return raw_value
    try:
        return json.loads(raw_value)
    except Exception:
        return default

def _strip_start(logs):
    """
    Для промежуточных FIX’ов обрезает всё от запуска GnBots.exe и дальше,
    чтобы старт происходил только в самом конце.
    """
    out = []
    for line in logs:
        if line.startswith("Запускаем GnBots.exe"):
            break
        out.append(line)
    return out

@app.route("/api/manage/account/<acc_id>/settings", methods=["GET"])
def api_manage_account_settings(acc_id):
    # Читаем общий JSON
    if not os.path.exists(PROFILE_PATH):
        return jsonify({"error": "profile not found"}), 404
    with open(PROFILE_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Находим аккаунт
    acc = next((a for a in data if a.get("Id") == acc_id), None)
    if not acc:
        return jsonify({"error": "acc not found"}), 404

    # Парсим строку Data в JSON
    settings = _parse_json_field(acc.get("Data", "[]"), [])
    menu     = _parse_json_field(acc.get("MenuData", "{}"), {})
    return jsonify({"Data": settings, "MenuData": menu})

@app.route("/api/manage/account/<acc_id>/settings/<int:step_idx>", methods=["PUT"])
def api_manage_account_setting_step(acc_id, step_idx):
    """
    PUT /api/manage/account/<acc_id>/settings/<step_idx>
    payload может содержать:
      - Config: {ключ: новое_значение, …}
      - IsActive: true|false
      - ScheduleRules: [ …новый массив правил… ]
    """
    payload     = request.get_json(silent=True) or {}
    cfg_updates = payload.get("Config", {})
    new_active  = payload.get("IsActive", None)
    new_rules   = payload.get("ScheduleRules", None)

    # 1) читаем текущий профиль
    with open(PROFILE_PATH, "r", encoding="utf-8") as f:
        all_accs = json.load(f)

    # 2) ищем нужный аккаунт
    for acc in all_accs:
        if acc.get("Id") == acc_id:
            data_list = _parse_json_field(acc.get("Data", "[]"), [])
            if not isinstance(data_list, list):
                return jsonify({"error": "invalid data format"}), 400

            # проверяем step_idx
            if step_idx < 0 or step_idx >= len(data_list):
                return jsonify({"error": "step_idx out of range"}), 400

            step = data_list[step_idx]

            # 3a) обновляем активность
            if new_active is not None:
                step["IsActive"] = bool(new_active)

            # 3b) обновляем конфиг
            conf = step.get("Config", {})
            for key, val in cfg_updates.items():
                if isinstance(conf.get(key), dict) and "value" in conf[key]:
                    conf[key]["value"] = val
                else:
                    conf[key] = val

            # 3c) обновляем расписание, если передали
            if new_rules is not None:
                step["ScheduleRules"] = new_rules

            # 4) сохраняем обратно в JSON-профиль (компактно, без пробелов)
            acc["Data"] = json.dumps(
                data_list,
                ensure_ascii=False,
                separators=(',', ':')   # ← убираем лишние пробелы
            )
            break
    else:
        return jsonify({"error": "acc not found"}), 404

    # 5) перезаписываем файл
    with open(PROFILE_PATH, "w", encoding="utf-8") as f:
        json.dump(all_accs, f, indent=2, ensure_ascii=False)

    return jsonify({"status": "ok"})



@app.route("/api/fix/batch", methods=["POST"])
def api_fix_batch():
    """
    Принимает JSON вида {"acc_ids": ["id1","id2",...]},
    прогоняет do_fix_logic для каждого acc_id, но старт GnBots.exe
    (последняя часть логов) оставляет только для последнего аккаунта.
    """
    data = request.get_json() or {}
    ids  = data.get("acc_ids", [])
    backup_dir = (data.get("backup_dir") or "").strip()

    cfg_override = data.get("cfg_src_override") or (
        os.path.join(BACKUP_CONFIG_DST_ROOT, backup_dir) if backup_dir else None
    )

    if not ids:
        return jsonify({"error": "acc_ids missing or empty"}), 400

    batch_logs = []
    total = len(ids)
    for idx, acc_id in enumerate(ids):
        # собственно фиксим
        single = do_fix_logic(acc_id,
                      only_config=False,
                      cfg_src_override=cfg_override)
        # для всех кроме последнего — отрезаем часть со стартом
        if idx < total - 1:
            single = _strip_start(single)
        batch_logs.extend(single)

    return jsonify({"logs": batch_logs})


# ───── вставьте рядом с другими /api/fix/... роутами ─────
@app.route("/api/fix/replace_configs", methods=["POST"])
def api_replace_configs():
    """
    JSON {"backup_dir":"ДД__ММ__ГГГГ"}  – копирует ВСЕ config’ы.
             "" или параметра нет → стандартный SRC_VMS\\config
    """
    data = request.get_json() or {}
    bdir = data.get("backup_dir", "") or None
    logs = emergency_replace_configs(bdir)
    return jsonify({"logs": logs})


@app.route("/api/check_ld", methods=["POST"])
def api_check_ld():
    # полный путь до скрипта
    script_path = os.path.join(BASE_DIR, "LD_check.py")
    logs = []

    if not os.path.isfile(script_path):
        return jsonify({"logs": [f"LD_check.py не найден по пути {script_path}"]}), 404

    try:
        # запускаем скрипт по абсолютному пути
        proc = subprocess.Popen(
            ["python", script_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True
        )
        # читаем построчно
        for line in proc.stdout:
            logs.append(line.rstrip())
        proc.wait()
    except Exception as e:
        logs.append(f"Error running LD_check.py: {e}")

    return jsonify({"logs": logs})


@app.route("/api/crashedEmus")
def api_crashed_emu():
    path = r"C:\LDPlayer\ldChecker\crashed.json"
    if not os.path.exists(path):
        return jsonify([])
    with open(path, "r", encoding="utf-8") as f:
        arr = json.load(f)
    return jsonify(arr)  # например ["leidian5.config", "leidian36.config"]




@app.route("/api/paths", methods=["GET"])
def api_get_paths():
    # просто отдаем содержимое config.json
    return jsonify(CONFIG)

@app.route("/api/paths", methods=["PUT"])
def api_put_paths():
    data = request.get_json()
    # сохраняем только ключи, которые уже есть в config.json
    for k in list(CONFIG):
        if k in data and isinstance(data[k], str):
            CONFIG[k] = data[k]
    # перезаписываем файл
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(CONFIG, f, ensure_ascii=False, indent=2)
    return jsonify({"status": "ok", "paths": CONFIG})

# === I18N endpoints (Flask) =======================================
from flask import Blueprint, request, jsonify
from pathlib import Path
import json, tempfile, os

i18n_bp = Blueprint('i18n', __name__)

I18N_DIR = Path("./static/i18n")
I18N_DIR.mkdir(parents=True, exist_ok=True)
def _i18n_path(lang:str)->Path:
    safe = "".join(c for c in lang if c.isalnum() or c in ('-','_')).strip() or "ru"
    return I18N_DIR / f"{safe}.json"

@i18n_bp.route("/api/manage/i18n", methods=["GET"])
def get_i18n():
    lang = request.args.get("lang", "ru")
    p = _i18n_path(lang)
    if not p.exists():
        return jsonify({"script_labels":{}, "config_labels":{}, "option_labels":{}, "order_map":{}})
    try:
        with p.open("r", encoding="utf-8") as f:
            data = json.load(f)
        for k in ("script_labels","config_labels","option_labels","order_map"):
            data.setdefault(k, {})
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": f"i18n read error: {e}"}), 500

@i18n_bp.route("/api/manage/i18n", methods=["PUT"])
def put_i18n():
    lang = request.args.get("lang", "ru")
    p = _i18n_path(lang)
    payload = request.get_json(force=True, silent=True) or {}
    for k in ("script_labels","config_labels","option_labels","order_map"):
        payload.setdefault(k, {})
    fd, tmp_name = tempfile.mkstemp(prefix="i18n_", suffix=".json", dir=str(I18N_DIR))
    os.close(fd)
    try:
        with open(tmp_name, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        os.replace(tmp_name, p)
        return jsonify({"ok": True})
    except Exception as e:
        try: os.remove(tmp_name)
        except: pass
        return jsonify({"error": f"i18n write error: {e}"}), 500

# === регистрация блюпринта ===
# app.register_blueprint(i18n_bp)


# ДОБАВИТЬ где-нибудь рядом с другими /api/… роутами
@app.route("/api/backup_dirs")
def api_backup_dirs():
    """
    Возвращает массив из максимум 5 последних папок-дат
    внутри C:\\LD_backup\\configs\\
    """
    root = BACKUP_CONFIG_DST_ROOT
    if not os.path.isdir(root):
        return jsonify([])

    def is_date_dir(d):
        try:
            datetime.strptime(d, "%d__%m__%Y")
            return True
        except ValueError:
            return False

    dirs = [d for d in os.listdir(root)
            if os.path.isdir(os.path.join(root, d)) and is_date_dir(d)]
    dirs.sort(key=lambda s: datetime.strptime(s, "%d__%m__%Y"), reverse=True)
    return jsonify(dirs[:5])


@app.route("/api/forceRefreshToday", methods=["POST"])
def api_force_refresh_today():
    """
    Полностью перечитывает сегодняшние логи, игнорируя offsets,
    и снова парсит их в базу.
    """
    today_str = datetime.now().strftime("%Y%m%d")

    # 1) Сбрасываем offsets для любых файлов, у которых fname.startswith("bot" + today_str)
    conn_log = open_db(LOGS_DB)
    c_log = conn_log.cursor()
    # Получим все offsets
    rows = c_log.execute("SELECT filename FROM files_offset").fetchall()
    for (fname,) in rows:
        if fname.startswith("bot"+today_str) and fname.endswith(".txt"):
            c_log.execute("DELETE FROM files_offset WHERE filename=?", (fname,))
    conn_log.commit()
    conn_log.close()

    # 2) parse_logs() заново всё перечитает
    parse_logs()

    return {"status":"ok","message":"All logs for today re-read.", "timestamp": datetime.now().isoformat()}

@app.route("/api/manage/account/<acc_id>", methods=["PUT"])
def api_manage_account_update(acc_id):
    """Получаем {Active:true/false}, записываем в JSON."""
    req_data = request.json
    new_active = bool(req_data.get("Active", False))

    # Читаем JSON
    if not os.path.exists(PROFILE_PATH):
        return jsonify({"error":"profile not found"}),404
    with open(PROFILE_PATH,"r",encoding="utf-8") as f:
        data = json.load(f)  # массив

    # Ищем нужный аккаунт
    found = None
    for acc in data:
        if acc.get("Id")==acc_id:
            found=acc
            break
    if not found:
        return jsonify({"error":"acc not found"}),404

    # меняем
    found["Active"] = new_active

    # сохраняем
    with open(PROFILE_PATH,"w",encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

    return jsonify({"status":"ok","acc_id":acc_id,"Active":new_active})


@app.route("/api/resources")
def api_resources():
    acts= load_profiles()
    active_ids= {a["Id"] for a in acts}
    inst_map= {a["Id"]: a.get("InstanceId",-1) for a in acts}

    conn= open_db(RESOURCES_DB)
    c= conn.cursor()
    rows= c.execute("SELECT id,nickname,food,wood,stone,gold,gems,last_updated FROM resources").fetchall()
    conn.close()

    today_str= datetime.now().strftime("%Y-%m-%d")
    conn= open_db(RESOURCES_DB)
    c= conn.cursor()
    base_rows= c.execute("""
      SELECT id,food,wood,stone,gold,gems
      FROM daily_baseline
      WHERE baseline_date=?
    """,(today_str,)).fetchall()
    base_map={}
    for br in base_rows:
        base_map[br[0]]= (br[1],br[2],br[3],br[4],br[5])
    conn.close()

    totf=0; totw=0; tots=0; totg=0; totm=0
    accounts=[]
    for (acc_id,nick,f,w,s,g,m,lastupd) in rows:
        if acc_id not in active_ids:
            continue
        totf+=f; totw+=w; tots+=s; totg+=g; totm+=m
        bf=bw=bs=bg=bgems=0
        if acc_id in base_map:
            bf,bw,bs,bg,bgems= base_map[acc_id]
        gf= f-bf; gw= w-bw; gs= s-bs; gg= g-bg
        dayGain= gf+gw+gs+gg

        # Формируем food_view...
        def format_view(curVal,diffVal):
            base= shorten_number(curVal)
            if diffVal==0: 
                return base
            sign="+" if diffVal>0 else "-"
            dif= shorten_number(abs(diffVal))
            return f"{base}<span class='gainValue'>{sign}{dif}</span>"

        fv= format_view(f, gf)
        wv= format_view(w, gw)
        sv= format_view(s, gs)
        gv= format_view(g, gg)
        mv= shorten_number(m)
        #   —‑ тариф из account_meta
        if 'meta_map' not in globals():
            connT = open_db(RESOURCES_DB)
            meta_map = dict(connT.execute("SELECT id, tariff_rub FROM account_meta").fetchall())
            connT.close()
        tariff = meta_map.get(acc_id, 0)
        tariff_view = "0₽" if tariff is None else f"{tariff:,}₽".replace(",", " ")


        accounts.append({
          "id": acc_id,
          "nickname": nick,
          "instanceId": inst_map.get(acc_id,-1),

          "food_raw": f,
          "wood_raw": w,
          "stone_raw": s,
          "gold_raw": g,
        #   "gems_raw": m,

          "food_view": fv,
          "wood_view": wv,
          "stone_view": sv,
          "gold_view": gv,
        #   "gems_view": mv,
          "tariff_raw": tariff,
          "tariff_view": tariff_view,


          "today_gain": shorten_number(dayGain),
          "last_updated": lastupd
        })

    return jsonify({
      "accounts": accounts,
      "account_count": len(accounts),
      "totals":{
        "food": shorten_number(totf),
        "wood": shorten_number(totw),
        "stone": shorten_number(tots),
        "gold": shorten_number(totg),
        "gems": shorten_number(totm)
      }
    })

@app.route("/api/stop", methods=["POST"])
def api_stop():
    logs= do_stop_logic()
    return {"status":"ok","logs": logs}

@app.route("/api/reboot", methods=["POST"])
def api_reboot():
    logs= do_reboot_logic()
    return {"status":"ok","logs": logs}

@app.route("/api/serverStop", methods=["POST"])
def api_server_stop():
    name = request.args.get("name", "")
    server = next((s for s in SERVERS if s["name"] == name), None)
    if not server:
        return jsonify({"error": "server not found"}), 404
    logs = stop_remote_ssh(server)
    return jsonify({"status": "ok", "logs": logs})

@app.route("/api/serverReboot", methods=["POST"])
def api_server_reboot():
    name = request.args.get("name", "")
    server = next((s for s in SERVERS if s["name"] == name), None)
    if not server:
        return jsonify({"error": "server not found"}), 404
    logs = reboot_remote_ssh(server)
    return jsonify({"status": "ok", "logs": logs})

@app.route("/api/rs_cleanup", methods=["POST"])
def api_rs_cleanup():
    """
    Удаляет снапшоты за последние N дней (по умолчанию 7), чтобы пересчитать их заново из логов.
    Вызывай перед 'Перечитать логи'.
    """
    try:
        days = int(request.args.get("days", 7))
    except:
        days = 7
    conn = open_db(LOGS_DB)
    cut = (datetime.now(timezone.utc).astimezone() - timedelta(days=days))
    cut_s = cut.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + cut.strftime(" %z")
    conn.execute("DELETE FROM resource_snapshots WHERE dt >= ?", (cut_s,))
    conn.commit()
    conn.close()
    return jsonify({"ok": True, "deleted_since": cut_s, "days": days})


# ── после других @app.route("/api/manage/...") ──
@app.route("/api/manage/copy_settings", methods=["POST"])
def api_copy_settings():
    """
    JSON: {source_id:str, dest_ids:[str,…]}
    Копирует ДАННЫЕ (Data) источника во все dest_ids.
    """
    payload   = request.get_json() or {}
    src_id    = payload.get("source_id")
    dest_ids  = payload.get("dest_ids") or []
    if not src_id or not dest_ids:
        return jsonify({"err":"bad"}),400

    with open(PROFILE_PATH, "r", encoding="utf-8") as f:
        prof = json.load(f)

    # находим источник
    src = next((a for a in prof if a.get("Id")==src_id), None)
    if not src:
        return jsonify({"err":"src not found"}),404

    # Нормализуем JSON-текст Data источника (убираем лишние пробелы).
    src_data_raw = src.get("Data", "[]")
    try:
        src_data_norm = json.dumps(
            json.loads(src_data_raw),
            ensure_ascii=False,
            separators=(',', ':')  # ← компактный вид: без пробелов
        )
    except Exception:
        # Если вдруг не распарсилось — переносим как есть.
        src_data_norm = src_data_raw

    for acc in prof:
        if acc.get("Id") in dest_ids:
            # копируем ТОЛЬКО Data (шаги), MenuData не трогаем
            acc["Data"] = src_data_norm

    with open(PROFILE_PATH, "w", encoding="utf-8") as f:
        json.dump(prof, f, ensure_ascii=False, indent=2)

    return jsonify({"status":"ok"})



@app.route("/api/logstatus")
def api_logstatus():
    acts = load_profiles()
    active_ids = {acc["Id"] for acc in acts}

    conn = open_db(RESOURCES_DB)
    c = conn.cursor()
    rows = c.execute("SELECT id, nickname, food, wood, stone, gold, gems FROM resources").fetchall()
    conn.close()

    today_str = datetime.now().strftime("%Y-%m-%d")
    conn = open_db(RESOURCES_DB)
    c = conn.cursor()
    base_rows = c.execute("""
      SELECT id, food, wood, stone, gold, gems
      FROM daily_baseline
      WHERE baseline_date=?
    """,(today_str,)).fetchall()
    base_map={}
    for (bid, bf, bw, bs, bg, bgm) in base_rows:
        base_map[bid] = (bf, bw, bs, bg, bgm)
    conn.close()

    status = {}
    for (acc_id, nick, f, w, s, g, m) in rows:
        if acc_id not in active_ids:
            continue
        
        bf=bw=bs=bg=bgem=0
        if acc_id in base_map:
            bf,bw,bs,bg,bgem = base_map[acc_id]
        gf = f - bf
        gw = w - bw
        gs = s - bs
        gg = g - bg
        dayGain = gf+gw+gs+gg
        zeroGain = (dayGain == 0)

        status[acc_id] = {
          "nickname": nick,
          "zeroGain": zeroGain,
          "hasNoMoreMarches": False
        }

    connL = open_db(LOGS_DB)
    cL = connL.cursor()
    tday = datetime.now().strftime("%Y-%m-%d")
    for acid in status.keys():
        rows_m = cL.execute("""
          SELECT raw_line
          FROM cached_logs
          WHERE acc_id=?
          ORDER BY id DESC
          LIMIT 1000
        """,(acid,)).fetchall()
        foundNoMore=False
        foundUpdate=False
        for (line,) in rows_m:
            if line.startswith(tday):
                ll = line.lower()
                if ("no more marches left" in ll) or ("reached maximum of marches" in ll):
                    foundNoMore = True
                if "update the game" in ll:
                    foundUpdate = True    
                    break
        status[acid]["hasNoMoreMarches"] = foundNoMore
        status[acid]["hasUpdateGame"] = foundUpdate
    connL.close()

    return jsonify(status)


# Принимаем и GET, и POST. Читаем JSON-тело или query-параметры.
@app.route("/api/fix/do", methods=["GET", "POST"])
def api_fix_do():
    data = request.get_json(silent=True) or request.args

    acc_id_raw   = (data.get("acc_id") or "").strip()
    only_raw     = str(data.get("config_only", "0")).strip().lower()
    backup_dir   = (data.get("backup_dir") or "").strip()
    # Если явно передали cfg_src_override — используем его; иначе построим из backup_dir
    cfg_override = data.get("cfg_src_override") or (
        os.path.join(BACKUP_CONFIG_DST_ROOT, backup_dir) if backup_dir else None
    )

    only_config = (only_raw in ("1", "true", "yes", "on"))
    if not acc_id_raw:
        return jsonify({"error": "acc_id required"}), 400

    logs = do_fix_logic(
        acc_id_raw,
        only_config=only_config,
        cfg_src_override=cfg_override
    )
    return jsonify({"ok": True, "logs": logs})


@app.route("/api/fix/config_batch", methods=["POST"])
def api_fix_config_batch():
    data       = request.get_json() or {}
    ids        = data.get("acc_ids", [])
    backup_dir = (data.get("backup_dir") or "").strip()

    cfg_override = data.get("cfg_src_override") or (
        os.path.join(BACKUP_CONFIG_DST_ROOT, backup_dir) if backup_dir else None
    )

    if not ids:
        return jsonify({"error": "acc_ids missing or empty"}), 400

    out = []
    for acc_id in ids:
        out += do_fix_logic(
            acc_id,
            only_config=True,
            cfg_src_override=cfg_override
        )
    return jsonify({"ok": True, "logs": out})


@app.route("/api/logs")
def api_logs():
    acc_id= request.args.get("acc_id")
    if not acc_id:
        return {"error":"no acc_id"},400
    conn= open_db(LOGS_DB)
    c= conn.cursor()
    rows= c.execute("""
      SELECT dt, raw_line
      FROM cached_logs
      WHERE acc_id=?
      ORDER BY id DESC
      LIMIT 300
    """,(acc_id,)).fetchall()
    conn.close()

    lines=[]
    for (dt_part, ls) in rows:
        if "[DBG]" in ls:
            continue
        lines.append(transformLogLine(dt_part, ls))
    lines.reverse()
    if len(lines)>290:
        lines= lines[-290:]
    return {"acc_id":acc_id,"logs": lines}


if __name__=="__main__":
    if not os.path.exists(RESOURCES_DB):
        print("Создаём базу ресурсов:", RESOURCES_DB)
    init_resources_db()

    if not os.path.exists(LOGS_DB):
        print("Создаём базу логов:", LOGS_DB)
    init_logs_db()

    health_check()
    init_resources_db()
    init_accounts_db()

    sync_account_meta()
    parse_logs()

    ensure_today_backups()      # ➟ создаст бэкапы, если их ещё нет за сегодня
    _schedule_daily_backups()   # ➟ запустит фоновый планировщик на полуночь
    _schedule_pay_notifications()  # 09:00 & 18:00 Telegram-оповещения
    _schedule_inactive_checker()   # ← запуск «монитора 15 ч»



    LAST_UPDATE_TIME= datetime.now(timezone.utc)
    app.run(debug=True, host="0.0.0.0", port=5001)
