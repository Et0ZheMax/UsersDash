import os
import json
import re
import stat
import sqlite3
import psutil
import subprocess
import time
import shutil
import ctypes
import sys
import csv
import threading
from pathlib import Path
from datetime import datetime, timezone, date, timedelta
from io import BytesIO
from PIL import ImageGrab
import base64
import requests

from flask import Flask, jsonify, render_template, request
from flask_cors import CORS

# Пример: задаём своему скрипту заголовок «MyUniqueScript»
title = "RssV7_RSS"
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


BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# после BASE_DIR
CONFIG_PATH = os.path.join(BASE_DIR, "config.json")
with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    CONFIG = json.load(f)

# Заменяем твои прежние константы на:
LOGS_DIR        = CONFIG["LOGS_DIR"]
PROFILE_PATH    = CONFIG["PROFILE_PATH"]
SRC_VMS         = CONFIG["SRC_VMS"]
DST_VMS         = CONFIG["DST_VMS"]
GNBOTS_SHORTCUT = CONFIG["GNBOTS_SHORTCUT"]
SERVER          = CONFIG.get("SERVER_NAME") or "RSS4SALE"
USERSDASH_API_URL   = CONFIG.get("USERSDASH_API_URL", "")
USERSDASH_API_TOKEN = CONFIG.get("USERSDASH_API_TOKEN", "")

USERDASH_DB = os.path.abspath(os.path.join(BASE_DIR, "..", "UsersDash", "data", "app.db"))



LOG_DIR = r"C:\Program Files (x86)\GnBots\logs"   # как в настройках
RESOURCES_DB = os.path.join(BASE_DIR, "resources_web.db")
LOGS_DB      = os.path.join(BASE_DIR, "logs_cache.db")
CRASHED_JSON_PATH = r"C:\LDPlayer\ldChecker\crashed.json"
LOCAL_CRASHED_JSON = os.path.join(BASE_DIR, "crashed.json")

APP_START_TIME = datetime.now(timezone.utc)
DAILY_BACKUP_THREAD = None
BACKGROUND_FLAGS = {"daily_backup_scheduler": False}

# ──────────── Ш А Б Л О Н 650 ────────────
TEMPLATES = {
    "650": r"""[{"ScriptId":"vikingbot.base.gathervip","Uid":"vikingbot.base.gathervip_3","OrderId":6,"Config":{"LevelStartAt":{"value":"3","options":["1","2","3","4","5","6"]},"Monster":false,"Niflung":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Divine":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Farm":{"value":"3","options":["off","1","2","3","4","5","6","7","8","9"]},"Sawmill":{"value":"3","options":["off","1","2","3","4","5","6","7","8","9"]},"Quarry":{"value":"3","options":["off","1","2","3","4","5","6","7","8","9"]},"Gold":{"value":"3","options":["off","1","2","3","4","5","6","7","8","9"]},"RallyTime":{"value":"5min","options":["5min","10min","30min","8hours"]},"reduceLevel":false,"marches":"5","farmLowestResource":false},"Id":6,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.gathervip","Uid":"vikingbot.base.gathervip_4","OrderId":6,"Config":{"LevelStartAt":{"value":"3","options":["1","2","3","4","5","6"]},"Monster":false,"Niflung":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Divine":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Farm":{"value":"3","options":["off","1","2","3","4","5","6","7","8","9"]},"Sawmill":{"value":"3","options":["off","1","2","3","4","5","6","7","8","9"]},"Quarry":{"value":"3","options":["off","1","2","3","4","5","6","7","8","9"]},"Gold":{"value":"3","options":["off","1","2","3","4","5","6","7","8","9"]},"RallyTime":{"value":"5min","options":["5min","10min","30min","8hours"]},"reduceLevel":false,"marches":"5","farmLowestResource":false},"Id":6,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.buffs","Uid":"vikingbot.base.buffs","OrderId":2,"Config":{"Attack":{"value":"Off","options":["Off","Any"]},"Defense":{"value":"Off","options":["Off","Any"]},"Gather":{"value":"Any","options":["Off","Any"]},"Workers":{"value":"Off","options":["Off","Any"]},"Deception":{"value":"Off","options":["Off","Any"]},"Trade":{"value":"Off","options":["Off","Any"]},"Patrol":{"value":"Off","options":["Off","Any"]},"useGems":false},"Id":2,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|12:00 AM|1:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.buffs","Uid":"vikingbot.base.buffs_1","OrderId":6,"Config":{"Attack":{"value":"Off","options":["Off","Any"]},"Defense":{"value":"Off","options":["Off","Any"]},"Gather":{"value":"Any","options":["Off","Any"]},"Workers":{"value":"Off","options":["Off","Any"]},"Deception":{"value":"Off","options":["Off","Any"]},"Trade":{"value":"Off","options":["Off","Any"]},"Patrol":{"value":"Off","options":["Off","Any"]},"useGems":false},"Id":6,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|8:00 AM|9:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.buffs","Uid":"vikingbot.base.buffs_2","OrderId":6,"Config":{"Attack":{"value":"Off","options":["Off","Any"]},"Defense":{"value":"Off","options":["Off","Any"]},"Gather":{"value":"Any","options":["Off","Any"]},"Workers":{"value":"Off","options":["Off","Any"]},"Deception":{"value":"Off","options":["Off","Any"]},"Trade":{"value":"Off","options":["Off","Any"]},"Patrol":{"value":"Off","options":["Off","Any"]},"useGems":false},"Id":6,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|4:00 PM|5:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.gathervip","Uid":"vikingbot.base.gathervip_1","OrderId":3,"Config":{"LevelStartAt":{"value":"3","options":["1","2","3","4","5","6"]},"Monster":false,"Niflung":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Divine":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Farm":{"value":"3","options":["off","1","2","3","4","5","6","7","8","9"]},"Sawmill":{"value":"3","options":["off","1","2","3","4","5","6","7","8","9"]},"Quarry":{"value":"3","options":["off","1","2","3","4","5","6","7","8","9"]},"Gold":{"value":"3","options":["off","1","2","3","4","5","6","7","8","9"]},"RallyTime":{"value":"5min","options":["5min","10min","30min","8hours"]},"reduceLevel":false,"marches":"5","farmLowestResource":false},"Id":3,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]}]""",

    "PREM": r"""[{"ScriptId":"vikingbot.base.gathervip","Uid":"vikingbot.base.gathervip_2","OrderId":0,"Config":{"LevelStartAt":{"value":"6","options":["1","2","3","4","5","6"]},"Monster":false,"Niflung":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Divine":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Farm":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Sawmill":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Quarry":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Gold":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"RallyTime":{"value":"10min","options":["5min","10min","30min","8hours"]},"reduceLevel":false,"marches":"5","farmLowestResource":false},"Id":0,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.gathervip","Uid":"vikingbot.base.gathervip_2","OrderId":1,"Config":{"LevelStartAt":{"value":"6","options":["1","2","3","4","5","6"]},"Monster":false,"Niflung":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Divine":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Farm":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Sawmill":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Quarry":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Gold":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"RallyTime":{"value":"10min","options":["5min","10min","30min","8hours"]},"reduceLevel":false,"marches":"5","farmLowestResource":false},"Id":1,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.dailies","Uid":"vikingbot.base.dailies_2","OrderId":2,"Config":{"quest":true,"recruit":true,"vip":true,"worker":{"value":"Off","options":["Off","Common","Rare","Legend","All"]},"gems":true,"errands":{"value":"10","options":["Off","5","10","15","20"]},"specialFarmer":true,"skipVoyageLushLand":true,"events":false,"collectCrystals":true},"Id":2,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|3:00 AM|5:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.dailies","Uid":"vikingbot.base.dailies_2","OrderId":3,"Config":{"quest":true,"recruit":true,"vip":true,"worker":{"value":"Off","options":["Off","Common","Rare","Legend","All"]},"gems":true,"errands":{"value":"10","options":["Off","5","10","15","20"]},"specialFarmer":true,"skipVoyageLushLand":true,"events":false,"collectCrystals":true},"Id":3,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|12:00 PM|2:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.dailies","Uid":"vikingbot.base.dailies_2","OrderId":4,"Config":{"quest":true,"recruit":true,"vip":true,"worker":{"value":"Off","options":["Off","Common","Rare","Legend","All"]},"gems":true,"errands":{"value":"10","options":["Off","5","10","15","20"]},"specialFarmer":true,"skipVoyageLushLand":true,"events":false,"collectCrystals":true},"Id":4,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|6:00 PM|8:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.alliancedonation","Uid":"vikingbot.base.alliancedonation","OrderId":5,"Config":{"allianceGift":true,"allianceDonation":{"value":"Recommended","options":["Recommended","Development","Territory","War","Skills","Off"]}},"Id":5,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|12:00 AM|3:00 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.alliancedonation","Uid":"vikingbot.base.alliancedonation_2","OrderId":6,"Config":{"allianceGift":true,"allianceDonation":{"value":"Recommended","options":["Recommended","Development","Territory","War","Skills","Off"]}},"Id":6,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|10:00 AM|1:00 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.alliancedonation","Uid":"vikingbot.base.alliancedonation_1","OrderId":7,"Config":{"allianceGift":true,"allianceDonation":{"value":"Recommended","options":["Recommended","Development","Territory","War","Skills","Off"]}},"Id":7,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|7:00 PM|9:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.buffs","Uid":"vikingbot.base.buffs","OrderId":8,"Config":{"Attack":{"value":"Off","options":["Off","Any"]},"Defense":{"value":"Off","options":["Off","Any"]},"Gather":{"value":"Any","options":["Off","Any"]},"Workers":{"value":"Off","options":["Off","Any"]},"Deception":{"value":"Off","options":["Off","Any"]},"Trade":{"value":"Off","options":["Off","Any"]},"Patrol":{"value":"Off","options":["Off","Any"]},"useGems":false},"Id":8,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|12:00 AM|2:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.buffs","Uid":"vikingbot.base.buffs_1","OrderId":9,"Config":{"Attack":{"value":"Off","options":["Off","Any"]},"Defense":{"value":"Off","options":["Off","Any"]},"Gather":{"value":"Any","options":["Off","Any"]},"Workers":{"value":"Off","options":["Off","Any"]},"Deception":{"value":"Off","options":["Off","Any"]},"Trade":{"value":"Off","options":["Off","Any"]},"Patrol":{"value":"Off","options":["Off","Any"]},"useGems":false},"Id":9,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|8:00 AM|10:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.buffs","Uid":"vikingbot.base.buffs_2","OrderId":10,"Config":{"Attack":{"value":"Off","options":["Off","Any"]},"Defense":{"value":"Off","options":["Off","Any"]},"Gather":{"value":"Any","options":["Off","Any"]},"Workers":{"value":"Off","options":["Off","Any"]},"Deception":{"value":"Off","options":["Off","Any"]},"Trade":{"value":"Off","options":["Off","Any"]},"Patrol":{"value":"Off","options":["Off","Any"]},"useGems":false},"Id":10,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|4:00 PM|6:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.recruitment","Uid":"vikingbot.base.recruitment","OrderId":11,"Config":{"Infantry":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"Archer":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"Pikemen":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"Porter":{"value":"1","options":["Off","Auto","1","2","3","4","5","6","7"]},"Amount":{"value":"100%","options":["100%","75%","50%","25%"]},"UpgradeInfantry":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradeArcher":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradePikemen":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradePorter":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"useResources":true,"useSpeedUps":false},"Id":11,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|12:00 AM|2:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.recruitment","Uid":"vikingbot.base.recruitment_1","OrderId":12,"Config":{"Infantry":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"Archer":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"Pikemen":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"Porter":{"value":"1","options":["Off","Auto","1","2","3","4","5","6","7"]},"Amount":{"value":"100%","options":["100%","75%","50%","25%"]},"UpgradeInfantry":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradeArcher":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradePikemen":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradePorter":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"useResources":true,"useSpeedUps":false},"Id":12,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|8:00 AM|10:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.recruitment","Uid":"vikingbot.base.recruitment_2","OrderId":13,"Config":{"Infantry":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"Archer":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"Pikemen":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"Porter":{"value":"1","options":["Off","Auto","1","2","3","4","5","6","7"]},"Amount":{"value":"100%","options":["100%","75%","50%","25%"]},"UpgradeInfantry":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradeArcher":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradePikemen":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradePorter":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"useResources":true,"useSpeedUps":false},"Id":13,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|4:00 PM|6:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.upgrade","Uid":"vikingbot.base.upgrade","OrderId":14,"Config":{"Upgrade":{"value":"MainHall","options":["MainHall","Specfic","Villages"]},"EagleNest":false,"Warehouse":false,"HallofValor":false,"TribeHall":true,"DivinationShack":false,"Academy":false,"Watchtower":false,"Infirmary":false,"Infantry":false,"Archer":false,"Porter":false,"Pikemen":false,"SquadBase":false,"VillageHall":false,"Workshop":false,"Prison":false,"DefenderCamp":false,"SuppyHub":false,"Market":false,"useSpeedUps":false,"useResources":true},"Id":14,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|12:00 AM|2:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.upgrade","Uid":"vikingbot.base.upgrade_2","OrderId":15,"Config":{"Upgrade":{"value":"MainHall","options":["MainHall","Specfic","Villages"]},"EagleNest":false,"Warehouse":false,"HallofValor":false,"TribeHall":true,"DivinationShack":false,"Academy":false,"Watchtower":false,"Infirmary":false,"Infantry":false,"Archer":false,"Porter":false,"Pikemen":false,"SquadBase":false,"VillageHall":false,"Workshop":false,"Prison":false,"DefenderCamp":false,"SuppyHub":false,"Market":false,"useSpeedUps":false,"useResources":true},"Id":15,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|8:00 AM|10:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.upgrade","Uid":"vikingbot.base.upgrade_2","OrderId":16,"Config":{"Upgrade":{"value":"MainHall","options":["MainHall","Specfic","Villages"]},"EagleNest":false,"Warehouse":false,"HallofValor":false,"TribeHall":true,"DivinationShack":false,"Academy":false,"Watchtower":false,"Infirmary":false,"Infantry":false,"Archer":false,"Porter":false,"Pikemen":false,"SquadBase":false,"VillageHall":false,"Workshop":false,"Prison":false,"DefenderCamp":false,"SuppyHub":false,"Market":false,"useSpeedUps":false,"useResources":true},"Id":16,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|4:00 PM|6:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.research","Uid":"vikingbot.base.research","OrderId":17,"Config":{"research":{"value":"Economy","options":["Economy","Military"]},"upgrade":true,"useResources":true,"useSpeedUps":false},"Id":17,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|12:00 AM|2:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.research","Uid":"vikingbot.base.research_1","OrderId":18,"Config":{"research":{"value":"Economy","options":["Economy","Military"]},"upgrade":true,"useResources":true,"useSpeedUps":false},"Id":18,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|8:00 AM|10:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.research","Uid":"vikingbot.base.research_2","OrderId":19,"Config":{"research":{"value":"Economy","options":["Economy","Military"]},"upgrade":true,"useResources":true,"useSpeedUps":false},"Id":19,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|4:00 PM|6:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.divinationshack","Uid":"vikingbot.base.divinationshack","OrderId":20,"Config":{"SpeedUp":false,"Food":false,"Stones":false,"Gold":true,"Gems":false,"Lumber":false,"ConstructionSpeed":false,"TrainExpansion":false,"ForgingSpeed":false,"ResearchSpeed":false,"TrainingSpeed":false,"ForgingConsumption":false,"HealingSpeed":false,"TrainingConsumption":false,"HealingConsumption":false},"Id":20,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|12:00 AM|2:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.divinationshack","Uid":"vikingbot.base.divinationshack_1","OrderId":21,"Config":{"SpeedUp":false,"Food":false,"Stones":false,"Gold":true,"Gems":false,"Lumber":false,"ConstructionSpeed":false,"TrainExpansion":false,"ForgingSpeed":false,"ResearchSpeed":false,"TrainingSpeed":false,"ForgingConsumption":false,"HealingSpeed":false,"TrainingConsumption":false,"HealingConsumption":false},"Id":21,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|8:00 AM|10:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.divinationshack","Uid":"vikingbot.base.divinationshack_2","OrderId":22,"Config":{"SpeedUp":false,"Food":false,"Stones":false,"Gold":true,"Gems":false,"Lumber":false,"ConstructionSpeed":false,"TrainExpansion":false,"ForgingSpeed":false,"ResearchSpeed":false,"TrainingSpeed":false,"ForgingConsumption":false,"HealingSpeed":false,"TrainingConsumption":false,"HealingConsumption":false},"Id":22,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|4:00 PM|6:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.mail","Uid":"vikingbot.base.mail","OrderId":23,"Config":{"skip":0},"Id":23,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|3:00 AM|5:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.mail","Uid":"vikingbot.base.mail_1","OrderId":24,"Config":{"skip":0},"Id":24,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|6:00 PM|8:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.dragoncave","Uid":"vikingbot.base.dragoncave","OrderId":25,"Config":{"Resources":true,"Speedups":false,"Buffs":false,"Equipment":false,"Mounts":false,"Others":false,"ResourcesUseGold":true,"Gray":false,"Green":false,"Blue":false,"Purple":false},"Id":25,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|3:00 AM|5:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.exploration","Uid":"vikingbot.base.exploration","OrderId":26,"Config":{"AtheronSnowfields":true,"NovaForest":true,"DanaPlains":true,"MtKhajag":true,"AsltaRange":true,"Dornfjord":true,"GertlandIsland":true,"highestMission":true,"lowestMission":false,"fastestMission":false},"Id":26,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.commission","Uid":"vikingbot.base.commission","OrderId":27,"Config":{"skip":0},"Id":27,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.gathervip","Uid":"vikingbot.base.gathervip_2","OrderId":28,"Config":{"LevelStartAt":{"value":"6","options":["1","2","3","4","5","6"]},"Monster":false,"Niflung":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Divine":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Farm":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Sawmill":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Quarry":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Gold":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"RallyTime":{"value":"10min","options":["5min","10min","30min","8hours"]},"reduceLevel":false,"marches":"5","farmLowestResource":false},"Id":28,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]}]""",

    "1100": r"""[{"ScriptId":"vikingbot.base.gathervip","Uid":"vikingbot.base.gathervip_2","OrderId":0,"Config":{"LevelStartAt":{"value":"6","options":["1","2","3","4","5","6"]},"Monster":false,"Niflung":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Divine":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Farm":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Sawmill":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Quarry":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Gold":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"RallyTime":{"value":"10min","options":["5min","10min","30min","8hours"]},"reduceLevel":false,"marches":"5","farmLowestResource":false},"Id":0,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.gathervip","Uid":"vikingbot.base.gathervip_2","OrderId":1,"Config":{"LevelStartAt":{"value":"6","options":["1","2","3","4","5","6"]},"Monster":false,"Niflung":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Divine":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Farm":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Sawmill":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Quarry":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Gold":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"RallyTime":{"value":"10min","options":["5min","10min","30min","8hours"]},"reduceLevel":false,"marches":"5","farmLowestResource":false},"Id":1,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.dailies","Uid":"vikingbot.base.dailies_2","OrderId":2,"Config":{"quest":true,"recruit":true,"vip":true,"worker":{"value":"Off","options":["Off","Common","Rare","Legend","All"]},"gems":true,"errands":{"value":"10","options":["Off","5","10","15","20"]},"specialFarmer":true,"skipVoyageLushLand":true,"events":false,"collectCrystals":true},"Id":2,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|3:00 AM|5:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.dailies","Uid":"vikingbot.base.dailies_2","OrderId":3,"Config":{"quest":true,"recruit":true,"vip":true,"worker":{"value":"Off","options":["Off","Common","Rare","Legend","All"]},"gems":true,"errands":{"value":"10","options":["Off","5","10","15","20"]},"specialFarmer":true,"skipVoyageLushLand":true,"events":false,"collectCrystals":true},"Id":3,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|12:00 PM|2:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.dailies","Uid":"vikingbot.base.dailies_2","OrderId":4,"Config":{"quest":true,"recruit":true,"vip":true,"worker":{"value":"Off","options":["Off","Common","Rare","Legend","All"]},"gems":true,"errands":{"value":"10","options":["Off","5","10","15","20"]},"specialFarmer":true,"skipVoyageLushLand":true,"events":false,"collectCrystals":true},"Id":4,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|6:00 PM|8:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.alliancedonation","Uid":"vikingbot.base.alliancedonation","OrderId":5,"Config":{"allianceGift":true,"allianceDonation":{"value":"Recommended","options":["Recommended","Development","Territory","War","Skills","Off"]}},"Id":5,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|12:00 AM|3:00 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.alliancedonation","Uid":"vikingbot.base.alliancedonation_2","OrderId":6,"Config":{"allianceGift":true,"allianceDonation":{"value":"Recommended","options":["Recommended","Development","Territory","War","Skills","Off"]}},"Id":6,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|10:00 AM|1:00 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.alliancedonation","Uid":"vikingbot.base.alliancedonation_1","OrderId":7,"Config":{"allianceGift":true,"allianceDonation":{"value":"Recommended","options":["Recommended","Development","Territory","War","Skills","Off"]}},"Id":7,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|7:00 PM|9:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.buffs","Uid":"vikingbot.base.buffs","OrderId":8,"Config":{"Attack":{"value":"Off","options":["Off","Any"]},"Defense":{"value":"Off","options":["Off","Any"]},"Gather":{"value":"Any","options":["Off","Any"]},"Workers":{"value":"Off","options":["Off","Any"]},"Deception":{"value":"Off","options":["Off","Any"]},"Trade":{"value":"Off","options":["Off","Any"]},"Patrol":{"value":"Off","options":["Off","Any"]},"useGems":false},"Id":8,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|12:00 AM|2:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.buffs","Uid":"vikingbot.base.buffs_1","OrderId":9,"Config":{"Attack":{"value":"Off","options":["Off","Any"]},"Defense":{"value":"Off","options":["Off","Any"]},"Gather":{"value":"Any","options":["Off","Any"]},"Workers":{"value":"Off","options":["Off","Any"]},"Deception":{"value":"Off","options":["Off","Any"]},"Trade":{"value":"Off","options":["Off","Any"]},"Patrol":{"value":"Off","options":["Off","Any"]},"useGems":false},"Id":9,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|8:00 AM|10:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.buffs","Uid":"vikingbot.base.buffs_2","OrderId":10,"Config":{"Attack":{"value":"Off","options":["Off","Any"]},"Defense":{"value":"Off","options":["Off","Any"]},"Gather":{"value":"Any","options":["Off","Any"]},"Workers":{"value":"Off","options":["Off","Any"]},"Deception":{"value":"Off","options":["Off","Any"]},"Trade":{"value":"Off","options":["Off","Any"]},"Patrol":{"value":"Off","options":["Off","Any"]},"useGems":false},"Id":10,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|4:00 PM|6:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.mail","Uid":"vikingbot.base.mail","OrderId":23,"Config":{"skip":0},"Id":23,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|3:00 AM|5:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.mail","Uid":"vikingbot.base.mail_1","OrderId":24,"Config":{"skip":0},"Id":24,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|6:00 PM|8:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.gathervip","Uid":"vikingbot.base.gathervip_2","OrderId":28,"Config":{"LevelStartAt":{"value":"6","options":["1","2","3","4","5","6"]},"Monster":false,"Niflung":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Divine":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Farm":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Sawmill":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Quarry":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Gold":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"RallyTime":{"value":"10min","options":["5min","10min","30min","8hours"]},"reduceLevel":false,"marches":"5","farmLowestResource":false},"Id":28,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]}]""",

    "TRAIN": r"""[{"ScriptId":"vikingbot.base.stagingpost","Uid":"vikingbot.base.stagingpost","OrderId":0,"Config":{"redMission":false,"marches":"10","ignoreSuicide":false},"Id":0,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.gathervip","Uid":"vikingbot.base.gathervip_2","OrderId":1,"Config":{"LevelStartAt":{"value":"6","options":["1","2","3","4","5","6"]},"Monster":false,"Niflung":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Divine":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Farm":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Sawmill":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Quarry":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Gold":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"RallyTime":{"value":"10min","options":["5min","10min","30min","8hours"]},"reduceLevel":false,"marches":"5","farmLowestResource":true},"Id":1,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.gathervip","Uid":"vikingbot.base.gathervip_2","OrderId":2,"Config":{"LevelStartAt":{"value":"6","options":["1","2","3","4","5","6"]},"Monster":false,"Niflung":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Divine":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Farm":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Sawmill":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Quarry":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Gold":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"RallyTime":{"value":"10min","options":["5min","10min","30min","8hours"]},"reduceLevel":false,"marches":"5","farmLowestResource":true},"Id":2,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.dailies","Uid":"vikingbot.base.dailies_2","OrderId":3,"Config":{"quest":true,"recruit":true,"vip":true,"worker":{"value":"Off","options":["Off","Common","Rare","Legend","All"]},"gems":true,"errands":{"value":"10","options":["Off","5","10","15","20"]},"specialFarmer":true,"skipVoyageLushLand":true,"events":false,"collectCrystals":true},"Id":3,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|3:00 AM|5:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.dailies","Uid":"vikingbot.base.dailies_2","OrderId":4,"Config":{"quest":true,"recruit":true,"vip":true,"worker":{"value":"Off","options":["Off","Common","Rare","Legend","All"]},"gems":true,"errands":{"value":"10","options":["Off","5","10","15","20"]},"specialFarmer":true,"skipVoyageLushLand":true,"events":false,"collectCrystals":true},"Id":4,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|12:00 PM|2:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.dailies","Uid":"vikingbot.base.dailies_2","OrderId":5,"Config":{"quest":true,"recruit":true,"vip":true,"worker":{"value":"Off","options":["Off","Common","Rare","Legend","All"]},"gems":true,"errands":{"value":"10","options":["Off","5","10","15","20"]},"specialFarmer":true,"skipVoyageLushLand":true,"events":false,"collectCrystals":true},"Id":5,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|6:00 PM|8:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.alliancedonation","Uid":"vikingbot.base.alliancedonation","OrderId":6,"Config":{"allianceGift":true,"allianceDonation":{"value":"Off","options":["Recommended","Development","Territory","War","Skills","Off"]}},"Id":6,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|12:00 AM|3:00 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.alliancedonation","Uid":"vikingbot.base.alliancedonation_2","OrderId":7,"Config":{"allianceGift":true,"allianceDonation":{"value":"Off","options":["Recommended","Development","Territory","War","Skills","Off"]}},"Id":7,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|10:00 AM|1:00 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.alliancedonation","Uid":"vikingbot.base.alliancedonation_1","OrderId":8,"Config":{"allianceGift":true,"allianceDonation":{"value":"Off","options":["Recommended","Development","Territory","War","Skills","Off"]}},"Id":8,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|7:00 PM|9:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.build","Uid":"vikingbot.base.build","OrderId":9,"Config":{"skip":0},"Id":9,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.buffs","Uid":"vikingbot.base.buffs","OrderId":10,"Config":{"Attack":{"value":"Off","options":["Off","Any"]},"Defense":{"value":"Off","options":["Off","Any"]},"Gather":{"value":"Any","options":["Off","Any"]},"Workers":{"value":"Off","options":["Off","Any"]},"Deception":{"value":"Off","options":["Off","Any"]},"Trade":{"value":"Off","options":["Off","Any"]},"Patrol":{"value":"Off","options":["Off","Any"]},"useGems":false},"Id":10,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|12:00 AM|2:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.buffs","Uid":"vikingbot.base.buffs_1","OrderId":11,"Config":{"Attack":{"value":"Off","options":["Off","Any"]},"Defense":{"value":"Off","options":["Off","Any"]},"Gather":{"value":"Any","options":["Off","Any"]},"Workers":{"value":"Off","options":["Off","Any"]},"Deception":{"value":"Off","options":["Off","Any"]},"Trade":{"value":"Off","options":["Off","Any"]},"Patrol":{"value":"Off","options":["Off","Any"]},"useGems":false},"Id":11,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|8:00 AM|10:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.buffs","Uid":"vikingbot.base.buffs_2","OrderId":12,"Config":{"Attack":{"value":"Off","options":["Off","Any"]},"Defense":{"value":"Off","options":["Off","Any"]},"Gather":{"value":"Any","options":["Off","Any"]},"Workers":{"value":"Off","options":["Off","Any"]},"Deception":{"value":"Off","options":["Off","Any"]},"Trade":{"value":"Off","options":["Off","Any"]},"Patrol":{"value":"Off","options":["Off","Any"]},"useGems":false},"Id":12,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|4:00 PM|6:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.recruitment","Uid":"vikingbot.base.recruitment","OrderId":13,"Config":{"Infantry":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"Archer":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"Pikemen":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"Porter":{"value":"1","options":["Off","Auto","1","2","3","4","5","6","7"]},"Amount":{"value":"100%","options":["100%","75%","50%","25%"]},"UpgradeInfantry":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradeArcher":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradePikemen":{"value":"Auto","options":["Off","Auto","1","2","3","4","5","6","7"]},"UpgradePorter":{"value":"Off","options":["Off","Auto","1","2","3","4","5","6","7"]},"useResources":true,"useSpeedUps":false},"Id":13,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.upgrade","Uid":"vikingbot.base.upgrade","OrderId":16,"Config":{"Upgrade":{"value":"MainHall","options":["MainHall","Specfic","Villages"]},"EagleNest":false,"Warehouse":false,"HallofValor":false,"TribeHall":true,"DivinationShack":false,"Academy":false,"Watchtower":false,"Infirmary":false,"Infantry":false,"Archer":false,"Porter":false,"Pikemen":false,"SquadBase":false,"VillageHall":false,"Workshop":false,"Prison":false,"DefenderCamp":false,"SuppyHub":false,"Market":false,"useSpeedUps":false,"useResources":true},"Id":16,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.research","Uid":"vikingbot.base.research","OrderId":19,"Config":{"research":{"value":"Economy","options":["Economy","Military"]},"upgrade":true,"useResources":true,"useSpeedUps":false},"Id":19,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.divinationshack","Uid":"vikingbot.base.divinationshack","OrderId":22,"Config":{"SpeedUp":true,"Food":false,"Stones":false,"Gold":false,"Gems":false,"Lumber":false,"ConstructionSpeed":false,"TrainExpansion":false,"ForgingSpeed":false,"ResearchSpeed":false,"TrainingSpeed":false,"ForgingConsumption":false,"HealingSpeed":false,"TrainingConsumption":false,"HealingConsumption":false},"Id":22,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|12:00 AM|2:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.divinationshack","Uid":"vikingbot.base.divinationshack_1","OrderId":23,"Config":{"SpeedUp":true,"Food":false,"Stones":false,"Gold":false,"Gems":false,"Lumber":false,"ConstructionSpeed":false,"TrainExpansion":false,"ForgingSpeed":false,"ResearchSpeed":false,"TrainingSpeed":false,"ForgingConsumption":false,"HealingSpeed":false,"TrainingConsumption":false,"HealingConsumption":false},"Id":23,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|8:00 AM|10:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.divinationshack","Uid":"vikingbot.base.divinationshack_2","OrderId":24,"Config":{"SpeedUp":true,"Food":false,"Stones":false,"Gold":false,"Gems":false,"Lumber":false,"ConstructionSpeed":false,"TrainExpansion":false,"ForgingSpeed":false,"ResearchSpeed":false,"TrainingSpeed":false,"ForgingConsumption":false,"HealingSpeed":false,"TrainingConsumption":false,"HealingConsumption":false},"Id":24,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|4:00 PM|6:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.mail","Uid":"vikingbot.base.mail","OrderId":25,"Config":{"skip":0},"Id":25,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|3:00 AM|5:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.mail","Uid":"vikingbot.base.mail_1","OrderId":26,"Config":{"skip":0},"Id":26,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|6:00 PM|8:59 PM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.eaglenest","Uid":"vikingbot.base.eaglenest","OrderId":27,"Config":{"skip":0},"Id":27,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.villages","Uid":"vikingbot.base.villages","OrderId":28,"Config":{"skip":0,"marches":"15"},"Id":28,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.exploration","Uid":"vikingbot.base.exploration","OrderId":29,"Config":{"AtheronSnowfields":true,"NovaForest":true,"DanaPlains":true,"MtKhajag":true,"AsltaRange":true,"Dornfjord":true,"GertlandIsland":true,"highestMission":true,"lowestMission":false,"fastestMission":false},"Id":29,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.commission","Uid":"vikingbot.base.commission","OrderId":30,"Config":{"skip":0},"Id":30,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]},{"ScriptId":"vikingbot.base.dragoncave","Uid":"vikingbot.base.dragoncave","OrderId":31,"Config":{"Resources":false,"Speedups":true,"Buffs":false,"Equipment":false,"Mounts":false,"Others":false,"ResourcesUseGold":false,"Gray":false,"Green":false,"Blue":false,"Purple":false},"Id":31,"IsActive":true,"IsCopy":false,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[{"Val":-1.0,"Val1":"mon,tue,wed,thu,fri,sat,sun|3:00 AM|5:59 AM","IntervalType":0,"Type":5}]},{"ScriptId":"vikingbot.base.gathervip","Uid":"vikingbot.base.gathervip_2","OrderId":32,"Config":{"LevelStartAt":{"value":"6","options":["1","2","3","4","5","6"]},"Monster":false,"Niflung":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Divine":{"value":"off","options":["off","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63","64","65","66","67","68","69","70"]},"Farm":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Sawmill":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Quarry":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"Gold":{"value":"6","options":["off","1","2","3","4","5","6","7","8","9"]},"RallyTime":{"value":"10min","options":["5min","10min","30min","8hours"]},"reduceLevel":false,"marches":"5","farmLowestResource":true},"Id":32,"IsActive":true,"IsCopy":true,"ScheduleData":{"Active":false,"Last":"0001-01-01T00:00:00","Daily":false,"Hourly":false,"Weekly":false},"ScheduleRules":[]}]""",


}
# ──────────────────────────────────────────────────────────────────────

# --- Prices storage ---
PRICES_PATH = os.path.join(BASE_DIR, "prices.json")
PRICES_LOCK = threading.Lock()
DEFAULT_PRICES = {"fws100": 300.0, "gold100": 500.0, "percent": 32.0}

def load_prices():
    if os.path.exists(PRICES_PATH):
        try:
            with open(PRICES_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
            out = DEFAULT_PRICES.copy()
            out.update({k: float(v) for k, v in data.items() if k in out})
            return out
        except Exception:
            pass
    return DEFAULT_PRICES.copy()

def save_prices(p):
    with open(PRICES_PATH, "w", encoding="utf-8") as f:
        json.dump(p, f, ensure_ascii=False, indent=2)

PRICES = load_prices()


app = Flask(__name__, template_folder="templates")
CORS(app)

LAST_UPDATE_TIME = None

LOG_PATTERN = re.compile(
    r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} \+\d{2}:\d{2}) "
    r"\[DBG\] DEBUG\|(.*?)\|CityResourcesAmount:\{Food:(\d+), Wood:(\d+), Stone:(\d+), Gold:(\d+), Gems:(\d+)"
)


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
        start_process(r"C:\Users\rss\Desktop\GnBots.lnk")
    except Exception as e:
        logs.append("Ошибка запуска GnBots.exe: "+str(e))
    logs.append("Reboot завершён.")
    return logs

##############################
# ИНИЦИАЛИЗАЦИЯ БАЗ
##############################

def init_resources_db():
    conn = sqlite3.connect(RESOURCES_DB)
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

def init_logs_db():
    conn = sqlite3.connect(LOGS_DB)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS files_offset (
            filename TEXT PRIMARY KEY,
            last_pos INTEGER NOT NULL
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS cached_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            acc_id TEXT,
            nickname TEXT,
            dt TEXT,
            raw_line TEXT
        )
    """)
    conn.commit()
    conn.close()


def init_accounts_db():
    conn = sqlite3.connect(RESOURCES_DB)
    c = conn.cursor()
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS account_meta(
          id          TEXT PRIMARY KEY,
          email       TEXT,
          passwd      TEXT,
          igg         TEXT,
          pay_until   TEXT,
          tariff_rub  INTEGER DEFAULT 0,
          server      TEXT,
          tg_tag      TEXT
        )
        """
    )

    existing_cols = {row[1] for row in c.execute("PRAGMA table_info(account_meta)").fetchall()}
    if "pay_until" not in existing_cols:
        c.execute("ALTER TABLE account_meta ADD COLUMN pay_until TEXT")
    if "tariff_rub" not in existing_cols:
        c.execute("ALTER TABLE account_meta ADD COLUMN tariff_rub INTEGER DEFAULT 0")
    if "server" not in existing_cols:
        c.execute("ALTER TABLE account_meta ADD COLUMN server TEXT")
    if "tg_tag" not in existing_cols:
        c.execute("ALTER TABLE account_meta ADD COLUMN tg_tag TEXT")

    conn.commit()
    conn.close()

##############################
# Работа с профилями
##############################

def load_profiles(*, return_status: bool = False):
    ok = True
    profiles: list[dict] = []
    try:
        if not os.path.exists(PROFILE_PATH):
            print(f"PROFILE not found: {PROFILE_PATH}")
            ok = False
        else:
            with open(PROFILE_PATH, "r", encoding="utf-8") as f:
                profiles = json.load(f)
    except Exception as exc:
        ok = False
        print(f"Ошибка чтения PROFILE ({PROFILE_PATH}): {exc}")

    active = [acc for acc in profiles if acc.get("Active")]
    if return_status:
        return active, ok
    return active

# вверху, рядом с load_profiles()
def load_active_names():
    """возвращает [(Id, Name)] активных аккаунтов"""
    json_path = PROFILE_PATH            # ← вместо BASE_DIR …
    if not os.path.exists(json_path):
        return []
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return [(a["Id"], a.get("Name","")) for a in data if a.get("Active")]



def ensure_active_in_db(active_accounts):
    conn= sqlite3.connect(RESOURCES_DB)
    c=conn.cursor()
    for acc in active_accounts:
        c.execute("""
            INSERT OR IGNORE INTO resources
            (id, nickname, food, wood, stone, gold, gems, last_updated)
            VALUES(?, ?, 0,0,0,0,0, '1970-01-01T00:00:00')
        """,(acc["Id"], acc["Name"]))
    conn.commit()
    conn.close()


def _normalize_date_str(value: str) -> str:
    if not value:
        return ""
    try:
        dt = datetime.fromisoformat(str(value).strip())
        return dt.date().isoformat()
    except Exception:
        try:
            dt = datetime.strptime(str(value).split()[0], "%Y-%m-%d")
            return dt.date().isoformat()
        except Exception:
            return ""


def load_accounts_meta_full(ids: set[str] | None = None) -> list[dict]:
    profile = []
    if os.path.exists(PROFILE_PATH):
        with open(PROFILE_PATH, "r", encoding="utf-8") as f:
            for a in json.load(f):
                if not a.get("Active"):
                    continue
                pid = a.get("Id")
                if pid is None:
                    continue
                if ids and str(pid) not in ids:
                    continue

                email = passwd = igg = ""
                try:
                    md = json.loads(a.get("MenuData", "{}"))
                    cfg = md.get("Config", {})
                    email  = cfg.get("Email", "") or ""
                    passwd = cfg.get("Password", "") or ""
                    igg    = cfg.get("Custom", "") or ""
                except Exception:
                    pass

                profile.append(
                    {
                        "id": str(pid),
                        "name": a.get("Name", ""),
                        "email": email,
                        "passwd": passwd,
                        "igg": igg,
                        "server": SERVER,
                    }
                )

    conn = sqlite3.connect(RESOURCES_DB)
    c = conn.cursor()
    meta = {
        str(r[0]): {
            "email": r[1] or "",
            "passwd": r[2] or "",
            "igg": r[3] or "",
            "pay_until": r[4] or "",
            "tariff_rub": r[5] or 0,
            "server": r[6] or "",
            "tg_tag": r[7] or "",
        }
        for r in c.execute(
            """
                SELECT id, email, passwd, igg, pay_until, tariff_rub, server, tg_tag
                FROM account_meta
            """
        )
    }
    conn.close()

    out = []
    for p in profile:
        m = meta.get(p["id"], {})
        merged = {
            "id": p.get("id"),
            "name": p.get("name", ""),
            "email": p.get("email") or m.get("email", ""),
            "passwd": p.get("passwd") or m.get("passwd", ""),
            "igg": p.get("igg") or m.get("igg", ""),
            "pay_until": _normalize_date_str(m.get("pay_until", "")),
            "tariff_rub": m.get("tariff_rub", 0) or 0,
            "server": p.get("server") or m.get("server") or SERVER,
            "tg_tag": m.get("tg_tag", ""),
        }
        out.append(merged)

    return out


@app.route("/api/accounts_meta_full")
def api_accounts_meta_full():
    try:
        ids = set(filter(None, request.args.get("ids", "").split(","))) or None
        out = load_accounts_meta_full(ids)

        return jsonify({"ok": True, "server": SERVER, "count": len(out), "items": out})
    except Exception as exc:
        app.logger.exception("api_accounts_meta_full failed")
        return jsonify({"ok": False, "error": str(exc)}), 500


def _load_usersdash_from_db(server_name: str) -> tuple[list[dict], list[str]]:
    errors: list[str] = []
    if not os.path.exists(USERDASH_DB):
        errors.append(f"UsersDash DB не найден: {USERDASH_DB}")
        return [], errors

    conn = sqlite3.connect(USERDASH_DB)
    c = conn.cursor()
    srv_row = c.execute(
        "SELECT id, name FROM servers WHERE name=? LIMIT 1",
        (server_name,),
    ).fetchone()

    if not srv_row:
        conn.close()
        errors.append(f"Сервер '{server_name}' не найден в UsersDash")
        return [], errors

    srv_id = srv_row[0]
    rows = c.execute(
        """
            SELECT a.id, a.name, a.internal_id, a.is_active, a.blocked_for_payment,
                   fd.email, fd.login, fd.password, fd.igg_id, fd.server, fd.telegram_tag
            FROM accounts a
            LEFT JOIN farm_data fd
              ON fd.user_id = a.owner_id AND fd.farm_name = a.name
            WHERE a.server_id=?
              AND a.is_active IS NOT 0
              AND (a.blocked_for_payment IS NULL OR a.blocked_for_payment = 0)
        """,
        (srv_id,),
    ).fetchall()
    conn.close()

    items = []
    for r in rows:
        email = r[5] or r[6] or ""
        items.append(
            {
                "usersdash_id": r[0],
                "name": r[1] or "",
                "internal_id": str(r[2]) if r[2] is not None else "",
                "is_active": bool(r[3]),
                "email": email,
                "password": r[7] or "",
                "igg_id": r[8] or "",
                "server": r[9] or "",
                "telegram": r[10] or "",
            }
        )

    return items, errors


def _load_usersdash_from_api(server_name: str) -> tuple[list[dict], list[str]]:
    errors: list[str] = []
    api_url = (USERSDASH_API_URL or "").rstrip("/")

    if not api_url:
        return [], errors
    if not USERSDASH_API_TOKEN:
        errors.append("USERSDASH_API_TOKEN не задан")
        return [], errors

    full_url = api_url + "/api/farms/v1"
    try:
        resp = requests.get(
            full_url,
            params={"server": server_name, "token": USERSDASH_API_TOKEN},
            timeout=20,
        )
        resp.raise_for_status()
        payload = resp.json()
    except Exception as exc:
        errors.append(f"Ошибка запроса UsersDash: {exc}")
        return [], errors

    if not isinstance(payload, dict):
        errors.append("Некорректный ответ UsersDash")
        return [], errors

    if payload.get("ok") is not True:
        errors.append(str(payload.get("error") or "UsersDash вернул ошибку"))
        return [], errors

    remote_server = str(payload.get("server") or "").strip()
    if remote_server and server_name and remote_server != server_name:
        errors.append(
            f"UsersDash вернул данные для '{remote_server}', ожидали '{server_name}'"
        )

    items: list[dict] = []
    for row in payload.get("items") or []:
        if not isinstance(row, dict):
            continue
        if row.get("is_active") is False or row.get("active") is False:
            continue

        items.append(
            {
                "usersdash_id": row.get("id") or row.get("usersdash_id"),
                "name": row.get("name") or "",
                "internal_id": str(row.get("internal_id") or ""),
                "is_active": bool(row.get("is_active", True)),
                "email": row.get("email") or row.get("login") or "",
                "password": row.get("password") or "",
                "igg_id": row.get("igg_id") or "",
                "server": row.get("kingdom") or row.get("server") or remote_server,
                "telegram": row.get("telegram_tag") or row.get("telegram") or "",
            }
        )

    return items, errors


def load_usersdash_accounts(server_name: str) -> tuple[list[dict], list[str]]:
    errors: list[str] = []

    api_items, api_errors = _load_usersdash_from_api(server_name)
    errors.extend(api_errors)
    if USERSDASH_API_URL:
        if not api_errors:
            return api_items, errors
        if not os.path.exists(USERDASH_DB):
            return api_items, errors

    db_items, db_errors = _load_usersdash_from_db(server_name)
    errors.extend(db_errors)
    return db_items, errors


@app.route("/api/usersdash_sync_preview")
def api_usersdash_sync_preview():
    try:
        local_items = load_accounts_meta_full(None)
        remote_items, errors = load_usersdash_accounts(SERVER)

        remote_by_internal = {
            r.get("internal_id"): r for r in remote_items if r.get("internal_id")
        }
        remote_by_name = {r.get("name"): r for r in remote_items if r.get("name")}

        changes: list[dict] = []

        field_map = [
            ("email", "email", "email"),
            ("password", "passwd", "password"),
            ("igg_id", "igg", "igg_id"),
            ("server", "server", "server"),
            ("telegram", "tg_tag", "telegram"),
        ]

        for loc in local_items:
            lid = str(loc.get("id") or "")
            lname = loc.get("name") or ""

            rem = None
            if lid and lid in remote_by_internal:
                rem = remote_by_internal[lid]
            elif lname and lname in remote_by_name:
                rem = remote_by_name[lname]

            if not rem:
                continue

            for field, local_key, remote_key in field_map:
                lv = str(loc.get(local_key, "") or "")
                rv = str(rem.get(remote_key, "") or "")

                if lv == rv:
                    continue

                changes.append(
                    {
                        "id": lid,
                        "name": lname,
                        "field": field,
                        "local": lv,
                        "remote": rv,
                        "usersdash_id": rem.get("usersdash_id"),
                    }
                )

        return jsonify({"ok": True, "changes": changes, "errors": errors})
    except Exception as exc:
        app.logger.exception("api_usersdash_sync_preview failed")
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/usersdash_sync_apply", methods=["POST"])
def api_usersdash_sync_apply():
    try:
        payload = request.get_json(silent=True) or {}
        changes = payload.get("changes") or []
        if not isinstance(changes, list) or not changes:
            return jsonify({"ok": False, "error": "Нет изменений для применения"}), 400

        updates: dict[str, dict] = {}
        for ch in changes:
            acc_id = str(ch.get("id") or ch.get("internal_id") or "").strip()
            field = ch.get("field")
            remote_val = ch.get("remote")
            if not acc_id or not field:
                continue

            rec = updates.setdefault(acc_id, {"id": acc_id})

            if field == "email":
                rec["email"] = remote_val or ""
            elif field == "password":
                rec["passwd"] = remote_val or ""
            elif field == "igg_id":
                rec["igg"] = remote_val or ""
            elif field == "server":
                rec["server"] = remote_val or ""
            elif field == "telegram":
                rec["tg_tag"] = remote_val or ""

        if not updates:
            return jsonify({"ok": False, "error": "Нет валидных изменений"}), 400

        conn = sqlite3.connect(RESOURCES_DB)
        c = conn.cursor()

        existing: dict[str, dict] = {}
        placeholders = ",".join("?" for _ in updates)
        if placeholders:
            for row in c.execute(
                f"SELECT id, email, passwd, igg, server, tg_tag "
                f"FROM account_meta WHERE id IN ({placeholders})",
                tuple(updates.keys()),
            ):
                existing[str(row[0])] = {
                    "email": row[1] or "",
                    "passwd": row[2] or "",
                    "igg": row[3] or "",
                    "server": row[4] or "",
                    "tg_tag": row[5] or "",
                }

        for acc_id, data in updates.items():
            current = existing.get(acc_id, {})
            merged = {
                "email": data.get("email", current.get("email", "")),
                "passwd": data.get("passwd", current.get("passwd", "")),
                "igg": data.get("igg", current.get("igg", "")),
                "server": data.get("server", current.get("server", "")),
                "tg_tag": data.get("tg_tag", current.get("tg_tag", "")),
            }

            c.execute(
                """
                INSERT INTO account_meta(id,email,passwd,igg,server,tg_tag)
                VALUES(?,?,?,?,?,?)
                ON CONFLICT(id) DO UPDATE SET
                  email=excluded.email,
                  passwd=excluded.passwd,
                  igg=excluded.igg,
                  server=excluded.server,
                  tg_tag=excluded.tg_tag
                """,
                (
                    acc_id,
                    merged["email"],
                    merged["passwd"],
                    merged["igg"],
                    merged["server"],
                    merged["tg_tag"],
                ),
            )

        conn.commit()
        conn.close()

        return jsonify({"ok": True, "updated": len(updates)})
    except Exception as exc:
        app.logger.exception("api_usersdash_sync_apply failed")
        return jsonify({"ok": False, "error": str(exc)}), 500


def sync_account_meta():
    profiles, ok = load_profiles(return_status=True)
    if not ok and not profiles:
        print("PROFILE read failed — skip sync_account_meta")
        return

    active_ids = {str(p.get("Id")) for p in profiles if p.get("Id") is not None}

    conn = sqlite3.connect(RESOURCES_DB)
    c = conn.cursor()

    if active_ids:
        marks = ",".join("?" * len(active_ids))
        c.execute(f"DELETE FROM account_meta WHERE id NOT IN ({marks})", tuple(active_ids))
    else:
        c.execute("DELETE FROM account_meta")

    if active_ids:
        placeholders = ",".join("(?, '', '', '', '', 0, '', '')" for _ in active_ids)
        c.execute(
            f"""
            INSERT OR IGNORE INTO account_meta
            (id,email,passwd,igg,pay_until,tariff_rub,server,tg_tag)
            VALUES {placeholders}
            """,
            tuple(active_ids),
        )

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
    conn_res = sqlite3.connect(RESOURCES_DB)
    c_res= conn_res.cursor()

    conn_log= sqlite3.connect(LOGS_DB)
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
                        for acid, nick in acc_map.items():
                            if acid in line_str:
                                dt_part= line_str[:23]
                                c_log.execute("""
                                  INSERT INTO cached_logs(acc_id,nickname,dt,raw_line)
                                  VALUES(?,?,?,?)
                                """,(acid,nick,dt_part,line_str))

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
# BACKUP
##############################


# ────────────────────────── настройки ──────────────────────────
BACKUP_CONFIG_SRC      = r"C:\LDPlayer\LDPlayer9\vms\config"
BACKUP_CONFIG_DST_ROOT = r"C:\LD_backup\configs"
BACKUP_ACCS_DST_ROOT   = r"C:\LD_backup\accs_data"
FIX_BACKUP_ROOT = r"C:\LD_backup\fix_backup"   # ← NEW


# ────────────────────────── вспомогалки ───────────────────────
def _ensure_dir(path: str):
    """Создаёт каталог *path* вместе со всеми промежуточными."""
    os.makedirs(path, exist_ok=True)

# ────────────────────────── BACKUP CONFIGS ────────────────────
def backup_configs() -> None:
    r"""
    Копирует все файлы из …\\vms\\config в
      C:\\LD_backup\\configs\\<ДД__ММ__ГГГГ>\\
    """
    dst = os.path.join(BACKUP_CONFIG_DST_ROOT,
                       datetime.now().strftime("%d__%m__%Y"))
    _ensure_dir(dst)
    for fname in os.listdir(BACKUP_CONFIG_SRC):
        src = os.path.join(BACKUP_CONFIG_SRC, fname)
        if os.path.isfile(src):
            shutil.copy2(src, dst)
    print(f"[BACKUP] configs  →  {dst}")


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
    conn   = sqlite3.connect(RESOURCES_DB)
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

# ─────────────────── проверка «уже есть ли за сегодня» ───────────────────
def ensure_today_backups() -> None:
    """При старте приложения проверяет, есть ли бэкап за сегодня; если нет — делает."""
    today_stamp = datetime.now().strftime("%d__%m__%Y")
    cfg_dir  = os.path.join(BACKUP_CONFIG_DST_ROOT,  today_stamp)
    acc_dir  = os.path.join(BACKUP_ACCS_DST_ROOT, f"{SERVER}_{today_stamp}")
    cfg_ok   = os.path.isdir(cfg_dir) and os.listdir(cfg_dir)
    acc_ok   = os.path.isfile(os.path.join(acc_dir, "accounts.csv"))
    if not cfg_ok:
        backup_configs()
    if not acc_ok:
        backup_accounts_csv()

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
            except Exception as e:
                print("[BACKUP] error:", e, flush=True)
    global DAILY_BACKUP_THREAD
    DAILY_BACKUP_THREAD = threading.Thread(target=_worker, daemon=True)
    DAILY_BACKUP_THREAD.start()
    BACKGROUND_FLAGS["daily_backup_scheduler"] = True

# === BACKUP END ===



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

# ───────────────────── диагностика и агрегирование ───────────────────────────
def _safe_parse_dt(dt_str: str) -> datetime | None:
    try:
        return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S.%f")
    except Exception:
        return None


def _load_crashed_entries() -> list[str]:
    """Возвращает список упавших эмуляторов из crashed.json (основной/локальный путь)."""
    for path in (CRASHED_JSON_PATH, LOCAL_CRASHED_JSON):
        if not path or not os.path.exists(path):
            continue
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                return [str(x) for x in data]
        except Exception:
            app.logger.exception("Не удалось прочитать crashed.json: %s", path)
    return []


def _collect_problem_summary(window_hours: int = 24) -> dict:
    """Собирает ошибки/варнинги из кэша логов за указанное окно."""
    stats = {
        "total": 0,
        "by_keyword": {},
        "examples": [],
        "checked": 0,
    }
    if not os.path.exists(LOGS_DB):
        return stats

    keywords = {
        "error": ["error", "ошибка", "exception", "traceback", "fail"],
        "warning": ["warning", "warn", "предупр", "deprecated"],
        "disconnect": ["disconnect", "connection lost", "timeout"],
    }

    cutoff = datetime.now() - timedelta(hours=window_hours)

    try:
        conn = sqlite3.connect(LOGS_DB)
        c = conn.cursor()
        rows = c.execute(
            "SELECT acc_id, dt, raw_line FROM cached_logs ORDER BY id DESC LIMIT 5000"
        ).fetchall()
        conn.close()
    except Exception:
        app.logger.exception("Не удалось прочитать cached_logs")
        return stats

    for acc_id, dt_str, line in rows:
        stats["checked"] += 1
        dt_val = _safe_parse_dt(dt_str)
        if dt_val and dt_val < cutoff:
            continue
        lower = line.lower()
        for key, patterns in keywords.items():
            if any(p in lower for p in patterns):
                stats["total"] += 1
                stats["by_keyword"].setdefault(key, 0)
                stats["by_keyword"][key] += 1
                if len(stats["examples"]) < 20:
                    stats["examples"].append(
                        {
                            "acc_id": acc_id,
                            "dt": dt_str,
                            "text": line[:200],
                            "type": key,
                        }
                    )
                break
    return stats


def _calculate_cycle_times(
    *, window_hours: int, min_gap_minutes: int, max_gap_hours: int
) -> list[dict]:
    """Вычисляет интервалы между сессиями аккаунтов по cached_logs."""
    if not os.path.exists(LOGS_DB):
        return []

    cutoff = datetime.now() - timedelta(hours=window_hours)
    try:
        conn = sqlite3.connect(LOGS_DB)
        c = conn.cursor()
        rows = c.execute(
            "SELECT acc_id, dt FROM cached_logs ORDER BY id DESC LIMIT 5000"
        ).fetchall()
        conn.close()
    except Exception:
        app.logger.exception("Не удалось прочитать данные для расчёта циклов")
        return []

    per_acc: dict[str, list[datetime]] = {}
    for acc_id, dt_str in rows:
        dt_val = _safe_parse_dt(dt_str)
        if not dt_val or dt_val < cutoff:
            continue
        key = acc_id or "unknown"
        per_acc.setdefault(key, []).append(dt_val)

    cycles: list[dict] = []
    for acc_id, points in per_acc.items():
        if len(points) < 2:
            continue
        points.sort()
        gaps: list[float] = []
        for prev, curr in zip(points, points[1:]):
            delta_min = (curr - prev).total_seconds() / 60
            if delta_min < min_gap_minutes:
                continue
            if delta_min > max_gap_hours * 60:
                continue
            gaps.append(delta_min)

        if gaps:
            avg = sum(gaps) / len(gaps)
            cycles.append(
                {
                    "acc_id": acc_id,
                    "count": len(gaps),
                    "avg_minutes": round(avg, 1),
                    "min_minutes": round(min(gaps), 1),
                    "max_minutes": round(max(gaps), 1),
                    "last_cycle_minutes": round(gaps[-1], 1),
                }
            )

    return cycles


def _int_arg(name: str, default: int) -> int:
    """Безопасно извлекает int-параметр из query string."""
    try:
        return int(request.args.get(name, default))
    except (TypeError, ValueError):
        return default

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


@app.route("/api/server/self_status")
def api_self_status():
    """Отдаёт базовое состояние сервера и фоновых задач."""
    try:
        now = datetime.now(timezone.utc)
        uptime_seconds = (now - APP_START_TIME).total_seconds()
        background = {
            "daily_backup_scheduler": {
                "started": BACKGROUND_FLAGS.get("daily_backup_scheduler", False),
                "alive": bool(DAILY_BACKUP_THREAD and DAILY_BACKUP_THREAD.is_alive()),
            },
            "last_update_time": LAST_UPDATE_TIME.isoformat() if LAST_UPDATE_TIME else None,
            "threads": len(threading.enumerate()),
        }
        health_checks = {
            "logs_db_exists": os.path.exists(LOGS_DB),
            "resources_db_exists": os.path.exists(RESOURCES_DB),
            "profile_accessible": os.path.exists(PROFILE_PATH),
        }
        status = "ok" if all(health_checks.values()) else "degraded"
        return jsonify(
            {
                "server": SERVER,
                "start_time": APP_START_TIME.isoformat(),
                "uptime_seconds": uptime_seconds,
                "background": background,
                "health": {"status": status, **health_checks},
            }
        )
    except Exception as exc:
        app.logger.exception("api_self_status failed")
        return jsonify({"error": str(exc)}), 500


@app.route("/api/problems/summary")
def api_problems_summary():
    window_hours = _int_arg("window_hours", 24)
    try:
        crashed = _load_crashed_entries()
        log_stats = _collect_problem_summary(window_hours)
        return jsonify(
            {
                "window_hours": window_hours,
                "crashed": {"count": len(crashed), "items": crashed},
                "log_problems": log_stats,
                "sources": {
                    "logs_db": os.path.exists(LOGS_DB),
                    "crashed_json": os.path.exists(CRASHED_JSON_PATH)
                    or os.path.exists(LOCAL_CRASHED_JSON),
                },
            }
        )
    except Exception as exc:
        app.logger.exception("api_problems_summary failed")
        return jsonify({"error": str(exc)}), 500


@app.route("/api/cycle_time")
def api_cycle_time():
    window_hours = _int_arg("window_hours", 24)
    min_gap_minutes = _int_arg("min_gap_minutes", 30)
    max_gap_hours = _int_arg("max_gap_hours", 12)

    try:
        cycles = _calculate_cycle_times(
            window_hours=window_hours,
            min_gap_minutes=min_gap_minutes,
            max_gap_hours=max_gap_hours,
        )
        total_cycles = sum(item["count"] for item in cycles)
        overall = {
            "total_cycles": total_cycles,
            "accounts": len(cycles),
            "avg_minutes": 0,
            "min_minutes": None,
            "max_minutes": None,
        }
        if total_cycles:
            weighted = sum(item["avg_minutes"] * item["count"] for item in cycles)
            overall["avg_minutes"] = round(weighted / total_cycles, 1)
            overall["min_minutes"] = min(item["min_minutes"] for item in cycles)
            overall["max_minutes"] = max(item["max_minutes"] for item in cycles)

        return jsonify(
            {
                "window_hours": window_hours,
                "min_gap_minutes": min_gap_minutes,
                "max_gap_hours": max_gap_hours,
                "overall": overall,
                "cycles": sorted(cycles, key=lambda x: x.get("acc_id", "")),
            }
        )
    except Exception as exc:
        app.logger.exception("api_cycle_time failed")
        return jsonify({"error": str(exc)}), 500


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
    settings = json.loads(acc.get("Data", "[]"))
    menu     = json.loads(acc.get("MenuData", "{}"))
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
            data_list = json.loads(acc.get("Data", "[]"))

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

            # 4) сохраняем обратно в JSON-профиль
            acc["Data"] = json.dumps(data_list, ensure_ascii=False)
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
    # СТАЛО
    data = request.get_json() or {}
    ids  = data.get("acc_ids", [])
    backup_dir = data.get("backup_dir", "")  # "" → дефолт

    cfg_override = (os.path.join(BACKUP_CONFIG_DST_ROOT, backup_dir)
                    if backup_dir else None)
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
    crashed = _load_crashed_entries()
    return jsonify(crashed)  # например ["leidian5.config", "leidian36.config"]


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
    conn_log = sqlite3.connect(LOGS_DB)
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

@app.route("/api/prices", methods=["GET"])
def api_get_prices():
    with PRICES_LOCK:
        return jsonify(PRICES)

@app.route("/api/prices", methods=["PUT"])
def api_put_prices():
    data = (request.get_json() or {})
    with PRICES_LOCK:
        # валидация и обновление
        if "fws100" in data:
            try: PRICES["fws100"] = max(0.0, float(data["fws100"]))
            except: pass
        if "gold100" in data:
            try: PRICES["gold100"] = max(0.0, float(data["gold100"]))
            except: pass
        if "percent" in data:
            try:
                v = float(data["percent"])
                PRICES["percent"] = min(100.0, max(0.0, v))
            except:
                pass
        save_prices(PRICES)
        return jsonify(PRICES)


@app.route("/api/resources")
def api_resources():
    acts= load_profiles()
    active_ids= {a["Id"] for a in acts}
    inst_map= {a["Id"]: a.get("InstanceId",-1) for a in acts}

    conn= sqlite3.connect(RESOURCES_DB)
    c= conn.cursor()
    rows= c.execute("SELECT id,nickname,food,wood,stone,gold,gems,last_updated FROM resources").fetchall()
    conn.close()

    today_str= datetime.now().strftime("%Y-%m-%d")
    conn= sqlite3.connect(RESOURCES_DB)
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

        accounts.append({
          "id": acc_id,
          "nickname": nick,
          "instanceId": inst_map.get(acc_id,-1),

          "food_raw": f,
          "wood_raw": w,
          "stone_raw": s,
          "gold_raw": g,
          "gems_raw": m,

          "food_view": fv,
          "wood_view": wv,
          "stone_view": sv,
          "gold_view": gv,
          "gems_view": mv,

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


# ───── применить шаблон к аккаунту ─────
@app.route("/api/manage/account/<acc_id>/apply_template", methods=["POST"])
def api_apply_template(acc_id):
    """POST {template:"650"} → заменяет поле Data у аккаунта на шаблон."""
    tmpl_name = (request.json or {}).get("template","")
    if tmpl_name not in TEMPLATES:
        return jsonify({"error":"template not found"}),404

    # читаем профиль
    with open(PROFILE_PATH, "r", encoding="utf-8") as f:
        prof = json.load(f)

    # ищем аккаунт
    acc = next((a for a in prof if a.get("Id")==acc_id), None)
    if not acc:
        return jsonify({"error":"acc not found"}),404

    acc["Data"] = TEMPLATES[tmpl_name]           # MenuData НЕ трогаем!

    # сохраняем
    with open(PROFILE_PATH, "w", encoding="utf-8") as f:
        json.dump(prof, f, ensure_ascii=False, indent=2)

    return jsonify({"status":"ok","acc_id":acc_id,"template":tmpl_name})

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

    for acc in prof:
        if acc.get("Id") in dest_ids:
            # копируем ТОЛЬКО Data (шаги), MenuData не трогаем
            acc["Data"] = src.get("Data", "[]")


    with open(PROFILE_PATH, "w", encoding="utf-8") as f:
        json.dump(prof, f, ensure_ascii=False, indent=2)

    return jsonify({"status":"ok"})


@app.route("/api/logstatus")
def api_logstatus():
    acts = load_profiles()
    active_ids = {acc["Id"] for acc in acts}

    conn = sqlite3.connect(RESOURCES_DB)
    c = conn.cursor()
    rows = c.execute("SELECT id, nickname, food, wood, stone, gold, gems FROM resources").fetchall()
    conn.close()

    today_str = datetime.now().strftime("%Y-%m-%d")
    conn = sqlite3.connect(RESOURCES_DB)
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

    connL = sqlite3.connect(LOGS_DB)
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
                if "no more marches left" or "Reached Maximum of Marches" in line.lower():
                    foundNoMore=True
                if "update the game" in line.lower():
                    foundUpdate=True    
                    break
        status[acid]["hasNoMoreMarches"] = foundNoMore
        status[acid]["hasUpdateGame"] = foundUpdate
    connL.close()

    return jsonify(status)

@app.route("/api/fix/do")
def api_fix_do():
    acc_id      = request.args.get("acc_id")
    only_config = bool(int(request.args.get("config_only", "0")))
    backup_dir  = request.args.get("backup_dir", "")  # "" → дефолт

    cfg_override = (os.path.join(BACKUP_CONFIG_DST_ROOT, backup_dir)
                    if backup_dir else None)

    return {"logs": do_fix_logic(acc_id,
                                only_config=only_config,
                                cfg_src_override=cfg_override)}

@app.route("/api/fix/config_batch", methods=["POST"])
def api_fix_config_batch():
    data       = request.get_json() or {}
    ids        = data.get("acc_ids", [])
    backup_dir = data.get("backup_dir", "")

    cfg_override = (os.path.join(BACKUP_CONFIG_DST_ROOT, backup_dir)
                    if backup_dir else None)

    out = []
    for acc_id in ids:
        out += do_fix_logic(acc_id,
                            only_config=True,
                            cfg_src_override=cfg_override)
    return {"logs": out}

@app.route("/api/logs")
def api_logs():
    acc_id= request.args.get("acc_id")
    if not acc_id:
        return {"error":"no acc_id"},400
    conn= sqlite3.connect(LOGS_DB)
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
    if len(lines)>100:
        lines= lines[-100:]
    return {"acc_id":acc_id,"logs": lines}


if __name__=="__main__":
    if not os.path.exists(RESOURCES_DB):
        print("Создаём базу ресурсов:", RESOURCES_DB)
    init_resources_db()
    init_accounts_db()

    if not os.path.exists(LOGS_DB):
        print("Создаём базу логов:", LOGS_DB)
    init_logs_db()




    parse_logs()
    try:
        sync_account_meta()
    except Exception as exc:
        print(f"sync_account_meta failed: {exc}")
    ensure_today_backups()      # ➟ создаст бэкапы, если их ещё нет за сегодня
    _schedule_daily_backups()   # ➟ запустит фоновый планировщик на полуночь

    LAST_UPDATE_TIME= datetime.now(timezone.utc)
    app.run(debug=True, host="0.0.0.0", port=5001)
