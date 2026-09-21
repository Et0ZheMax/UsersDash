@echo off
setlocal
set "PYTHONIOENCODING=utf-8"
set "RSSV7_DEBUG=0"
cd /d "%~dp0"
"C:\Windows\py.exe" -3 -X utf8 RssCounterWebV7.py 1>>rssv7-live.log 2>>rssv7-live-error.log

