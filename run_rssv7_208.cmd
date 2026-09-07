@echo off
setlocal
set PYTHONIOENCODING=utf-8
set RSSV7_DEBUG=0
cd /d C:\Users\rss\Desktop\RssCounterV8\GIT\UsersDash\RSSv7
"C:\Users\rss\AppData\Local\Programs\Python\Python314\python.exe" -X utf8 RssCounterWebV7.py 1>>rssv7-live.log 2>>rssv7-live-error.log
