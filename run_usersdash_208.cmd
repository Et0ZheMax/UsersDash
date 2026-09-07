@echo off
setlocal
set PYTHONIOENCODING=utf-8
set MULTIDASH_SKIP_ELEVATION=1
set MULTIDASH_DEBUG=0
cd /d C:\Users\rss\Desktop\RssCounterV8\GIT\UsersDash\UsersDash
"C:\Users\rss\AppData\Local\Programs\Python\Python314\python.exe" -X utf8 app.py 1>>data\usersdash-live.log 2>>data\usersdash-live-error.log
