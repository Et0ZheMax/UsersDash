@echo off
setlocal
set PYTHONIOENCODING=utf-8
if "%~1"=="" (
  echo Укажите имя фермы: run_viking_recovery_cli.cmd FARM_NAME
  exit /b 2
)
cd /d "%~dp0"
if not exist logs mkdir logs
"C:\Users\rss\AppData\Local\Programs\Python\Python314\python.exe" -X utf8 viking_recovery.py --farm "%~1" 1>>logs\viking_recovery_cli.log 2>&1
exit /b %ERRORLEVEL%
