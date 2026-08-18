@echo off
chcp 65001 >nul
title LDPlayer VM Mover
cd /d "%~dp0"
if exist "%~dp0LDPlayer_VM_Mover.exe" (
    start "" "%~dp0LDPlayer_VM_Mover.exe"
    exit /b 0
)
where py >nul 2>nul
if not errorlevel 1 (
    py -3 "%~dp0ldplayer_mover_gui.pyw"
    goto finish
)
where python >nul 2>nul
if not errorlevel 1 (
    python "%~dp0ldplayer_mover_gui.pyw"
    goto finish
) else (
    echo ОШИБКА: Python 3.10 или новее не найден.
)
:finish
echo.
pause
