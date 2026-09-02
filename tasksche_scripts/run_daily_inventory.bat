@echo off
setlocal EnableExtensions

REM Force redirected Python output to UTF-8 for Task Scheduler.
chcp 65001 >nul
set "PYTHONUTF8=1"
set "PYTHONIOENCODING=utf-8"
set "PYTHONUNBUFFERED=1"

REM Main paths.
set "WORK_DIR=C:\Users\anniec\Documents\TAWA\AutoScript"
set "PYTHONPATH=%WORK_DIR%;%PYTHONPATH%"
set "PYTHON_EXE=C:\Users\anniec\AppData\Local\anaconda3\python.exe"
set "VPN_SCRIPT=%WORK_DIR%\ETL_SAP\tasksche_scripts\Start-VPN-ETL.ps1"
set "DRY_SCRIPT=%WORK_DIR%\ETL_SAP\pipelines\etl_daily_inventory_dry.py"
set "LOCAL_SCRIPT=%WORK_DIR%\ETL_SAP\pipelines\etl_daily_inventory_dan.py"

REM One combined log per day; reruns on the same day append to the same file.
set "LOG_DIR=%WORK_DIR%\ETL_SAP\logs\daily_inventory_logs"
for /f %%d in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd"') do set "DATE_STAMP=%%d"
if not defined DATE_STAMP set "DATE_STAMP=unknown_date"
set "LOG_FILE=%LOG_DIR%\daily_inventory_%DATE_STAMP%.log"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
cd /d "%WORK_DIR%"

call :log "================================================================"
call :log "Daily inventory automation started."

call :log "Starting VPN connection check."
powershell -NoProfile -ExecutionPolicy Bypass -File "%VPN_SCRIPT%" >> "%LOG_FILE%" 2>&1
set "VPN_EXIT=%ERRORLEVEL%"
call :log "Finished VPN connection check with exit code %VPN_EXIT%."

call :log "Runtime information."
>> "%LOG_FILE%" echo Working directory: %CD%
>> "%LOG_FILE%" echo Python: %PYTHON_EXE%
>> "%LOG_FILE%" set PYTHONPATH

@REM call :log "Starting Dry Grocery."
@REM "%PYTHON_EXE%" "%DRY_SCRIPT%" >> "%LOG_FILE%" 2>&1
@REM set "DRY_EXIT=%ERRORLEVEL%"
@REM call :log "Finished Dry Grocery with exit code %DRY_EXIT%."

set "DRY_EXIT=0"
call :log "Dry Grocery temporarily skipped."

call :log "Starting Local Dry + Non Food."
"%PYTHON_EXE%" "%LOCAL_SCRIPT%" >> "%LOG_FILE%" 2>&1
set "LOCAL_EXIT=%ERRORLEVEL%"
call :log "Finished Local Dry + Non Food with exit code %LOCAL_EXIT%."

set "FINAL_EXIT=0"
if not "%VPN_EXIT%"=="0" set "FINAL_EXIT=1"
if not "%DRY_EXIT%"=="0" set "FINAL_EXIT=1"
if not "%LOCAL_EXIT%"=="0" set "FINAL_EXIT=1"

call :log "Daily inventory automation finished with exit code %FINAL_EXIT%."
call :log "================================================================"
exit /b %FINAL_EXIT%

:log
>> "%LOG_FILE%" echo [%DATE% %TIME%] %~1
exit /b 0
