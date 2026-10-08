@echo off
setlocal EnableExtensions

REM ================================================================
REM Daily Inventory Automation
REM Order: VPN -> Local Dry + Non Food -> Dry Grocery
REM ================================================================

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
set "LOCAL_SCRIPT=%WORK_DIR%\ETL_SAP\pipelines\etl_daily_inventory_dan.py"
set "DRY_SCRIPT=%WORK_DIR%\ETL_SAP\pipelines\etl_daily_inventory_dry.py"

REM Dry Grocery switch: 1 = run, 0 = skip for this day.
set "RUN_DRY=1"

REM One combined log per day; same-day manual reruns append to the same file.
set "LOG_DIR=%WORK_DIR%\ETL_SAP\logs\daily_inventory_logs"
for /f %%d in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd"') do set "DATE_STAMP=%%d"
if not defined DATE_STAMP set "DATE_STAMP=unknown_date"
set "LOG_FILE=%LOG_DIR%\daily_inventory_%DATE_STAMP%.log"
set "RUN_ID=%DATE_STAMP%_%RANDOM%"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
cd /d "%WORK_DIR%"

REM Initialize all exit codes so the final result can never use an undefined value.
set "VPN_EXIT=1"
set "LOCAL_EXIT=1"
set "DRY_EXIT=1"
set "FINAL_EXIT=0"

>> "%LOG_FILE%" echo [%DATE% %TIME%] ================================================================
>> "%LOG_FILE%" echo [%DATE% %TIME%] Daily inventory automation started. Run ID: %RUN_ID%
>> "%LOG_FILE%" echo [%DATE% %TIME%] Batch file: %~f0

REM ----------------------------------------------------------------
REM 1. VPN
REM ----------------------------------------------------------------
>> "%LOG_FILE%" echo [%DATE% %TIME%] Starting VPN connection check.
powershell -NoProfile -ExecutionPolicy Bypass -File "%VPN_SCRIPT%" >> "%LOG_FILE%" 2>&1
set "VPN_EXIT=%ERRORLEVEL%"
>> "%LOG_FILE%" echo [%DATE% %TIME%] Finished VPN connection check with exit code %VPN_EXIT%.

>> "%LOG_FILE%" echo [%DATE% %TIME%] Runtime information.
>> "%LOG_FILE%" echo Working directory: %CD%
>> "%LOG_FILE%" echo Python: %PYTHON_EXE%
>> "%LOG_FILE%" set PYTHONPATH

REM ----------------------------------------------------------------
REM 2. Local Dry + Non Food -- runs exactly once.
REM ----------------------------------------------------------------
>> "%LOG_FILE%" echo [%DATE% %TIME%] Starting Local Dry + Non Food.
"%PYTHON_EXE%" "%LOCAL_SCRIPT%" >> "%LOG_FILE%" 2>&1
set "LOCAL_EXIT=%ERRORLEVEL%"
>> "%LOG_FILE%" echo [%DATE% %TIME%] Finished Local Dry + Non Food with exit code %LOCAL_EXIT%.

REM ----------------------------------------------------------------
REM 3. Dry Grocery -- runs once after Local, or is skipped by RUN_DRY=0.
REM ----------------------------------------------------------------
if "%RUN_DRY%"=="1" >> "%LOG_FILE%" echo [%DATE% %TIME%] Starting Dry Grocery.
if "%RUN_DRY%"=="1" "%PYTHON_EXE%" "%DRY_SCRIPT%" >> "%LOG_FILE%" 2>&1
if "%RUN_DRY%"=="1" set "DRY_EXIT=%ERRORLEVEL%"
if "%RUN_DRY%"=="1" >> "%LOG_FILE%" echo [%DATE% %TIME%] Finished Dry Grocery with exit code %DRY_EXIT%.

if "%RUN_DRY%"=="0" set "DRY_EXIT=0"
if "%RUN_DRY%"=="0" >> "%LOG_FILE%" echo [%DATE% %TIME%] Dry Grocery skipped because RUN_DRY=0.

if not "%RUN_DRY%"=="0" if not "%RUN_DRY%"=="1" >> "%LOG_FILE%" echo [%DATE% %TIME%] ERROR: RUN_DRY must be 0 or 1; received "%RUN_DRY%".

REM ----------------------------------------------------------------
REM 4. Final result.
REM ----------------------------------------------------------------
if not "%VPN_EXIT%"=="0" set "FINAL_EXIT=1"
if not "%LOCAL_EXIT%"=="0" set "FINAL_EXIT=1"
if not "%DRY_EXIT%"=="0" set "FINAL_EXIT=1"

>> "%LOG_FILE%" echo [%DATE% %TIME%] Daily inventory automation finished with exit code %FINAL_EXIT%.
>> "%LOG_FILE%" echo [%DATE% %TIME%] ================================================================

endlocal & exit /b %FINAL_EXIT%
