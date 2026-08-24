@echo off
setlocal EnableExtensions

REM ================================================================
REM Daily Inventory ETL
REM 1. Connect/check VPN
REM 2. Run Local Dry + Non Food inventory update
REM 3. Run Dry Grocery inventory update
REM The two GUI automations run sequentially to avoid SAP/Chrome conflicts.
REM ================================================================

set "PROJECT_ROOT=C:\Users\anniec\Documents\TAWA\AutoScript"
set "ETL_ROOT=%PROJECT_ROOT%\ETL_SAP"
set "PYTHON_EXE=C:\Users\anniec\AppData\Local\anaconda3\python.exe"
set "VPN_SCRIPT=%ETL_ROOT%\tasksche_scripts\Start-VPN-ETL.ps1"

set "LOCAL_DNF_SCRIPT=%ETL_ROOT%\pipelines\etl_daily_inventory_dan.py"
set "DRY_SCRIPT=%ETL_ROOT%\pipelines\etl_daily_inventory_dry.py"

set "LOG_DIR=%ETL_ROOT%\logs\daily_inventory_logs"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

FOR /F %%d IN ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd"') DO SET "DATE_STAMP=%%d"

set "SCHEDULER_LOG=%LOG_DIR%\scheduler_%DATE_STAMP%.txt"
set "LOCAL_DNF_LOG=%LOG_DIR%\local_dnf_%DATE_STAMP%.txt"
set "DRY_LOG=%LOG_DIR%\dry_%DATE_STAMP%.txt"

echo ================================================================ >> "%SCHEDULER_LOG%"
echo [%DATE% %TIME%] Daily inventory task started. >> "%SCHEDULER_LOG%"

REM Connect/check VPN before starting either ETL.
echo [%DATE% %TIME%] Starting VPN connection check... >> "%SCHEDULER_LOG%"
powershell -NoProfile -ExecutionPolicy Bypass -File "%VPN_SCRIPT%" >> "%SCHEDULER_LOG%" 2>&1
set "VPN_EXIT=%ERRORLEVEL%"
if not "%VPN_EXIT%"=="0" (
    echo [%DATE% %TIME%] WARNING: VPN script returned exit code %VPN_EXIT%. ETL will still attempt to run. >> "%SCHEDULER_LOG%"
)

REM Capture PATH for troubleshooting Task Scheduler environment issues.
echo %PATH% > "%LOG_DIR%\path_debug.txt"

REM Make the ETL_SAP package importable when Task Scheduler starts in System32.
set "PYTHONPATH=%PROJECT_ROOT%;%PYTHONPATH%"

pushd "%PROJECT_ROOT%"
if errorlevel 1 (
    echo [%DATE% %TIME%] ERROR: Cannot open project directory: %PROJECT_ROOT% >> "%SCHEDULER_LOG%"
    endlocal & exit /b 3
)

call :RUN_JOB "Local Dry + Non Food" "%LOCAL_DNF_SCRIPT%" "%LOCAL_DNF_LOG%"
set "LOCAL_DNF_EXIT=%ERRORLEVEL%"

call :RUN_JOB "Dry Grocery" "%DRY_SCRIPT%" "%DRY_LOG%"
set "DRY_EXIT=%ERRORLEVEL%"

popd

set "FINAL_EXIT=0"
if not "%LOCAL_DNF_EXIT%"=="0" set "FINAL_EXIT=1"
if not "%DRY_EXIT%"=="0" set "FINAL_EXIT=1"

echo [%DATE% %TIME%] Local DNF exit code: %LOCAL_DNF_EXIT% >> "%SCHEDULER_LOG%"
echo [%DATE% %TIME%] Dry Grocery exit code: %DRY_EXIT% >> "%SCHEDULER_LOG%"
echo [%DATE% %TIME%] Daily inventory task finished with exit code %FINAL_EXIT%. >> "%SCHEDULER_LOG%"

endlocal & exit /b %FINAL_EXIT%


:RUN_JOB
set "JOB_NAME=%~1"
set "JOB_SCRIPT=%~2"
set "JOB_LOG=%~3"

echo ================================================================ >> "%JOB_LOG%"
echo [%DATE% %TIME%] Starting %JOB_NAME%. >> "%JOB_LOG%"

if not exist "%JOB_SCRIPT%" (
    echo [%DATE% %TIME%] ERROR: Script not found: %JOB_SCRIPT% >> "%JOB_LOG%"
    exit /b 2
)

"%PYTHON_EXE%" "%JOB_SCRIPT%" >> "%JOB_LOG%" 2>&1
set "JOB_EXIT=%ERRORLEVEL%"

echo [%DATE% %TIME%] Finished %JOB_NAME% with exit code %JOB_EXIT%. >> "%JOB_LOG%"
exit /b %JOB_EXIT%
