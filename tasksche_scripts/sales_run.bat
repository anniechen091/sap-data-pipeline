@echo off

REM 錯誤除錯：將環境變量 PATH 輸出到一個臨時文件
echo %PATH% > C:\Users\anniec\Documents\TAWA\AutoScript\ETL_SAP\logs\sales_path_debug.txt

REM *** 1. 使用 PowerShell 獲取穩定的 YYYYMMDD 格式 ***
FOR /F %%d IN ('powershell -Command "Get-Date -Format yyyyMMdd"') DO SET "DATE_STAMP=%%d"

REM 2. 設定路徑變量
set "PYTHON_EXE=C:\Users\anniec\AppData\Local\anaconda3\python.exe"
set "SCRIPT_PATH=C:\Users\anniec\Documents\TAWA\AutoScript\ETL_SAP\run_all.py"
set "LOG_DIR=C:\Users\anniec\Documents\TAWA\AutoScript\ETL_SAP\logs\sales_logs"
set "LOG_FILE=%LOG_DIR%\sales_log_%DATE_STAMP%.txt"

cd /d "C:\Users\anniec\Documents\TAWA\AutoScript\ETL_SAP"

REM 3. 確保日誌目錄存在
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

REM *** 4. 關鍵修正：啟用 Anaconda 環境，然後執行腳本 ***
REM 這裡我們直接執行 Python，如果失敗，請使用下一行註釋掉的代碼
"%PYTHON_EXE%" "%SCRIPT_PATH%" >> "%LOG_FILE%" 2>&1

REM **備用方法：如果直接執行 Python 失敗，請取消下面兩行的註釋，並註釋掉上面的 Python 執行行**
REM call "C:\Users\anniec\AppData\Local\anaconda3\Scripts\activate.bat"
REM call "%PYTHON_EXE%" "%SCRIPT_PATH%" >> "%LOG_FILE%" 2>&1

timeout /t 5
endlocal