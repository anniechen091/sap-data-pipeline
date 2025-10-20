import os
import time
import pandas as pd
import datetime
import win32com.client
from ETL_SAP.sap_scripts.sap_utils import *
from ETL_SAP.sap_scripts.login import sap_login
from dotenv import load_dotenv
from ETL_SAP.sap_scripts.downloader_zmb51 import download_zmb51

load_dotenv()

def run_zstpromo_query(session, start_date, end_date, export_path, export_dir, filename):
    try:
        print(f"🟡 查詢 ZSTPROMO: {start_date} ~ {end_date}")
        session.findById("wnd[0]/tbar[0]/okcd").text = "ZSTPROMO"
        session.findById("wnd[0]").sendVKey(0)
        session.findById("wnd[0]").maximize()

        # 輸入條件
        session.findById("wnd[0]/usr/ctxtVKORG-LOW").text = "s100"
        session.findById("wnd[0]/usr/ctxtKUNAG-LOW").text = "1003"
        session.findById("wnd[0]/usr/ctxtFKDAT-LOW").text = start_date
        session.findById("wnd[0]/usr/ctxtFKDAT-HIGH").text = end_date
        # session.findById("wnd[0]/usr/chkP_LOCAL").selected = True
        # session.findById("wnd[0]/usr/ctxtP_FILE_L").text = export_path
        # session.findById("wnd[0]/usr/ctxtP_VARI").text = "/AC-ZST"

        # >= s100
        session.findById("wnd[0]/usr/txt%_VKORG_%_APP_%-TEXT").setFocus()
        session.findById("wnd[0]/usr/txt%_VKORG_%_APP_%-TEXT").caretPosition = 18
        session.findById("wnd[0]").sendVKey(2)
        session.findById("wnd[1]/usr/cntlOPTION_CONTAINER/shellcont/shell").setCurrentCell(1, "TEXT")
        session.findById("wnd[1]/usr/cntlOPTION_CONTAINER/shellcont/shell").selectedRows = "1"
        session.findById("wnd[1]/tbar[0]/btn[0]").press()

        # >= 1003
        session.findById("wnd[0]/usr/txt%_KUNAG_%_APP_%-TEXT").setFocus()
        session.findById("wnd[0]/usr/txt%_KUNAG_%_APP_%-TEXT").caretPosition = 4
        session.findById("wnd[0]").sendVKey(2)
        session.findById("wnd[1]/usr/cntlOPTION_CONTAINER/shellcont/shell").setCurrentCell(1, "TEXT")
        session.findById("wnd[1]/usr/cntlOPTION_CONTAINER/shellcont/shell").selectedRows = "1"
        session.findById("wnd[1]/tbar[0]/btn[0]").press()

        # 執行查詢
        session.findById("wnd[0]").sendVKey(8)

        # 避免查詢卡死
        try:
            wait_for_table(session, timeout=2400)   # 40 分鐘
        except TimeoutError as te:
            print("⚠️ 報表逾時，準備重試…")
            # import subprocess             # 殺掉可能已卡死的 saplogon.exe
            # subprocess.run("taskkill /IM saplogon.exe /F", shell=True)
            raise  # 讓 safe_query() 捕捉並重登

        select_layout(session, "AC-ZSTPROMO")

        # 等待查詢結果
        # 開啟「List → Export → Local File」
        session.findById("wnd[0]/mbar/menu[0]/menu[3]/menu[2]").select()
        # 選擇「Text with Tabs」(第一列)
        session.findById("wnd[1]/usr/subSUBSCREEN_STEPLOOP:SAPLSPO5:0150/sub:SAPLSPO5:0150/radSPOPLI-SELFLAG[1,0]").select()
        session.findById("wnd[1]/usr/subSUBSCREEN_STEPLOOP:SAPLSPO5:0150/"
                        "sub:SAPLSPO5:0150/radSPOPLI-SELFLAG[1,0]").select()
        session.findById("wnd[1]/tbar[0]/btn[0]").press()         # 確定

        # ❻ 在 Save-As 視窗輸入資料夾、檔名、編碼
        session.findById("wnd[1]/usr/ctxtDY_PATH").text          = export_dir
        session.findById("wnd[1]/usr/ctxtDY_FILENAME").text      = filename
        session.findById("wnd[1]/usr/ctxtDY_FILE_ENCODING").text = "4110"
        # session.findById("wnd[1]/tbar[0]/btn[0]").press()        # Save
        time.sleep(2)
        session.findById("wnd[1]/tbar[0]/btn[11]").press()  # 用覆蓋存檔的按鈕

        wait_for_file(export_path)
        print(f"✅ 匯出完成：{export_path}")

        # 返回主畫面
        session.findById("wnd[0]/tbar[0]/okcd").text = "/n"
        session.findById("wnd[0]").sendVKey(0)
        time.sleep(1)

        return True

    except Exception as e:
        raise RuntimeError(f"{start_date}~{end_date} ZSTPROMO 查詢錯誤：{e}")


def safe_query(session, start_date, end_date, export_path, EXPORT_DIR, filename, max_retries=2):
    for attempt in range(1, max_retries + 1):
        try:
            success = run_zstpromo_query(session, start_date, end_date, export_path, EXPORT_DIR, filename)
            if success:
                return True, session
        except Exception as e:
            print(f"⚠️ 第 {attempt} 次查詢失敗：{e}")
            log_error("zstpromo", str(e), start=start_date, end=end_date)
            close_all_sap_sessions()
            time.sleep(3)
            try:
                session = sap_login()
                print("🔁 已重新登入 SAP")
            except Exception as login_err:
                log_error("zstpromo", f"Login Failed: {login_err}", start=start_date)
                return False
    return False


def download_zstpromo(DATE_FILE, EXPORT_DIR):
    session = sap_login()
    if not os.path.exists(EXPORT_DIR):
        os.makedirs(EXPORT_DIR)

    print("🔹 開始讀取 Excel 檔案...")
    df = pd.read_excel(DATE_FILE, engine='openpyxl')
    for col in ['Start', 'End']:
        df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%m/%d/%Y')
    df = df.dropna(subset=['Start', 'End'])

    for _, row in df.iterrows():
        start_date = row['Start']
        end_date = row['End']
        done_key = f"{start_date}_{end_date}"
        filename = f"ZSTPROMO_{start_date.replace('/', '')}_{end_date.replace('/', '')}.txt"
        export_path = os.path.join(EXPORT_DIR, filename)

        if is_already_done("zstpromo", done_key):
            print(f"✅ 已完成：{done_key}，略過")
            continue

        success, session = safe_query(session, start_date, end_date, export_path, EXPORT_DIR, filename)
        if success:
            record_done("zstpromo", done_key)
        else:
            print(f"❌ 中斷於：{done_key}，將於下次重新執行時繼續查詢")
            return

    print("🎉 ZSTPROMO 所有查詢與匯出已完成")
    return True



if __name__ == "__main__":
    try:
        DATE_FILE = os.getenv("DATE_FILE_ZSTPROMO")
        EXPORT_DIR = os.getenv("EXPORT_DIR_ZSTPROMO")
        download_zstpromo(DATE_FILE, EXPORT_DIR)


        DATE_FILE = os.getenv("DATE_FILE_ZMB51")
        EXPORT_DIR = os.getenv("EXPORT_DIR_ZMB51")
        download_zmb51(DATE_FILE, EXPORT_DIR)
        
    except Exception as e:
        print(f"❌ 執行 download_zmb51 主流程過程中發生錯誤：{e}")