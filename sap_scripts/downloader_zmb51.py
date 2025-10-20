import os
import time
import pandas as pd
import datetime
import pyperclip
import win32com.client
from ETL_SAP.sap_scripts.sap_utils import *
from ETL_SAP.sap_scripts.login import sap_login
from dotenv import load_dotenv

load_dotenv()


# =========單次查詢函式 ==========

def run_zmb51_query(session, start_date, end_date, site_range_type, export_dir, filename):
    try:
        print(f"🟡 查詢 {site_range_type}: {start_date} ~ {end_date}")

        site_info = {
            "NCA_EC": {
                "low": "1601",
                "high": "1799",
                "exclude": False
            },
            "SCA": {
                "low": "1000",
                "high": "5999",
                "exclude": True,
                "exclude_low": "1601",
                "exclude_high": "1799"
            }
        }[site_range_type]

        time.sleep(1)
        session.findById("wnd[0]/tbar[0]/okcd").text = "ZMB51"
        session.findById("wnd[0]").sendVKey(0)
        time.sleep(1)

        # Layout
        session.findById("wnd[0]/usr/ctxtALV_DEF").text = "/AC-251"
        
        # Site 範圍
        session.findById("wnd[0]/usr/ctxtWERKS-LOW").text = site_info["low"]
        session.findById("wnd[0]/usr/ctxtWERKS-HIGH").text = site_info["high"]

        # 排除範圍
        if site_info.get("exclude"):
            session.findById("wnd[0]/usr/btn%_WERKS_%_APP_%-VALU_PUSH").press()
            session.findById("wnd[1]/usr/tabsTAB_STRIP/tabpNOINT").select()
            session.findById("wnd[1]/usr/tabsTAB_STRIP/tabpNOINT/ssubSCREEN_HEADER:SAPLALDB:3040/tblSAPLALDBINTERVAL_E/ctxtRSCSEL_255-ILOW_E[1,0]").text = site_info["exclude_low"]
            session.findById("wnd[1]/usr/tabsTAB_STRIP/tabpNOINT/ssubSCREEN_HEADER:SAPLALDB:3040/tblSAPLALDBINTERVAL_E/ctxtRSCSEL_255-IHIGH_E[2,0]").text = site_info["exclude_high"]
            session.findById("wnd[1]/tbar[0]/btn[8]").press() # 確定排除範圍

        # Movement Type
        session.findById("wnd[0]/usr/ctxtBWART-LOW").text = "251"
        session.findById("wnd[0]/usr/ctxtBWART-HIGH").text = "252"

        # Posting Date
        session.findById("wnd[0]/usr/ctxtBUDAT-LOW").text = start_date
        session.findById("wnd[0]/usr/ctxtBUDAT-HIGH").text = end_date

        # 執行查詢
        session.findById("wnd[0]").sendVKey(8)
        # 避免查詢卡死
        wait_for_table(session, timeout=2400)   # 40 分鐘
        # wait_for_export_menu_for_local_file(session)

        # 等待查詢結果
        # 開啟「List → Export → Local File」
        session.findById("wnd[0]/mbar/menu[0]/menu[1]/menu[2]").select()
        # 選擇「Text with Tabs」(第一列)
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

        full_path = os.path.join(export_dir, filename)
        wait_for_file(full_path)
        time.sleep(2)

        print(f"✅ 成功匯出：{os.path.join(export_dir, filename)}")

        # 返回主畫面
        session.findById("wnd[0]/tbar[0]/okcd").text = "/n"
        session.findById("wnd[0]").sendVKey(0)
        time.sleep(1)
        
        return True

    except Exception as e:
        raise RuntimeError(f"{start_date}~{end_date} {site_range_type} 執行查詢函式 run_zmb51_query 時發生錯誤：{e}")


# ========== 自訂 safe_query 專用於 ZMB51 ========== 
def safe_query(session, start_date, end_date, site_range_type, export_dir, filename, max_retries=2):
    for attempt in range(1, max_retries + 1):
        try:
            success = run_zmb51_query(session, start_date, end_date, site_range_type, export_dir, filename)

            if success:
                return True, session  # 成功查詢與匯出
            else:
                raise Exception("Safe Query Error: Query did not complete successfully")
        except Exception as e:
            print(f"⚠️ {site_range_type} 第 {attempt} 次查詢失敗：{e}")
            log_error("zmb51", str(e), start=start_date, end=end_date, site_range=site_range_type)
            close_all_sap_sessions()
            time.sleep(3)
            try:
                session = sap_login()
                print("🔁 已重新登入 SAP")
            except Exception as login_err:
                log_error("zmb51", f"Login Failed: {login_err}", start=start_date, end=end_date, site_range=site_range_type)
                return False
            
    return False



# ========== 主流程 ==========
def download_zmb51(DATE_FILE, EXPORT_DIR):

    session = sap_login()
    if not os.path.exists(EXPORT_DIR):
        os.makedirs(EXPORT_DIR)
        

    print("🔹 開始讀取 Excel 檔案...")
    
    dates_df = pd.read_excel(DATE_FILE, engine='openpyxl')
    date_columns = ['Start', 'End']
    for col in date_columns:
        dates_df[col] = pd.to_datetime(dates_df[col], errors='coerce').dt.strftime('%m/%d/%Y')
    dates_df = dates_df.dropna(subset=['Start', 'End'])

    for _, row in dates_df.iterrows():

        for site_range_type in ["NCA_EC", "SCA"]:
            start_date = row["Start"]
            end_date = row["End"]

            done_key = f"{start_date}_{end_date}_{site_range_type}"
            filename = f"ZMB51_{done_key.replace('/', '')}.txt"
            file_path = os.path.join(EXPORT_DIR, filename)


            # 檢查是否已完成
            if is_already_done("zmb51", done_key):
                print(f"✅ 已完成：{done_key}，略過")
                continue

            # if os.path.exists(file_path):
            #     print(f"✅ 檔案已存在，略過：{file_path}")
            #     continue           

            success, session = safe_query(
                session=session,
                start_date=start_date,
                end_date=end_date,
                site_range_type=site_range_type,
                export_dir=EXPORT_DIR,
                filename=filename
            )
            
            if success:
                record_done("zmb51", done_key)
            else:
                print(f"❌ 主流程 download_zmb51發生錯誤，中斷於：{done_key}，將於下次重新執行時繼續查詢")
                return  

    print("🎉 ZMB51 所有查詢與匯出已完成")
    return True



# 執行 main
if __name__ == "__main__":
    try:
        DATE_FILE = os.getenv("DATE_FILE_ZMB51")
        EXPORT_DIR = os.getenv("EXPORT_DIR_ZMB51")
        download_zmb51(DATE_FILE, EXPORT_DIR)
    except Exception as e:
        print(f"❌ 執行 download_zmb51 主流程過程中發生錯誤：{e}")