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

def run_zmachk_query(session, site_range_type, export_dir, filename):
    try:
        print(f"🟡 查詢 ZMACHK 第 {site_range_type} 段: ")

        article_info = {
            "1": {
                "low": "1000000",
                "high": "1399999",
            },
            "2": {
                "low": "1400000",
                "high": "1799999",
            },
            "3": {
                "low": "1800000",
                "high": "2019999",
            },
            "4": {
                "low": "2020000",
                "high": "2049999",
            },
            "5": {
                "low": "2050000",
                "high": "2079999",
            },
            "6": {
                "low":  "2080000",
                "high": "2091999",
            },
            "7": {
                "low":  "2092000",
                "high": "2094999",
            },
            "8": {
                "low":  "2095000",
                "high": "9999999",
            }
        }[site_range_type]

        time.sleep(1)
        session.findById("wnd[0]/tbar[0]/okcd").text = "ZMACHK"
        session.findById("wnd[0]").sendVKey(0)
        time.sleep(1)
        
        # Article 範圍
        session.findById("wnd[0]/usr/ctxtMATNR-LOW").text = article_info["low"]
        session.findById("wnd[0]/usr/ctxtMATNR-HIGH").text = article_info["high"]

        # 執行查詢
        session.findById("wnd[0]").sendVKey(8)


        print(f"等待查詢結果...")
        # wait_for_table(session, timeout=2400)     # 40 分鐘

        try:
            # wait_for_table(session, timeout=2400)   # 40 分鐘
            run_with_hard_timeout(
                wait_for_table,
                session=session
            )
        except TimeoutError as te:
            print("⚠️ 報表逾時，準備重試…")       
            os.system("taskkill /f /im saplogon.exe")  # 殺掉可能已卡死的 saplogon.exe
            raise  # 讓 safe_query() 捕捉並重登

        select_layout(session, "/AC-ZMACHK")
        wait_for_export_menu(session)

        session.findById("wnd[0]/mbar/menu[0]/menu[3]/menu[1]").select()
        time.sleep(1)
        session.findById("wnd[1]/usr/ctxtDY_PATH").text = export_dir
        session.findById("wnd[1]/usr/ctxtDY_FILENAME").text = filename
        session.findById("wnd[1]/tbar[0]/btn[11]").press()  # 用覆蓋存檔的按鈕
        print(f"✅ 匯出完成：{filename}")
        close_exported_excel(filename)

        print(f"✅ 成功匯出：{os.path.join(export_dir, filename)}")
        time.sleep(1)

        # 返回主畫面
        session.findById("wnd[0]/tbar[0]/okcd").text = "/n"
        session.findById("wnd[0]").sendVKey(0)
        time.sleep(1)
        
        return True
    

    except Exception as e:
        raise RuntimeError(f"{site_range_type} 執行查詢函式 run_zmachk_query 時發生錯誤：{e}")


# ========== 自訂 safe_query 專用於 ZMB51 ========== 
def safe_query(session, site_range_type, export_dir, filename, max_retries=2):
    for attempt in range(1, max_retries + 1):
        try:
            success = run_zmachk_query(session, site_range_type, export_dir, filename)

            if success:
                return True, session  # 成功查詢與匯出
            else:
                raise Exception("Safe Query Error: Query did not complete successfully")
        except Exception as e:
            print(f"⚠️ {site_range_type} 第 {attempt} 次查詢失敗：{e}")
            log_error("zmachk", str(e), datetime.datetime.now().strftime('%m%d%Y'), site_range=site_range_type)
            close_all_sap_sessions()
            time.sleep(3)
            try:
                session = sap_login()
                print("🔁 已重新登入 SAP")
            except Exception as login_err:
                log_error("zmachk", f"Login Failed: {login_err}", datetime.datetime.now().strftime('%m%d%Y'), site_range=site_range_type)
                return False
            
    return False



# ========== 主流程 ==========
def download_zmachk(EXPORT_DIR):

    session = sap_login()
    if not os.path.exists(EXPORT_DIR):
        os.makedirs(EXPORT_DIR)
        
    print("🔹 開始 download_zmachk...")

    for site_range_type in ["1", "2", "3", "4", "5", "6", "7"]:

        done_key = f"{datetime.datetime.now().strftime('%m%d%Y')}_{site_range_type}"
        filename = f"ZMACHK_{done_key}.xlsx"
        file_path = os.path.join(EXPORT_DIR, filename)

        print(file_path)

        # 檢查是否已完成
        if is_already_done("zmachk", done_key):
            print(f"✅ 已完成：{done_key}，略過")
            continue

        success, session = safe_query(
            session=session,
            site_range_type=site_range_type,
            export_dir=EXPORT_DIR,
            filename=filename
        )
        
        if success:
            record_done("zmachk", done_key)
        else:
            print(f"❌ 主流程 download_zmachk 發生錯誤，中斷於：{done_key}，將於下次重新執行時繼續查詢")
            return

    print("🎉 ZMACHK 所有查詢與匯出已完成")
    return True



# 執行 main
if __name__ == "__main__":
    try:
        EXPORT_DIR = os.getenv("EXPORT_DIR_ZMACHK")
        print(EXPORT_DIR)
        download_zmachk(EXPORT_DIR)
    except Exception as e:
        print(f"❌ 執行 download_zmachk 主流程過程中發生錯誤：{e}")