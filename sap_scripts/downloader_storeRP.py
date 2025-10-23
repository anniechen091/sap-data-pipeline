import os
import time
import pandas as pd
import datetime
import pyperclip
from requests import session
import win32com.client
from ETL_SAP.sap_scripts.sap_utils import *
from ETL_SAP.sap_scripts.login import sap_login
from dotenv import load_dotenv

load_dotenv()

def run_storeRP_query(session, site_range_type, export_dir, filename):
    try:
        print(f"🟡 查詢 StoreRP 第 {site_range_type} 段: ")

        article_info = {
            "1": {
                "low": "1000000",
                "high": "1499999",
            },
            "2": {
                "low": "1500000",
                "high": "1599999",
            },
            "3": {
                "low": "1600000",
                "high": "1699999",
            },
            "4": {
                "low": "1700000",
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
                "high": "4999999",
            }
        }[site_range_type]

        time.sleep(1)
        session.findById("wnd[0]/tbar[0]/okcd").text = "ZMACHK"
        session.findById("wnd[0]").sendVKey(0)
        time.sleep(1)
        
        # Article 範圍
        session.findById("wnd[0]/usr/ctxtMATNR-LOW").text = article_info["low"]
        session.findById("wnd[0]/usr/ctxtMATNR-HIGH").text = article_info["high"]

        session.findById("wnd[0]/usr/radRA7").select()

        session.findById("wnd[0]/usr/ctxtS_WERKS-LOW").text = "1000"
        session.findById("wnd[0]/usr/ctxtS_WERKS-HIGH").text = "5999"
        session.findById("wnd[0]/usr/ctxtS_DISMM-LOW").text = "RP"

        # 執行查詢
        session.findById("wnd[0]").sendVKey(8)


        print(f"等待查詢結果...")
        wait_for_table(session, timeout=2400)     # 40 分鐘
        select_layout(session, "AC-StoreRP")
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
        raise RuntimeError(f"{site_range_type} 執行查詢函式 run_storeRP_query 時發生錯誤：{e}")


# ========== 自訂 safe_query 專用於 ZMB51 ========== 
def safe_query(session, site_range_type, export_dir, filename, max_retries=2):
    for attempt in range(1, max_retries + 1):
        try:
            success = run_storeRP_query(session, site_range_type, export_dir, filename)

            if success:
                return True, session  # 成功查詢與匯出
            else:
                raise Exception("Safe Query Error: Query did not complete successfully")
        except Exception as e:
            print(f"⚠️ {site_range_type} 第 {attempt} 次查詢失敗：{e}")
            log_error("storeRP", str(e), datetime.datetime.now().strftime('%m%d%Y'), site_range=site_range_type)
            close_all_sap_sessions()
            time.sleep(3)
            try:
                session = sap_login()
                print("🔁 已重新登入 SAP")
            except Exception as login_err:
                log_error("storeRP", f"Login Failed: {login_err}", datetime.datetime.now().strftime('%m%d%Y'), site_range=site_range_type)
                return False
            
    return False



# ========== 主流程 ==========
def download_storeRP(EXPORT_DIR):

    session = sap_login()
    if not os.path.exists(EXPORT_DIR):
        os.makedirs(EXPORT_DIR)

    print("🔹 開始 download_storeRP...")

    for site_range_type in ["1", "2", "3", "4", "5", "6", "7"]:

        done_key = f"{datetime.datetime.now().strftime('%m%d%Y')}_{site_range_type}"
        filename = f"StoreRP_{done_key}.xlsx"
        file_path = os.path.join(EXPORT_DIR, filename)

        print(file_path)

        # 檢查是否已完成
        if is_already_done("storeRP", done_key):
            print(f"✅ 已完成：{done_key}，略過")
            continue

        success, session = safe_query(
            session=session,
            site_range_type=site_range_type,
            export_dir=EXPORT_DIR,
            filename=filename
        )
        
        if success:
            record_done("storeRP", done_key)
        else:
            print(f"❌ 主流程 download_storeRP 發生錯誤，中斷於：{done_key}，將於下次重新執行時繼續查詢")
            return

    print("🎉 STORE RP 所有查詢與匯出已完成")
    return True



# 執行 main
if __name__ == "__main__":
    try:
        EXPORT_DIR = os.getenv("EXPORT_DIR_StoreRP")
        print(EXPORT_DIR)
        download_storeRP(EXPORT_DIR)
    except Exception as e:
        print(f"❌ 執行 download_storeRP 主流程過程中發生錯誤：{e}")