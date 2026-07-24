#-----------------所有Dept-------------------------------------------------------------------------------

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

# === 查詢函式 ===

def run_zmmidr_query(session, dc_code, dept_code, period_str, export_dir, filename):
    """執行 ZMMIDR 查詢並匯出結果"""
    try:
        print(f"🟡 開始查詢 {dc_code}")
        session.findById("wnd[0]/tbar[0]/okcd").text = "ZMMIDR"
        session.findById("wnd[0]").sendVKey(0)
        time.sleep(2)

        session.findById("wnd[0]/usr/ctxtS_WERKS-LOW").text = dc_code
        session.findById("wnd[0]/usr/ctxtP_MON").text = period_str
        time.sleep(1)

        session.findById("wnd[0]/usr/ctxtS_MATKL-LOW").text = f"{dept_code}00000"
        if dept_code != 103:
            session.findById("wnd[0]/usr/ctxtS_MATKL-HIGH").text = f"{dept_code}99999"
        else:
            session.findById("wnd[0]/usr/ctxtS_MATKL-HIGH").text = f"10499999"
        
        if dept_code == 106:
            session.findById("wnd[0]/usr/btn%_SMATNR_%_APP_%-VALU_PUSH").press()
            session.findById("wnd[1]/usr/tabsTAB_STRIP/tabpNOSV").select()
            session.findById("wnd[1]/usr/tabsTAB_STRIP/tabpNOSV/ssubSCREEN_HEADER:SAPLALDB:3030/tblSAPLALDBSINGLE_E/ctxtRSCSEL_255-SLOW_E[1,0]").text = "3604942"
            session.findById("wnd[1]/usr/tabsTAB_STRIP/tabpNOSV/ssubSCREEN_HEADER:SAPLALDB:3030/tblSAPLALDBSINGLE_E/ctxtRSCSEL_255-SLOW_E[1,1]").text = "3604189"
            session.findById("wnd[1]/usr/tabsTAB_STRIP/tabpNOSV/ssubSCREEN_HEADER:SAPLALDB:3030/tblSAPLALDBSINGLE_E/ctxtRSCSEL_255-SLOW_E[1,2]").text = "3604910"
            session.findById("wnd[1]/usr/tabsTAB_STRIP/tabpNOSV/ssubSCREEN_HEADER:SAPLALDB:3030/tblSAPLALDBSINGLE_E/ctxtRSCSEL_255-SLOW_E[1,3]").text = "3604914"
            session.findById("wnd[1]/usr/tabsTAB_STRIP/tabpNOSV/ssubSCREEN_HEADER:SAPLALDB:3030/tblSAPLALDBSINGLE_E/ctxtRSCSEL_255-SLOW_E[1,4]").text = "3604907"
            session.findById("wnd[1]/usr/tabsTAB_STRIP/tabpNOSV/ssubSCREEN_HEADER:SAPLALDB:3030/tblSAPLALDBSINGLE_E/ctxtRSCSEL_255-SLOW_E[1,5]").text = "3604190"
            session.findById("wnd[1]/usr/tabsTAB_STRIP/tabpNOSV/ssubSCREEN_HEADER:SAPLALDB:3030/tblSAPLALDBSINGLE_E/ctxtRSCSEL_255-SLOW_E[1,6]").text = "3604908"
            session.findById("wnd[1]/usr/tabsTAB_STRIP/tabpNOSV/ssubSCREEN_HEADER:SAPLALDB:3030/tblSAPLALDBSINGLE_E/ctxtRSCSEL_255-SLOW_E[1,7]").text = "3604919"
            session.findById("wnd[1]/tbar[0]/btn[8]").press() 
        if dept_code == 105: 
            session.findById("wnd[0]/usr/btn%_SMATNR_%_APP_%-VALU_PUSH").press()
            session.findById("wnd[1]/usr/tabsTAB_STRIP/tabpNOSV").select()
            session.findById("wnd[1]/usr/tabsTAB_STRIP/tabpNOSV/ssubSCREEN_HEADER:SAPLALDB:3030/tblSAPLALDBSINGLE_E/ctxtRSCSEL_255-SLOW_E[1,0]").text = "3677426"
            session.findById("wnd[1]/usr/tabsTAB_STRIP/tabpNOSV/ssubSCREEN_HEADER:SAPLALDB:3030/tblSAPLALDBSINGLE_E/ctxtRSCSEL_255-SLOW_E[1,1]").text = "3675048"
            session.findById("wnd[1]/usr/tabsTAB_STRIP/tabpNOSV/ssubSCREEN_HEADER:SAPLALDB:3030/tblSAPLALDBSINGLE_E/ctxtRSCSEL_255-SLOW_E[1,2]").text = "3671481"
            session.findById("wnd[1]/tbar[0]/btn[8]").press() 


        session.findById("wnd[0]/usr/radP_OUNIT").select()
        session.findById("wnd[0]").sendVKey(8)

        print(f"等待查詢結果...")
        # wait_for_table(session, "wnd[0]/usr/cntlGRID1/shellcont/shell/shellcont[1]/shell", timeout=1800)     # 30 分鐘

        try:
            # wait_for_table(session, timeout=2400)   # 60 分鐘
            run_with_hard_timeout(
                wait_for_table,
                session=session,
                timeout=3600,
                grid_id="wnd[0]/usr/cntlGRID1/shellcont/shell/shellcont[1]/shell"
            )
        except TimeoutError as te:
            print("⚠️ 報表逾時，準備重試…")       
            os.system("taskkill /f /im saplogon.exe")  # 殺掉可能已卡死的 saplogon.exe
            raise  # 讓 safe_query() 捕捉並重登

        select_layout(session, "AC-ZMMIDR")
        wait_for_export_menu(session)

        session.findById("wnd[0]/mbar/menu[0]/menu[3]/menu[1]").select()
        time.sleep(1)
        session.findById("wnd[1]/usr/ctxtDY_PATH").text = export_dir
        session.findById("wnd[1]/usr/ctxtDY_FILENAME").text = filename

        session.findById("wnd[1]/tbar[0]/btn[11]").press()  # 用覆蓋存檔的按鈕

        print(f"✅ 匯出完成：{filename}")
        close_exported_excel(filename)

        # 回到主畫面
        session.findById("wnd[0]/tbar[0]/okcd").text = "/n"
        session.findById("wnd[0]").sendVKey(0)
        time.sleep(1)

        return True

    except Exception as e:
        print(f"❌ D/C {dc_code} 發生錯誤：{e}")
        raise


# === 自訂 safe_query 專用於 Zmmidr ===
def safe_query(session, dept_code, dc_code, period_str, export_dir, filename, max_retries=3):
    for attempt in range(1, max_retries + 1):
        try:
            run_zmmidr_query(session, dc_code, dept_code, period_str, export_dir, filename)
            return True, session  # 成功查詢與匯出

        except Exception as e:
            print(f"⚠️ 第 {attempt} 次失敗：{e}")
            log_error('zmmidr', msg=f"Attempt {attempt}: {e}", dept=dept_code, dc=dc_code)
            close_all_sap_sessions()
            time.sleep(3)
            session = sap_login()
            print("🔁 已重新登入 SAP")

    return False, session


# === 主要下載程式 ===
def download_zmmidr_OUn(EXPORT_DIR):

    session = sap_login()

    # === 設定參數 ===
    DC_CODES = ["9801", "9891", "9900", "9901", "9790", "9793", "9905"]
    dept_name_to_code = {

    'Leisure Food': 101,
    'Alcohol': 102,
    'Dairy & Frozen': 103,
    'Non Food': 105,
    'Dry Grocery': 106,
    
    }

    if not os.path.exists(EXPORT_DIR):
        os.makedirs(EXPORT_DIR)

    # === 開始查詢 ===
    print("🔹 開始進行查詢...")
    depts = ['Alcohol', 'Leisure Food', 'Dairy & Frozen', 'Dry Grocery', 'Non Food']
    period_str = datetime.datetime.today().strftime("%m/%Y")
    time_stamp = datetime.datetime.now().strftime("%m%d%Y")

    for dept_name in depts:

        dept_code = dept_name_to_code.get(dept_name)
        if not dept_code:
            print(f"⚠️ 找不到部門代碼：{dept_name}, pass")
            continue

        print(f"\n🔹 部門：{dept_name}（{dept_code}）")

        # === 逐個執行所有 D/C ===
        for dc_code in DC_CODES:

            done_key = f"{dept_code}_{dc_code}_{time_stamp}"

            # 檢查是否已完成
            if is_already_done("zmmidr_oun", dept_code, dc_code, time_stamp):
                print(f"✅ 已完成：{done_key}，略過")
                continue
        
            # 建立合法檔名
            filename = f"Zmmidr_oun_{done_key}.xlsx".strip()
            success, session = safe_query(
                session=session,
                dept_code=dept_code,
                dc_code=dc_code,
                period_str=period_str,
                export_dir=EXPORT_DIR,
                filename=filename
            )

            if success:
                record_done("zmmidr_oun", dept_code, dc_code, time_stamp)
            else:
                print(f"❌ download_zmmidr_oun 發生錯誤，中斷於：{dept_code}-{dc_code}-{time_stamp}，將於下次重新執行時繼續查詢")
                return              

    print("🎉 所有 D/C 查詢與匯出已完成")
    return True


if __name__ == "__main__":
    try:
        download_zmmidr_OUn(os.getenv("EXPORT_DIR_ZMMIDR_OUn"))
    except Exception as e:
        print(f"❌ 執行 download_zmmidr_OUn 過程中發生錯誤：{e}")