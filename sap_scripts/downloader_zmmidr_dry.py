#-----------------單獨執行成功 ->>> 改成所有DC, 只有Dry Grocery -------------------------------------------------------------------------------

import os
import time
import pandas as pd
import datetime
import pyperclip
import win32com.client
from ETL_SAP.sap_scripts.sap_utils import *
# from ETL_SAP.sap_scripts.sap_utils import get_multiple_sessions, wait_for_table, wait_for_export_menu, close_exported_excel

# === 查詢函式 ===
def run_zmmidr_query(session, dc_code, period_str, mch_text, EXPORT_DIR):
    """執行 ZMMIDR 查詢並匯出結果"""
    try:
        print(f"🟡 開始查詢 {dc_code}")
        session.findById("wnd[0]/tbar[0]/okcd").text = "ZMMIDR"
        session.findById("wnd[0]").sendVKey(0)
        time.sleep(2)

        session.findById("wnd[0]/usr/ctxtS_WERKS-LOW").text = dc_code
        session.findById("wnd[0]/usr/ctxtP_MON").text = period_str
        session.findById("wnd[0]/usr/btn%_S_MATKL_%_APP_%-VALU_PUSH").press()
        time.sleep(1)

        pyperclip.copy(mch_text)
        session.findById("wnd[1]/tbar[0]/btn[24]").press()
        time.sleep(1)
        session.findById("wnd[1]/tbar[0]/btn[8]").press()

        session.findById("wnd[0]/usr/radP_OUNIT").select()
        session.findById("wnd[0]").sendVKey(8)
        print(f"⌛ 等待查詢結果...")

        wait_for_table(session)
        wait_for_export_menu(session)

        session.findById("wnd[0]/mbar/menu[0]/menu[3]/menu[1]").select()
        time.sleep(1)
        filename = f"Zmmidr_{dc_code}.xlsx"
        session.findById("wnd[1]/usr/ctxtDY_PATH").text = EXPORT_DIR
        session.findById("wnd[1]/usr/ctxtDY_FILENAME").text = filename
        session.findById("wnd[1]/tbar[0]/btn[0]").press()

        print(f"✅ 匯出完成：{filename}")
        close_exported_excel(filename)

        # 回到主畫面
        session.findById("wnd[0]/tbar[0]/okcd").text = "/n"
        session.findById("wnd[0]").sendVKey(0)
        time.sleep(1)

    except Exception as e:
        print(f"❌ D/C {dc_code} 發生錯誤：{e}")


def download_zmmidr_all(MCH_FILE, EXPORT_DIR):

    # === SAP GUI session ===
    sap_gui = win32com.client.GetObject("SAPGUI")
    application = sap_gui.GetScriptingEngine
    session = application.Children(0).Children(0)
    print("✅ 已連接 SAP GUI")

    # === 設定參數 ===
    DC_CODES = ["9801", "9900", "9901", "9902", "9905"]

    if not os.path.exists(EXPORT_DIR):
        os.makedirs(EXPORT_DIR)

    # === 載入 MCH 並轉為文字 ===
    mch_df = pd.read_excel(MCH_FILE, sheet_name="CM", header=1)
    mch_df = mch_df[mch_df['Dept - EN'] == 'Dry Grocery']
    mch_list = mch_df["MCH"].dropna().astype(str).tolist()
    mch_text = "\r\n".join(mch_list)
    period_str = datetime.datetime.today().strftime("%m/%Y")

    # === 逐個執行所有 D/C ===
    for dc_code in DC_CODES:
        run_zmmidr_query(session, dc_code, period_str, mch_text, EXPORT_DIR)

    print("🎉 所有 D/C 查詢與匯出已完成")


if __name__ == "__main__":
    try:
        MCH_FILE = r"C:\Users\anniec\Documents\TAWA\Report\Assortment Report\All Region Assortment - 06.05.2025.xlsx"
        EXPORT_DIR = r"C:\Users\anniec\Documents\TAWA\SAP\ZMMIDR"
        download_zmmidr_all(MCH_FILE, EXPORT_DIR)
    except Exception as e:
        print(f"❌ 執行過程中發生錯誤：{e}")



#-----------------單獨執行成功---------------------------------------------------------------------------------------------

# import os
# import time
# import pandas as pd
# import datetime
# import pyperclip
# import win32com.client
# from ETL_SAP.sap_scripts.sap_utils import get_multiple_sessions, wait_for_table, wait_for_export_menu


# # === SAP GUI session ===
# sap_gui = win32com.client.GetObject("SAPGUI")
# application = sap_gui.GetScriptingEngine
# session = application.Children(0).Children(application.Children(0).Children.Count - 1)
# print("✅ 已連接 SAP GUI")

# # === 設定參數 ===
# DC_CODE = "9801"
# MCH_FILE = r"C:\Users\anniec\Documents\TAWA\Report\Assortment Report\All Region Assortment - 06.05.2025.xlsx"
# EXPORT_DIR = r"C:\Users\anniec\Documents\TAWA\SAP\ZMMIDR"

# mch_df = pd.read_excel(MCH_FILE, sheet_name="CM", header=1)
# mch_df = mch_df[mch_df['Dept - EN'] == 'Dry Grocery']
# mch_list = mch_df["MCH"].dropna().astype(str).tolist()
# mch_text = "\r\n".join(mch_list)
# pyperclip.copy(mch_text)

# period_str = datetime.datetime.today().strftime("%m/%Y")

# # === 查詢函式 ===
# def run_zmmidr_query(session, dc_code):
#     print(f"🟡 開始查詢 {dc_code}")
#     session.findById("wnd[0]/tbar[0]/okcd").text = "ZMMIDR"
#     session.findById("wnd[0]").sendVKey(0)
#     time.sleep(2)

#     # 填 D/C
#     session.findById("wnd[0]/usr/ctxtS_WERKS-LOW").text = dc_code 

#     # 填 Period
#     session.findById("wnd[0]/usr/ctxtP_MON").text = period_str

#     # 開啟 MCH 多值輸入
#     session.findById("wnd[0]/usr/btn%_S_MATKL_%_APP_%-VALU_PUSH").press()
#     time.sleep(1)

#     # 貼上 clipboard
#     session.findById("wnd[1]/tbar[0]/btn[24]").press()
#     time.sleep(1)
#     session.findById("wnd[1]/tbar[0]/btn[8]").press()

#     # 選擇 Order Unit
#     session.findById("wnd[0]/usr/radP_OUNIT").select()

#     session.findById("wnd[0]").sendVKey(8)
#     print(f"⌛ 等待查詢結果...")

#     wait_for_table(session)  # 永遠等到出現

#      # 導出 Excel
#     session.findById("wnd[0]/mbar/menu[0]/menu[3]").select()
#     time.sleep(5)
#     wait_for_export_menu(session)  # 等待導出選單可用
#     session.findById("wnd[0]/mbar/menu[0]/menu[3]/menu[1]").select()
#     time.sleep(1)

#     filename = f"ZMMIDR_{dc_code}.xlsx"
#     session.findById("wnd[1]/usr/ctxtDY_PATH").text = EXPORT_DIR
#     session.findById("wnd[1]/usr/ctxtDY_FILENAME").text = filename
#     session.findById("wnd[1]/tbar[0]/btn[0]").press()

#     print(f"✅ 匯出完成：{filename}")

# # === 直接呼叫執行（方便 Debug）===
# run_zmmidr_query(session, DC_CODE)




#---------------------------還在嘗試-----------------------------------------------------------------------------------
# import os
# import time
# import pandas as pd
# import datetime
# import pyperclip
# import threading
# import win32com.client
# from ETL_SAP.sap_scripts.sap_utils import get_multiple_sessions, wait_for_table, wait_for_export_menu


# sap_gui = win32com.client.GetObject("SAPGUI")
# application = sap_gui.GetScriptingEngine
# session = application.Children(0).Children(application.Children(0).Children.Count - 1)
# print("✅ 連接到 SAP GUI Scripting 引擎成功")


# # === 設定參數 ===
# DC_CODES = ["9801", "9900", "9901", "9902", "9905"]
# MCH_FILE = r"C:\Users\anniec\Documents\TAWA\Report\Assortment Report\All Region Assortment - 06.05.2025.xlsx"
# EXPORT_DIR = r"C:\Users\anniec\Documents\TAWA\SAP\ZMMIDR"

# if not os.path.exists(EXPORT_DIR):
#     os.makedirs(EXPORT_DIR)


# # ---------- 參數設定 ----------

# mch_df = pd.read_excel(MCH_FILE, sheet_name="CM", header=1)
# mch_df = mch_df[mch_df['Dept - EN'] == 'Dry Grocery']
# mch_list = mch_df["MCH"].dropna().astype(str).tolist()
# mch_text = "\r\n".join(mch_list)  # 正確格式為 Windows 換行
# period_str = datetime.datetime.today().strftime("%m/%Y")


# # 建立 clipboard 操作鎖
# clipboard_lock = threading.Lock()

# # ---------- 定義查詢函式 ----------
# def run_zmmidr_query(session, dc_code, mch_text, period_str):
#     try:
#         print(f"🟡 開始查詢 {dc_code}")
#         session.findById("wnd[0]/tbar[0]/okcd").text = "ZMMIDR"
#         session.findById("wnd[0]").sendVKey(0)
#         time.sleep(2)

#         # 填入 Article 任一筆（可略過）
#         # session.findById("wnd[0]/usr/ctxtSMATNR-LOW").text = "2004723"

#         # 填 D/C、Period
#         session.findById("wnd[0]/usr/ctxtS_WERKS-LOW").text = dc_code 
#         session.findById("wnd[0]/usr/ctxtP_MON").text = period_str

#         # 開啟 MCH 多值輸入
#         session.findById("wnd[0]/usr/btn%_S_MATKL_%_APP_%-VALU_PUSH").press()
#         time.sleep(1)

#         # 使用 clipboard 並貼上
#         pyperclip.copy(mch_text)
#         session.findById("wnd[1]/tbar[0]/btn[24]").press()  # 貼上
#         time.sleep(1)
#         session.findById("wnd[1]/tbar[0]/btn[8]").press()   # 確認

#         # 點選 order unit
#         session.findById("wnd[0]/usr/radP_OUNIT").select()

#         # 執行查詢
#         session.findById("wnd[0]").sendVKey(8)
#         print(f"⌛ 等待查詢結果 ({dc_code})...")
#         wait_for_table(session)


#         # 導出 Excel
#         session.findById("wnd[0]/mbar/menu[0]/menu[3]").select()
#         time.sleep(5)
#         wait_for_export_menu(session)  # 等待導出選單可用

#         session.findById("wnd[0]/mbar/menu[0]/menu[3]/menu[1]").select()
#         time.sleep(1)

#         filename = f"ZMMIDR_{dc_code}.xlsx"
#         session.findById("wnd[1]/usr/ctxtDY_PATH").text = EXPORT_DIR
#         session.findById("wnd[1]/usr/ctxtDY_FILENAME").text = filename
#         session.findById("wnd[1]/tbar[0]/btn[0]").press()

#         print(f"✅ 匯出完成：{filename}")
#     except Exception as e:
#         print(f"❌ D/C {dc_code} 發生錯誤：{e}")



# # === 建立 GUI Session 視窗 ===
# sessions = get_multiple_sessions(len(DC_CODES))

# # === 開始每個視窗的查詢 ===
# threads = []
# for dc_code, sess in zip(DC_CODES, sessions):
#     t = threading.Thread(target=run_zmmidr_query, args=(sess, dc_code, mch_text, period_str))
#     t.start()
#     threads.append(t)

# # 等待所有視窗查詢完成
# for t in threads:
#     t.join()

# print("🎉 所有 D/C 查詢與匯出已完成")
#--------------------------------------------------------------------------------------------------------------



# # ---------- 建立多個 SAP GUI Session ----------
# sessions = get_multiple_sessions(len(DC_CODES))
# sessions = get_multiple_sessions(len(dc_codes))
# export_path = "output/zmmidr"

# if not os.path.exists(export_path):
#     os.makedirs(export_path)




# # =================

# for dc, session in zip(dc_codes, sessions):
#     print(f"🔍 開始查詢 D/C: {dc}")

#     # 進入 T-code ZMMIDR
#     session.findById("wnd[0]/tbar[0]/okcd").text = "ZMMIDR"
#     session.findById("wnd[0]").sendVKey(0)
#     time.sleep(1)

#     # 輸入 D/C
#     session.findById("wnd[0]/usr/ctxtS_WERKS-LOW").text = dc

#     # 設定 Period
#     session.findById("wnd[0]/usr/ctxtP_PERID").text = period

#     # 輸入 MCH（多值貼上）
#     session.findById("wnd[0]/usr/btn%_S_MCH_%_APP_%-VALU_PUSH").press()
#     time.sleep(1)
#     session.findById("wnd[1]/tbar[0]/btn[24]").press()  # 貼上剪貼簿
#     time.sleep(1)
#     session.findById("wnd[1]/tbar[0]/btn[8]").press()   # 確定

#     # 選取 Order Unit 單位
#     session.findById("wnd[0]/usr/radP_OUNIT").select()

#     # 執行查詢
#     session.findById("wnd[0]/tbar[1]/btn[8]").press()
#     print("⏳ 執行查詢中...請稍候")





# if __name__ == "__main__":
#     try:
#         sap_gui = win32com.client.GetObject("SAPGUI")
#         application = sap_gui.GetScriptingEngine
#         session = application.Children(0).Children(application.Children(0).Children.Count - 1)
#     except Exception as e:
#         print("❌ 無法連接到 SAP GUI Scripting 引擎，錯誤如下:")
#         exit(e)