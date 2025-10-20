import os
import time
import win32com.client
from dotenv import load_dotenv
import subprocess
import pyautogui
from ETL_SAP.sap_scripts.sap_utils import close_all_sap_sessions

# 載入 .env 檔案
load_dotenv()

# 檢查 SAP Logon 是否正在運行
def is_saplogon_running():
    result = subprocess.run('tasklist /FI "IMAGENAME eq saplogon.exe"', capture_output=True, text=True)
    return "saplogon.exe" in result.stdout

# 關閉所有 SAP GUI 的彈出視窗
def close_all_sap_popups():
    for win in pyautogui.getWindowsWithTitle("SAP GUI"):
        try:
            win.activate()
            time.sleep(0.2)
            pyautogui.press("enter")
        except:
            continue

# 登入 SAP GUI 的函式
def sap_login():
    username = os.getenv("SAP_USERNAME")
    password = os.getenv("SAP_PASSWORD")
    client = os.getenv("SAP_CLIENT", "800")
    system = os.getenv("SAP_SYSTEM", "ECC Production")
    try:
        if not is_saplogon_running():
            os.system("start saplogon.exe")
            time.sleep(5)

        # 連接到 SAP GUI Scripting
        SapGuiAuto = win32com.client.GetObject("SAPGUI")
        application = SapGuiAuto.GetScriptingEngine
        connection = application.OpenConnection(system, True)
        session = connection.Children(0)
        time.sleep(2)

        session.findById("wnd[0]/usr/txtRSYST-MANDT").text = client
        session.findById("wnd[0]/usr/txtRSYST-BNAME").text = username
        session.findById("wnd[0]/usr/pwdRSYST-BCODE").text = password
        session.findById("wnd[0]").sendVKey(0)
        time.sleep(2)  # 等待登入完成

        # 多重登入處理：強制踢掉其他 session
        try:
            session.findById("wnd[1]/usr/radMULTI_LOGON_OPT1").select()
            session.findById("wnd[1]/tbar[0]/btn[0]").press()
            print("⚠️ 多重登入，已選 opt1: 結束其他登入")

            # 偵測錯誤視窗標題出現後自動按 OK
            close_all_sap_popups()
        except:
            pass

        print("✅ SAP 登入成功")
    # session.findById("wnd[0]/tbar[0]/okcd").text = "ZINV_MCH"
    # session.findById("wnd[0]").sendVKey(0)
    except Exception as e:
        raise Exception(f"❌ 登入 SAP 失敗，錯誤如下: {e}")
        close_all_sap_sessions()

    return session



if __name__ == "__main__":
    try:
        session = sap_login()
        print("✅ SAP 登入成功")
    except Exception as e:
        print("❌ 登入失敗，錯誤如下：")
        print(e)