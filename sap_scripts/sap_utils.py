import win32com.client
import time
import win32gui
import pyautogui
import win32con
import datetime
import os
import pandas as pd
from datetime import timedelta
from dotenv import load_dotenv

load_dotenv()

# === 錯誤記錄函式 ===
def record_done(flow_name, *keys):
    """紀錄查詢完成項目，keys 可包含 dept, dc, date 等"""
    os.makedirs("logs", exist_ok=True)
    done_key = "_".join(str(k) for k in keys)
    with open(f"ETL_SAP/logs/{flow_name}_done.txt", "a") as f:
        f.write(done_key + "\n")


def is_already_done(flow_name, *keys):
    """確認是否已查詢完成"""
    done_key = "_".join(str(k) for k in keys)
    path = f"ETL_SAP/logs/{flow_name}_done.txt"
    if not os.path.exists(path):
        return False
    with open(path) as f:
        return done_key in f.read()
    
    
def select_layout(session, layout_name):
    # 點選 layout 按鈕，開啟 layout 選單
    session.findById("wnd[0]/tbar[1]/btn[33]").press()
    
    # 指向 layout 選單的 table
    layout_table = session.findById("wnd[1]/usr/ssubD0500_SUBSCREEN:SAPLSLVC_DIALOG:0501/cntlG51_CONTAINER/shellcont/shell")

    # 逐列搜尋 layout 名稱
    row_count = layout_table.RowCount
    found = False

    for i in range(row_count):
        cell_value = layout_table.GetCellValue(i, "TEXT")
        if cell_value.strip() == layout_name:
            layout_table.currentCellRow = i
            layout_table.selectedRows = str(i)
            layout_table.clickCurrentCell()
            found = True
            break

    if not found:
        raise Exception(f"❗ Layout 名稱「{layout_name}」未找到，請確認是否存在")

    print(f"Layout {layout_name} 選取完成")
    

# ------- 取得目前的 SAP GUI Session ------- 
# def get_current_session():
#     sap_gui = win32com.client.GetObject("SAPGUI")
#     application = sap_gui.GetScriptingEngine
#     return application.Children(0).Children(0)

#  ------- 等待 Table 的方法 ------- 
def wait_for_table(session, grid_id="wnd[0]/usr/cntlGRID1/shellcont/shell",
                   timeout=2400, poll=1.0):
    t0 = time.time()
    while time.time() - t0 < timeout:
        # GUI 還忙 → 再等等
        if session.busy:
            time.sleep(poll)
            print("busy 中…")
            continue
        # 嘗試抓到 Grid
        try:
            grid = session.findById(grid_id)
            # 只要能取到 RowCount 就視為成功
            _ = grid.RowCount
            print("找到 grid，RowCount =", grid.RowCount)
            return
        except Exception:
            # Grid 還沒生成
            time.sleep(poll)
    raise TimeoutError(f"SAP 報表 {grid_id} 等待逾時 {timeout}s")


#  ------- 等待導出選單的方法 ------- 
def wait_for_export_menu(session, interval=5):
    """無限等待，直到『導出』選單可點選"""
    while True:
        try:
            session.findById("wnd[0]/mbar/menu[0]/menu[3]").select()
            return
        except:
            time.sleep(interval)


def wait_for_export_menu_for_local_file(session, interval=3, max_wait=360):
    """等待直到 Local File 導出選單出現為止"""
    waited = 0
    while waited < max_wait:
        try:
            session.findById("wnd[0]/mbar/menu[0]/menu[3]/menu[1]").select()
            session.findById("wnd[1]/usr/ctxtDY_PATH")  # 檢查匯出視窗是否出現
            return
        except:
            time.sleep(interval)
            waited += interval
    raise TimeoutError("⚠️ 匯出選單（Local File）逾時未出現")


def wait_for_file(path: str, timeout: float = 10.0) -> None:
    t0 = time.time()
    while True:
        if os.path.isfile(path) and os.path.getsize(path) > 0:
            return
        if time.time() - t0 > timeout:
            raise RuntimeError(f"檔案未生成或為 0 byte：{path}")
        time.sleep(0.2)

# ------- 關閉已匯出的 Excel 檔案 -------
def close_exported_excel(target_filename: str):
    try:
        time.sleep(0.5)
        excel = win32com.client.GetActiveObject("Excel.Application")
    except Exception as e:
        print(f"⚠️ 無法取得 Excel 應用程式: {e}")
        return
    
    for _ in range(10 * 2):  # 最多等 5 秒
        try:
            for wb in excel.Workbooks:
                if target_filename.lower() in wb.Name.lower():
                    wb.Close(SaveChanges=False)
                    print(f"✅ 關閉: {wb.Name}")
                    return
        except Exception as e:
            print(f"⚠️ 錯誤發生在關閉 Excel: {e}")
            return
        time.sleep(0.5)

    print("⚠️ 找不到目標 Excel 檔案來關閉")



# ------- 關閉所有 SAP Session -------
def close_all_sap_sessions():
    try:
        sap_gui = win32com.client.GetObject("SAPGUI")
        application = sap_gui.GetScriptingEngine
        for i in range(application.Children.Count):
            application.Children(i).CloseSession(0)
        print("✅ 已關閉所有 SAP Sessions")
    except Exception as e:
        print(f"⚠️ 無法關閉 SAP Sessions:{e}")


# ------- 錯誤記錄 -------
def log_error(flow_name, msg, log_file=None, **kwargs):
    """
    通用錯誤紀錄器。
    - flow_name: 流程名稱（如 'zmmidr', 'zmb51'）
    - msg: 錯誤訊息
    - kwargs: 其他標記資訊，例如 dept='103', dc='9801', date='03/2025'
    """
    if not log_file:
        log_file = f"logs/{flow_name}_error_log.txt"
    
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    key_parts = [f"{k}: {v}" for k, v in kwargs.items()]
    key_str = " | ".join(key_parts)

    with open(log_file, "a", encoding="utf-8") as f:
        f.write(f"[{now}] {key_str} | Error: {msg}\n")


#--------------更新日期 --------------

def update_sales_search_date(file_path, fill_missing=True):
    file_path = file_path
    df = pd.read_excel(file_path)
    df["Start"] = pd.to_datetime(df["Start"])
    df["End"] = pd.to_datetime(df["End"])

    added = 0
    last_end = df["End"].max()
    today = pd.Timestamp.today().normalize()

    # 滿一週（或多週）就補
    while today >= (last_end + timedelta(days=7)):
        new_start = last_end + timedelta(days=1)
        new_end = new_start + timedelta(days=6)
        if not ((df["Start"] == new_start) & (df["End"] == new_end)).any():
            df = pd.concat(
                [df, pd.DataFrame([{"Start": new_start, "End": new_end}])],
                ignore_index=True
            )
            added += 1
        last_end = new_end

    if added > 0:
        df["Start"] = df["Start"].dt.strftime("%m/%d/%Y").astype(str)
        df["End"] = df["End"].dt.strftime("%m/%d/%Y").astype(str)
        df.to_excel(file_path, index=False)
        print(f"Added {added} new week(s). Latest week: {last_end.strftime('%m/%d/%Y')}")
    else:
        print("No new week to add - up to date.")



#--------------未用到的 --------------

def get_multiple_sessions(count: int = 1):
    """
    開啟 n 個 SAP GUI window, 回傳 session 物件清單
    """
    sap_gui = win32com.client.GetObject("SAPGUI") 
    app = sap_gui.GetScriptingEngine
    connection = app.Children(0)

    sessions = [connection.Children(0)]  # 目前開著的視窗（主）

    for _ in range(count - 1):

        bring_sap_to_front()  # 聚焦到 SAP
        time.sleep(1)
        pyautogui.hotkey("ctrl", "n")  # 模擬 Ctrl+N 開新視窗
        time.sleep(1)
        # 等待新 session 出現
        while connection.Children.Count < len(sessions) + 1:
            time.sleep(0.5)

        new_session = connection.Children(connection.Children.Count - 1)

        # 等到它的 wnd[0] 出現
        while True:
            try:
                new_session.findById("wnd[0]")
                break
            except:
                time.sleep(0.5)

        sessions.append(new_session)
    return sessions

# ------- 把 SAP GUI 視窗拉到前景 ------- 
def bring_sap_to_front():
    """讓 SAP GUI 成為前景視窗，確保 Ctrl+N 送到正確位置"""
    def enum_handler(hwnd, _):
        if win32gui.IsWindowVisible(hwnd):
            title = win32gui.GetWindowText(hwnd)
            if "SAP Easy Access" in title:
                win32gui.ShowWindow(hwnd, win32con.SW_RESTORE)
                win32gui.SetForegroundWindow(hwnd)
    win32gui.EnumWindows(enum_handler, None)