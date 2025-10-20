import time
import win32com.client

def list_all_gui_elements(session):
    def traverse(element, indent=""):
        try:
            print(f"{indent}{element.Id} | {element.Text}")
        except:
            print(f"{indent}{element.Id}")
        try:
            for child in element.Children:
                traverse(child, indent + "  ")
        except:
            pass

    root = session.findById("wnd[0]/usr")
    traverse(root)

if __name__ == "__main__":
    sap_gui = win32com.client.GetObject("SAPGUI")
    application = sap_gui.GetScriptingEngine
    session = application.Children(0).Children(application.Children(0).Children.Count - 1)
    print("✅ 已連接到 SAP GUI Session")

    session.findById("wnd[0]/tbar[0]/okcd").text = "ZMMIDR"
    session.findById("wnd[0]").sendVKey(0)
    time.sleep(2)

    # 🔍 執行列出元件
    list_all_gui_elements(session)

    
