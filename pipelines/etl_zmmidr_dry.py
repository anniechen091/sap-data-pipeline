import os
import pandas as pd
from ETL_SAP.common.loader import upload_to_sql
from ETL_SAP.common.config import get_sql_engine
from datetime import datetime
from sqlalchemy.types import VARCHAR, NVARCHAR, DECIMAL, INTEGER, Date, DateTime
from sap_scripts.downloader_zmmidr_dry import download_zmmidr_all

def clean_number(val):
    if pd.isna(val):
        return 0.0
    val = str(val).replace(',', '').strip()
    if val.endswith('-') and val[:-1].replace('.', '', 1).isdigit():
        val = '-' + val[:-1]  # 處理尾巴負號
    try:
        return float(val)
    except:
        return 0.0

def load_zmmidr_file(filepath, region):
    df = pd.read_excel(filepath, dtype={'Article No': str, 'MCH': str})
    df.drop(df.index[-1], inplace=True)  # 刪除合計列
    df['Article No'] = df['Article No'].str.lstrip('0')
    df = df.rename(columns={'Article No': 'Article'})
    df['Article'] = df['Article'].astype(int)
    df.insert(0, 'Region', region)
    df.insert(2, 'Dept', df['MCH'].astype(str).str[:3])
    # df['Dept'] = df['MCH'].astype(str).str[:3]

    # df = df[['Article', 'Unrestricted-Use Stock', 'On order Stock']]
    return df

def run_etl_zmmidr(folder_path):
    print("🔹 開始清理 Zmmidr 檔案...")

    region_files = {
        '9801': 'Zmmidr_9801.XLSX',
        '9900': 'Zmmidr_9900.XLSX',
        '9905': 'Zmmidr_9905.XLSX',
        '9901': 'Zmmidr_9901.XLSX',
        '9902': 'Zmmidr_9902.XLSX',
    }

    dfs = {r: load_zmmidr_file(os.path.join(folder_path, f), r) for r, f in region_files.items()}

    # 合併 EC 區域
    df_ec = pd.concat([dfs['9905'], dfs['9901'], dfs['9902']], ignore_index=True)
    df_ec = df_ec.groupby("Article", as_index=False).agg({
        'Region': 'first',
        'Article': 'first',
        'MCH': 'first',
        'Dept': 'first',
        'Article Description': 'first',
        'Article Description.1': 'first',
        'Pack size': 'first',
        'D/C MAP': 'first',
        'Unrestricted-Use Stock': 'sum',
        'Allocation Qty': 'sum',
        'On order Stock': 'sum',
        'Unrestricted Stock Value': 'sum',
        'PTD MVMT': 'sum', 
        'YTD MVMT': 'sum', 
        'SCA Assortment': 'first', 
        'Assortment grade': 'first', 
        'Asrt.Grade Description': 'first', 
    })

    df_ec['Region'] = 'EC'

    df_all = pd.concat([dfs['9801'], dfs['9900'], df_ec], ignore_index=True)

    # 建立 Article NoDC
    df_all.insert(0, 'Article NoDC', df_all['Region'].astype(str) + df_all['Article'].astype(str))

    # 數字清洗
    df_all['Unrestricted-Use Stock'] = df_all['Unrestricted-Use Stock'].apply(clean_number)
    df_all['On order Stock'] = df_all['On order Stock'].apply(clean_number)

    # 導出 Excel
    export_path = os.path.join(folder_path, "df_Zmmidr.xlsx")
    df_Dry = df_all[df_all['Dept'] == '106']
    df_Dry[['Article NoDC', 'Region', 'Article', 'Unrestricted-Use Stock', 'On order Stock']].to_excel(export_path, index=False)
    print(f"已匯出到 {export_path}")
    df_all = df_all.drop(columns=['Article NoDC'], errors='ignore')

    # 加上日期戳記欄位
    df_all.insert(0, 'Snapshot Date', datetime.today().date())

    # 上傳至 SQL Server
    engine = get_sql_engine()
    column_types = {    
    'Region': NVARCHAR(10),
    'Article': NVARCHAR(20),
    'MCH': NVARCHAR(50),                      
    'Article Description': NVARCHAR(255),      
    'Article Description.1': NVARCHAR(255),    
    'Pack size': NVARCHAR(50),                 
    'D/C MAP': DECIMAL(10, 2),                 
    'Unrestricted-Use Stock': DECIMAL(14, 2),       # 庫存量（數量）
    'Allocation Qty': DECIMAL(14, 2),               # 分配量（數量）
    'On order Stock': DECIMAL(14, 2),               # 已訂購庫存（數量）
    'Unrestricted Stock Value': DECIMAL(14, 2),# 庫存價值（高金額）
    'PTD MVMT': DECIMAL(14, 2),                     # 本期移動量
    'YTD MVMT': DECIMAL(14, 2),                     # 年累計移動量
    'SCA Assortment': NVARCHAR(50),            
    'Assortment grade': NVARCHAR(20),          
    'Asrt.Grade Description': NVARCHAR(100),
    'Snapshot Date': Date, 
    }  

    # 上傳至 Zmmidr - replace
    upload_to_sql(df_all, "zmmidr", column_types, 'replace')
    print("完成 Zmmidr 上傳流程")

    # 上傳至 inventory_snapshot - append
    df_all['Snapshot Date'] = datetime.now()
    column_types = {    
    'Region': NVARCHAR(10),
    'Article': NVARCHAR(20),
    'MCH': NVARCHAR(50),                      
    'Article Description': NVARCHAR(255),      
    'Article Description.1': NVARCHAR(255),    
    'Pack size': NVARCHAR(50),                 
    'D/C MAP': DECIMAL(10, 2),                 
    'Unrestricted-Use Stock': DECIMAL(14, 2),       # 庫存量（數量）
    'Allocation Qty': DECIMAL(14, 2),               # 分配量（數量）
    'On order Stock': DECIMAL(14, 2),               # 已訂購庫存（數量）
    'Unrestricted Stock Value': DECIMAL(14, 2),# 庫存價值（高金額）
    'PTD MVMT': DECIMAL(14, 2),                     # 本期移動量
    'YTD MVMT': DECIMAL(14, 2),                     # 年累計移動量
    'SCA Assortment': NVARCHAR(50),            
    'Assortment grade': NVARCHAR(20),          
    'Asrt.Grade Description': NVARCHAR(100),
    'Snapshot Date': DateTime, 
    }  

    upload_to_sql(df_all, "inventory_snapshot", column_types, 'replace')

    print("完成 inventory_snapshot 上傳流程")

    return df_all

if __name__ == "__main__":
    # download_zmmidr_all()
    folder = r"C:\Users\anniec\Documents\TAWA\SAP\ZMMIDR"
    df = run_etl_zmmidr(folder)
