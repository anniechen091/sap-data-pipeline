import sys, os, io
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')


import os
import re
import shutil
import pandas as pd
from pathlib import Path
from collections import defaultdict
from ETL_SAP.common.loader import *
from ETL_SAP.common.config import get_sql_engine
from datetime import datetime
from sqlalchemy.types import VARCHAR, NVARCHAR, DECIMAL, INTEGER, Date, DateTime
from ETL_SAP.sap_scripts.downloader_zmmidr_oun import download_zmmidr_OUn
from ETL_SAP.pipelines.etl_utils import *
from dotenv import load_dotenv

from ETL_SAP.sap_scripts.downloader_zmmidr_bun import download_zmmidr_BUn
from ETL_SAP.pipelines.etl_zmmidr_bun import run_etl_zmmidr_BUn

load_dotenv()

def load_zmmidr_file(filepath, dc):
    df = pd.read_excel(filepath, dtype={'Article No': str, 'MCH': str})
    df.drop(df.index[-1], inplace=True)  # 刪除合計列
    df['Article No'] = df['Article No'].str.lstrip('0')
    df = df.rename(columns={'Article No': 'Article'})
    df.insert(0, 'DC', dc)

    return df

def run_etl_zmmidr_OUn(folder_path):
    print("🔹 開始清理 Zmmidr_OUn 檔案...")
    
    # 遍歷所有 Excel 檔案，格式如 Zmmidr_106_9801_06012025.xlsx
    file_pattern = re.compile(r'Zmmidr_oun_(\d{3})_(\d{4})_\d{8}\.xlsx')
    dept_dfs = defaultdict(list)
    processed_dir = os.path.join(folder_path, "processed")
    os.makedirs(processed_dir, exist_ok=True)

    for fname in os.listdir(folder_path):
        m = file_pattern.match(fname)
        if not m:
            continue
        dept, dc = m.groups()
        full_path = os.path.join(folder_path, fname)
        df = load_zmmidr_file(full_path, dc)
        dept_dfs[dept].append(df)

    # 各 Dept 先 concat，自成一張；再 concat 全部
    combined_all = [
        pd.concat(df_list, ignore_index=True)
        for df_list in dept_dfs.values()
    ]

    if not combined_all:
        print("⚠️ 沒有任何可合併的資料，跳過 Zmmidr_OUn 上傳流程")
        return 

    df_all = pd.concat(combined_all, ignore_index=True)

    print(f"✅ 合併完成，共 {len(df_all)} 筆")

    # 數字清洗
    df_all['Unrestricted-Use Stock'] = df_all['Unrestricted-Use Stock'].apply(clean_number)
    df_all['On order Stock'] = df_all['On order Stock'].apply(clean_number)
    df_all.insert(0, 'Date', datetime.today().date())

    if df_all.duplicated(subset=['Date', 'DC', 'Article']).any():
        duplicate_count = len(df_all.duplicated(subset=['Date', 'DC', 'Article']).sum())
        print(f" {duplicate_count} duplicates found, have been removed.")
        print(df_all[df_all.duplicated(subset=['Date', 'DC', 'Article'])])
        df_all = df_all.drop_duplicates(subset=['Date', 'DC', 'Article'], keep='last').reset_index(drop=True)

    df_all.to_excel(os.path.join(folder_path, f"df_Zmmidr_OUn_{datetime.today().strftime('%m%d%Y')}.xlsx"), index=False)
    df_all.to_excel(rf"\\TAWASHARE2\Replenishment_Data\Jimmy\Zmmidr\df_Zmmidr_OUn_{datetime.today().strftime('%m%d%Y')}.xlsx", index=False)
    print(f"已匯出所有部門的 df_Zmmidr_OUn_date.xlsx")
    Process_Dry_Zmmidr(df_all)
   

    # 上傳至 SQL Server
    print(f"🔹 開始上傳 Zmmidr_oun 資料到 {os.getenv("SQL_DB")}...")
    engine = get_sql_engine()
    column_types = {
    'Date': Date,     
    'DC': NVARCHAR(10),
    'Article': NVARCHAR(20),
    'MCH': NVARCHAR(50),                         
    'Pack size': NVARCHAR(50),
    'Unit': NVARCHAR(10),                 
    'D/C MAP': DECIMAL(14, 6),                 
    'Unrestricted-Use Stock': DECIMAL(14, 6),       # 庫存量（數量）
    'Allocation Qty': DECIMAL(14, 6),               # 分配量（數量）
    'On order Stock': DECIMAL(14, 6),               # 已訂購庫存（數量）
    'Unrestricted Stock Value': DECIMAL(14, 6),# 庫存價值（高金額）
    'PTD MVMT': DECIMAL(14, 6),                     # 本期移動量
    'YTD MVMT': DECIMAL(14, 6),                     # 年累計移動量
    }  


    # 上傳至 Zmmidr - replace

    upsert_batch(
        df=df_all,
        target_table=os.getenv("TABLE_ZMMIDR_OUn"),
        unique_keys=["DC", "Article", "Date"],
        column_types=column_types
    )
    
    # upload_to_sql(df_all, os.getenv("TABLE_ZMMIDR_OUn"), column_types, 'replace')

    # ---------- 移動到 processed ----------
    kill_excel()
    time.sleep(2)
    for filename in os.listdir(folder_path):
        if "Zmmidr_" in filename:
            src_path = os.path.join(folder_path, filename)
            if os.path.isfile(src_path):
                name, ext = os.path.splitext(filename)
                new_name = f"{name}_{ext}"
                dest_path = os.path.join(processed_dir, new_name)
                shutil.move(src_path, dest_path)
                print(f"Moved: {filename} → {new_name}")

    print("完成 Zmmidr_OUn 上傳流程")




def Process_Dry_Zmmidr(df):
    # df = pd.read_excel(os.path.join(folder_path, "df_Zmmidr_all.xlsx"))
    df = df.copy()
    # df['Dept'] = df['MCH'].astype(str).str[:3]
    # df = df[df['Dept']=='106']
    # df.drop(columns='Dept', inplace=True)
    df.dropna(subset='Article', inplace=True)
    df = df[['DC', 'Article', 'Unrestricted-Use Stock', 'On order Stock']]

    df_SCA = df[df['DC'].astype(str).isin(['9891', '9801'])]
    df_SCA = df_SCA.groupby("Article", as_index=False, observed=True).agg({
        'DC':'first',
        'Unrestricted-Use Stock':'sum',
        'On order Stock':'sum'
    })
    df_SCA['DC']='9891'
    print("length of 9891: ", len(df[df['DC']=='9891']))
    print("length of 9890: ", len(df[df['DC']=='9801']))
    print("length of SCA: ", len(df_SCA))


    df_EC = df[df['DC'].astype(str).isin(['9790', '9901'])]
    df_EC = df_EC.groupby("Article", as_index=False, observed=True).agg({
        'DC':'first',
        'Unrestricted-Use Stock':'sum',
        'On order Stock':'sum'
    })
    df_EC['DC']='9790'

    print("length of 9790: ", len(df[df['DC']=='9790']))
    print("length of 9901: ", len(df[df['DC']=='9901']))
    print("length of EC: ", len(df_EC))

    df_NCA = df[df['DC'].astype(str).isin(['9900'])]
    df_9793 = df[df['DC'].astype(str).isin(['9793'])]


    df_all = pd.concat([df_SCA, df_NCA, df_EC, df_9793], ignore_index=True)
    df_all.insert(0, 'Article NoDC', df_all['DC'].astype(str) + df_all['Article'].astype(str))

    current_inv_fp = os.getenv("current_inv_fp")
    history_inv_fp = os.getenv("history_inv_fp")
    
    df_all.to_excel(os.path.join(current_inv_fp, "df_Zmmidr_OUn.xlsx"), index=False)
    df_all.to_excel(os.path.join(history_inv_fp, f"df_Zmmidr_OUn_{datetime.today().date().strftime('%m%d%Y')}.xlsx"), index=False)
    df_all.to_excel(r"\\TAWASHARE2\Replenishment_Data\Jimmy\Zmmidr\df_Zmmidr_OUn.xlsx", index=False)
    print(f"已匯出合併庫存 Zmmidr 至 {current_inv_fp} 及 {history_inv_fp} 及 共享資料夾")


if __name__ == "__main__":

    download_zmmidr_OUn(os.getenv("EXPORT_DIR_ZMMIDR_OUn"))
    run_etl_zmmidr_OUn(os.getenv("EXPORT_DIR_ZMMIDR_OUn"))


    
    
    





    # # 合併 EC 區域 code =======================================================
    # dept_dfs = defaultdict(list)

    # for file in os.listdir(folder_path):
    #     match = file_pattern.match(file)
    #     if match:
    #         dept, dc = match.groups()
    #         full_path = os.path.join(folder_path, file)
    #         df = load_zmmidr_file(full_path, dc)
    #         dept_dfs[dept].append((dc, df))

    # combined_all = []
    # for dept, dc_df_list in dept_dfs.items():
    #     dfs = {dc: df for dc, df in dc_df_list}

    #     # 合併 EC 倉庫：9901 + 9902 + 9905
    #     ec_sources = [dfs[dc] for dc in ['9901', '9902', '9905'] if dc in dfs]
    #     if ec_sources:
    #         df_ec = pd.concat(ec_sources, ignore_index=True)
    #         df_ec = df_ec.groupby("Article", as_index=False).agg({
    #             'DC': 'first',
    #             'Article': 'first',
    #             'MCH': 'first',
    #             'Dept': 'first',
    #             'Article Description': 'first',
    #             'Article Description.1': 'first',
    #             'Pack size': 'first',
    #             'D/C MAP': 'first',
    #             'Unrestricted-Use Stock': 'sum',
    #             'Allocation Qty': 'sum',
    #             'On order Stock': 'sum',
    #             'Unrestricted Stock Value': 'sum',
    #             'PTD MVMT': 'sum', 
    #             'YTD MVMT': 'sum', 
    #             'SCA Assortment': 'first', 
    #             'Assortment grade': 'first', 
    #             'Asrt.Grade Description': 'first', 
    #         })
    #         df_ec['DC'] = 'EC'
    #     else:
    #         df_ec = pd.DataFrame()

    #     # 合併這個部門的所有倉庫
    #     dfs_list = [dfs[dc] for dc in ['9801', '9900'] if dc in dfs]
    #     if not df_ec.empty:
    #         dfs_list.append(df_ec)

    #     if not dfs_list:
    #         continue

    #     df_dept_combined = pd.concat(dfs_list, ignore_index=True)
    #     combined_all.append(df_dept_combined)

    # # 合併所有部門結果
    # df_all = pd.concat(combined_all, ignore_index=True)