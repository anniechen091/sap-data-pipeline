import os
import re
import shutil
import pandas as pd
from math import ceil
from pathlib import Path
from sqlalchemy import text
from collections import defaultdict
from ETL_SAP.common.loader import upload_to_sql, upsert_batch
from ETL_SAP.common.config import get_sql_engine
from datetime import datetime
from sqlalchemy.types import VARCHAR, NVARCHAR, DECIMAL, INTEGER, Date, DateTime
from ETL_SAP.sap_scripts.downloader_zmachk import download_zmachk
from dotenv import load_dotenv
from ETL_SAP.pipelines.etl_utils import *

load_dotenv()

def run_etl_zmachk(folder_path):

    processed_dir = Path(folder_path, "processed")
    processed_dir.mkdir(exist_ok=True)

    dfs = []
    files = sorted(Path(folder_path).glob("ZMACHK_*.xlsx"))
    if not files:
        print(f"❗ 沒有找到任何 ZMACHK 檔案在 {folder_path}")
        return
    for fp in files:
        print(f"Processing file: {fp}")
        df = pd.read_excel(fp, dtype=str)
        df = df[df['Status'] == 'Y']
        df.drop(columns=['Status'], inplace=True)
        dfs.append(df)

    batch_df = pd.concat(dfs, ignore_index=True)
    batch_df = batch_df.drop_duplicates(subset=['Article'])
    print("Length:", len(batch_df), " \nContent: \n", batch_df)

    # ---------- 欄位整理 ----------
    legacy_source_cols = [
        "Article",
        "Article Description",
        "Chinese Desc.",
        "Merchandise Category",
        "Valid-From Date",
        "Size/dimensions",
        "BUn",
        "BUn Conv.",
        "D/I",
        "D/I Conv.",
        "SUn",
        "SUn Conv.",
        "OUn",
        "Oun to Bun Conv",
        "FI Wtunit",
        "FIWtconv.",
        "Brand Name",
        "Country of origin of the article",
        "Minimum Remaining Shelf Life",
        "Total shelf life",
        "Source of Supply",
        "Assortment",
        "Ethnicity",
        "Product Type",
        "DOH Target",
        "Lead Time",
        "Stock Plan Frequency",
        "Supplier Channel",
        "Seasonal",
        "Item Status",
        "Status WS E",
        "Status WS W",
        "Status SCA",
        "Status NCA",
        "Status TX",
        "Status EC",
        "Retail Channel",
        "Status Online",
        "WholeSale Channel",
        "Wacine Ordering",
    ]
    missing_cols = [c for c in legacy_source_cols if c not in batch_df.columns]
    if missing_cols:
        print("❌ 缺少以下舊欄位：")
        print(missing_cols)
        raise ValueError(f"{fp.name} 缺少必要欄位")
    
    batch_df = batch_df[legacy_source_cols].copy()


    batch_df.rename(columns={
            "Article Description": "Article_Description",
            "Chinese Desc.":  "Chinese_Desc",
            "Merchandise Category": "MCH",
            "Valid-From Date": "Valid_From_Date",
            "Size/dimensions": "Size_dimensions",
            "BUn Conv.": "BUn_Conv",
            "D/I": "DI",
            "D/I Conv.": "DI_Conv",
            "SUn Conv.": "SUn_Conv",
            "Oun to Bun Conv": "Oun_to_Bun_Conv",
            "FI Wtunit": "FI_Wtunit",
            "FIWtconv.": "FIWt_Conv",
            "Brand Name": "Brand_Name",
            "Country of origin of the article": "Origin_Country",
            "Minimum Remaining Shelf Life": "Min_Remaining_Shelf_Life",
            "Total shelf life": "Total_Shelf_Life",
            "Source of Supply": "Source_of_Supply",
            "Product Type": "Product_Type",
            "DOH Target": "DOH_Target",
            "Lead Time": "Lead_Time",
            "Stock Plan Frequency": "Stock_Plan_Frequency",
            "Supplier Channel": "Supplier_Channel",
            "Item Status": "Item_Status",
            "Status WS E": "Status_WS_E",
            "Status WS W": "Status_WS_W",
            "Status SCA": "Status_SCA",
            "Status NCA": "Status_NCA",
            "Status TX": "Status_TX",
            "Status EC": "Status_EC",
            "Retail Channel": "Retail_Channel",
            "Status Online": "Status_Online",
            "WholeSale Channel": "WholeSale_Channel",
            "Wacine Ordering": "Wachine_Ordering",

        }, inplace=True)

    # 數字清洗
    batch_df['Valid_From_Date'] = pd.to_datetime(batch_df['Valid_From_Date']).dt.strftime('%Y-%m-%d')
    batch_df['DOH_Target'] = pd.to_numeric(batch_df['DOH_Target'].replace('-', None), errors='coerce')
    batch_df['Lead_Time'] = pd.to_numeric(batch_df['Lead_Time'].replace('-', None), errors='coerce')

    print(f"清洗後資料：\n{batch_df.head(2)}\n"
        f"清洗後資料筆數：{len(batch_df)}\n")
    
    # for col in batch_df.columns:
    #     if batch_df[col].dtype == 'object':
    #         max_len = batch_df[col].astype(str).map(len).max()
    #         print(f"{col}: 最大長度 = {max_len}")

    # numeric_cols = ['BUn_Conv', 'DI_Conv', 'SUn_Conv', 'Oun_to_Bun_Conv', 'FIWt_Conv', 'Min_Remaining_Shelf_Life', 'Total_Shelf_Life', 'DOH_Target', 'Lead_Time']

    # for col in numeric_cols:
    #     batch_df[col] = pd.to_numeric(batch_df[col], errors='coerce')  # 將無法轉成數字的變成 NaN
    #     invalid_rows = batch_df[batch_df[col].isna()]
    #     if not invalid_rows.empty:
    #         print(f"❗ 無效數字資料在欄位 {col}：")
    #         print(invalid_rows[[col, 'Article']].head())

    # for col in ['DOH_Target', 'Lead_Time']:
    #     # 找出那些不能轉為數字的原始值
    #     invalid_mask = pd.to_numeric(batch_df[col], errors='coerce').isna() & batch_df[col].notna()
    #     invalid_rows = batch_df.loc[invalid_mask, [col, 'Article']]
        
    #     if not invalid_rows.empty:
    #         print(f"❗ {col} 欄位中無法轉成數字的原始值：")
    #         print(invalid_rows.head(10))


    # 上傳 ZMACHK 至 SQL Server
    print(f"🔹 開始上傳 Article_MasterData 資料到 {os.getenv('SQL_DB')}...")

    engine = get_sql_engine()
    with engine.connect() as conn:
        # 確保目標資料表存在
        existing_articles = pd.read_sql(text("SELECT Article FROM dbo.dim_Article"), conn)
    existing_set = set(existing_articles['Article'])
    new_rows = batch_df[~batch_df['Article'].isin(existing_set)]

    column_types = {
        "Article": NVARCHAR(20),
        "Article_Description": NVARCHAR(255),
        "Chinese_Desc": NVARCHAR(255),
        "MCH": NVARCHAR(20),
        "Valid_From_Date": Date(),
        "Size_dimensions": NVARCHAR(50),
        "BUn": NVARCHAR(10),
        "BUn_Conv": DECIMAL(10, 4),
        "DI": NVARCHAR(10),
        "DI_Conv": DECIMAL(10, 4),
        "SUn": NVARCHAR(10),
        "SUn_Conv": DECIMAL(10, 4),
        "OUn": NVARCHAR(10),
        "Oun_to_Bun_Conv": DECIMAL(10, 4),
        "FI_Wtunit": NVARCHAR(10),
        "FIWt_Conv": DECIMAL(18, 6),
        "Brand_Name": NVARCHAR(100),
        "Origin_Country": NVARCHAR(50),
        "Min_Remaining_Shelf_Life": INTEGER(),
        "Total_Shelf_Life": INTEGER(),
        "Source_of_Supply": INTEGER(),
        "Assortment": NVARCHAR(30),
        "Ethnicity": NVARCHAR(20),
        "Product_Type": NVARCHAR(30),
        "DOH_Target": DECIMAL(10, 4),
        "Lead_Time": DECIMAL(10, 4),
        "Stock_Plan_Frequency": NVARCHAR(50),
        "Supplier_Channel": NVARCHAR(50),
        "Seasonal": NVARCHAR(20),
        "Item_Status": NVARCHAR(5),
        "Status_WS_E": NVARCHAR(5),
        "Status_WS_W": NVARCHAR(5),
        "Status_SCA": NVARCHAR(5),
        "Status_NCA": NVARCHAR(5),
        "Status_TX": NVARCHAR(5),
        "Status_EC": NVARCHAR(5),
        "Retail_Channel": NVARCHAR(5),
        "Status_Online": NVARCHAR(5),
        "WholeSale_Channel": NVARCHAR(5),
        "Wachine_Ordering": NVARCHAR(30),
    }
    
    batch_df = clean_df_by_sql_schema(batch_df, "dbo.dim_Article")

    # Python 端先檢查文字欄位長度
    diagnose_df_against_column_types(
        batch_df,
        column_types,
        output_dir=Path(folder_path)
    )
    
    upsert_batch(
        df=batch_df,
        target_table=os.getenv("TABLE_Article_MasterData"),
        unique_keys=["Article"],
        column_types=column_types
        )
    print(f"✅ uploaded {os.getenv('TABLE_Article_MasterData')} {len(batch_df):,} rows\n")
        
    sql_export = """
    SELECT *
    FROM dbo.dim_Article
    """
    engine = get_sql_engine()
    with engine.connect() as conn:
        new_zmachk = pd.read_sql(text(sql_export), conn)
    
    new_zmachk.to_csv(r"C:\Users\anniec\Documents\TAWA\AutoScript\DC Forecast - Seasonality\TawaWalong\ZMACHK_ALL.csv", index=False, encoding='utf-8-sig')
    new_zmachk.to_excel(r"C:\Users\anniec\Documents\TAWA\AutoScript\DC Forecast - Seasonality\TawaWalong\ZMACHK_ALL.xlsx", index=False)

    output_path = Path(folder_path) / "new_articles" / f"New_Article_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    
    new_rows.to_excel(output_path, index=False)
    print(f"已將新增的 {len(new_rows)} 筆資料匯出到 {output_path}")


    # ---------- 移動到 processed ----------
    kill_excel()
    time.sleep(2)
    txt_files = sorted(Path(folder_path).glob("ZMACHK_*.xlsx"))
    for fp in txt_files:
        dest = processed_dir / fp.name
        # 若同名檔已存在就加時間戳避免覆寫
        if dest.exists():
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            dest = processed_dir / f"{fp.stem}_{timestamp}{fp.suffix}"
        shutil.move(fp, dest)
        print(f"✅ 已移動檔案：{fp} 到 {dest}")
    print(f"ZMACHK 檔案已移至 {processed_dir}\n")

    print("🎉 全部批次處理結束")


def diagnose_df_against_column_types(df, column_types, output_dir=None):
    """
    用 Python 端檢查 DataFrame 的文字欄位長度是否超過 column_types 定義的 NVARCHAR/VARCHAR 長度。
    不需要資料先進 SQL。
    """

    if output_dir is None:
        output_dir = Path.cwd()
    else:
        output_dir = Path(output_dir)

    output_dir.mkdir(exist_ok=True)

    reports = []
    issues = []

    for col, sql_type in column_types.items():
        if col not in df.columns:
            continue

        # 只檢查有 length 的文字欄位，例如 NVARCHAR(255), VARCHAR(50)
        sql_max_len = getattr(sql_type, "length", None)

        if sql_max_len is None:
            continue

        # 避免 NaN 被轉成字串 "nan"
        s = df[col].where(df[col].notna(), "").astype(str)
        lengths = s.str.len()

        max_len = int(lengths.max()) if len(lengths) > 0 else 0
        max_idx = lengths.idxmax() if len(lengths) > 0 else None

        sample_article = df.loc[max_idx, "Article"] if max_idx is not None and "Article" in df.columns else None
        sample_value = df.loc[max_idx, col] if max_idx is not None else None

        reports.append({
            "Column": col,
            "SQL_Max_Length": sql_max_len,
            "DF_Max_Length": max_len,
            "Sample_Article": sample_article,
            "Sample_Value": sample_value,
        })

        bad_mask = lengths > sql_max_len

        if bad_mask.any():
            bad = df.loc[bad_mask, ["Article", col]].copy() if "Article" in df.columns else df.loc[bad_mask, [col]].copy()
            bad["Column"] = col
            bad["Actual_Length"] = lengths.loc[bad_mask].values
            bad["SQL_Max_Length"] = sql_max_len
            bad["Value"] = df.loc[bad_mask, col].values

            keep_cols = ["Column"]
            if "Article" in bad.columns:
                keep_cols.append("Article")
            keep_cols += ["Actual_Length", "SQL_Max_Length", "Value"]

            issues.append(bad[keep_cols])

    report_df = pd.DataFrame(reports).sort_values(
        by=["DF_Max_Length", "SQL_Max_Length"],
        ascending=False
    )

    print("📏 文字欄位最大長度檢查結果：")
    print(report_df.to_string(index=False))

    if issues:
        issue_df = pd.concat(issues, ignore_index=True)
        issue_path = output_dir / "ZMACHK_truncation_issues.xlsx"
        issue_df.to_excel(issue_path, index=False)

        print("❌ 找到會造成 String or binary data would be truncated 的資料：")
        print(issue_df.head(100).to_string(index=False))
        print(f"📄 問題資料已匯出：{issue_path}")

        raise ValueError("DataFrame contains values longer than SQL column length.")

    print("✅ Python 端檢查通過：所有文字欄位長度都符合 column_types")



if __name__ == "__main__":

    # download_zmachk(os.getenv("EXPORT_DIR_ZMACHK"))
    run_etl_zmachk(os.getenv("EXPORT_DIR_ZMACHK")) 