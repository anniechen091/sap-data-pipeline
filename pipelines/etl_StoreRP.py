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
from ETL_SAP.sap_scripts.downloader_storeRP import download_storeRP
from dotenv import load_dotenv
from ETL_SAP.pipelines.etl_utils import *

load_dotenv()

def run_etl_storeRP(folder_path):

    processed_dir = Path(folder_path, "processed")
    processed_dir.mkdir(exist_ok=True)

    dfs = []
    files = sorted(Path(folder_path).glob("StoreRP_*.xlsx"))
    if not files:
        print(f"❗ Did not find any StoreRP files in {folder_path}")
        return
    for fp in files:
        print(f"Processing file: {fp}")
        df = pd.read_excel(fp, dtype=str)
        dfs.append(df)

    batch_df = pd.concat(dfs, ignore_index=True)
    print("Length:", len(batch_df), " \nContent: \n", batch_df)

    batch_df.rename(columns={
            "Article No.":"Article", 
            "RP Type":"RP_Type", 
            "Reorder Point":"Reorder", 
            "Stock Planner":"Stock_Planner",
            "Rounding value":"Rounding", 
            "Targ.stock":"Target",
        }, inplace=True)

    # 數字清洗
    batch_df.insert(0, 'Date', datetime.today().date())
    batch_df['Reorder'] = pd.to_numeric(batch_df['Reorder'].replace('-', None), errors='coerce')
    batch_df['Rounding'] = pd.to_numeric(batch_df['Rounding'].replace('-', None), errors='coerce')
    batch_df['Target'] = pd.to_numeric(batch_df['Target'].replace('-', None), errors='coerce')

    print(f"Data after cleaning: \n{batch_df.head(2)}\n"
        f"Data count after cleaning: {len(batch_df)}\n")

    # 上傳 StoreRP 至 SQL Server
    print(f"🔹 Start uploading StoreRP data to {os.getenv('SQL_DB')}...")

    column_types = {

        "Article":NVARCHAR(20),
        "Site":NVARCHAR(10),
        "RP_Type":NVARCHAR(5), 
        "Reorder":DECIMAL(10, 4),
        "Stock_Planner":NVARCHAR(10),
        "Rounding":DECIMAL(10, 4),
        "Target":DECIMAL(10, 4),
        "Date": types.DATE(),
    }

    upsert_batch(
        df=batch_df,
        target_table=os.getenv("TABLE_StoreRP"),
        unique_keys=["Article", "Site"],
        column_types=column_types
        )
    
    batch_df.to_csv(rf"C:\Users\anniec\Documents\TAWA\AutoScript\StoreRP\StoreRP_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", index=False, encoding='utf-8-sig')
    print(f"✅  已匯入 {os.getenv("TABLE_StoreRP")} {len(batch_df):,} 列\n")


    engine = get_sql_engine()
    export_store_rp_report(engine, r"C:\Users\anniec\Documents\TAWA\AutoScript\StoreRP\report")



    # ---------- 移動到 processed ----------
    txt_files = sorted(Path(folder_path).glob("StoreRP_*.xlsx"))
    for fp in txt_files:
        dest = processed_dir / fp.name
        # 若同名檔已存在就加時間戳避免覆寫
        if dest.exists():
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            dest = processed_dir / f"{fp.stem}_{timestamp}{fp.suffix}"
        shutil.move(fp, dest)
        print(f"✅ Has moved: {fp} to {dest}")
    print(f"StoreRP files have been moved to {processed_dir}\n")

    print("🎉 All batch processing has been completed")


def export_store_rp_report(engine, output_folder):
    print("🔹 Start executing Store RP Update report ...")
    query = text("""
    WITH Base AS(
        SELECT 
            Article,
            Site,
            AcctWk,
            SUM(Qty) AS QTY,
            ROW_NUMBER() OVER (PARTITION BY Article, Site ORDER BY AcctWk DESC) AS rn
        FROM [dbo].[fact_TawaSales_Weekly]
        GROUP BY Article, Site, AcctWk
    ),
    Sales AS (
        SELECT 
            Article,
            Site,
            COUNT(CASE WHEN Qty > 0 THEN 1 END) AS Wks,
            ROUND(AVG(CASE WHEN Qty > 0 THEN Qty END),1) AS Wkly_Avg
        FROM Base 
        WHERE rn <= 52
        GROUP BY Article, Site
    ),
    LatestRP AS (
        SELECT Article, Site, MAX([Date]) AS MaxDate
        FROM dbo.fact_Store_RP
        GROUP BY Article, Site
    ),
    Main AS (
        SELECT
            r.Article,
            r.Site AS Store,
            r.RP_Type,
            r.Stock_Planner,
            r.Reorder,
            r.Rounding,
            r.Target,
            s.Wkly_Avg,
            s.Wks,
            CASE WHEN r.Rounding*0.5 > s.Wkly_Avg*1.25 THEN CEILING(r.Rounding*0.5)
                 ELSE CEILING(s.Wkly_Avg*1.25) END AS [Sales_*1.25],
            CASE WHEN r.Rounding > s.Wkly_Avg*2 THEN CEILING(r.Rounding)
                 ELSE CEILING(s.Wkly_Avg*2) END AS [Sales_*2]
        FROM [dbo].[fact_Store_RP] AS r
        INNER JOIN Sales AS s
            ON r.Article = s.Article AND r.Site = s.Site
        JOIN LatestRP AS l 
            ON r.Article = l.Article AND r.Site = l.Site AND r.Date = l.MaxDate
    ),
    Condition AS (
        SELECT
            *,
            ABS(Reorder - [Sales_*1.25]) AS diff_ro,
            ABS(Target - [Sales_*2])    AS diff_tar,
            CASE WHEN 
                Wks > 38
                AND (ABS(Reorder - [Sales_*1.25]) > 2 OR ABS(Target - [Sales_*2]) > 2)
                THEN 'YES' ELSE '' END AS Change
        FROM Main
    )
    SELECT 
        *,
        CASE WHEN Change = 'YES' THEN [Sales_*1.25] END AS [New ReOdr],
        CASE WHEN Change = 'YES' THEN [Sales_*2] END AS [New Tgt]
    FROM Condition
    ORDER BY Article, Store
    """)

    try:
        df = pd.read_sql(query, con=engine)
        print(f"✅ Query completed, {len(df):,} records found.")

        os.makedirs(output_folder, exist_ok=True)
        output_file = os.path.join(
            output_folder,
            f"Store_RP_Update_{datetime.now():%Y%m%d_%H%M}.xlsx"
        )

        df.to_excel(output_file, index=False)
        print(f"Output Store RP Review to: {output_file}")

    except Exception as e:
        print("❌ An error occurred while exporting the Store RP Update report:")
        print(e)
        return None
    

if __name__ == "__main__":

    # download_storeRP(os.getenv("EXPORT_DIR_StoreRP"))
    run_etl_storeRP(os.getenv("EXPORT_DIR_StoreRP")) 
