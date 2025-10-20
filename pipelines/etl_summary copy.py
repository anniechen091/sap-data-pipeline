from datetime import datetime
import pandas as pd
import os
from sqlalchemy import text
from ETL_SAP.common.config import get_sql_engine
from ETL_SAP.common.loader import upsert_batch
from ETL_SAP.pipelines.etl_utils import sql_type_string
from sqlalchemy.types import VARCHAR, NVARCHAR, DECIMAL, INTEGER, Date, DateTime
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

def run_etl_weekly_sales(output_excel= None):
    engine = get_sql_engine()
    
    print("Loading data from ZMB51 + ZSTPROMO + related tables...")

    query = """
    DECLARE @min_wk INT = 202528, @max_wk INT = 202528;

    ;WITH zstpromo AS (
        -- 銷售金額先彙總到「週 × 商品 × 店」
        SELECT  c.AcctWk, t.Article, t.Site, SUM(t.Amt) AS Amt
        FROM    dbo.ZSTPROMO t
        JOIN    dbo.Calendar c ON c.Date = t.Date
        WHERE   c.AcctWk BETWEEN @min_wk AND @max_wk
        GROUP BY c.AcctWk, t.Article, t.Site
    ), zmb51 AS (
        -- 銷售數量和成本先彙總到「週 × 商品 × 店」
        SELECT  c.AcctWk, z.Article, z.Site,
                SUM(z.Quantity) AS Qty,
                SUM(z.Cost)     AS Cost
        FROM    dbo.ZMB51 z
        JOIN    dbo.Calendar c ON c.Date = z.Date
        WHERE   c.AcctWk BETWEEN @min_wk AND @max_wk
        GROUP BY c.AcctWk, z.Article, z.Site
    )
    SELECT  COALESCE(p.AcctWk , s.AcctWk)   AS AcctWk,
            COALESCE(p.Article, s.Article)  AS Article,
            COALESCE(p.Site   , s.Site)     AS Site,
            COALESCE(s.Qty ,0)  AS Qty,
            COALESCE(s.Cost,0)  AS Cost,
            COALESCE(p.Amt ,0)  AS Amt
    FROM    zmb51  s
    FULL JOIN zstpromo p
        ON p.AcctWk  = s.AcctWk
        AND p.Article = s.Article
        AND p.Site    = s.Site;
 
    """

    df = pd.read_sql(text(query), con=engine)
    if "MCH" not in df.columns:
        df["MCH"] = None
    print(f"Data loaded: {len(df)} rows.")
    print(df.head(5))

    # Optional: write to Excel backup
    try:
        if output_excel:
            today_str = datetime.today().strftime('%Y%m%d')
            filename = f"Weekly_Sales_{today_str}.csv"
            full_path = os.path.join(output_excel, filename)

            df.to_csv(full_path, index=False)
            print(f"Excel export completed: {full_path}")
    except Exception as e:
        print(f"Error exporting to Excel: {e}")

    print(f"Uploading to SQL table {os.getenv('TABLE_WEEKLY_SALES')}...")

    # Define column types and primary key for UPSERT
    column_types = {
        'Article': NVARCHAR(50),
        'AcctWk': NVARCHAR(10),
        'Site': NVARCHAR(10),
        'Qty': DECIMAL(18,2),
        'Cost': DECIMAL(18,2),
        'Amt': DECIMAL(18,2),
        'MCH': NVARCHAR(8),
    }


    upsert_batch(
        df=df,
        target_table=os.getenv("TABLE_WEEKLY_SALES"),
        unique_keys=["Article", "AcctWk", "Site"],
        column_types=column_types,
    )
    print("Data uploaded successfully.")
    # print("Updating MCH in Weekly Sales...")

    # with engine.begin() as conn:
    #     conn.execute(text("""
    # UPDATE w
    # SET    w.MCH = a.MCH
    # FROM   dbo.Weekly_Sales AS w
    # JOIN   dbo.Article_MasterData AS a
    #     ON a.Article = w.Article
    # WHERE  w.AcctWk BETWEEN @min_wk AND @max_wk
    # AND (w.MCH IS NULL OR w.MCH <> a.MCH);
    
    # """))

    print(f"ETL for Weekly Sales completed. Uploaded to {os.getenv('TABLE_WEEKLY_SALES')}.")


if __name__ == "__main__":
    output_path = os.getenv('EXPORT_DIR_WEEKLY_SALES')
    # run_etl_weekly_sales(output_excel=output_path)
    run_etl_weekly_sales()
