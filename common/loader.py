from __future__ import annotations
import time
import pandas as pd
from sqlalchemy import text, inspect
from typing import Sequence, Mapping
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from ETL_SAP.common.config import get_sql_engine
from ETL_SAP.pipelines.etl_utils import sql_type_string

def upload_to_sql(df, table_name, column_types, if_exists="append", max_retries: int = 3):
    SQL_ENGINE = get_sql_engine()

    for attempt in range(1, max_retries + 1):
        try:
            with SQL_ENGINE.begin() as conn:  # 使用 begin() 可自動 commit/rollback
                try:
                    before = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
                except SQLAlchemyError:
                    before = 0  # 表示表格不存在

                df.to_sql(
                    table_name, 
                    con=conn, 
                    if_exists=if_exists, 
                    index=False, 
                    dtype=column_types,
                    chunksize=200,
                )

                after = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
                print(f"✅ 成功 {if_exists} {len(df)} 筆資料到 {table_name}")
                print(f"{table_name}: 筆數從 {before} → {after}，共新增 {after - before} 筆。")
        except OperationalError as e:
            print(f"⚠️  上傳失敗 ({attempt}/{max_retries})：{e}")
            time.sleep(5)
            SQL_ENGINE.dispose()          # 關掉舊連線，下一輪重建

    raise RuntimeError(f"{table_name} 連續 {max_retries} 次寫入失敗")


def upsert_batch(
    df: pd.DataFrame,
    target_table: str,                    # e.g. dbo.ZMB51
    unique_keys: Sequence[str],           # ['Article','Site','Date']
    column_types: Mapping[str, object],   # to_sql dtype dict
    stg_table: str | None = None,         # None → 自動 <table>_stg
    chunksize: int = 200,
    ):
    """
    通用批次 UPSERT：
    1. TRUNCATE 暫存表
    2. 將 df 批量寫入暫存表
    3. MERGE 暫存→正式表 (matched=UPDATE, not matched=INSERT)

    失敗自動 rollback；成功才會改動正式表。
    """
    engine = get_sql_engine()
    stg_table = stg_table or f"{target_table}_stg"

    # 1) 動態產生 MERGE SQL
    tgt = target_table
    stg = stg_table
    on_cond   = " AND ".join([f"T.[{k}] = S.[{k}]" for k in unique_keys])
    ins_cols  = ", ".join([f"[{c}]" for c in df.columns])
    ins_vals  = ", ".join([f"S.[{c}]" for c in df.columns])
    upd_set   = ", ".join([f"T.[{c}] = S.[{c}]" for c in df.columns
                           if c not in unique_keys])

    merge_sql = f"""
    MERGE {tgt} AS T
    USING {stg} AS S
        ON ({on_cond})
    WHEN MATCHED THEN
        UPDATE SET {upd_set}
    WHEN NOT MATCHED THEN
        INSERT ({ins_cols})
        VALUES ({ins_vals});
    """


    for attempt in range(3):
        try:
            with engine.begin() as conn:
                # 確認正式表是否存在，若不存在就建立
                table_exists = conn.execute(text(f"""
                    SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_SCHEMA = '{tgt.split('.')[0]}' 
                      AND TABLE_NAME = '{tgt.split('.')[1]}'
                """)).scalar()

                if not table_exists:
                    print(f"找不到正式表 {tgt}，正在自動建立...")
                    cols_def = ",\n    ".join([f"[{col}] {sql_type_string(col_type)}"
                                               for col, col_type in column_types.items()])
                    pk_def = f"CONSTRAINT PK_{tgt.replace('.', '_')} PRIMARY KEY ({', '.join(f'[{k}]' for k in unique_keys)})"
                    create_sql = f"""
                    CREATE TABLE {tgt} (
                        {cols_def},
                        {pk_def}
                    );
                    """
                    conn.execute(text(create_sql))

                # 讀取筆數（before）
                try:
                    before = conn.execute(text(f"SELECT COUNT(*) FROM {target_table}")).scalar()
                except SQLAlchemyError:
                    before = 0  # 表示表格不存在
                # (A) 確保暫存表存在（不存在就建立）
                conn.execute(text(f"""
                    IF OBJECT_ID('{stg}','U') IS NULL
                        SELECT TOP 0 * INTO {stg} FROM {tgt};
                """))

                # (B) 清空暫存表
                conn.execute(text(f"TRUNCATE TABLE {stg};"))
                result = conn.execute(text(f"SELECT COUNT(*) FROM {stg}")).scalar()
                print(f"📊 {stg} 資料筆數 after TRUNCATE: {result}")

                # (C) 批量寫入暫存表
                df.to_sql(stg.split('.')[-1],  # 只傳表名給 to_sql
                        schema=stg.split('.')[0] if '.' in stg else None,
                        con=conn,
                        index=False,
                        if_exists="append",
                        dtype=column_types,
                        method=None, # method="multi"
                        chunksize=chunksize)

                # (D) MERGE 更新正式表
                conn.execute(text(merge_sql))
                after = conn.execute(text(f"SELECT COUNT(*) FROM {target_table}")).scalar()
                print(f"✅ 成功新增 {len(df)} 筆資料到 {target_table}")
                print(f"{target_table}: 筆數從 {before} → {after}，共新增 {after - before} 筆。")

                # (F) 移除暫存表
                conn.execute(text(f"""
                    IF OBJECT_ID('{stg}', 'U') IS NOT NULL
                        DROP TABLE {stg};
                """))
                
                print(f"暫存表 {stg} 已刪除")
                        
            print(f"✅ {target_table} upsert 完成（{len(df):,} rows）")
            break

        except (OperationalError, SQLAlchemyError) as e:
            print(f"🚨 第 {attempt+1} 次失敗：{e}")
            engine.dispose()
            time.sleep(5)
            if attempt + 1 == 3:
                print("❌ 最多重試次數已達，放棄此次上傳")
                raise