# 🧩 Tawa SAP ETL Pipeline

A fully automated Python-based ETL system that downloads operational data from SAP GUI,
cleans and transforms it, and uploads it to SQL Server for analytics and reporting.

---

## 🚀 Features
- Automated SAP GUI login and data extraction for T-codes (`ZMMIDR`, `ZSTPROMO`, `ZMB51`, etc.)
- Data cleaning and transformation with pandas
- SQL Server upsert with schema validation and upsert logic
- Built-in retry and error recovery for unstable SAP connections
- Airflow integration for scheduling and monitoring

---

## 🧱 Project Structure
```
ETL_SAP/
│
├── dags/                     # Airflow DAG definitions (To be added)
│   ├── zmmidr_dag.py
│   ├── zstpromo_dag.py
│   ├── zmb51_dag.py
│   ├── zrssale_dag.py
│   ├── zmachk_dag.py
│   └── StoreRP_dag.py
│
├── pipelines/                # ETL logic per module
│   ├── etl_zmb51.py
│   ├── etl_zstpromo.py
│   ├── etl_zmachk.py
│   ├── etl_weekly_sales.py
│   ├── etl_zrssale.py
│   ├── etl_zmmidr_oun.py
│   ├── etl_StoreRP.py
│   ├── etl_upload_excel.py
│   └── etl_utils.py
│
├── sap_scripts/              # SAP GUI automation scripts
│   ├── login.py
│   ├── downloader_zmb51
│   ├── downloader_zstpromo.py
│   ├── downloader_zmachk.py
│   ├── downloader_zrssale.py
│   ├── downloader_zmmidr_oun
│   ├── downloader_zmmidr_bun.py
│   └── downloader_storeRP.py
│
├── utils/                    # Shared modules
│   ├── sql_loader.py
│   └── config.py
│
├── logs/                   
│
├── requirements.txt          # Python dependencies
├── run_all.py
├── .gitignore
├── .env
├── LICENSE
└── README.md

```
