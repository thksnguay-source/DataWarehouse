# utils.py
import json
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
from config.db_config import get_mysql_url


def create_mysql_engine():
    url = get_mysql_url()
    if '?' in url:
        url += '&charset=utf8mb4'
    else:
        url += '?charset=utf8mb4'
    return create_engine(url, pool_pre_ping=True, connect_args={"charset": "utf8mb4"})


# ETL LOG
def start_etl_log():
    engine = create_mysql_engine()
    with engine.begin() as conn:
        result = conn.execute(text("""
            INSERT INTO etl_log (batch_id, source_table, target_table, status)
            VALUES (:batch_id, :source_table, :target_table, 'running')
        """), {
            "batch_id": f"batch_{datetime.now().strftime('%Y%m%d%H%M%S')}",
            "source_table": "stg_products",
            "target_table": "dim_*"
        })
        etl_id = result.lastrowid

    print(f"[LOG] Start ETL batch: {etl_id}")
    return etl_id


def update_etl_log(etl_id, status="running", inserted=0):
    engine = create_mysql_engine()
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE etl_log
            SET status=:status,
                records_inserted=:inserted,
                end_time=IF(:status <> 'running', NOW(), NULL)
            WHERE etl_id=:etl_id
        """), { 
            "status": status,
            "inserted": inserted,
            "etl_id": etl_id
        })
