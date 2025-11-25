#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from config.datamart_cofig import CONFIG, connect_db, DIMENSION_MAPPINGS
import pandas as pd
from datetime import datetime
import sys
import pymysql
from datetime import timezone

PROCESS_ID = 5

# =========================
# 🔹 LOG ETL CHUNG
# =========================
def execute_log_query(sql, data=None):
    conn = None
    try:
        conn = connect_db(CONFIG["CONTROL_DB"])
        with conn.cursor() as cursor:
            cursor.execute(sql, data)
            conn.commit()
            return cursor.lastrowid
    except pymysql.MySQLError as err:
        print(f"⚠️ Lỗi ghi/xóa log vào CONTROL_DB: {err}")
        return None
    finally:
        if conn:
            conn.close()

### 4. Viết Log Start ETL
def log_start(batch_id, source_table, target_table):
    sql = """
        INSERT INTO etl_log (batch_id, process_id, source_table, target_table, status, start_time)
        VALUES (%s, %s, %s, %s, 'started', NOW())
    """
    etl_id = execute_log_query(sql, (batch_id, PROCESS_ID, source_table, target_table))
    print(f"✅ Log START: etl_id={etl_id}, batch_id={batch_id}")
    return etl_id

### 9. 10. Ghi log success/failed
def log_end(etl_id, status, inserted=0, updated=0, error_message=''):
    sql = """
        UPDATE etl_log
        SET end_time = NOW(),
            status = %s,
            records_inserted = %s,
            records_updated = %s,
            error_message = %s
        WHERE etl_id = %s
    """
    error_message = error_message[:99] if error_message else ''
    execute_log_query(sql, (status, inserted, updated, error_message, etl_id))
    print(f"✅ Log END: etl_id={etl_id}, status={status}")

### 11. Xóa log start
def log_delete_started(etl_id):
    sql = "DELETE FROM etl_log WHERE etl_id=%s AND status='started' AND end_time IS NULL"
    execute_log_query(sql, (etl_id,))
    print(f"🗑️ Đã xóa log START etl_id={etl_id}")

# =========================
# 🔹 CHECK LẦN CHẠY P4
# =========================
def get_last_success_log(process_id: int):
    conn = connect_db(CONFIG["CONTROL_DB"])
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute("""
                SELECT batch_id, records_inserted, records_updated, end_time
                FROM etl_log
                WHERE process_id=%s AND status='success'
                ORDER BY end_time DESC LIMIT 1
            """, (process_id,))
            return cur.fetchone()
    finally:
        conn.close()

def get_last_success_end_time(process_id: int):
    log = get_last_success_log(process_id)
    return log.get('end_time') if log else None

def get_last_dwh_success_time():
    return get_last_success_end_time(4)

### 1. Kiểm tra có dữ liệu mới từ LoadDataWarehouse.
def should_run_process_5() -> bool:
    end_time_4 = get_last_success_end_time(4)
    end_time_5 = get_last_success_end_time(5)

    print(f"✅ End time P4 (Latest Success): {end_time_4}")
    print(f"✅ End time P5 (Latest Success): {end_time_5}")

    if end_time_4 is None:
        print("⚠️ Chưa có DWH Load chạy thành công (P4). Dừng ETL P5.")
        return False

    # Sửa: Trả về False nếu P5 đã mới hơn/bằng P4
    if end_time_5 is not None and end_time_4 <= end_time_5:
        print("⚠️ Lần chạy DWH Load cũ hơn/bằng Data Mart gần nhất (P4 <= P5). Dừng ETL P5.")
        return False

    # Trường hợp còn lại: P4 mới hơn P5 (bao gồm cả P5 is None)
    print("✅ DWH Load mới hơn Data Mart (P4 > P5 hoặc P5 chưa chạy). Bắt đầu ETL P5.")
    return True

### 3. Kiểm tra dữ liệu dimension có thay đổi sau khi loadDatawarehouse?
def check_dwh_table_update(dwh_table_name: str, batch_id: str) -> bool:
    conn = connect_db(CONFIG["CONTROL_DB"])
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute("""
                SELECT records_inserted, records_updated
                FROM etl_log
                WHERE process_id=4 AND target_table=%s AND batch_id=%s AND status='success'
                ORDER BY end_time DESC LIMIT 1
            """, (dwh_table_name, batch_id))
            log_entry = cur.fetchone()
            if not log_entry:
                return False
            return (log_entry.get('records_inserted',0) + log_entry.get('records_updated',0)) > 0
    finally:
        conn.close()

# =========================
# 🔹 EXTRACT DIMENSION
# =========================
### 7. Transform cột price.
def clean_price(price_str):
    if not price_str:
        return 0.0
    # Loại bỏ ký tự không phải số
    clean = price_str.replace('đ','').replace('.','').replace(',','').strip()
    try:
        return float(clean)
    except ValueError:
        return 0.0
### 5. Extract dữ liệu từ DWH
def extract_dimension_from_dwh(dim_name: str, batch_id: str):
    conn = connect_db(CONFIG["DWH_DB_NAME"])
    last_success_time_p4 = get_last_dwh_success_time()
    columns = DIMENSION_MAPPINGS.get(dim_name)
    if not columns:
        raise KeyError(f"Không có mapping cho {dim_name}")
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            if dim_name == 'dim_product':
                if not last_success_time_p4:
                    return pd.DataFrame()
                time_str = last_success_time_p4.strftime("%Y-%m-%d %H:%M:%S")
                sql = f"""
                    SELECT P.product_id, P.ten_san_pham, P.category, P.sale_price_vnd,
                           P.Chip, P.RAM, P.ROM, P.`Hệ điều hành`, P.`Công nghệ màn hình`,
                           P.Pin, P.ngay_crawl, P.expiry_date, P.date_key,
                           COALESCE(B.brand_key,0) AS brand_key
                    FROM dim_product P
                    LEFT JOIN dim_brand B ON P.brand = B.brand_name
                    WHERE P.ngay_crawl >= '{time_str}' OR P.expiry_date >= '{time_str}'
                """
                print(f"📄 SQL dim_product: {sql}")
                cur.execute(sql)
            else:
                cols_str = ", ".join(columns.keys())
                sql = f"SELECT {cols_str} FROM {dim_name}"
                print(f"📄 SQL {dim_name}: {sql}")
                cur.execute(sql)
            rows = cur.fetchall()
            df = pd.DataFrame(rows)
            return df.rename(columns=columns)
    finally:
        conn.close()

# =========================
# 🔹8. Load dữ liệu vào Datamart.
# =========================
def load_dimension_to_datamart(df: pd.DataFrame, table_name: str, pk_cols: list):
    inserted, updated = 0, 0
    if df.empty:
        return inserted, updated
    conn = connect_db(CONFIG["DATA_MART_DB"])
    try:
        with conn.cursor() as cur:
            for _, row in df.iterrows():
                where_clause = " AND ".join([f"`{col}`=%s" for col in pk_cols])
                pk_values = [row[col] for col in pk_cols]
                cur.execute(f"SELECT 1 FROM {table_name} WHERE {where_clause}", pk_values)
                if cur.fetchone():
                    update_cols = [c for c in df.columns if c not in pk_cols]
                    update_sql = f"UPDATE {table_name} SET {', '.join([f'`{c}`=%s' for c in update_cols])} WHERE {where_clause}"
                    cur.execute(update_sql, [row[c] for c in update_cols] + pk_values)
                    updated +=1
                else:
                    insert_sql = f"INSERT INTO {table_name} ({','.join(df.columns)}) VALUES ({','.join(['%s']*len(df.columns))})"
                    cur.execute(insert_sql, list(row.values))
                    inserted +=1
            conn.commit()
    finally:
        conn.close()
    return inserted, updated

# =========================
# 🔹 RUN ETL
# =========================
def run_etl(manual_batch_id=None):
    if manual_batch_id:
        print("⚠️ Bỏ qua kiểm tra điều kiện P4 > P5 vì đang chạy batch_id thủ công.")
    else:
        if not should_run_process_5():
            print("❌ Điều kiện ETL không thỏa mãn. Dừng P5.")
            return

    last_dwh_log = get_last_success_log(4)
    if not last_dwh_log:
        print("⚠️ Không tìm thấy log DWH thành công. Dừng ETL.")
        return

    if manual_batch_id:
        batch_id = manual_batch_id
        print(f"📌 Sử dụng batch_id thủ công: {batch_id}")
    else:
    # Nếu không truyền -> lấy batch_id mới nhất
        last_dwh_log = get_last_success_log(4)
        if not last_dwh_log:
            print("⚠️ Không tìm thấy log DWH thành công. Dừng ETL.")
            return
        batch_id = last_dwh_log['batch_id']
        print(f"📌 Batch ID tự động: {batch_id}")

    ### 2. Duyệt danh sách Dimension: dim_brand, date_dims, dim_product
    REQUIRED_ORDER = [
        ('dim_brand','dim_brand',['brand_key']),
        ('date_dims','date_dims',['date_sk']),
        ('dim_product','dim_product',['product_id'])
    ]

    for dim_name, table_name, pk_cols in REQUIRED_ORDER:
        print(f"\n▶️ Xử lý Dimension: {dim_name}")
        if not check_dwh_table_update(dim_name, batch_id):
            print(f"⚠️ Bỏ qua {dim_name} do P4 không có thay đổi.")
            continue

        etl_id = log_start(batch_id, dim_name, table_name)
        inserted, updated = 0,0
        try:
            df = extract_dimension_from_dwh(dim_name, batch_id)

            ### 6. Kiểm tra có phải bảng dim_product.
            if not df.empty and dim_name == 'dim_product':
                df['price'] = df['price'].apply(clean_price)
            if not df.empty:
                inserted, updated = load_dimension_to_datamart(df, table_name, pk_cols)
                print(f"📊 {dim_name}: inserted={inserted}, updated={updated}")
            if inserted+updated>0:
                log_end(etl_id,'success',inserted,updated)
            else:
                log_delete_started(etl_id)
        except Exception as e:
            print(f"❌ Lỗi ETL {dim_name}: {e}")
            log_end(etl_id,'failed',0,0,str(e))

    print("\n🎉 ETL Data Mart hoàn thành.")

# =========================
# ENTRY POINT
# =========================
if __name__ == "__main__":
    print("🚀 BẮT ĐẦU QUY TRÌNH ETL DATA MART")

    batch_id_arg = None

    if len(sys.argv) > 1:
        batch_id_arg = sys.argv[1]
        print(f"📌 Batch ID thủ công: {batch_id_arg}")
    # else:
    #     batch = input("👉 Nhập batch_id (Enter để chạy batch mới nhất): ").strip()
    #     if batch:
    #         batch_id_arg = batch

    try:
        run_etl(batch_id_arg)
    except Exception as e:
        print(f"\n🛑 Lỗi toàn cục ETL: {e}")
        sys.exit(1)
