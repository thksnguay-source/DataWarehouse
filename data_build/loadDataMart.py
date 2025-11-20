#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pymysql
import pandas as pd
from datetime import datetime
import traceback
import sys
import os
from typing import Optional, Dict, Any

# =========================
# ⚙️ CẤU HÌNH
# =========================
# LƯU Ý: Thay đổi giá trị thực tế của bạn tại đây
CONFIG: Dict[str, Any] = {
    "DB_USER": "root",
    "DB_PASS": "",  # Điền mật khẩu MySQL nếu có
    "DB_HOST": "localhost",
    "DB_PORT": 3306,

    # DB Names
    "DWH_DB_NAME": "datawh",
    "DATA_MART_DB": "data_mart_prod",
    "CONTROL_DB": "control",

    # Bảng nguồn
    "SOURCE_TABLE": "dim_product"  # Tên bảng nguồn trong DWH
}


# =========================
# Kết nối DB
# =========================
def connect_db(db_name: Optional[str] = None):
    """Tạo kết nối tới cơ sở dữ liệu MySQL."""
    # Kết nối không chỉ định DB nếu db_name là None (Dùng để tạo DB nếu cần)
    db_to_connect = db_name if db_name else None

    return pymysql.connect(
        host=CONFIG["DB_HOST"],
        user=CONFIG["DB_USER"],
        password=CONFIG["DB_PASS"],
        database=db_to_connect,
        port=CONFIG["DB_PORT"],
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )


def setup_databases():
    """Đảm bảo các cơ sở dữ liệu (Schema) cần thiết đã tồn tại."""
    db_list = [CONFIG["CONTROL_DB"], CONFIG["DWH_DB_NAME"], CONFIG["DATA_MART_DB"]]
    print("--- 🛠️ Setup Databases ---")
    conn_no_db = connect_db(None)
    try:
        with conn_no_db.cursor() as cur:
            for db_name in db_list:
                cur.execute(f"CREATE DATABASE IF NOT EXISTS {db_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
                print(f"✅ Đã kiểm tra/tạo database: {db_name}")
        conn_no_db.commit()
    finally:
        conn_no_db.close()


# =========================
# Setup/Log
# =========================
def setup_log_table():
    """Tạo bảng etl_log trong Control DB nếu chưa tồn tại."""
    conn = connect_db(CONFIG["CONTROL_DB"])
    ddl = """
          CREATE TABLE IF NOT EXISTS etl_log \
          ( \
              log_id \
              INT \
              AUTO_INCREMENT \
              PRIMARY \
              KEY, \
              batch_id \
              VARCHAR \
          ( \
              50 \
          ) NOT NULL,
              process_name VARCHAR \
          ( \
              100 \
          ) NOT NULL,
              status ENUM \
          ( \
              'started', \
              'success', \
              'failed' \
          ) NOT NULL,
              start_time DATETIME NOT NULL,
              end_time DATETIME DEFAULT NULL,
              records_extracted INT DEFAULT 0,
              records_inserted INT DEFAULT 0,
              records_updated INT DEFAULT 0,
              error_message TEXT DEFAULT NULL,
              created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
              KEY idx_batch \
          ( \
              batch_id \
          ),
              KEY idx_status \
          ( \
              status \
          )
              ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4; \
          """
    try:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
        print("✅ Đã kiểm tra/tạo bảng etl_log.")
    finally:
        conn.close()


def upsert_log(batch_id, process_name, status, start_time,
               records_extracted=None, records_inserted=None, records_updated=None,
               error_message=None, log_id=None) -> int:
    """Tạo mới hoặc cập nhật bản ghi log ETL."""
    conn = connect_db(CONFIG["CONTROL_DB"])
    end_time = datetime.now() if status in ('success', 'failed') else None

    try:
        with conn.cursor() as cur:
            # 1. Cập nhật bản ghi theo log_id CỤ THỂ
            if log_id:
                end_time_param = end_time if status != 'started' else None
                cur.execute("""
                            UPDATE etl_log
                            SET status=%s,
                                end_time=%s,
                                records_extracted=COALESCE(%s, records_extracted),
                                records_inserted=COALESCE(%s, records_inserted),
                                records_updated=COALESCE(%s, records_updated),
                                error_message=%s
                            WHERE log_id = %s
                            """, (
                                status, end_time_param,
                                records_extracted, records_inserted, records_updated,
                                error_message,
                                log_id
                            ))
                conn.commit()
                return log_id

            # 2. TẠO MỚI bản ghi
            cur.execute("""
                        INSERT INTO etl_log (batch_id, process_name, status, start_time, end_time,
                                             records_extracted, records_inserted, records_updated, error_message)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            batch_id, process_name, status, start_time, end_time,
                            records_extracted, records_inserted, records_updated, error_message
                        ))
            conn.commit()
            return cur.lastrowid  # Trả về log_id mới tạo
    finally:
        conn.close()


def insert_df_to_mysql(df: pd.DataFrame, table_name: str, db_name: str) -> int:
    """Hàm hỗ trợ chèn DataFrame vào bảng MySQL."""
    if df.empty:
        return 0

    conn = connect_db(db_name)
    try:
        cols = ", ".join(f"`{col}`" for col in df.columns)  # Bọc tên cột trong dấu `
        placeholders = ", ".join(["%s"] * len(df.columns))
        sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
        # Chuyển đổi DataFrame thành list of tuples
        # Thao tác này xử lý các kiểu dữ liệu Pandas (như Int64) sang kiểu Python chuẩn
        rows = [tuple(row) for row in df.itertuples(index=False, name=None)]

        with conn.cursor() as cur_insert:
            # Sử dụng executemany để chèn hiệu quả
            cur_insert.executemany(sql, rows)
        conn.commit()
        return len(rows)
    finally:
        conn.close()


# =========================
# ETL chính
# =========================
def run_etl():
    """Thực thi toàn bộ quy trình ETL (Extract, Transform, Load) từ DWH sang Data Mart."""

    # 0. Setup ban đầu
    setup_databases()
    setup_log_table()

    current_time = datetime.now()
    batch_id = current_time.strftime("%Y%m%d%H%M%S")

    # Khởi tạo log ID cho LOAD_DATA_MART
    log_id_load_dm: Optional[int] = None
    log_start_dm = datetime.now()  # Thời điểm bắt đầu của toàn bộ quá trình ETL

    # Ghi log STARTED
    try:
        log_id_load_dm = upsert_log(batch_id, "LOAD_DATA_MART", "started", log_start_dm,
                                    error_message="Bắt đầu tiến trình ETL.")
        print(f"✅ Ghi log STARTED cho LOAD_DATA_MART: log_id={log_id_load_dm}")
    except Exception as log_e:
        print(f"❌ KHÔNG THỂ GHI LOG STARTED: {log_e}")
        traceback.print_exc()
        sys.exit(1)

    # Khối try-except lớn bọc toàn bộ quá trình ETL để đảm bảo log FAIL khi có lỗi
    try:
        # ---------- 1. Kiểm tra log & Extract từ DWH (CHECK_DWH_NEW_DATA) ----------
        print("\n--- 1. Kiểm tra dữ liệu mới & Extract từ DWH ---")

        # 1.1 Lấy thời điểm log thành công gần nhất
        conn_control = connect_db(CONFIG["CONTROL_DB"])
        last_dm_time: Optional[datetime] = None
        last_dwh_time: Optional[datetime] = None

        try:
            with conn_control.cursor() as cur:
                # Lấy last successful LOAD_DATA_MART
                cur.execute("""
                            SELECT MAX(end_time) as last_dm_time
                            FROM etl_log
                            WHERE process_name = 'LOAD_DATA_MART'
                              AND status = 'success'
                            """)
                last_dm_time = cur.fetchone()["last_dm_time"]

                # Lấy last successful LOAD_DATAWH
                cur.execute("""
                            SELECT MAX(end_time) as last_dwh_time
                            FROM etl_log
                            WHERE process_name = 'LOAD_DATAWH'
                              AND status = 'success'
                            """)
                last_dwh_time = cur.fetchone()["last_dwh_time"]
        finally:
            conn_control.close()

        # 1.2 Quyết định có đọc DWH hay không
        if not last_dwh_time:
            raise Exception(
                "CHECK_DWH_NEW_DATA FAILED: DWH chưa có dữ liệu được load thành công (LOAD_DATAWH log not found).")

        if last_dm_time and last_dwh_time <= last_dm_time:
            print("❌ Không có dữ liệu mới từ DWH. Dừng ETL.")
            upsert_log(batch_id, "LOAD_DATA_MART", "success", log_start_dm,
                       records_extracted=0, records_inserted=0, log_id=log_id_load_dm,
                       error_message="Không có dữ liệu mới từ DWH so với Data Mart. ETL kết thúc sớm.")
            return

        # 1.3 Truy vấn DWH
        conn_dwh = connect_db(CONFIG["DWH_DB_NAME"])
        df_new = pd.DataFrame()
        try:
            with conn_dwh.cursor() as cur:
                if last_dm_time:
                    # Lấy dữ liệu mới/cập nhật kể từ lần load Data Mart gần nhất
                    sql = f"""
                        SELECT *
                        FROM `{CONFIG['SOURCE_TABLE']}`
                        WHERE `Ngày_crawl` > %s
                    """
                    cur.execute(sql, (last_dm_time,))
                else:
                    # Lấy toàn bộ dữ liệu (lần chạy đầu tiên)
                    sql = f"SELECT * FROM `{CONFIG['SOURCE_TABLE']}`"
                    cur.execute(sql)
                rows = cur.fetchall()
            df_new = pd.DataFrame(rows)
        finally:
            conn_dwh.close()

        if df_new.empty:
            print("❌ Không có dữ liệu mới từ DWH. Kết thúc ETL.")
            upsert_log(batch_id, "LOAD_DATA_MART", "success", log_start_dm,
                       records_extracted=0, records_inserted=0, log_id=log_id_load_dm,
                       error_message="Không có dữ liệu mới hoặc cập nhật từ DWH. ETL kết thúc sớm.")
            return

        print(f"✅ Đã đọc {len(df_new)} bản ghi mới từ DWH.")

        # ---------- 2. Tiền xử lý (PREPROCESS_DWH) ----------
        print("\n--- 2. Tiền xử lý dữ liệu (Transform) ---")
        df = df_new.copy()

        # Rename cột
        df.rename(columns={
            "Tên sản phẩm": "product_name",
            "Brand": "brand",
            "Category": "category",
            "sale_price_vnd": "price",
            "Ngày_crawl": "date_collected",
            "Chip": "cpu",
            "Ram": "ram",
            "Rom": "storage",
            "HDH": "os",
            "Công nghệ màn hình": "screen_size",
            "Pin": "battery"
        }, inplace=True)

        # Chuyển đổi kiểu dữ liệu
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        df["date_collected"] = pd.to_datetime(df["date_collected"], errors="coerce")

        # Xóa các bản ghi thiếu dữ liệu bắt buộc
        required_columns = ["product_name", "brand", "category", "price"]
        present_required = [c for c in required_columns if c in df.columns]
        df = df.dropna(subset=present_required)

        if df.empty:
            raise Exception("PREPROCESS_DWH FAILED: Dữ liệu sau khi tiền xử lý (dropna) không còn bản ghi nào.")

        print(f"✅ Sau tiền xử lý còn {len(df)} bản ghi hợp lệ.")

        # ---------- 3. Tạo dimension (BUILD_DIMS) ----------
        print("\n--- 3. Tạo Dimension Tables ---")

        # 3.1 dim_brand
        dim_brand = df[["brand"]].drop_duplicates().reset_index(drop=True).copy()
        # Tạo khóa giả (Surrogate Key)
        dim_brand["brand_key"] = dim_brand.index + 1
        dim_brand.rename(columns={"brand": "brand_name"}, inplace=True)

        # 3.2 dim_date
        dim_date = pd.DataFrame(columns=["date_key", "date", "year", "month", "day"])
        if "date_collected" in df.columns:
            # Tạo date_key ở định dạng YYYYMMDD
            df["date_key"] = df["date_collected"].dt.strftime("%Y%m%d").astype("Int64")
            dim_date = df[["date_key", "date_collected"]].drop_duplicates().copy()
            dim_date["year"] = dim_date["date_collected"].dt.year
            dim_date["month"] = dim_date["date_collected"].dt.month
            dim_date["day"] = dim_date["date_collected"].dt.day
            dim_date.rename(columns={"date_collected": "date"}, inplace=True)
            dim_date = dim_date.dropna(subset=["date_key"])

        # 3.3 dim_product (Fact/Dimension lai - Lưu trữ thông tin sản phẩm)
        # Merge brand_key vào DataFrame chính
        df = df.merge(dim_brand[["brand_name", "brand_key"]], left_on="brand", right_on="brand_name", how="left")

        # Chọn các cột cần thiết cho dim_product và loại bỏ trùng lặp dựa trên khóa
        dim_product = df[[
            "product_name", "brand_key", "category", "price",
            "cpu", "ram", "storage", "os", "screen_size", "battery", "date_collected", "date_key"
        ]].drop_duplicates(subset=["product_name", "brand_key"]).copy()

        dim_product.rename(columns={"date_collected": "date_collected_raw"}, inplace=True)

        print(f"✅ Tạo dim_brand ({len(dim_brand)}), dim_date ({len(dim_date)}), dim_product ({len(dim_product)})")

        # ---------- 4. Load Data Mart (LOAD_DATA_MART) ----------
        print("\n--- 4. Load Data Mart ---")

        conn_dm = connect_db(CONFIG["DATA_MART_DB"])
        inserted_product = 0
        inserted_brand = 0
        inserted_date = 0

        try:
            with conn_dm.cursor() as cur:
                # 4.1 Tạo bảng (Tái tạo toàn bộ - Phù hợp cho Data Mart nhỏ hoặc khi cần đảm bảo tính toàn vẹn)
                print("   - Tái tạo bảng Data Mart...")
                cur.execute("DROP TABLE IF EXISTS dim_product")
                cur.execute("DROP TABLE IF EXISTS dim_date")
                cur.execute(
                    "DROP TABLE IF EXISTS dim_brand")  # Drop theo thứ tự ngược lại để tránh lỗi khóa ngoại nếu có

                # dim_brand
                cur.execute(f"""
                    CREATE TABLE dim_brand (
                        brand_key INT PRIMARY KEY,
                        brand_name VARCHAR(255)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """)
                # dim_date
                cur.execute(f"""
                    CREATE TABLE dim_date (
                        date_key INT PRIMARY KEY,
                        date DATE,
                        year INT,
                        month INT,
                        day INT
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """)
                # dim_product (Thêm FOREIGN KEY là một ý tưởng hay, nhưng tạm bỏ để giữ logic gốc)
                cur.execute(f"""
                    CREATE TABLE dim_product (
                        product_name VARCHAR(255),
                        brand_key INT,
                        category VARCHAR(255),
                        price FLOAT,
                        cpu VARCHAR(255),
                        ram VARCHAR(50),
                        storage VARCHAR(50),
                        os VARCHAR(100),
                        screen_size VARCHAR(50),
                        battery VARCHAR(50),
                        date_collected_raw DATETIME,
                        date_key INT,
                        PRIMARY KEY (product_name, brand_key)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """)
            conn_dm.commit()

            # 4.2 Chèn dữ liệu
            print("   - Chèn dữ liệu vào bảng...")
            inserted_brand = insert_df_to_mysql(dim_brand, "dim_brand", CONFIG["DATA_MART_DB"])
            inserted_date = insert_df_to_mysql(dim_date, "dim_date", CONFIG["DATA_MART_DB"])
            inserted_product = insert_df_to_mysql(dim_product, "dim_product", CONFIG["DATA_MART_DB"])

        finally:
            conn_dm.close()

        # Ghi log SUCCESS cho LOAD_DATA_MART
        upsert_log(batch_id, "LOAD_DATA_MART", "success", log_start_dm,
                   records_extracted=len(df_new), records_inserted=inserted_product,
                   log_id=log_id_load_dm,
                   error_message=f"Tải Data Mart thành công. Prod: {inserted_product}, Brand: {inserted_brand}, Date: {inserted_date} bản ghi.")

        print(f"✅ Load Data Mart hoàn tất. Tổng sản phẩm chèn: {inserted_product} bản ghi.")

    except Exception as e:
        tb = traceback.format_exc()
        error_message_summary = f"ETL FAILED at: {e.__class__.__name__}: {str(e).splitlines()[0]}"

        # Cập nhật log FAIL cho LOAD_DATA_MART
        if log_id_load_dm is not None:
            upsert_log(batch_id, "LOAD_DATA_MART", "failed", log_start_dm,
                       log_id=log_id_load_dm,
                       error_message=f"{error_message_summary}\n\nTraceback:\n{tb}")

        print(f"\n\n❌ LỖI NGHIÊM TRỌNG: {error_message_summary}")
        print("Log LOAD_DATA_MART đã được cập nhật FAIL.")

        # Vẫn raise exception để entry point (if __name__ == "__main__":) bắt
        raise

    # =========================


# Entry point
# =========================
if __name__ == "__main__":
    print("====================================")
    print("🚀 BẮT ĐẦU QUY TRÌNH ETL")
    print("====================================")

    try:
        run_etl()
    except Exception:
        print("\n====================================")
        print("🛑 ETL kết thúc với lỗi. Xem log trong database để biết chi tiết.")
        print("====================================")
        sys.exit(1)
    else:
        print("\n====================================")
        print("🎉 ETL kết thúc thành công.")
        print("====================================")
        sys.exit(0)