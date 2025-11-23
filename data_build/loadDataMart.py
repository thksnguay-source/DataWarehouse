
from config.datamart_cofig import CONFIG, connect_db
import pandas as pd
from datetime import datetime
import traceback
import sys

# =========================
# ⚙️ CẤU HÌNH CHO QUY TRÌNH ETL
# =========================
PROCESS_ID = 5
SOURCE_DB_NAME = CONFIG["DWH_DB_NAME"]
TARGET_DB_NAME = CONFIG["DATA_MART_DB"]
SOURCE_TABLE = CONFIG['SOURCE_TABLE']  # 'dim_product' trong datawh
TARGET_TABLE = "dim_product"  # 'dim_product' trong data_mart_prod

FULL_SOURCE_TABLE = f"{SOURCE_DB_NAME}.{SOURCE_TABLE}"
FULL_TARGET_TABLE = f"{TARGET_DB_NAME}.{TARGET_TABLE}"

# =========================
# Ghi log ETL (ĐÃ SỬA: Bỏ cột error_message)
# =========================
def upsert_log(batch_id, process_id, status, start_time,
               source_table=None, target_table=None,
               records_inserted=None, records_updated=None,
               records_skipped=None, log_id=None) -> int:
    conn = connect_db(CONFIG["CONTROL_DB"])
    end_time = datetime.now() if status in ('success', 'failed') else None
    try:
        with conn.cursor() as cur:
            if log_id:
                # CẬP NHẬT LOG ĐÃ CÓ
                cur.execute("""
                            UPDATE etl_log
                            SET status=%s,
                                end_time=%s,
                                source_table=COALESCE(%s, source_table),
                                target_table=COALESCE(%s, target_table),
                                records_inserted=COALESCE(%s, records_inserted),
                                records_updated=COALESCE(%s, records_updated),
                                records_skipped=COALESCE(%s, records_skipped)
                            WHERE etl_id = %s
                            """, (status, end_time, source_table, target_table, records_inserted,
                                  records_updated, records_skipped, log_id))
                conn.commit()
                return log_id
            else:
                # CHÈN LOG MỚI
                cur.execute("""
                            INSERT INTO etl_log (batch_id, process_id, status, start_time, end_time,
                                                 source_table, target_table, records_inserted,
                                                 records_updated, records_skipped)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """, (batch_id, process_id, status, start_time, end_time,
                                  source_table, target_table, records_inserted, records_updated,
                                  records_skipped))
                conn.commit()
                return cur.lastrowid
    finally:
        conn.close()


# =========================
# Chèn DataFrame vào MySQL
# =========================
def insert_df_to_mysql(df: pd.DataFrame, table_name: str, db_name: str) -> int:
    if df.empty:
        return 0
    conn = connect_db(db_name)
    try:
        cols = ", ".join(f"`{c}`" for c in df.columns)
        placeholders = ", ".join(["%s"] * len(df.columns))
        sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
        rows = [tuple(r) for r in df.itertuples(index=False, name=None)]
        with conn.cursor() as cur:
            cur.executemany(sql, rows)
        conn.commit()
        return len(rows)
    finally:
        conn.close()


# =========================
# Kiểm tra DWH có dữ liệu mới không
# =========================
def has_new_data_in_dwh_log_only() -> bool:
    conn = connect_db(CONFIG["CONTROL_DB"])
    try:
        with conn.cursor() as cur:
            # Lấy lần chạy thành công gần nhất của quy trình Load Data Warehouse (process_id=4)
            cur.execute("""
                        SELECT records_inserted, records_updated
                        FROM etl_log
                        WHERE process_id = 4
                          AND status = 'success'
                        ORDER BY end_time DESC LIMIT 1
                        """)
            last_run = cur.fetchone()

            # Nếu chưa từng chạy -> cần ETL
            if not last_run:
                return False

            # Lấy giá trị từ dict cursor
            records_inserted = last_run.get('records_inserted') or 0
            records_updated = last_run.get('records_updated') or 0


            # Nếu insert/update > 0 → có dữ liệu mới
            return (records_inserted + records_updated) > 0
    finally:
        conn.close()


# =========================
# ETL chính
# =========================
def run_etl():
    current_time = datetime.now()
    batch_id = current_time.strftime("%Y%m%d%H%M%S")
    records_extracted = 0
    skipped_records = 0
    df = pd.DataFrame()  # Khởi tạo df để tránh lỗi nếu exception xảy ra sớm

    # START log
    log_id = upsert_log(batch_id, PROCESS_ID, "started", current_time,
                        source_table=None,
                        target_table=None)
    print(f"✅ Ghi log STARTED: log_id={log_id}")

    try:
        # Kiểm tra dữ liệu mới từ LoadDataWarehouse
        if not has_new_data_in_dwh_log_only():
            print("❌ Không có dữ liệu mới từ LoadDataWarehouse. ETL Data Mart bỏ qua.")
            upsert_log(
                batch_id, PROCESS_ID, "success", current_time,
                source_table=None, target_table=None,
                records_inserted=0, log_id=log_id
            )
            return

        print("✅ Có dữ liệu mới từ LoadDataWarehouse. Bắt đầu chạy ETL Data Mart...")

        # ================= 1. Extract dữ liệu còn hiệu lực từ DWH =================
        conn_dw = connect_db(CONFIG["DWH_DB_NAME"])
        try:
            with conn_dw.cursor() as cur:
                # SỬA: Dùng dấu nháy ngược cho các cột tiếng Việt để đảm bảo tên cột chính xác
                cur.execute(f"""
                    SELECT product_id, `ten_san_pham`, brand, category, `sale_price_vnd` AS price,
                           Chip AS cpu, RAM AS ram, ROM AS storage, `Hệ điều hành` AS os,
                           `Công nghệ màn hình` AS screen_size, Pin AS battery, `ngay_crawl` AS date_collected
                    FROM `{CONFIG['SOURCE_TABLE']}`
                    WHERE expiry_date IS NULL
                """)
                rows = cur.fetchall()
            df = pd.DataFrame(rows)
        finally:
            conn_dw.close()

        records_extracted = len(df)

        if df.empty:
            print("❌ Không có dữ liệu nào từ DWH.")
            upsert_log(batch_id, PROCESS_ID, "success", current_time,
                       source_table=FULL_SOURCE_TABLE, target_table=FULL_TARGET_TABLE,
                       records_inserted=0, log_id=log_id)
            return

        print(f"✅ Đã đọc {records_extracted} bản ghi từ DWH.")

        # ================= 2. Transform / Preprocess =================
        df.rename(columns={
            "ten_san_pham": "product_name"
        }, inplace=True)

        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        df["date_collected"] = pd.to_datetime(df["date_collected"], errors="coerce")

        # Xác định các cột không được phép là NULL
        required_cols = ["product_id", "product_name", "brand", "category", "price", "date_collected"]
        df_cleaned = df.dropna(subset=[c for c in required_cols if c in df.columns])

        skipped_records = records_extracted - len(df_cleaned)
        df = df_cleaned

        if df.empty:
            # Ghi log fail nếu không còn bản ghi nào sau tiền xử lý
            raise Exception("Dữ liệu sau tiền xử lý không còn bản ghi nào.")

        print(f"✅ Sau tiền xử lý còn {len(df)} bản ghi hợp lệ. Bỏ qua: {skipped_records}")

        # ================= 3. Build Dimensions =================
        # ... (Phần xây dựng Dim giữ nguyên) ...
        dim_brand = df[["brand"]].drop_duplicates().reset_index(drop=True).copy()
        dim_brand["brand_key"] = dim_brand.index + 1
        dim_brand.rename(columns={"brand": "brand_name"}, inplace=True)

        df = df.merge(dim_brand[["brand_name", "brand_key"]], left_on="brand", right_on="brand_name", how="left")

        df["date_key"] = df["date_collected"].dt.strftime("%Y%m%d").astype("Int64")
        dim_date = df[["date_key", "date_collected"]].drop_duplicates().copy()
        dim_date["year"] = dim_date["date_collected"].dt.year
        dim_date["month"] = dim_date["date_collected"].dt.month
        dim_date["day"] = dim_date["date_collected"].dt.day
        dim_date.rename(columns={"date_collected": "date"}, inplace=True)

        # Xây dựng dim_product (Fact/Dimension table chính)
        dim_product = df[[
            "product_id", "product_name", "brand_key", "category", "price",
            "cpu", "ram", "storage", "os", "screen_size", "battery",
            "date_collected", "date_key"
        ]].drop_duplicates(subset=["product_id"]).copy()
        dim_product.rename(columns={"date_collected": "date_collected"}, inplace=True)

        print(f"✅ Tạo dim_brand ({len(dim_brand)}), dim_date ({len(dim_date)}), dim_product ({len(dim_product)})")

        # ================= 4. Load Data Mart (Incremental) =================
        conn_dm = connect_db(CONFIG["DATA_MART_DB"])
        inserted_product = updated_product = 0
        try:
            with conn_dm.cursor() as cur:
                # Chèn dim_brand
                for _, row in dim_brand.iterrows():
                    cur.execute("SELECT 1 FROM dim_brand WHERE brand_key=%s", (row["brand_key"],))
                    if cur.fetchone() is None:
                        cur.execute("INSERT INTO dim_brand (brand_key, brand_name) VALUES (%s, %s)",
                                    (row["brand_key"], row["brand_name"]))

                # Chèn dim_date
                for _, row in dim_date.iterrows():
                    cur.execute("SELECT 1 FROM dim_date WHERE date_key=%s", (row["date_key"],))
                    if cur.fetchone() is None:
                        cur.execute(
                            "INSERT INTO dim_date (date_key, date, year, month, day) VALUES (%s, %s, %s, %s, %s)",
                            (row["date_key"], row["date"], row["year"], row["month"], row["day"]))

                # Chèn/update dim_product (SCD Type 1 - Ghi đè)
                for _, row in dim_product.iterrows():
                    data_tuple = tuple(row[["product_id", "product_name", "brand_key", "category", "price",
                                            "cpu", "ram", "storage", "os", "screen_size", "battery",
                                            "date_collected", "date_key"]])

                    cur.execute("SELECT 1 FROM dim_product WHERE product_id=%s", (row["product_id"],))
                    if cur.fetchone() is None:
                        # Insert mới
                        cur.execute("""
                                    INSERT INTO dim_product (product_id, product_name, brand_key, category, price,
                                                             cpu, ram, storage, os, screen_size, battery,
                                                             date_collected, date_key)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                    """, data_tuple)
                        inserted_product += 1
                    else:
                        # Update
                        cur.execute("""
                                    UPDATE dim_product
                                    SET product_name=%s,
                                        brand_key=%s,
                                        category=%s,
                                        price=%s,
                                        cpu=%s,
                                        ram=%s,
                                        storage=%s,
                                        os=%s,
                                        screen_size=%s,
                                        battery=%s,
                                        date_collected=%s,
                                        date_key=%s
                                    WHERE product_id = %s
                                    """, (
                                        row["product_name"], row["brand_key"], row["category"], row["price"],
                                        row["cpu"], row["ram"], row["storage"], row["os"], row["screen_size"],
                                        row["battery"], row["date_collected"], row["date_key"], row["product_id"]
                                    ))
                        updated_product += 1
            conn_dm.commit()
        finally:
            conn_dm.close()

        # Success log
        upsert_log(batch_id, PROCESS_ID, "success", current_time,
                   source_table=FULL_SOURCE_TABLE, target_table=FULL_TARGET_TABLE,
                   records_inserted=inserted_product,
                   records_updated=updated_product,
                   records_skipped=skipped_records,
                   log_id=log_id)

        print(
            f"🎉 ETL Data Mart hoàn tất. Inserted: {inserted_product}, Updated: {updated_product}, Skipped: {skipped_records}")

    except Exception as e:
        # Ghi log lỗi khi exception xảy ra
        print("❌ LỖI NGHIÊM TRỌNG TRONG QUY TRÌNH ETL")
        # Ghi lại lỗi cuối cùng vào log để biết nguyên nhân
        upsert_log(batch_id, PROCESS_ID, "failed", current_time,
                   source_table=FULL_SOURCE_TABLE, target_table=FULL_TARGET_TABLE,
                   records_inserted=0, records_updated=0,
                   records_skipped=skipped_records,
                   log_id=log_id)

        # In traceback ra màn hình để debug
        traceback.print_exc()
        raise


# =========================
# Entry point
# =========================
if __name__ == "__main__":
    print("🚀 BẮT ĐẦU QUY TRÌNH ETL DATA MART")
    try:
        run_etl()
    except Exception:
        print("🛑 ETL kết thúc với lỗi. Vui lòng kiểm tra lại kết nối DB và Traceback ở trên.")
        sys.exit(1)
    else:
        print("🎉 ETL kết thúc thành công.")
        sys.exit(0)