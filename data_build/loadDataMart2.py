
import pymysql
import pandas as pd
from datetime import datetime
import traceback
import sys

# =========================
# ⚙️ CẤU HÌNH
# =========================
CONFIG = {
    "DB_USER": "root",
    "DB_PASS": "",
    "DB_HOST": "localhost",
    "DB_PORT": 3306,

    # DB
    "DWH_DB_NAME": "datawh",
    "DATA_MART_DB": "data_mart_prod",
    "CONTROL_DB": "control",

    # Bảng nguồn
    "SOURCE_TABLE": "dim_product"
}


# =========================
# Kết nối DB
# =========================
def connect_db(db_name):
    return pymysql.connect(
        host=CONFIG["DB_HOST"],
        user=CONFIG["DB_USER"],
        password=CONFIG["DB_PASS"],
        database=db_name,
        port=CONFIG["DB_PORT"],
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )


# =========================
# Setup bảng log trong Control DB (Bỏ cột updated_at)
# =========================
def setup_log_table():
    conn = connect_db(CONFIG["CONTROL_DB"])
    ddl = """
        CREATE TABLE IF NOT EXISTS etl_log (
            log_id INT AUTO_INCREMENT PRIMARY KEY,
            batch_id VARCHAR(50) NOT NULL,
            process_name VARCHAR(100) NOT NULL,
            status ENUM('started','success','failed') NOT NULL,
            start_time DATETIME NOT NULL,
            end_time DATETIME DEFAULT NULL,
            records_extracted INT DEFAULT 0,
            records_inserted INT DEFAULT 0,
            records_updated INT DEFAULT 0,
            error_message TEXT DEFAULT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            KEY idx_batch (batch_id),
            KEY idx_status (status)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    try:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
    finally:
        conn.close()


# =========================
# Upsert log (Đã tối giản logic log_id, bỏ updated_at)
# Trả về log_id của bản ghi đã được tạo/cập nhật
# =========================
def upsert_log(batch_id, process_name, status, start_time,
               records_extracted=None, records_inserted=None, records_updated=None,
               error_message=None, log_id=None):
    conn = connect_db(CONFIG["CONTROL_DB"])
    end_time = datetime.now() if status in ('success', 'failed') else None

    try:
        with conn.cursor() as cur:

            # Trường hợp 1: Cập nhật bản ghi theo log_id CỤ THỂ (Dùng cho status success/failed)
            if log_id:
                end_time_param = end_time
                if status == 'started':
                    end_time_param = None

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

            # Trường hợp 2: TẠO MỚI bản ghi (Chỉ dùng khi status='started' và log_id=None)
            cur.execute("""
                        INSERT INTO etl_log (batch_id, process_name, status, start_time, end_time,
                                             records_extracted, records_inserted, records_updated, error_message)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            batch_id, process_name, status, start_time, end_time,
                            records_extracted, records_inserted, records_updated, error_message
                        ))
            conn.commit()
            return cur.lastrowid
    finally:
        conn.close()


# =========================
# ETL chính
# =========================
def run_etl():
    setup_log_table()
    current_time = datetime.now()
    batch_id = current_time.strftime("%Y%m%d%H%M%S")

    # Khởi tạo log ID cho LOAD_DATA_MART (Chỉ log tiến trình này)
    log_id_load_dm = None
    log_start_dm = datetime.now() # Thời điểm bắt đầu của toàn bộ quá trình ETL (tương đương LOAD_DATA_MART started)

    # Ghi log STARTED cho tiến trình LOAD_DATA_MART
    try:
        log_id_load_dm = upsert_log(batch_id, "LOAD_DATA_MART", "started", log_start_dm,
                                    error_message="Bắt đầu tiến trình ETL.")
        print(f"✅ Ghi log STARTED cho LOAD_DATA_MART: log_id={log_id_load_dm}")

    except Exception as log_e:
        print(f"❌ KHÔNG THỂ GHI LOG STARTED: {log_e}")
        traceback.print_exc()
        sys.exit(1)

    # Khối try-except lớn bọc toàn bộ quá trình ETL
    try:
        # ---------- 1. Kiểm tra log ETL & đọc DWH (CHECK_DWH_NEW_DATA) ----------
        print("--- 1. Bắt đầu kiểm tra dữ liệu mới từ DWH ---")
        df_new = pd.DataFrame()

        conn_control = connect_db(CONFIG["CONTROL_DB"])
        try:
            with conn_control.cursor() as cur:
                # Lấy last successful LOAD_DATA_MART
                cur.execute("""
                            SELECT MAX(end_time) as last_dm_time
                            FROM etl_log
                            WHERE process_name = 'LOAD_DATA_MART' AND status = 'success'
                            """)
                dm_log = cur.fetchone()
                last_dm_time = dm_log["last_dm_time"] if dm_log else None

                # Lấy last successful LOAD_DATAWH
                cur.execute("""
                            SELECT MAX(end_time) as last_dwh_time
                            FROM etl_log
                            WHERE process_name = 'LOAD_DATAWH' AND status = 'success'
                            """)
                dwh_log = cur.fetchone()
                last_dwh_time = dwh_log["last_dwh_time"] if dwh_log else None
        finally:
            conn_control.close()

        # Quyết định có đọc DWH hay không
        if not last_dwh_time:
            # Nếu không có DWH thành công, coi là lỗi
            raise Exception("CHECK_DWH_NEW_DATA FAILED: DWH chưa có dữ liệu được load thành công.")

        if last_dm_time and last_dwh_time <= last_dm_time:
            print("❌ Không có dữ liệu mới từ DWH. Dừng ETL.")
            # Ghi log SUCCESS cho LOAD_DATA_MART (vì ETL kết thúc thành công - không có gì để làm)
            upsert_log(batch_id, "LOAD_DATA_MART", "success", log_start_dm,
                       records_extracted=0, records_inserted=0, log_id=log_id_load_dm,
                       error_message="Không có dữ liệu mới từ DWH so với Data Mart. ETL kết thúc sớm.")
            return

        # ---------- Truy vấn DWH ----------
        conn_dwh = connect_db(CONFIG["DWH_DB_NAME"])
        try:
            with conn_dwh.cursor() as cur:
                if last_dm_time:
                    sql = f"""
                        SELECT *
                        FROM {CONFIG['SOURCE_TABLE']}
                        WHERE `Ngày_crawl` > %s
                    """
                    cur.execute(sql, (last_dm_time,))
                else:
                    sql = f"SELECT * FROM {CONFIG['SOURCE_TABLE']}"
                    cur.execute(sql)
                rows = cur.fetchall()
            df_new = pd.DataFrame(rows)
        finally:
            conn_dwh.close()

        if df_new.empty:
            print("❌ Không có dữ liệu mới từ DWH. Kết thúc ETL.")
            # Ghi log SUCCESS cho LOAD_DATA_MART (vì ETL kết thúc thành công - không có gì để làm)
            upsert_log(batch_id, "LOAD_DATA_MART", "success", log_start_dm,
                       records_extracted=0, records_inserted=0, log_id=log_id_load_dm,
                       error_message="Không có dữ liệu mới hoặc cập nhật từ DWH. ETL kết thúc sớm.")
            return

        print(f"✅ Đã đọc {len(df_new)} bản ghi mới từ DWH.")

        # ---------- 2. Tiền xử lý (PREPROCESS_DWH) ----------
        print("--- 2. Bắt đầu Tiền xử lý dữ liệu ---")
        df = df_new.copy()

        # Thử gây ra lỗi tiền xử lý (Ví dụ: truy cập cột không tồn tại)
        # df["non_existent_column"] # Uncomment dòng này để test lỗi

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

        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        df["date_collected"] = pd.to_datetime(df["date_collected"], errors="coerce")

        required_columns = ["product_name", "brand", "category", "price"]
        present_required = [c for c in required_columns if c in df.columns]
        df = df.dropna(subset=present_required)

        print(f"✅ Sau tiền xử lý còn {len(df)} bản ghi hợp lệ.")

        # Kiểm tra xem còn dữ liệu để xử lý không
        if df.empty:
            raise Exception("PREPROCESS_DWH FAILED: Dữ liệu sau khi tiền xử lý (dropna) không còn bản ghi nào.")

        # ---------- 3. Tạo dimension (BUILD_DIMS) ----------
        print("--- 3. Bắt đầu Tạo Dimension ---")



        dim_brand = df[["brand"]].drop_duplicates().reset_index(drop=True).copy()
        dim_brand["brand_key"] = dim_brand.reset_index().index + 1
        dim_brand.rename(columns={"brand": "brand_name"}, inplace=True)

        if "date_collected" in df.columns:
            df["date_key"] = df["date_collected"].dt.strftime("%Y%m%d").astype("Int64")
            dim_date = df[["date_key", "date_collected"]].drop_duplicates().copy()
            dim_date["year"] = dim_date["date_collected"].dt.year
            dim_date["month"] = dim_date["date_collected"].dt.month
            dim_date["day"] = dim_date["date_collected"].dt.day
            dim_date.rename(columns={"date_collected": "date"}, inplace=True)
            dim_date = dim_date.dropna(subset=["date_key"])
        else:
            dim_date = pd.DataFrame(columns=["date_key", "date", "year", "month", "day"])

        df = df.merge(dim_brand[["brand_name", "brand_key"]], left_on="brand", right_on="brand_name", how="left")

        dim_product = df[[
            "product_name", "brand_key", "category", "price",
            "cpu", "ram", "storage", "os", "screen_size", "battery", "date_collected", "date_key"
        ]].drop_duplicates(subset=["product_name", "brand_key"]).copy()
        dim_product.rename(columns={"date_collected": "date_collected_raw"}, inplace=True)

        print(f"✅ Tạo dim_brand ({len(dim_brand)}), dim_date ({len(dim_date)}), dim_product ({len(dim_product)})")

        # ---------- 4. Load Data Mart (LOAD_DATA_MART) ----------
        print("--- 4. Bắt đầu Load Data Mart ---")

        conn_dm = connect_db(CONFIG["DATA_MART_DB"])
        try:
            with conn_dm.cursor() as cur:
                # Tạo bảng
                cur.execute("DROP TABLE IF EXISTS dim_brand")
                cur.execute("DROP TABLE IF EXISTS dim_date")
                cur.execute("DROP TABLE IF EXISTS dim_product")

                cur.execute(f"""
                    CREATE TABLE dim_brand (
                        brand_key INT PRIMARY KEY,
                        brand_name VARCHAR(255)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """)
                cur.execute(f"""
                    CREATE TABLE dim_date (
                        date_key INT PRIMARY KEY,
                        date DATE,
                        year INT,
                        month INT,
                        day INT
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """)
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

            def insert_df(df, table_name, conn):
                if df.empty:
                    return 0
                cols = ", ".join(df.columns)
                placeholders = ", ".join(["%s"] * len(df.columns))
                sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"

                rows = [tuple(row) for row in df.itertuples(index=False, name=None)]

                with conn.cursor() as cur_insert:
                    cur_insert.executemany(sql, rows)
                conn.commit()
                return len(rows)

            inserted_brand = insert_df(dim_brand, "dim_brand", conn_dm)
            inserted_date = insert_df(dim_date, "dim_date", conn_dm)
            inserted_product = insert_df(dim_product, "dim_product", conn_dm)

        finally:
            conn_dm.close()

        # Ghi log SUCCESS cho LOAD_DATA_MART (kết thúc toàn bộ quá trình ETL)
        upsert_log(batch_id, "LOAD_DATA_MART", "success", log_start_dm,
                   records_extracted=len(dim_product), records_inserted=inserted_product,
                   log_id=log_id_load_dm,
                   error_message=f"Tải Data Mart thành công. Prod: {inserted_product}, Brand: {inserted_brand}, Date: {inserted_date} bản ghi.")
        print(f"✅ Load Data Mart hoàn tất ({inserted_product} bản ghi sản phẩm).")

    except Exception as e:
        tb = traceback.format_exc()
        error_message_summary = f"ETL FAILED at: {e.__class__.__name__}: {str(e).splitlines()[0]}"

        # Cập nhật log FAIL cho LOAD_DATA_MART (Bất kể lỗi xảy ra ở bước nào)
        upsert_log(batch_id, "LOAD_DATA_MART", "failed", log_start_dm,
                   log_id=log_id_load_dm,
                   error_message=f"{error_message_summary}\n\nTraceback:\n{tb}")

        print(f"\n\n❌ LỖI NGHIÊM TRỌNG: {error_message_summary}")
        print("Log LOAD_DATA_MART đã được cập nhật FAIL.")
        raise # Vẫn raise exception để `if __name__ == "__main__":` bắt

# =========================
# Entry point
# =========================
if __name__ == "__main__":
    try:
        run_etl()
    except Exception as e:
        print("ETL kết thúc với lỗi. Xem log trong database để biết chi tiết.")
        sys.exit(1)
    else:
        print("ETL kết thúc thành công.")
        sys.exit(0)