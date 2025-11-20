import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime

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
    "CONTROL_DB": "control_db",

    # Bảng nguồn
    "SOURCE_TABLE": "dim_product"
}

current_time = datetime.now()

# ------------------------------
# Kết nối các DB
# ------------------------------
engine_dwh = create_engine(
    f"mysql+pymysql://{CONFIG['DB_USER']}:{CONFIG['DB_PASS']}@{CONFIG['DB_HOST']}:{CONFIG['DB_PORT']}/{CONFIG['DWH_DB_NAME']}"
)
engine_dm = create_engine(
    f"mysql+pymysql://{CONFIG['DB_USER']}:{CONFIG['DB_PASS']}@{CONFIG['DB_HOST']}:{CONFIG['DB_PORT']}/{CONFIG['DATA_MART_DB']}"
)
engine_control = create_engine(
    f"mysql+pymysql://{CONFIG['DB_USER']}:{CONFIG['DB_PASS']}@{CONFIG['DB_HOST']}:{CONFIG['DB_PORT']}/{CONFIG['CONTROL_DB']}"
)

# ------------------------------
# Khởi tạo bảng log trong Control DB
# ------------------------------
def setup_log_table(engine):
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS etl_log (
                log_id INT AUTO_INCREMENT PRIMARY KEY,
                batch_id VARCHAR(50),
                process_name VARCHAR(100),
                status ENUM('started','success','failed') NOT NULL,
                start_time DATETIME,
                end_time DATETIME,
                rows_processed INT,
                message TEXT
            )
        """))
        conn.commit()

def insert_log(engine, batch_id, process_name, status, start_time, rows_processed=None, message=""):
    end_time = datetime.now()
    log_data = {
        'batch_id': batch_id,
        'process_name': process_name,
        'status': status,
        'start_time': start_time,
        'end_time': end_time,
        'rows_processed': rows_processed,
        'message': message
    }
    pd.DataFrame([log_data]).to_sql("etl_log", engine, if_exists="append", index=False)

setup_log_table(engine_control)

# =========================
# 1️⃣ Kiểm tra dữ liệu mới từ DWH
# =========================
log_start = datetime.now()
batch_id = current_time.strftime("%Y%m%d%H%M%S")

try:
    df_dwh = pd.read_sql(f"SELECT * FROM {CONFIG['SOURCE_TABLE']}", engine_dwh)
    if df_dwh.empty:
        insert_log(engine_control, batch_id, "CHECK_DWH_NEW_DATA", "success", log_start,
                   message="Không có dữ liệu mới trong DWH.")
        print("❌ Không có dữ liệu mới từ DWH. Kết thúc ETL.")
        exit()
    insert_log(engine_control, batch_id, "CHECK_DWH_NEW_DATA", "success", log_start,
               rows_processed=len(df_dwh),
               message=f"Phát hiện {len(df_dwh)} bản ghi mới từ DWH.")
    print(f"✅ Đã đọc {len(df_dwh)} bản ghi từ DWH.")
except Exception as e:
    insert_log(engine_control, batch_id, "CHECK_DWH_NEW_DATA", "failed", log_start, message=str(e))
    raise

# ------------------------------
# Tiền xử lý
# ------------------------------
df_dwh.rename(columns={
    "Tên sản phẩm": "product_name",
    "Brand": "brand",
    "Category": "category",
    "sale_price_vnd": "price",
    "Ngày_crawl": "date_collected",
    "Chip": "cpu",
    "Ram": "ram",
    "Rom": "storage",
    "Công nghệ màn hình": "screen_size",
    "Pin": "battery",
    "HDH": "os"
}, inplace=True)

# Chuyển kiểu dữ liệu
df_dwh["price"] = pd.to_numeric(df_dwh["price"], errors="coerce")
df_dwh["date_collected"] = pd.to_datetime(df_dwh["date_collected"], errors="coerce")

# Loại bỏ NA
df_dwh = df_dwh.dropna(subset=["product_name","brand","category","price"])
print(f"✅ Sau tiền xử lý còn {len(df_dwh)} bản ghi hợp lệ.")

# =========================
# 2️⃣ Tạo các DIMENSION
# =========================
# dim_brand
dim_brand = df_dwh[["brand"]].drop_duplicates().reset_index(drop=True)
dim_brand["brand_key"] = dim_brand.index + 1
dim_brand.rename(columns={"brand":"brand_name"}, inplace=True)

# dim_time
df_dwh["time_key"] = df_dwh["date_collected"].dt.strftime("%Y%m%d").astype(int)
dim_time = df_dwh[["time_key","date_collected"]].drop_duplicates().copy()
dim_time["day_of_week"] = dim_time["date_collected"].dt.day_name()
dim_time["day_of_month"] = dim_time["date_collected"].dt.day
dim_time["month"] = dim_time["date_collected"].dt.month
dim_time["quarter"] = dim_time["date_collected"].dt.quarter
dim_time["year"] = dim_time["date_collected"].dt.year
dim_time["created_at"] = current_time
dim_time = dim_time.drop(columns=["date_collected"])

# dim_product
df_dwh = pd.merge(df_dwh, dim_brand[["brand_name","brand_key"]], left_on="brand", right_on="brand_name", how="left")
dim_product = df_dwh[["product_name","brand_key","category","price","cpu","ram","storage","os","screen_size","battery","date_collected"]].copy()
dim_product = dim_product.drop_duplicates(subset=["product_name","brand_key"])

# =========================
# 3️⃣ Load vào Data Mart
# =========================
log_start_dm = datetime.now()
try:
    insert_log(engine_control, batch_id, "LOAD_DATA_MART", "started", log_start_dm,
               message="Bắt đầu tải dữ liệu vào Data Mart.")

    # Ghi dim tables
    dim_brand.to_sql("dim_brand", engine_dm, if_exists="replace", index=False)
    dim_time.to_sql("dim_time", engine_dm, if_exists="replace", index=False)
    dim_product.to_sql("dim_product", engine_dm, if_exists="replace", index=False)

    insert_log(engine_control, batch_id, "LOAD_DATA_MART", "success", log_start_dm,
               rows_processed=len(dim_product),
               message=f"Tải Data Mart thành công với {len(dim_product)} bản ghi.")
    print(f"✅ Load Data Mart hoàn tất ({len(dim_product)} bản ghi).")

except Exception as e:
    insert_log(engine_control, batch_id, "LOAD_DATA_MART", "failed", log_start_dm, message=str(e))
    raise
