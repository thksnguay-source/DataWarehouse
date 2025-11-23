# ============================================================
# FILE: config_datamart.py
# MỤC ĐÍCH: Chứa cấu hình và hàm connect DB cho Data Mart ETL
# ============================================================

import pymysql
from typing import Optional, Dict, Any

# =========================
# ⚙️ CẤU HÌNH
# =========================
CONFIG: Dict[str, Any] = {
    "DB_USER": "root",
    "DB_PASS": "",
    "DB_HOST": "localhost",
    "DB_PORT": 3306,

    # DB Names
    "DWH_DB_NAME": "datawh",
    "DATA_MART_DB": "data_mart_prod",
    "CONTROL_DB": "crawl_controller",

    # Bảng nguồn
    "SOURCE_TABLE": "dim_product"
}

# =========================
# Hàm kết nối DB
# =========================
def connect_db(db_name: Optional[str] = None):
    """
    Tạo kết nối tới cơ sở dữ liệu MySQL.
    Nếu db_name=None → kết nối không chọn DB.
    """
    return pymysql.connect(
        host=CONFIG["DB_HOST"],
        user=CONFIG["DB_USER"],
        password=CONFIG["DB_PASS"],
        database=db_name,
        port=CONFIG["DB_PORT"],
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
