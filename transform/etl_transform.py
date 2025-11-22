import json
import re
import hashlib
from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.types import Text as SQLText

# ============================================
# MYSQL CONNECTION
# ============================================
MYSQL_DB = "datawarehouse"  # Giữ đúng tên schema đang sử dụng trong MySQL
CONTROL_DB = "control"      # Database phục vụ ghi log quy trình

def get_mysql_url():
    return "mysql+pymysql://root:@localhost:3306/datawarehouse?charset=utf8mb4"

def create_mysql_engine():
    return create_engine(get_mysql_url(), pool_pre_ping=True)

def get_control_mysql_url():
    return "mysql+pymysql://root:@localhost:3306/control?charset=utf8mb4"

def create_control_engine():
    return create_engine(get_control_mysql_url(), pool_pre_ping=True)

# ============================================
# ETL LOG FUNCTIONS
# ============================================
def start_etl_log():
    engine = create_mysql_engine()
    batch_id = f"batch_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    try:
        with engine.begin() as conn:
            # Tạo bảng etl_log nếu chưa có (theo cấu trúc thực tế)
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS etl_log (
                    etl_id INT AUTO_INCREMENT PRIMARY KEY,
                    batch_id VARCHAR(50) NOT NULL,
                    source_table VARCHAR(50) NOT NULL DEFAULT '',
                    target_table VARCHAR(50) NOT NULL DEFAULT '',
                    records_inserted INT DEFAULT 0,
                    records_updated INT DEFAULT 0,
                    records_skipped INT DEFAULT 0,
                    status ENUM('running','success','failed') DEFAULT 'running',
                    start_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    end_time TIMESTAMP NULL DEFAULT NULL
                )
            """))
            conn.execute(text("""
                INSERT INTO etl_log (batch_id, source_table, target_table, status) 
                VALUES (:batch_id, 'general', 'stg_products,dim_product', 'running')
            """), {"batch_id": batch_id})
            res_id = conn.execute(text("SELECT LAST_INSERT_ID()")).scalar()
        print(f" Bắt đầu ETL batch: {batch_id} (ID: {res_id})")
        return res_id, batch_id
    except Exception as e:
        print(f" Không thể ghi log: {e}")
        return None, batch_id

def update_error_log(etl_id, error_msg):
    if not etl_id:
        return
    engine = create_mysql_engine()
    with engine.begin() as conn:
        # Bảng etl_log không có cột error_msg, chỉ cập nhật status
        conn.execute(text("""
            UPDATE etl_log
            SET status='failed',
                end_time=NOW()
            WHERE etl_id = :id
        """), {"id": etl_id})
        # In thông báo lỗi ra console
        print(f"   ⚠️  Lỗi ETL: {str(error_msg)[:200]}")

def update_success_log(etl_id, inserted_count, updated_count=0):
    if not etl_id:
        return
    engine = create_mysql_engine()
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE etl_log
            SET status='success',
                records_inserted=:inserted,
                records_updated=:updated,
                end_time=NOW()
            WHERE etl_id = :id
        """), {"inserted": inserted_count, "updated": updated_count, "id": etl_id})
    print(f" Đã cập nhật Log: Success (Inserted: {inserted_count}, Updated: {updated_count})")

# Đường dẫn gốc dự án (ví dụ: D:\datawh)
PROJECT_ROOT = Path(__file__).resolve().parent.parent

CONTROL_PROCESS_METADATA = {
    "extract": {
        "name": "Extract",
        "description": "Trích xuất dữ liệu từ nguồn vào bảng general.",
        "order": 1,
    },
    "transform": {
        "name": "Transform",
        "description": "Chuẩn hóa dữ liệu trung gian trước khi load.",
        "order": 2,
    },
    "load_staging": {
        "name": "Load_Staging",
        "description": "Đưa dữ liệu chuẩn hóa vào stg_products.",
        "order": 3,
    },
    "load_dwh": {
        "name": "LoadDataWarehouse",
        "description": "Đồng bộ và ghi nhận SCD vào dim_product.",
        "order": 4,
    },
}


def resolve_simulated_datetime(simulated_date):
    """
    Hỗ trợ parse ngày giả lập (ví dụ '21/11/2025') để đồng bộ xuyên suốt ETL.
    """
    if simulated_date is None:
        return None
    if isinstance(simulated_date, datetime):
        return simulated_date
    if isinstance(simulated_date, pd.Timestamp):
        return simulated_date.to_pydatetime()
    if isinstance(simulated_date, str):
        cleaned = simulated_date.strip()
        date_formats = [
            "%d/%m/%Y",
            "%Y-%m-%d",
            "%d-%m-%Y",
            "%d/%m/%Y %H:%M:%S",
            "%Y%m%d",
        ]
        for fmt in date_formats:
            try:
                return datetime.strptime(cleaned, fmt)
            except ValueError:
                continue
    raise ValueError(
        f"Không thể chuyển đổi ngày giả lập: {simulated_date}. "
        "Hãy sử dụng định dạng dd/mm/YYYY hoặc YYYY-mm-dd."
    )

def _normalize_value_for_hash(value):
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    if isinstance(value, (datetime, pd.Timestamp)):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True, ensure_ascii=False)
    return str(value).strip()


def compute_record_hash(row, columns):
    normalized = [_normalize_value_for_hash(row.get(col, None)) for col in columns]
    raw = "||".join(normalized)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def build_mysql_column_definition(col_name, data_type, max_length):
    col_name_escaped = f"`{col_name}`"
    data_type = (data_type or "").lower()

    if data_type == "text":
        sql_type = "TEXT"
    elif data_type == "varchar":
        length = max_length or 255
        sql_type = f"VARCHAR({length})"
    elif data_type == "datetime":
        sql_type = "DATETIME"
    elif data_type in {"int", "bigint"}:
        sql_type = "INT"
    elif data_type == "decimal":
        sql_type = "DECIMAL(10,2)"
    else:
        sql_type = data_type.upper() if data_type else "TEXT"

    return f"{col_name_escaped} {sql_type} NULL"


def ensure_dim_product_structure(conn, columns_info):
    """
    Đảm bảo dim_product tồn tại với đầy đủ cột (bao gồm metadata phục vụ SCD2).
    """
    column_definitions = {}
    for col_name, data_type, max_length in columns_info:
        column_definitions[col_name] = build_mysql_column_definition(col_name, data_type, max_length)

    metadata_definitions = {
        "record_hash": "CHAR(64) NOT NULL",
        # Thêm DEFAULT để tránh lỗi strict mode khi bảng đã có sẵn dữ liệu
        "effective_start": "DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP",
        "effective_end": "DATETIME NULL",
        "is_current": "TINYINT(1) NOT NULL DEFAULT 1",
        "version_no": "INT NOT NULL DEFAULT 1",
    }

    create_columns = (
        ["product_id INT AUTO_INCREMENT PRIMARY KEY"]
        + list(column_definitions.values())
        + [f"`{name}` {definition}" for name, definition in metadata_definitions.items()]
    )

    conn.execute(
        text(
            f"""
            CREATE TABLE IF NOT EXISTS dim_product (
                {', '.join(create_columns)}
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci
        """
        )
    )

    existing_columns = {
        row[0]
        for row in conn.execute(
            text(
                """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = :schema
                  AND TABLE_NAME = 'dim_product'
            """
            ),
            {"schema": MYSQL_DB},
        )
    }

    for col_name, col_def in column_definitions.items():
        if col_name not in existing_columns:
            conn.execute(text(f"ALTER TABLE dim_product ADD COLUMN {col_def}"))
            existing_columns.add(col_name)

    for col_name, col_def in metadata_definitions.items():
        if col_name not in existing_columns:
            conn.execute(text(f"ALTER TABLE dim_product ADD COLUMN `{col_name}` {col_def}"))
            existing_columns.add(col_name)

    unique_exists = conn.execute(
        text(
            """
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.STATISTICS
            WHERE TABLE_SCHEMA = :schema
              AND TABLE_NAME = 'dim_product'
              AND INDEX_NAME = 'unique_product'
        """
        ),
        {"schema": MYSQL_DB},
    ).scalar()

    if unique_exists:
        conn.execute(text("ALTER TABLE dim_product DROP INDEX unique_product"))

    idx_exists = conn.execute(
        text(
            """
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.STATISTICS
            WHERE TABLE_SCHEMA = :schema
              AND TABLE_NAME = 'dim_product'
              AND INDEX_NAME = 'idx_dim_product_ten'
        """
        ),
        {"schema": MYSQL_DB},
    ).scalar()

    if not idx_exists:
        try:
            conn.execute(text("CREATE INDEX idx_dim_product_ten ON dim_product (ten_san_pham)"))
        except Exception:
            pass

    ordered_columns = list(column_definitions.keys()) + list(metadata_definitions.keys())
    return ordered_columns


def normalize_date_key(value):
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    value_str = str(value).strip()
    if value_str.lower() in {"", "nan", "none", "nat", "null"}:
        return None
    return value_str

# ============================================
# CONTROL DB LOGGING (ETL MONITORING)
# ============================================

def _ensure_control_tables(conn):
    """
    Đảm bảo các bảng control.process & control.etl_log tồn tại đúng cấu trúc.
    """
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS process (
            process_id INT(11) NOT NULL AUTO_INCREMENT,
            process_name VARCHAR(100) NOT NULL,
            process_description VARCHAR(255) DEFAULT NULL,
            step_order INT(11) NOT NULL COMMENT 'Thứ tự thực hiện của process',
            PRIMARY KEY (process_id),
            UNIQUE KEY uq_process_name (process_name)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci
    """))

    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS etl_log (
            etl_id INT(11) NOT NULL AUTO_INCREMENT,
            batch_id VARCHAR(50) NOT NULL,
            process_id INT(11) NOT NULL,
            source_table VARCHAR(50) DEFAULT NULL,
            target_table VARCHAR(50) DEFAULT NULL,
            records_inserted INT(11) DEFAULT 0,
            records_updated INT(11) DEFAULT 0,
            records_skipped INT(11) DEFAULT 0,
            status ENUM('started','success','failed') DEFAULT 'started',
            start_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            end_time TIMESTAMP NULL DEFAULT NULL,
            PRIMARY KEY (etl_id),
            KEY fk_etl_log_process (process_id),
            CONSTRAINT fk_etl_log_process FOREIGN KEY (process_id)
                REFERENCES process (process_id) ON UPDATE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci
    """))


def _ensure_control_process(conn, process_key):
    """
    Thêm metadata process nếu chưa tồn tại (không thay đổi thuộc tính hiện có).
    """
    meta = CONTROL_PROCESS_METADATA[process_key]
    process_id = conn.execute(
        text("SELECT process_id FROM process WHERE process_name = :name"),
        {"name": meta["name"]},
    ).scalar()
    if process_id:
        return process_id

    conn.execute(
        text("""
            INSERT INTO process (process_name, process_description, step_order)
            VALUES (:name, :desc, :order_no)
        """),
        {"name": meta["name"], "desc": meta["description"], "order_no": meta["order"]},
    )
    return conn.execute(text("SELECT LAST_INSERT_ID()")).scalar()


def control_log_start(process_key, batch_id, source_table="", target_table=""):
    """
    Ghi nhận thời điểm bắt đầu 1 process trong DB control.
    """
    engine = create_control_engine()
    with engine.begin() as conn:
        _ensure_control_tables(conn)
        process_id = _ensure_control_process(conn, process_key)
        conn.execute(
            text("""
                INSERT INTO etl_log (batch_id, process_id, source_table, target_table, status)
                VALUES (:batch_id, :process_id, :source_table, :target_table, 'started')
            """),
            {
                "batch_id": batch_id,
                "process_id": process_id,
                "source_table": source_table or "",
                "target_table": target_table or "",
            },
        )
        return conn.execute(text("SELECT LAST_INSERT_ID()")).scalar()


def control_log_finish(log_id, status="success", inserted=0, updated=0, skipped=0):
    """
    Cập nhật trạng thái cho process log tương ứng.
    """
    if not log_id:
        return
    engine = create_control_engine()
    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE etl_log
                SET status = :status,
                    records_inserted = :inserted,
                    records_updated = :updated,
                    records_skipped = :skipped,
                    end_time = NOW()
                WHERE etl_id = :etl_id
            """),
            {
                "status": status,
                "inserted": inserted or 0,
                "updated": updated or 0,
                "skipped": skipped or 0,
                "etl_id": log_id,
            },
        )

# ============================================
# EXTRACT
# ============================================
def extract_from_json(json_path=None):
    """
    Đọc dữ liệu từ file JSON unified_products.json
    (danh sách các object giống như ví dụ user cung cấp).
    """
    print("\n" + "="*60)
    print("BƯỚC 1: EXTRACT - Đọc dữ liệu từ file JSON unified_products1.json")
    print("="*60)
    if json_path is None:
        json_path = PROJECT_ROOT / "crawed" / "unified_products1.json"
    else:
        json_path = Path(json_path)
        if not json_path.is_absolute():
            json_path = (PROJECT_ROOT / json_path).resolve()

    try:
        print(f"   → File: {json_path}")
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        if isinstance(data, dict):
            # Phòng trường hợp json là object { "data": [...] }
            # thì ưu tiên key 'data' hoặc giá trị list đầu tiên.
            if "data" in data and isinstance(data["data"], list):
                records = data["data"]
            else:
                # Lấy list đầu tiên tìm được
                records = None
                for v in data.values():
                    if isinstance(v, list):
                        records = v
                        break
                if records is None:
                    raise ValueError("Cấu trúc JSON không đúng định dạng list bản ghi.")
        else:
            records = data

        df = pd.DataFrame(records)
        print(f" Đã đọc {len(df)} dòng từ file {json_path}")
        return df
    except Exception as e:
        print(f" Lỗi khi đọc dữ liệu từ JSON: {e}")
        raise


def load_raw_json_to_general(json_path=None):
    """
    Nạp toàn bộ dữ liệu JSON (dạng text) vào bảng general.
    Các cột của general sẽ tự động khớp theo cột trong file JSON.
    """
    print("\n" + "="*60)
    print("BƯỚC 0: LOAD RAW - Nạp dữ liệu JSON thô vào bảng general")
    print("="*60)
    df_raw = extract_from_json(json_path)
    if df_raw.empty:
        print(" ⚠️ File JSON không có dữ liệu, bỏ qua bước nạp general.")
        return 0

    # Đảm bảo mọi giá trị (ngoại trừ NULL) đều ở dạng chuỗi để lưu đúng TEXT
    df_text = df_raw.copy()
    for col in df_text.columns:
        df_text[col] = df_text[col].apply(
            lambda v: None if v is None or (isinstance(v, float) and pd.isna(v)) else str(v)
        )

    engine = create_mysql_engine()
    dtype_map = {col: SQLText() for col in df_text.columns}

    try:
        df_text.to_sql('general', engine, if_exists='replace', index=False, dtype=dtype_map, chunksize=1000)
        print(f" ✅ Đã nạp {len(df_text)} dòng vào bảng general (kiểu TEXT cho mọi cột)")
        return len(df_text)
    except Exception as e:
        print(f" ❌ Lỗi khi nạp dữ liệu vào general: {e}")
        raise


def extract_from_general():
    print("\n" + "="*60)
    print("BƯỚC 1: EXTRACT - Đọc dữ liệu từ bảng general")
    print("="*60)
    engine = create_mysql_engine()
    try:
        query = "SELECT * FROM general"
        df = pd.read_sql(query, engine)
        print(f" Đã đọc {len(df)} dòng từ bảng general")
        return df
    except Exception as e:
        print(f" Lỗi khi đọc dữ liệu: {e}")
        raise

# ============================================
# TRANSFORM
# ============================================
def transform_data(df, simulated_date=None):
    print("\n" + "="*60)
    print("BƯỚC 2: TRANSFORM - Làm sạch và chuẩn hóa dữ liệu")
    print("="*60)
    df = df.copy()
    crawl_dt = resolve_simulated_datetime(simulated_date) if simulated_date else datetime.now()

    # Lọc dữ liệu rác
    initial_count = len(df)
    df = df.dropna(subset=['Tên sản phẩm'])
    df = df[df['Tên sản phẩm'] != 'Không tìm thấy']
    df = df[df['Tên sản phẩm'].astype(str).str.strip() != '']
    print(f" 🔍 Loại bỏ {initial_count - len(df)} dòng dữ liệu rác")

    # Rename các cột chính sang snake_case
    df.rename(columns={
        'Tên sản phẩm': 'ten_san_pham',
        'Giá': 'sale_price_vnd',
        'Nguồn': 'nguon'
    }, inplace=True)

    # Trích xuất Brand từ tên sản phẩm
    brands_dict = {
        'IPHONE': 'Apple',
        'SAMSUNG': 'Samsung',
        'XIAOMI': 'Xiaomi',
        'OPPO': 'Oppo',
        'REALME': 'Realme',
        'VIVO': 'Vivo',
        'NOKIA': 'Nokia',
        'TECNO': 'Tecno',
        'HONOR': 'Honor',
        'SONY': 'Sony',
        'ASUS': 'Asus',
        'INFINIX': 'Infinix',
        'POCO': 'Xiaomi',
        'NOTHING': 'Nothing',
        'NUBIA': 'Nubia',
        'GOOGLE': 'Google',
        'VSMART': 'Vsmart'
    }

    def extract_brand(name):
        if pd.isna(name) or name == 'nan' or str(name).strip() == '':
            return 'Other'
        n = str(name).upper()
        for k, v in brands_dict.items():
            if k in n:
                return v
        return 'Other'

    df['brand'] = df['ten_san_pham'].apply(extract_brand)

    # Phân loại Category
    def categorize(name):
        if pd.isna(name) or name == 'nan' or str(name).strip() == '':
            return 'Smartphone'
        n = str(name).upper()
        if any(x in n for x in ['FOLD', 'FLIP', 'GALAXY Z']):
            return 'Foldable'
        if 'TAB' in n or 'IPAD' in n:
            return 'Tablet'
        return 'Smartphone'

    df['category'] = df['ten_san_pham'].apply(categorize)

    # Metadata - Thêm thông tin ngày crawl (DATETIME) - cho phép giả lập ngày cố định
    df['ngay_crawl'] = crawl_dt
    df['date_key'] = crawl_dt.strftime("%Y%m%d")

    # Xử lý nguồn dữ liệu (VARCHAR)
    df['nguon'] = df['nguon'].fillna('CellphoneS')
    df['nguon'] = df['nguon'].astype(str).str.strip()

    # Xử lý giá - giữ nguyên format TEXT (vì có ký tự đặc biệt ₫, đ)
    df['sale_price_vnd'] = df['sale_price_vnd'].astype(str)
    df['sale_price_vnd'] = df['sale_price_vnd'].replace('nan', None)
    df['sale_price_vnd'] = df['sale_price_vnd'].replace('None', None)

    # ============================================
    # CHUYỂN ĐỔI KIỂU DỮ LIỆU CHO CÁC CỘT
    # ============================================

    # Xử lý tất cả các cột text - chuyển sang string và xử lý NULL
    # Giữ nguyên format text nhưng chuẩn bị để chuyển đổi kiểu dữ liệu trong SQL
    for col in df.columns:
        if col not in ['brand', 'category', 'ngay_crawl', 'date_key']:
            # Chuyển sang string nhưng giữ NULL values
            df[col] = df[col].astype(str)
            # Thay thế 'nan' và 'None' thành None (NULL trong SQL)
            df[col] = df[col].replace(['nan', 'None', 'NaT', '<NA>'], None)
            # Xử lý các giá trị rỗng
            df[col] = df[col].apply(lambda x: None if str(x).strip() in ['', 'nan', 'None', 'NaT', '<NA>'] else x)

    # Loại bỏ cột không cần thiết (id và created_at không có trong stg_products)
    for col in ['id', 'created_at']:
        if col in df.columns:
            df.drop(columns=[col], inplace=True)

    # Đảm bảo thứ tự cột đúng với stg_products (URL đầu tiên)
    if 'URL' in df.columns:
        cols = ['URL'] + [c for c in df.columns if c != 'URL']
        df = df[cols]

    print(f" ✅ Dữ liệu đã làm sạch. Tổng dòng: {len(df)}")
    print(f" 📊 Số cột sau transform: {len(df.columns)}")
    print(f" 📋 Các cột: {', '.join(df.columns[:5])}... (tổng {len(df.columns)} cột)")
    return df

# ============================================
# LOAD TO STAGING
# ============================================
def load_to_staging(df):
    print("\n" + "="*60)
    print("BƯỚC 3: LOAD - Nạp dữ liệu vào stg_products")
    print("="*60)
    engine = create_mysql_engine()
    try:
        # Sử dụng pandas to_sql để load dữ liệu
        df_to_load = df.copy()

        # Đảm bảo ngay_crawl là datetime object để pandas tự động detect
        if 'ngay_crawl' in df_to_load.columns:
            df_to_load['ngay_crawl'] = pd.to_datetime(df_to_load['ngay_crawl'], errors='coerce')

        # Load vào staging (pandas sẽ tự động tạo bảng với kiểu dữ liệu TEXT)
        df_to_load.to_sql('stg_products', engine, if_exists='replace', index=False, chunksize=1000)

        # Cập nhật kiểu dữ liệu sau khi load (ALTER TABLE)
        print(" 🔄 Đang chuyển đổi kiểu dữ liệu...")
        with engine.begin() as conn:
            # Cập nhật ngay_crawl thành DATETIME
            if 'ngay_crawl' in df.columns:
                try:
                    conn.execute(text("""
                        ALTER TABLE stg_products 
                        MODIFY COLUMN ngay_crawl DATETIME NULL
                    """))
                    print("   ✓ ngay_crawl -> DATETIME")
                except Exception as e:
                    print(f"   ⚠️  Không thể chuyển ngay_crawl sang DATETIME: {e}")

            # Cập nhật các cột VARCHAR (text ngắn)
            varchar_updates = {
                'nguon': 'VARCHAR(100)',
                'brand': 'VARCHAR(50)',
                'category': 'VARCHAR(50)',
                'date_key': 'VARCHAR(8)',
                'sale_price_vnd': 'VARCHAR(50)',
                'ten_san_pham': 'VARCHAR(255)',
                'Công nghệ NFC': 'VARCHAR(10)',
                'Hỗ trợ mạng': 'VARCHAR(10)',
                'Cổng sạc': 'VARCHAR(20)',
                'Hệ điều hành': 'VARCHAR(50)',
                'Chỉ số kháng nước, bụi': 'VARCHAR(10)',
                'Cảm biến vân tay': 'VARCHAR(50)',
                'Wi-Fi': 'VARCHAR(20)',
                'Bluetooth': 'VARCHAR(10)',
                'Thẻ SIM': 'VARCHAR(50)',
                'Loại CPU': 'VARCHAR(50)'
            }

            for col, dtype in varchar_updates.items():
                if col in df.columns:
                    try:
                        # Sử dụng backtick cho tên cột có dấu cách hoặc ký tự đặc biệt
                        col_name = f"`{col}`" if ' ' in col or '-' in col else col
                        conn.execute(text(f"""
                            ALTER TABLE stg_products 
                            MODIFY COLUMN {col_name} {dtype} NULL
                        """))
                        print(f"   ✓ {col} -> {dtype}")
                    except Exception as e:
                        print(f"   ⚠️  Không thể chuyển {col} sang {dtype}: {e}")

            # Các cột còn lại giữ nguyên TEXT (URL, mô tả dài, thông số kỹ thuật)
            print("   ✓ Các cột khác giữ nguyên TEXT")

        print(f" ✅ Đã load {len(df)} dòng vào bảng 'stg_products' với kiểu dữ liệu phù hợp")
        return len(df)
    except Exception as e:
        print(f" ❌ Lỗi load vào staging: {e}")
        raise


def build_staging_snapshot(engine, target_date=None):
    """
    Chuẩn hóa dữ liệu stg_products (lọc theo ngày nếu cần) để so sánh/làm SCD.
    """
    stg_df = pd.read_sql("SELECT * FROM stg_products", engine)
    if stg_df.empty:
        return stg_df

    if 'ten_san_pham' not in stg_df.columns:
        raise KeyError("Cột 'ten_san_pham' bắt buộc để xác định khóa tự nhiên.")

    stg_df = stg_df[~stg_df['ten_san_pham'].isna()].copy()
    if stg_df.empty:
        return stg_df

    if 'ngay_crawl' in stg_df.columns:
        stg_df['ngay_crawl'] = pd.to_datetime(stg_df['ngay_crawl'], errors='coerce')
    else:
        stg_df['ngay_crawl'] = pd.NaT

    if target_date:
        target_dt = resolve_simulated_datetime(target_date)
        stg_df = stg_df[stg_df['ngay_crawl'].dt.date == target_dt.date()].copy()
        if stg_df.empty:
            return stg_df

    if 'date_key' in stg_df.columns:
        stg_df['date_key'] = (
            stg_df['date_key']
            .astype(str)
            .str.replace(r"\.0$", "", regex=True)
        )
    else:
        stg_df['date_key'] = None
    stg_df['date_key'] = stg_df['date_key'].apply(normalize_date_key)

    compare_columns = [col for col in stg_df.columns if col not in {'ngay_crawl', 'date_key'}]
    stg_df['record_hash'] = stg_df.apply(lambda row: compute_record_hash(row, compare_columns), axis=1)

    stg_df = stg_df.sort_values(['ten_san_pham', 'ngay_crawl'], ascending=[True, False])
    stg_df = stg_df.drop_duplicates(subset=['ten_san_pham'], keep='first')
    return stg_df


def fetch_current_dim_lookup(engine):
    """
    Lấy dữ liệu dim_product hiện tại (is_current=1) để phục vụ so sánh.
    """
    try:
        dim_current = pd.read_sql(
            """
            SELECT product_id, ten_san_pham, record_hash, date_key, ngay_crawl, version_no
            FROM dim_product
            WHERE is_current = 1
        """,
            engine,
        )
    except Exception:
        # Trường hợp lần chạy đầu tiên chưa có dim_product
        return {}, pd.DataFrame()
    if dim_current.empty:
        return {}, dim_current
    dim_current['ten_san_pham'] = dim_current['ten_san_pham'].astype(str).str.strip()
    current_lookup = dim_current.set_index('ten_san_pham').to_dict('index')
    return current_lookup, dim_current


def detect_dim_changes(stg_df, current_lookup):
    """
    So sánh dữ liệu stg mới với dim_product hiện tại → xác định insert/update.
    """
    rows_to_insert = []
    rows_to_expire = []
    unchanged_rows = 0

    for _, row in stg_df.iterrows():
        product_key = str(row['ten_san_pham']).strip()
        if not product_key:
            continue

        new_hash = row['record_hash']
        new_start = row['ngay_crawl']
        if pd.isna(new_start):
            new_start = datetime.now()
            row['ngay_crawl'] = new_start

        existing = current_lookup.get(product_key)
        if not existing:
            row_dict = row.to_dict()
            row_dict['ten_san_pham'] = product_key
            row_dict['date_key'] = normalize_date_key(row_dict.get('date_key'))
            row_dict['ngay_crawl'] = new_start
            row_dict['effective_start'] = new_start
            row_dict['effective_end'] = None
            row_dict['is_current'] = 1
            row_dict['version_no'] = 1
            rows_to_insert.append(row_dict)
            continue

        if existing.get('record_hash') == new_hash:
            unchanged_rows += 1
            continue

        rows_to_expire.append(
            {
                "product_id": int(existing['product_id']),
                "end_ts": new_start,
            }
        )

        row_dict = row.to_dict()
        row_dict['ten_san_pham'] = product_key
        row_dict['date_key'] = normalize_date_key(row_dict.get('date_key'))
        row_dict['ngay_crawl'] = new_start
        row_dict['effective_start'] = new_start
        row_dict['effective_end'] = None
        row_dict['is_current'] = 1
        row_dict['version_no'] = int(existing.get('version_no') or 1) + 1
        rows_to_insert.append(row_dict)

    return rows_to_insert, rows_to_expire, unchanged_rows

# ============================================
# LOAD TO DIMENSION TABLE
# ============================================
def load_to_dim():
    print("\n" + "="*60)
    print("BƯỚC 4: LOAD - Nạp dữ liệu vào dim_product")
    print("="*60)
    engine = create_mysql_engine()

    with engine.begin() as conn:
        result = conn.execute(
            text(
                """
                SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = :schema
                  AND TABLE_NAME = 'stg_products'
                ORDER BY ORDINAL_POSITION
            """
            ),
            {"schema": MYSQL_DB},
        )
        columns_info = [tuple(row) for row in result.fetchall()]
        if not columns_info:
            print(" ❌ Không tìm thấy bảng stg_products")
            return 0, 0

        dim_columns_order = ensure_dim_product_structure(conn, columns_info)

    stg_df = build_staging_snapshot(engine)
    if stg_df.empty:
        print(" ⚠️ stg_products đang trống, bỏ qua load dim_product.")
        return 0, 0

    current_lookup, _ = fetch_current_dim_lookup(engine)
    rows_to_insert, rows_to_expire, unchanged_rows = detect_dim_changes(stg_df, current_lookup)

    if not rows_to_insert and not rows_to_expire:
        print(" ✅ dim_product không có thay đổi mới.")
        return 0, 0

    with engine.begin() as conn:
        if rows_to_expire:
            conn.execute(
                text("""
                    UPDATE dim_product
                    SET is_current = 0,
                        effective_end = :end_ts
                    WHERE product_id = :product_id
                """),
                rows_to_expire,
            )

        if rows_to_insert:
            insert_df = pd.DataFrame(rows_to_insert)
            for col in dim_columns_order:
                if col not in insert_df.columns:
                    insert_df[col] = None
            insert_df = insert_df[dim_columns_order]
            insert_df.to_sql('dim_product', conn, if_exists='append', index=False, chunksize=500)

    inserted_count = len(rows_to_insert)
    updated_count = len(rows_to_expire)
    print(f" ✅ Đã áp dụng SCD Type 2 cho dim_product – inserted: {inserted_count}, expired: {updated_count}, unchanged: {unchanged_rows}")
    return inserted_count, updated_count


def compare_staging_with_dim(target_date=None, sample_size=5):
    """
    So sánh dữ liệu mới nhất tại stg_products với dim_product (ngày cũ).
    """
    print("\n" + "="*60)
    print("SO SÁNH STG_PRODUCTS ↔ DIM_PRODUCT")
    print("="*60)
    engine = create_mysql_engine()
    stg_snapshot = build_staging_snapshot(engine, target_date=target_date)
    if stg_snapshot.empty:
        print(" ⚠️ Không có dữ liệu trong stg_products với điều kiện yêu cầu.")
        return {"total_stg": 0, "new_records": 0, "changed_records": 0}

    current_lookup, dim_current = fetch_current_dim_lookup(engine)
    rows_to_insert, rows_to_expire, unchanged_rows = detect_dim_changes(stg_snapshot, current_lookup)

    summary = {
        "total_stg": len(stg_snapshot),
        "dim_current": len(dim_current),
        "new_records": len([r for r in rows_to_insert if r['version_no'] == 1]),
        "changed_records": len(rows_to_insert) - len([r for r in rows_to_insert if r['version_no'] == 1]),
        "expire_candidates": len(rows_to_expire),
        "unchanged": unchanged_rows,
    }

    print(f" 📌 Tổng dòng stg: {summary['total_stg']}")
    print(f" 📌 Số bản ghi dim hiện tại: {summary['dim_current']}")
    print(f" ➕ Bản ghi mới hoàn toàn: {summary['new_records']}")
    print(f" 🔁 Bản ghi cần cập nhật phiên bản: {summary['changed_records']}")
    print(f" 💤 Bản ghi giữ nguyên: {summary['unchanged']}")

    if rows_to_insert:
        sample_df = pd.DataFrame(rows_to_insert[:sample_size])
        cols_to_show = [col for col in ['ten_san_pham', 'sale_price_vnd', 'brand', 'ngay_crawl', 'version_no'] if col in sample_df.columns]
        print("\n Ví dụ bản ghi sẽ được nạp/ cập nhật:")
        print(sample_df[cols_to_show].to_string(index=False))
    else:
        print("\n ✅ Không có sự khác biệt giữa ngày mới và dữ liệu dim hiện tại.")

    return summary


# ============================================
# SYNC DATE_KEY + DIM
# ============================================
def sync_date_key_and_dim(rebuild_dim=True):
    print("\n" + "="*60)
    print("BƯỚC 4: SYNC - Đồng bộ date_key & dim_product")
    print("="*60)
    engine = create_mysql_engine()

    with engine.begin() as conn:
        stg_count = conn.execute(text("SELECT COUNT(*) FROM stg_products")).scalar()
        if stg_count == 0:
            print(" ⚠️ stg_products đang trống, bỏ qua đồng bộ date_key.")
            return 0, 0

        date_count = conn.execute(text("SELECT COUNT(*) FROM date_dims")).scalar()
        if date_count == 0:
            raise ValueError("Bảng date_dims không có dữ liệu, không thể map date_key.")

        has_ngay = conn.execute(
            text("""
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = :schema
                  AND TABLE_NAME = 'stg_products'
                  AND COLUMN_NAME = 'ngay_crawl'
            """),
            {"schema": MYSQL_DB},
        ).scalar()

        if has_ngay == 0:
            raise KeyError("Không tìm thấy cột 'ngay_crawl' trong stg_products.")

        # Cập nhật date_key dựa trên date_dims
        conn.execute(text("""
            UPDATE stg_products s
            LEFT JOIN date_dims d ON DATE(s.ngay_crawl) = DATE(d.full_date)
            SET s.date_key = d.date_sk
        """))

        missing = conn.execute(text("""
            SELECT COUNT(*) 
            FROM stg_products s
            LEFT JOIN date_dims d ON DATE(s.ngay_crawl) = DATE(d.full_date)
            WHERE d.date_sk IS NULL
        """)).scalar()

    if missing:
        print(f" ⚠️ Có {missing} dòng chưa match được date_key trong date_dims.")
    else:
        print(" ✅ date_key trong stg_products đã đồng bộ với date_dims.")

    if rebuild_dim:
        print(" 🔄 Đang rebuild dim_product sau khi cập nhật date_key...")
        return load_to_dim()

    return 0, 0

# ============================================
# MAIN ETL PROCESS
# ============================================
def run_etl(simulated_date=None, stage_only=False, auto_compare=False):
    print("BẮT ĐẦU QUY TRÌNH ETL: JSON → GENERAL → STG_PRODUCTS → DIM_PRODUCT")
    etl_id, batch_id = start_etl_log()
    control_logs = {
        "extract": None,
        "transform": None,
        "load_staging": None,
        "load_dwh": None,
    }
    try:
        # Bước 0: nạp dữ liệu thô vào bảng general
        control_logs["extract"] = control_log_start(
            "extract",
            batch_id,
            source_table="unified_products1.json",
            target_table="general",
        )
        inserted_general = load_raw_json_to_general()
        control_log_finish(control_logs["extract"], "success", inserted=inserted_general)
        control_logs["extract"] = None

        # Bước 1: đọc dữ liệu từ bảng general để transform
        control_logs["transform"] = control_log_start(
            "transform",
            batch_id,
            source_table="general",
            target_table="pandas_dataframe",
        )
        df = extract_from_general()
        if len(df) == 0:
            control_log_finish(control_logs["transform"], "success", skipped=1)
            print("  Không có dữ liệu để xử lý!")
            return
        df_clean = transform_data(df, simulated_date=simulated_date)
        control_log_finish(control_logs["transform"], "success", inserted=len(df_clean))
        control_logs["transform"] = None

        control_logs["load_staging"] = control_log_start(
            "load_staging",
            batch_id,
            source_table="general",
            target_table="stg_products",
        )
        inserted_stg = load_to_staging(df_clean)
        control_log_finish(control_logs["load_staging"], "success", inserted=inserted_stg)
        control_logs["load_staging"] = None

        if stage_only:
            print(" ⏸️ Đã dừng theo yêu cầu sau bước load stg_products.")
            if auto_compare:
                compare_staging_with_dim(target_date=simulated_date)
            update_success_log(etl_id, 0, 0)
            return

        control_logs["load_dwh"] = control_log_start(
            "load_dwh",
            batch_id,
            source_table="stg_products",
            target_table="dim_product",
        )
        inserted_dim, updated_dim = sync_date_key_and_dim(rebuild_dim=True)
        control_log_finish(
            control_logs["load_dwh"],
            "success",
            inserted=inserted_dim,
            updated=updated_dim,
        )
        control_logs["load_dwh"] = None
        update_success_log(etl_id, inserted_dim, updated_dim)
        print("\nETL HOÀN TẤT THÀNH CÔNG!")
        print(
            f"  • Batch ID: {batch_id}"
            f"\n  • Dòng đã xử lý (staging): {inserted_stg}"
            f"\n  • Dim_product - bản ghi mới: {inserted_dim}, bản ghi đóng: {updated_dim}"
            f"\n  • Trạng thái: SUCCESS"
        )
    except Exception as e:
        print("❌ ETL THẤT BẠI!")
        print(f" Lỗi: {e}")
        # Đánh dấu các process đang dang dở là failed
        for key, log_id in control_logs.items():
            if log_id:
                control_log_finish(log_id, status="failed")
        update_error_log(etl_id, str(e))
        raise

# ============================================
# ENTRY POINT
# ============================================
if __name__ == "__main__":
    try:
        run_etl()
    except Exception as e:
        print(f"\nChương trình kết thúc với lỗi: {e}")
        exit(1)
