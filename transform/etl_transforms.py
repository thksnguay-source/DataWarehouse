import json
import re
import hashlib
from datetime import datetime
from pathlib import Path
import sys

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.types import Text as SQLText

# Bước 1-4: Khởi tạo và Extract
# Bước 5-13: Transform (làm sạch dữ liệu)
# Bước 14-18: Load Staging
# Bước 19-35: Load Dimension
# Import database configuration
sys.path.append(str(Path(__file__).resolve().parent.parent))
from config.db_config import get_mysql_url, get_mysql_url_control

# ============================================
# MYSQL CONNECTION
# ============================================
MYSQL_DB = "staging"  # Giữ đúng tên schema đang sử dụng trong MySQL
CONTROL_DB = "control"  # Database phục vụ ghi log quy trình


# 1 Kiểm tra kết nối db
def create_mysql_engine():
    return create_engine(get_mysql_url(), pool_pre_ping=True)


def create_control_engine():
    return create_engine(get_mysql_url_control(), pool_pre_ping=True)


# 2. ETL để ghi log vào bảng control.process
# Đường dẫn gốc dự án (ví dụ: D:\datawh)
PROJECT_ROOT = Path(__file__).resolve().parent.parent

CONTROL_PROCESS_METADATA = {
    "extract": {
        "name": "Extract",
        "description": "Trích xuất dữ liệu từ nguồn vào thư mục crawl.",
        "order": 1,
    },
    "load_staging": {
        "name": "Load_Staging",
        "description": "Tải dữ liệu từ thư mục crawl vào Database Staging.",
        "order": 2,
    },
    "transform": {
        "name": "Transform",
        "description": "Chuyển đổi dữ liệu từ Staging Database.",
        "order": 3,
    },
    "load_dwh": {
        "name": "LoadDataWarehouse",
        "description": "Tải dữ liệu đã chuyển đổi vào Data Warehouse Database.",
        "order": 4,
    },
    "load_datamarts": {
        "name": "LoadDatamarts",
        "description": "Xây dựng và tải dữ liệu từ Data Warehouse vào Product Data Mart.",
        "order": 5,
    },
}


# 3. nhập ngày mô phỏng với nhiều định dạng
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


# 4. So sánh 2 bản ghi có khác nhau hay không (scd type2)
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


# 5. xây dựng lại cấu trúc bảng

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

# Tự động tạo bảng nếu chưa có
# Thêm cột mới nếu thiếu
# Xóa cột cũ không dùng nữa
# Tạo index tối ưu tìm kiếm
def ensure_dim_product_structure(conn, columns_info):
    """
    Đảm bảo dim_product tồn tại với đầy đủ cột (bao gồm metadata phục vụ SCD2).
    Loại bỏ các cột cũ: Tên sản phẩm, Giá, Nguồn (đã được thay thế bằng ten_san_pham, sale_price_vnd, nguon).
    """
    # Danh sách các cột cũ cần loại bỏ
    columns_to_exclude = {'Tên sản phẩm', 'Giá', 'Nguồn'}

    # Lọc bỏ các cột không mong muốn
    filtered_columns_info = [
        (col_name, data_type, max_length)
        for col_name, data_type, max_length in columns_info
        if col_name not in columns_to_exclude
    ]

    column_definitions = {}
    for col_name, data_type, max_length in filtered_columns_info:
        column_definitions[col_name] = build_mysql_column_definition(col_name, data_type, max_length)

    metadata_definitions = {}

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

    # Loại bỏ các cột cũ nếu chúng tồn tại
    columns_to_drop = {'Tên sản phẩm', 'Giá', 'Nguồn'}
    for col_to_drop in columns_to_drop:
        if col_to_drop in existing_columns:
            try:
                conn.execute(text(f"ALTER TABLE dim_product DROP COLUMN `{col_to_drop}`"))
                existing_columns.discard(col_to_drop)
                print(f"   ✓ Đã loại bỏ cột cũ: {col_to_drop}")
            except Exception as e:
                print(f"   ⚠️  Không thể loại bỏ cột {col_to_drop}: {e}")

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
# xử lý giá

def parse_price_to_decimal(price_str):
    """
    Chuyển đổi giá từ string (có thể chứa ký tự đặc biệt như ₫, dấu chấm, phẩy) sang decimal.
    Ví dụ: "15.990.000 ₫" -> 15990000.00
    """
    if price_str is None or pd.isna(price_str):
        return None

    price_str = str(price_str).strip()
    if price_str.lower() in {"", "nan", "none", "nat", "null", "không có", "n/a"}:
        return None

    # Loại bỏ các ký tự không phải số, dấu chấm, phẩy
    # Giữ lại số, dấu chấm (.), dấu phẩy (,)
    # Loại bỏ tất cả ký tự không phải số, dấu chấm, dấu phẩy
    cleaned = re.sub(r'[^\d.,]', '', price_str)

    if not cleaned:
        return None

    # Xử lý dấu phẩy và chấm
    # Nếu có cả dấu chấm và phẩy, dấu phẩy thường là phân cách hàng nghìn, chấm là thập phân (hoặc ngược lại)
    if ',' in cleaned and '.' in cleaned:
        # Kiểm tra xem dấu nào đứng sau (thường là phần thập phân)
        if cleaned.rindex(',') > cleaned.rindex('.'):
            # Dấu phẩy là phần thập phân: "1.234,56" -> 1234.56
            cleaned = cleaned.replace('.', '').replace(',', '.')
        else:
            # Dấu chấm là phần thập phân: "1,234.56" -> 1234.56
            cleaned = cleaned.replace(',', '')
    elif ',' in cleaned:
        # Chỉ có dấu phẩy - có thể là phân cách hàng nghìn hoặc thập phân
        # Nếu có nhiều dấu phẩy -> phân cách hàng nghìn
        if cleaned.count(',') > 1:
            cleaned = cleaned.replace(',', '')
        else:
            # Có thể là thập phân hoặc hàng nghìn
            # Nếu sau dấu phẩy có 3 chữ số -> hàng nghìn, ngược lại -> thập phân
            parts = cleaned.split(',')
            if len(parts) == 2 and len(parts[1]) == 3:
                cleaned = cleaned.replace(',', '')
            else:
                cleaned = cleaned.replace(',', '.')
    elif '.' in cleaned:
        # Chỉ có dấu chấm
        # Nếu có nhiều dấu chấm -> phân cách hàng nghìn
        if cleaned.count('.') > 1:
            cleaned = cleaned.replace('.', '')
        # Nếu chỉ có 1 dấu chấm, giữ nguyên (có thể là thập phân)

    try:
        price_decimal = float(cleaned)
        return round(price_decimal, 2)
    except (ValueError, TypeError):
        return None


# ============================================
# CONTROL DB LOGGING (ETL MONITORING)
# ============================================

def _ensure_control_tables(conn):
    """
    Đảm bảo các bảng control.process & control.etl_log tồn tại đúng cấu trúc.
    """
    conn.execute(text("""
                      CREATE TABLE IF NOT EXISTS process
                      (
                          process_id
                          INT
                      (
                          11
                      ) NOT NULL AUTO_INCREMENT,
                          process_name VARCHAR
                      (
                          100
                      ) NOT NULL,
                          process_description VARCHAR
                      (
                          255
                      ) DEFAULT NULL,
                          step_order INT
                      (
                          11
                      ) NOT NULL COMMENT 'Thứ tự thực hiện của process',
                          PRIMARY KEY
                      (
                          process_id
                      ),
                          UNIQUE KEY uq_process_name
                      (
                          process_name
                      )
                          ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE =utf8mb4_general_ci
                      """))

    conn.execute(text("""
                      CREATE TABLE IF NOT EXISTS etl_log
                      (
                          etl_id
                          INT
                          NOT
                          NULL
                          AUTO_INCREMENT,
                          batch_id
                          VARCHAR
                      (
                          50
                      ) NOT NULL,
                          process_id INT NOT NULL,
                          source_table VARCHAR
                      (
                          50
                      ) NULL,
                          target_table VARCHAR
                      (
                          50
                      ) NULL,
                          records_inserted INT NULL DEFAULT 0,
                          records_updated INT NULL DEFAULT 0,
                          records_skipped INT NULL DEFAULT 0,
                          error_message VARCHAR
                      (
                          100
                      ) NULL,
                          status ENUM
                      (
                          'started',
                          'success',
                          'failed'
                      ) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT 'started',
                          start_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                          end_time TIMESTAMP NULL DEFAULT NULL,
                          PRIMARY KEY
                      (
                          etl_id
                      ) USING BTREE,
                          KEY fk_etl_log_process
                      (
                          process_id
                      )
                          ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE =utf8mb4_general_ci ROW_FORMAT= Dynamic
                      """))

    # Kiểm tra và thêm foreign key nếu chưa có
    fk_exists = conn.execute(
        text("""
             SELECT COUNT(*)
             FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
             WHERE CONSTRAINT_SCHEMA = :schema
               AND TABLE_NAME = 'etl_log'
               AND CONSTRAINT_NAME = 'fk_etl_log_process'
             """),
        {"schema": CONTROL_DB},
    ).scalar()

    if not fk_exists:
        try:
            conn.execute(text("""
                              ALTER TABLE etl_log
                                  ADD CONSTRAINT fk_etl_log_process
                                      FOREIGN KEY (process_id)
                                          REFERENCES process (process_id)
                                          ON DELETE RESTRICT
                                          ON UPDATE CASCADE
                              """))
        except Exception as e:
            # Nếu foreign key đã tồn tại hoặc có lỗi khác, bỏ qua
            pass


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


def control_log_finish(log_id, status="success", inserted=0, updated=0, skipped=0, error_message=None):
    """
    Cập nhật trạng thái cho process log tương ứng.
    """
    if not log_id:
        return
    engine = create_control_engine()
    with engine.begin() as conn:
        # Giới hạn error_message nếu quá dài
        if error_message and len(error_message) > 100:
            error_message = error_message[:97] + "..."

        conn.execute(
            text("""
                 UPDATE etl_log
                 SET status           = :status,
                     records_inserted = :inserted,
                     records_updated  = :updated,
                     records_skipped  = :skipped,
                     error_message    = :error_message,
                     end_time         = NOW()
                 WHERE etl_id = :etl_id
                 """),
            {
                "status": status,
                "inserted": inserted or 0,
                "updated": updated or 0,
                "skipped": skipped or 0,
                "error_message": error_message,
                "etl_id": log_id,
            },
        )


# ============================================
# EXTRACT
# ============================================
def extract_from_general():
    print("\n" + "=" * 60)
    print("BƯỚC 1: EXTRACT - Đọc dữ liệu từ bảng general")
    print("=" * 60)
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
    print("\n" + "=" * 60)
    print("BƯỚC 2: TRANSFORM - Làm sạch và chuẩn hóa dữ liệu")
    print("=" * 60)
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

    # Xử lý nguồn dữ liệu (VARCHAR)
    df['nguon'] = df['nguon'].fillna('CellphoneS')
    df['nguon'] = df['nguon'].astype(str).str.strip()

    # Xử lý giá - chuyển từ string sang decimal
    df['sale_price_vnd'] = df['sale_price_vnd'].apply(parse_price_to_decimal)

    # ============================================
    # CHUYỂN ĐỔI KIỂU DỮ LIỆU CHO CÁC CỘT
    # ============================================

    # Xử lý tất cả các cột text - chuyển sang string và xử lý NULL
    # Giữ nguyên format text nhưng chuẩn bị để chuyển đổi kiểu dữ liệu trong SQL
    for col in df.columns:
        if col not in ['brand', 'category']:
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
    print("\n" + "=" * 60)
    print("BƯỚC 3: LOAD - Nạp dữ liệu vào stg_products")
    print("=" * 60)
    engine = create_mysql_engine()
    try:
        # Sử dụng pandas to_sql để load dữ liệu
        df_to_load = df.copy()

        # Load vào staging (pandas sẽ tự động tạo bảng với kiểu dữ liệu TEXT)
        df_to_load.to_sql('stg_products', engine, if_exists='replace', index=False, chunksize=1000)

        # Cập nhật kiểu dữ liệu sau khi load (ALTER TABLE)
        print(" 🔄 Đang chuyển đổi kiểu dữ liệu...")
        with engine.begin() as conn:
            # Cập nhật các cột VARCHAR (text ngắn)
            varchar_updates = {
                'nguon': 'VARCHAR(100)',
                'brand': 'VARCHAR(50)',
                'category': 'VARCHAR(50)',
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

            # Cập nhật sale_price_vnd thành DECIMAL
            if 'sale_price_vnd' in df.columns:
                try:
                    conn.execute(text("""
                                      ALTER TABLE stg_products
                                          MODIFY COLUMN sale_price_vnd DECIMAL (15,2) NULL
                                      """))
                    print("   ✓ sale_price_vnd -> DECIMAL(15,2)")
                except Exception as e:
                    print(f"   ⚠️  Không thể chuyển sale_price_vnd sang DECIMAL: {e}")

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

    stg_df = stg_df.sort_values(['ten_san_pham'], ascending=[True])
    stg_df = stg_df.drop_duplicates(subset=['ten_san_pham'], keep='first')
    return stg_df


def fetch_current_dim_lookup(engine):
    """
    Lấy dữ liệu dim_product để phục vụ so sánh.
    Xử lý trường hợp có duplicate ten_san_pham bằng cách lấy bản ghi đầu tiên.
    """
    try:
        dim_current = pd.read_sql(
            """
            SELECT product_id, ten_san_pham
            FROM dim_product
            """,
            engine,
        )
    except Exception:
        # Trường hợp lần chạy đầu tiên chưa có dim_product
        return {}, pd.DataFrame()
    if dim_current.empty:
        return {}, dim_current
    dim_current['ten_san_pham'] = dim_current['ten_san_pham'].astype(str).str.strip()

    # Xử lý duplicate: nếu có nhiều bản ghi cùng ten_san_pham, lấy bản ghi đầu tiên
    if dim_current['ten_san_pham'].duplicated().any():
        dim_current = dim_current.drop_duplicates(subset=['ten_san_pham'], keep='first')

    current_lookup = dim_current.set_index('ten_san_pham').to_dict('index')
    return current_lookup, dim_current


def detect_dim_changes(stg_df, current_lookup):
    """
    So sánh dữ liệu stg mới với dim_product hiện tại → xác định insert/update.
    Chỉ insert các bản ghi mới (chưa tồn tại trong dim_product).
    """
    rows_to_insert = []
    rows_to_expire = []
    unchanged_rows = 0

    for _, row in stg_df.iterrows():
        product_key = str(row['ten_san_pham']).strip()
        if not product_key:
            continue

        existing = current_lookup.get(product_key)
        if not existing:
            # Bản ghi mới, cần insert
            row_dict = row.to_dict()
            row_dict['ten_san_pham'] = product_key
            rows_to_insert.append(row_dict)
        else:
            # Bản ghi đã tồn tại, bỏ qua
            unchanged_rows += 1
            continue

    return rows_to_insert, rows_to_expire, unchanged_rows


# ============================================
# LOAD TO DIMENSION TABLE
# ============================================
def load_to_dim():
    print("\n" + "=" * 60)
    print("BƯỚC 4: LOAD - Nạp dữ liệu vào dim_product")
    print("=" * 60)
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
        if rows_to_insert:
            insert_df = pd.DataFrame(rows_to_insert)
            # Loại bỏ các cột cũ nếu có trong DataFrame
            columns_to_exclude = {'Tên sản phẩm', 'Giá', 'Nguồn'}
            for col in columns_to_exclude:
                if col in insert_df.columns:
                    insert_df = insert_df.drop(columns=[col])

            for col in dim_columns_order:
                if col not in insert_df.columns:
                    insert_df[col] = None
            insert_df = insert_df[dim_columns_order]
            insert_df.to_sql('dim_product', engine, if_exists='append', index=False, chunksize=500)

    inserted_count = len(rows_to_insert)
    updated_count = len(rows_to_expire)
    print(
        f" ✅ Đã áp dụng SCD Type 2 cho dim_product – inserted: {inserted_count}, expired: {updated_count}, unchanged: {unchanged_rows}")
    return inserted_count, updated_count


def compare_staging_with_dim(target_date=None, sample_size=5):
    """
    So sánh dữ liệu mới nhất tại stg_products với dim_product (ngày cũ).
    """
    print("\n" + "=" * 60)
    print("SO SÁNH STG_PRODUCTS ↔ DIM_PRODUCT")
    print("=" * 60)
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
        "new_records": len(rows_to_insert),
        "unchanged": unchanged_rows,
    }

    print(f" 📌 Tổng dòng stg: {summary['total_stg']}")
    print(f" 📌 Số bản ghi dim hiện tại: {summary['dim_current']}")
    print(f" ➕ Bản ghi mới sẽ được insert: {summary['new_records']}")
    print(f" 💤 Bản ghi giữ nguyên: {summary['unchanged']}")

    if rows_to_insert:
        sample_df = pd.DataFrame(rows_to_insert[:sample_size])
        cols_to_show = [col for col in ['ten_san_pham', 'sale_price_vnd', 'brand'] if col in sample_df.columns]
        print("\n Ví dụ bản ghi sẽ được nạp:")
        print(sample_df[cols_to_show].to_string(index=False))
    else:
        print("\n ✅ Không có sự khác biệt giữa ngày mới và dữ liệu dim hiện tại.")

    return summary


# ============================================
# SYNC DATE_KEY + DIM
# ============================================
def sync_date_key_and_dim(rebuild_dim=True):
    """
    Đơn giản hóa: chỉ load vào dim_product, không còn sync date_key nữa.
    """
    if rebuild_dim:
        return load_to_dim()
    return 0, 0


# ============================================
# MAIN ETL PROCESS
# ============================================
def run_etl(simulated_date=None, stage_only=False, auto_compare=False):
    print("BẮT ĐẦU QUY TRÌNH ETL: GENERAL → STG_PRODUCTS → DIM_PRODUCT")
    batch_id = f"batch_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    control_logs = {
        "transform": None,
        "load_staging": None,
        "load_dwh": None,
    }
    try:
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
        error_msg = str(e)
        for key, log_id in control_logs.items():
            if log_id:
                control_log_finish(log_id, status="failed", error_message=error_msg)
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
