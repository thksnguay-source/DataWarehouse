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


# WORKFLOW: HÀM HỖ TRỢ - RESOLVE SIMULATED DATETIME
def resolve_simulated_datetime(simulated_date):
    """
    Hỗ trợ parse ngày giả lập (ví dụ '21/11/2025') để đồng bộ xuyên suốt ETL.
    Hỗ trợ nhiều định dạng ngày: %d/%m/%Y, %Y-%m-%d, %d-%m-%Y, %d/%m/%Y %H:%M:%S, %Y%m%d
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


# WORKFLOW STEP 21.2: HÀM HỖ TRỢ - BUILD MYSQL COLUMN DEFINITION
def build_mysql_column_definition(col_name, data_type, max_length):
    """
    Xây dựng định nghĩa cột MySQL từ thông tin cột:
    - text → TEXT
    - varchar → VARCHAR(length)
    - datetime → DATETIME
    - int/bigint → INT
    - decimal → DECIMAL(10,2)
    """
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
    WORKFLOW STEP 21: ENSURE DIM_PRODUCT STRUCTURE
    Đảm bảo bảng dim_product tồn tại với đầy đủ cột theo schema của stg_products
    - Bước 21.1: Loại bỏ cột cũ: 'Tên sản phẩm', 'Giá', 'Nguồn'
    - Bước 21.2: Xây dựng column definitions từ columns_info    
    - Bước 21.3: CREATE TABLE IF NOT EXISTS dim_product
    - Bước 21.4: Kiểm tra cột hiện có
    - Bước 21.5: DROP cột cũ nếu tồn tại
    - Bước 21.6: ADD cột mới nếu thiếu
    - Bước 21.7: DROP unique index cũ
    - Bước 21.8: Tạo index idx_dim_product_ten
    """
    """
    WORKFLOW STEP 21.1: LOẠI BỎ CỘT CŨ
    Danh sách các cột cũ cần loại bỏ: 'Tên sản phẩm', 'Giá', 'Nguồn'
    (đã được thay thế bằng ten_san_pham, sale_price_vnd, nguon)
    """
    columns_to_exclude = {'Tên sản phẩm', 'Giá', 'Nguồn'}  # WORKFLOW STEP 21.1: Danh sách cột cũ

    # Lọc bỏ các cột không mong muốn
    filtered_columns_info = [  # WORKFLOW STEP 21.1: Lọc bỏ cột cũ
        (col_name, data_type, max_length)
        for col_name, data_type, max_length in columns_info
        if col_name not in columns_to_exclude
    ]

    """
    WORKFLOW STEP 21.2: XÂY DỰNG COLUMN DEFINITIONS
    Xây dựng định nghĩa cột MySQL từ columns_info đã lọc
    """
    column_definitions = {}
    for col_name, data_type, max_length in filtered_columns_info:
        column_definitions[col_name] = build_mysql_column_definition(col_name, data_type, max_length)  # WORKFLOW STEP 21.2: Xây dựng column definitions

    metadata_definitions = {}

    # Chuẩn bị danh sách cột để CREATE TABLE
    create_columns = (
            ["product_id INT AUTO_INCREMENT PRIMARY KEY"]  # Primary key
            + list(column_definitions.values())  # Các cột từ stg_products
            + [f"`{name}` {definition}" for name, definition in metadata_definitions.items()]  # Metadata columns (nếu có)
    )

    # conn.execute(
    #     text(
    #         f"""
    #         CREATE TABLE IF NOT EXISTS dim_product (
    #             {', '.join(create_columns)}
    #         ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci
    #     """
    #     )
    # )
    """
    WORKFLOW STEP 21.3: CREATE TABLE IF NOT EXISTS
    Gọi stored procedure hoặc tạo bảng dim_product nếu chưa tồn tại
    """
    columns_str = ", ".join(create_columns)

    conn.execute(  # WORKFLOW STEP 21.3: CREATE TABLE (qua stored procedure)
        text("CALL transform(:cols)"),
        {"cols": columns_str}
    )

    """
    WORKFLOW STEP 21.4: KIỂM TRA CỘT HIỆN CÓ
    Query INFORMATION_SCHEMA để lấy danh sách cột hiện tại của dim_product
    """
    existing_columns = {  # WORKFLOW STEP 21.4: Lấy danh sách cột hiện có
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

    """
    WORKFLOW STEP 21.5: DROP CỘT CŨ NẾU TỒN TẠI
    Loại bỏ các cột cũ: 'Tên sản phẩm', 'Giá', 'Nguồn' nếu chúng vẫn còn trong bảng
    """
    columns_to_drop = {'Tên sản phẩm', 'Giá', 'Nguồn'}  # WORKFLOW STEP 21.5: Danh sách cột cần DROP
    for col_to_drop in columns_to_drop:
        if col_to_drop in existing_columns:
            try:
                conn.execute(text(f"ALTER TABLE dim_product DROP COLUMN `{col_to_drop}`"))  # WORKFLOW STEP 21.5: DROP cột cũ
                existing_columns.discard(col_to_drop)
                print(f"   ✓ Đã loại bỏ cột cũ: {col_to_drop}")
            except Exception as e:
                print(f"   ⚠️  Không thể loại bỏ cột {col_to_drop}: {e}")

    """
    WORKFLOW STEP 21.6: ADD CỘT MỚI NẾU THIẾU
    Thêm các cột mới từ column_definitions nếu chưa tồn tại trong bảng
    """
    for col_name, col_def in column_definitions.items():
        if col_name not in existing_columns:
            conn.execute(text(f"ALTER TABLE dim_product ADD COLUMN {col_def}"))  # WORKFLOW STEP 21.6: ADD cột mới
            existing_columns.add(col_name)

    for col_name, col_def in metadata_definitions.items():
        if col_name not in existing_columns:
            conn.execute(text(f"ALTER TABLE dim_product ADD COLUMN `{col_name}` {col_def}"))
            existing_columns.add(col_name)

    """
    WORKFLOW STEP 21.7: DROP UNIQUE INDEX CŨ
    Loại bỏ unique index cũ nếu tồn tại
    """
    unique_exists = conn.execute(  # WORKFLOW STEP 21.7: Kiểm tra unique index cũ
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
        conn.execute(text("ALTER TABLE dim_product DROP INDEX unique_product"))  # WORKFLOW STEP 21.7: DROP unique index

    """
    WORKFLOW STEP 21.8: TẠO INDEX IDX_DIM_PRODUCT_TEN
    Tạo index trên cột ten_san_pham để tối ưu tìm kiếm
    """
    idx_exists = conn.execute(  # WORKFLOW STEP 21.8: Kiểm tra index đã tồn tại?
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
            conn.execute(text("CREATE INDEX idx_dim_product_ten ON dim_product (ten_san_pham)"))  # WORKFLOW STEP 21.8: Tạo index
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
    WORKFLOW STEP 9: PARSE PRICE TO DECIMAL
    Chuyển đổi giá từ string (có thể chứa ký tự đặc biệt như ₫, dấu chấm, phẩy) sang decimal.
    - Loại bỏ ký tự đặc biệt (₫, dấu cách)
    - Xử lý dấu phẩy/chấm (phân cách hàng nghìn/thập phân)
    - Convert to float và round to 2 decimals
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


def get_latest_general_load_status():
    """
    Lấy trạng thái mới nhất của bước load JSON → general.
    """
    engine = create_control_engine()
    with engine.begin() as conn:
        _ensure_control_tables(conn)
        row = (
            conn.execute(
                text(
                    """
                    SELECT etl_id, batch_id, status, source_table, target_table, end_time
                    FROM etl_log
                    WHERE target_table = :target_table
                    ORDER BY etl_id DESC
                    LIMIT 1
                    """
                ),
                {"target_table": "general"},
            )
            .mappings()
            .fetchone()
        )
        return dict(row) if row else None


def ensure_general_load_success():
    """
    WORKFLOW STEP 2: KIỂM TRA ĐIỀU KIỆN TIÊN QUYẾT
    Ngăn chạy ETL nếu lần load JSON → general gần nhất thất bại:
    - Lấy trạng thái mới nhất của bước load JSON → general
    - Nếu chưa có log → tiếp tục chạy ETL
    - Nếu status != 'success' → RuntimeError và dừng ETL
    - Nếu status = 'success' → tiếp tục ETL
    """
    latest_log = get_latest_general_load_status()  # WORKFLOW STEP 2: Lấy log mới nhất
    if not latest_log:
        print(" ⚠️ Chưa có log load JSON → general. Tiếp tục chạy ETL.")
        return None

    status = (latest_log.get("status") or "").strip().lower()
    if status not in {"success"}:  # WORKFLOW STEP 2: Kiểm tra status = 'success'?
        # WORKFLOW STEP 2.1: RuntimeError - Dừng ETL nếu load general thất bại
        raise RuntimeError(
            "Không thể chạy ETL vì lần load JSON → general gần nhất "
            f"(etl_id={latest_log['etl_id']}, batch_id={latest_log['batch_id']}) "
            f"có trạng thái '{latest_log.get('status')}'. Vui lòng xử lý lỗi trước."
        )

    # WORKFLOW STEP 2: Load general thành công → tiếp tục ETL
    print(
        f" ✅ Log load JSON → general gần nhất (etl_id={latest_log['etl_id']}, "
        f"batch_id={latest_log['batch_id']}) có trạng thái success."
    )
    return latest_log


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
    """
    WORKFLOW STEP 4: EXTRACT FROM GENERAL
    - Query: SELECT * FROM general
    - Đọc toàn bộ dữ liệu từ bảng general vào pandas DataFrame
    - Trả về DataFrame hoặc raise Exception nếu có lỗi
    """
    print("\n" + "=" * 60)
    print("BƯỚC 1: EXTRACT - Đọc dữ liệu từ bảng general")
    print("=" * 60)
    engine = create_mysql_engine()
    try:
        query = "SELECT * FROM general"  # WORKFLOW STEP 4: Query lấy toàn bộ dữ liệu
        df = pd.read_sql(query, engine)  # WORKFLOW STEP 4: Đọc dữ liệu vào DataFrame
        print(f" Đã đọc {len(df)} dòng từ bảng general")  # WORKFLOW STEP 4: Log số dòng đã đọc
        return df
    except Exception as e:
        print(f" Lỗi khi đọc dữ liệu: {e}")
        raise


# ============================================
# TRANSFORM
# ============================================
def transform_data(df, simulated_date=None):
    """
    WORKFLOW STEP 5-12: TRANSFORM DATA
    Làm sạch và chuẩn hóa dữ liệu từ bảng general
    """
    print("\n" + "=" * 60)
    print("BƯỚC 2: TRANSFORM - Làm sạch và chuẩn hóa dữ liệu")
    print("=" * 60)
    df = df.copy()
    crawl_dt = resolve_simulated_datetime(simulated_date) if simulated_date else datetime.now()

    """
    WORKFLOW STEP 5.1: LỌC DỮ LIỆU RÁC
    - dropna(subset=['Tên sản phẩm']): Loại bỏ dòng có Tên sản phẩm = NULL
    - Loại bỏ dòng có Tên sản phẩm = 'Không tìm thấy'
    - Loại bỏ dòng có Tên sản phẩm = chuỗi rỗng
    """
    initial_count = len(df)
    df = df.dropna(subset=['Tên sản phẩm'])  # WORKFLOW STEP 5.1: Loại bỏ NULL
    df = df[df['Tên sản phẩm'] != 'Không tìm thấy']  # WORKFLOW STEP 5.1: Loại bỏ 'Không tìm thấy'
    df = df[df['Tên sản phẩm'].astype(str).str.strip() != '']  # WORKFLOW STEP 5.1: Loại bỏ chuỗi rỗng
    print(f" 🔍 Loại bỏ {initial_count - len(df)} dòng dữ liệu rác")

    """
    WORKFLOW STEP 5.2: RENAME COLUMNS
    Chuyển đổi tên cột từ tiếng Việt có dấu sang snake_case:
    - 'Tên sản phẩm' → 'ten_san_pham'
    - 'Giá' → 'sale_price_vnd'
    - 'Nguồn' → 'nguon'
    """
    df.rename(columns={  # WORKFLOW STEP 5.2: Rename columns
        'Tên sản phẩm': 'ten_san_pham',
        'Giá': 'sale_price_vnd',
        'Nguồn': 'nguon'
    }, inplace=True)

    """
    WORKFLOW STEP 6: TRÍCH XUẤT BRAND
    Trích xuất brand từ tên sản phẩm dựa trên từ khóa trong tên
    - Sử dụng brands_dict với 17 brands
    - Nếu không tìm thấy → 'Other'
    """
    brands_dict = {  # WORKFLOW STEP 6: Dictionary 17 brands
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

    def extract_brand(name):  # WORKFLOW STEP 6: Hàm trích xuất brand
        if pd.isna(name) or name == 'nan' or str(name).strip() == '':
            return 'Other'
        n = str(name).upper()
        for k, v in brands_dict.items():
            if k in n:
                return v
        return 'Other'

    df['brand'] = df['ten_san_pham'].apply(extract_brand)  # WORKFLOW STEP 6: Áp dụng extract_brand

    """
    WORKFLOW STEP 7: PHÂN LOẠI CATEGORY
    Phân loại sản phẩm thành 3 loại:
    - Foldable: chứa 'FOLD', 'FLIP', 'GALAXY Z'
    - Tablet: chứa 'TAB' hoặc 'IPAD'
    - Smartphone: mặc định
    """
    def categorize(name):  # WORKFLOW STEP 7: Hàm phân loại category
        if pd.isna(name) or name == 'nan' or str(name).strip() == '':
            return 'Smartphone'
        n = str(name).upper()
        if any(x in n for x in ['FOLD', 'FLIP', 'GALAXY Z']):  # WORKFLOW STEP 7: Foldable
            return 'Foldable'
        if 'TAB' in n or 'IPAD' in n:  # WORKFLOW STEP 7: Tablet
            return 'Tablet'
        return 'Smartphone'  # WORKFLOW STEP 7: Default

    df['category'] = df['ten_san_pham'].apply(categorize)  # WORKFLOW STEP 7: Áp dụng categorize

    """
    WORKFLOW STEP 8: XỬ LÝ NGUỒN
    - fillna('CellphoneS'): Mặc định nguồn là 'CellphoneS' nếu NULL
    - Chuyển sang string và strip()
    """
    df['nguon'] = df['nguon'].fillna('CellphoneS')  # WORKFLOW STEP 8: Fill NULL với 'CellphoneS'
    df['nguon'] = df['nguon'].astype(str).str.strip()  # WORKFLOW STEP 8: Chuyển string và strip

    """
    WORKFLOW STEP 9: PARSE PRICE
    Chuyển đổi giá từ string sang decimal:
    - Loại bỏ ký tự đặc biệt (₫, dấu cách)
    - Xử lý dấu phẩy/chấm (phân cách hàng nghìn/thập phân)
    - Convert to float và round to 2 decimals
    """
    df['sale_price_vnd'] = df['sale_price_vnd'].apply(parse_price_to_decimal)  # WORKFLOW STEP 9: Parse price

    """
    WORKFLOW STEP 10: CHUẨN HÓA KIỂU DỮ LIỆU
    - Tất cả cột (trừ brand, category) → chuyển sang string
    - Thay thế 'nan', 'None', 'NaT', '<NA>' → None (NULL trong SQL)
    - Xử lý giá trị rỗng → None
    - Loại bỏ cột không cần thiết: id, created_at
    """
    # Xử lý tất cả các cột text - chuyển sang string và xử lý NULL
    for col in df.columns:
        if col not in ['brand', 'category']:
            df[col] = df[col].astype(str)  # WORKFLOW STEP 10: Chuyển sang string
            df[col] = df[col].replace(['nan', 'None', 'NaT', '<NA>'], None)  # WORKFLOW STEP 10: Thay nan → None
            # Xử lý các giá trị rỗng
            df[col] = df[col].apply(lambda x: None if str(x).strip() in ['', 'nan', 'None', 'NaT', '<NA>'] else x)  # WORKFLOW STEP 10: Xử lý rỗng

    # Loại bỏ cột không cần thiết (id và created_at không có trong stg_products)
    for col in ['id', 'created_at']:  # WORKFLOW STEP 10: Loại bỏ cột không cần thiết
        if col in df.columns:
            df.drop(columns=[col], inplace=True)

    """
    WORKFLOW STEP 11: SẮP XẾP CỘT
    Đảm bảo URL là cột đầu tiên trong DataFrame
    """
    if 'URL' in df.columns:  # WORKFLOW STEP 11: Sắp xếp cột - URL đầu tiên
        cols = ['URL'] + [c for c in df.columns if c != 'URL']
        df = df[cols]

    """
    WORKFLOW STEP 12: KẾT THÚC TRANSFORM
    - Log tổng số dòng sau transform
    - Log số cột và danh sách cột
    - Trả về DataFrame đã được làm sạch
    """
    print(f" ✅ Dữ liệu đã làm sạch. Tổng dòng: {len(df)}")  # WORKFLOW STEP 12: Log kết quả
    print(f" 📊 Số cột sau transform: {len(df.columns)}")
    print(f" 📋 Các cột: {', '.join(df.columns[:5])}... (tổng {len(df.columns)} cột)")
    return df


# ============================================
# LOAD TO STAGING
# ============================================
def load_to_staging(df):
    """
    WORKFLOW STEP 14-16: LOAD TO STAGING
    Nạp dữ liệu đã transform vào bảng stg_products trong MySQL
    """
    print("\n" + "=" * 60)
    print("BƯỚC 3: LOAD - Nạp dữ liệu vào stg_products")
    print("=" * 60)
    engine = create_mysql_engine()
    try:
        """
        WORKFLOW STEP 14.1: SAO CHÉP DATAFRAME
        Tạo bản sao của DataFrame để tránh thay đổi dữ liệu gốc
        """
        df_to_load = df.copy()  # WORKFLOW STEP 14.1: Sao chép DataFrame

        """
        WORKFLOW STEP 14.2: PANDAS TO_SQL()
        - Load vào bảng stg_products
        - if_exists='replace': Thay thế toàn bộ dữ liệu cũ
        - index=False: Không lưu index của DataFrame
        - chunksize=1000: Load theo từng batch 1000 dòng
        - Mặc định pandas tạo bảng với kiểu TEXT cho tất cả cột
        """
        df_to_load.to_sql('stg_products', engine, if_exists='replace', index=False, chunksize=1000)  # WORKFLOW STEP 14.2: Load to SQL

        """
        WORKFLOW STEP 15: ALTER TABLE - CHUYỂN ĐỔI KIỂU DỮ LIỆU
        Cập nhật kiểu dữ liệu MySQL sau khi load để tối ưu storage và performance
        """
        print(" 🔄 Đang chuyển đổi kiểu dữ liệu...")
        with engine.begin() as conn:
            """
            WORKFLOW STEP 15.2: VARCHAR COLUMNS
            Định nghĩa các cột VARCHAR với độ dài phù hợp
            """
            varchar_updates = {  # WORKFLOW STEP 15.2: Dictionary cột VARCHAR
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

            """
            WORKFLOW STEP 15.1: SALE_PRICE_VND → DECIMAL(15,2)
            Chuyển đổi cột giá từ TEXT sang DECIMAL để tính toán chính xác
            """
            if 'sale_price_vnd' in df.columns:
                try:
                    conn.execute(text("""
                                      ALTER TABLE stg_products
                                          MODIFY COLUMN sale_price_vnd DECIMAL (15,2) NULL
                                      """))  # WORKFLOW STEP 15.1: Chuyển sale_price_vnd sang DECIMAL
                    print("   ✓ sale_price_vnd -> DECIMAL(15,2)")
                except Exception as e:
                    print(f"   ⚠️  Không thể chuyển sale_price_vnd sang DECIMAL: {e}")

            # WORKFLOW STEP 15.2: Cập nhật các cột VARCHAR
            for col, dtype in varchar_updates.items():
                if col in df.columns:
                    try:
                        col_name = f"`{col}`" if ' ' in col or '-' in col else col  # Backtick cho cột có dấu cách/ký tự đặc biệt
                        conn.execute(text(f"""
                            ALTER TABLE stg_products 
                            MODIFY COLUMN {col_name} {dtype} NULL
                        """))  # WORKFLOW STEP 15.2: ALTER TABLE cho từng cột VARCHAR
                        print(f"   ✓ {col} -> {dtype}")
                    except Exception as e:
                        print(f"   ⚠️  Không thể chuyển {col} sang {dtype}: {e}")

            """
            WORKFLOW STEP 15.3: TEXT COLUMNS GIỮ NGUYÊN
            Các cột như URL, mô tả dài, thông số kỹ thuật giữ nguyên kiểu TEXT
            """
            print("   ✓ Các cột khác giữ nguyên TEXT")  # WORKFLOW STEP 15.3: Giữ nguyên TEXT

        """
        WORKFLOW STEP 16: KẾT THÚC LOAD STAGING
        Log số dòng đã load và trả về số lượng
        """
        print(f" ✅ Đã load {len(df)} dòng vào bảng 'stg_products' với kiểu dữ liệu phù hợp")  # WORKFLOW STEP 16: Log kết quả
        return len(df)
    except Exception as e:
        print(f" ❌ Lỗi load vào staging: {e}")
        raise


def build_staging_snapshot(engine, target_date=None):
    """
    WORKFLOW STEP 22: BUILD STAGING SNAPSHOT
    Chuẩn hóa dữ liệu stg_products để so sánh/làm SCD:
    - Bước 22: Đọc toàn bộ dữ liệu từ stg_products
    - Bước 22.1: Kiểm tra stg_df empty?
    - Bước 22.2: Kiểm tra có cột ten_san_pham?
    - Bước 22.3: Lọc NULL trên ten_san_pham
    - Bước 22.4: Sort và deduplicate theo ten_san_pham
    """
    stg_df = pd.read_sql("SELECT * FROM stg_products", engine)  # WORKFLOW STEP 22: Đọc dữ liệu từ stg_products
    
    if stg_df.empty:  # WORKFLOW STEP 22.1: Kiểm tra stg_df empty?
        return stg_df

    if 'ten_san_pham' not in stg_df.columns:  # WORKFLOW STEP 22.2: Kiểm tra có cột ten_san_pham?
        raise KeyError("Cột 'ten_san_pham' bắt buộc để xác định khóa tự nhiên.")

    stg_df = stg_df[~stg_df['ten_san_pham'].isna()].copy()  # WORKFLOW STEP 22.3: Lọc NULL trên ten_san_pham
    if stg_df.empty:
        return stg_df

    stg_df = stg_df.sort_values(['ten_san_pham'], ascending=[True])  # WORKFLOW STEP 22.4: Sort theo ten_san_pham
    stg_df = stg_df.drop_duplicates(subset=['ten_san_pham'], keep='first')  # WORKFLOW STEP 22.4: Deduplicate
    return stg_df


def fetch_current_dim_lookup(engine):
    """
    WORKFLOW STEP 23: FETCH CURRENT DIM LOOKUP
    Lấy dữ liệu dim_product hiện tại để so sánh:
    - Query SELECT product_id, ten_san_pham FROM dim_product
    - Xử lý trường hợp bảng chưa tồn tại (lần chạy đầu tiên)
    - Chuẩn hóa ten_san_pham (strip)
    - Xử lý duplicate (lấy bản ghi đầu tiên)
    - Tạo lookup dictionary {ten_san_pham: {product_id: ...}}
    """
    try:
        dim_current = pd.read_sql(  # WORKFLOW STEP 23: Query dim_product
            """
            SELECT product_id, ten_san_pham
            FROM dim_product
            """,
            engine,
        )
    except Exception:
        # WORKFLOW STEP 23: Trường hợp lần chạy đầu tiên chưa có dim_product
        return {}, pd.DataFrame()
    
    if dim_current.empty:  # WORKFLOW STEP 23: Kiểm tra dim_current empty?
        return {}, dim_current
    
    dim_current['ten_san_pham'] = dim_current['ten_san_pham'].astype(str).str.strip()  # WORKFLOW STEP 23: Chuẩn hóa ten_san_pham

    # WORKFLOW STEP 23: Xử lý duplicate - nếu có nhiều bản ghi cùng ten_san_pham, lấy bản ghi đầu tiên
    if dim_current['ten_san_pham'].duplicated().any():
        dim_current = dim_current.drop_duplicates(subset=['ten_san_pham'], keep='first')

    current_lookup = dim_current.set_index('ten_san_pham').to_dict('index')  # WORKFLOW STEP 23: Tạo lookup dictionary
    return current_lookup, dim_current


def detect_dim_changes(stg_df, current_lookup):
    """
    WORKFLOW STEP 24: DETECT DIM CHANGES
    So sánh dữ liệu stg mới với dim_product hiện tại → xác định insert/update:
    - Lặp qua từng row trong stg_df
    - Lấy product_key = ten_san_pham.strip()
    - Kiểm tra tồn tại trong current_lookup
      + Nếu KHÔNG tồn tại → rows_to_insert (bản ghi mới)
      + Nếu tồn tại → unchanged_rows (bản ghi không thay đổi)
    - Trả về: rows_to_insert, rows_to_expire, unchanged_rows
    """
    rows_to_insert = []  # WORKFLOW STEP 24: Danh sách bản ghi mới cần insert
    rows_to_expire = []  # WORKFLOW STEP 24: Danh sách bản ghi cần expire (SCD Type 2, hiện tại = 0)
    unchanged_rows = 0  # WORKFLOW STEP 24: Số bản ghi không thay đổi

    for _, row in stg_df.iterrows():  # WORKFLOW STEP 24: Lặp qua từng row trong stg_df
        product_key = str(row['ten_san_pham']).strip()  # WORKFLOW STEP 24: Lấy product_key = ten_san_pham.strip()
        
        if not product_key:  # WORKFLOW STEP 24: Bỏ qua nếu product_key rỗng
            continue

        existing = current_lookup.get(product_key)  # WORKFLOW STEP 24: Kiểm tra tồn tại trong lookup
        if not existing:
            # WORKFLOW STEP 24: Bản ghi mới, cần insert
            row_dict = row.to_dict()
            row_dict['ten_san_pham'] = product_key
            rows_to_insert.append(row_dict)
        else:
            # WORKFLOW STEP 24: Bản ghi đã tồn tại, bỏ qua (không thay đổi)
            unchanged_rows += 1
            continue

    return rows_to_insert, rows_to_expire, unchanged_rows


# ============================================
# LOAD TO DIMENSION TABLE
# ============================================
def load_to_dim():
    """
    WORKFLOW STEP 19-33: LOAD TO DIMENSION TABLE
    Nạp dữ liệu từ stg_products vào dim_product với SCD Type 2
    """
    print("\n" + "=" * 60)
    print("BƯỚC 4: LOAD - Nạp dữ liệu vào dim_product")
    print("=" * 60)
    engine = create_mysql_engine()

    """
    WORKFLOW STEP 20: LẤY SCHEMA STG_PRODUCTS
    Query INFORMATION_SCHEMA.COLUMNS để lấy thông tin cột của bảng stg_products
    Bao gồm: COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
    """
    with engine.begin() as conn:
        result = conn.execute(  # WORKFLOW STEP 20: Query INFORMATION_SCHEMA
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
        columns_info = [tuple(row) for row in result.fetchall()]  # WORKFLOW STEP 20: Lưu columns_info
        
        """
        WORKFLOW STEP 20.1: KIỂM TRA CÓ COLUMNS_INFO?
        Nếu không tìm thấy bảng stg_products → return 0, 0 và dừng
        """
        if not columns_info:  # WORKFLOW STEP 20.1: Kiểm tra có columns_info?
            print(" ❌ Không tìm thấy bảng stg_products")
            return 0, 0

        """
        WORKFLOW STEP 21: ENSURE DIM_PRODUCT STRUCTURE
        Đảm bảo bảng dim_product tồn tại và có đầy đủ cột theo schema của stg_products
        - Loại bỏ cột cũ: 'Tên sản phẩm', 'Giá', 'Nguồn'
        - Xây dựng column definitions
        - CREATE TABLE IF NOT EXISTS
        - DROP/ADD columns nếu cần
        - Tạo indexes
        """
        dim_columns_order = ensure_dim_product_structure(conn, columns_info)  # WORKFLOW STEP 21: Đảm bảo cấu trúc dim_product

    """
    WORKFLOW STEP 22: BUILD STAGING SNAPSHOT
    Chuẩn hóa dữ liệu stg_products:
    - Đọc toàn bộ dữ liệu từ stg_products
    - Lọc NULL trên ten_san_pham
    - Sort và deduplicate theo ten_san_pham
    """
    stg_df = build_staging_snapshot(engine)  # WORKFLOW STEP 22: Build staging snapshot
    
    """
    WORKFLOW STEP 22.1: KIỂM TRA STG_DF EMPTY?
    Nếu stg_products đang trống → bỏ qua load dim_product
    """
    if stg_df.empty:  # WORKFLOW STEP 22.1: Kiểm tra stg_df empty?
        print(" ⚠️ stg_products đang trống, bỏ qua load dim_product.")
        return 0, 0

    """
    WORKFLOW STEP 23: FETCH CURRENT DIM LOOKUP
    Lấy dữ liệu dim_product hiện tại để so sánh:
    - Query SELECT product_id, ten_san_pham FROM dim_product
    - Xử lý duplicate (lấy bản ghi đầu tiên)
    - Tạo lookup dictionary {ten_san_pham: {product_id: ...}}
    """
    current_lookup, _ = fetch_current_dim_lookup(engine)  # WORKFLOW STEP 23: Fetch current dim lookup
    
    """
    WORKFLOW STEP 24: DETECT DIM CHANGES
    So sánh dữ liệu stg mới với dim_product hiện tại:
    - Xác định bản ghi mới (rows_to_insert)
    - Xác định bản ghi không thay đổi (unchanged_rows)
    """
    rows_to_insert, rows_to_expire, unchanged_rows = detect_dim_changes(stg_df, current_lookup)  # WORKFLOW STEP 24: Detect changes

    """
    WORKFLOW STEP 24.1: KIỂM TRA CÓ THAY ĐỔI?
    Nếu không có rows_to_insert và rows_to_expire → không có thay đổi, return
    """
    if not rows_to_insert and not rows_to_expire:  # WORKFLOW STEP 24.1: Kiểm tra có thay đổi?
        print(" ✅ dim_product không có thay đổi mới.")
        return 0, 0

    """
    WORKFLOW STEP 25-31: TRANSACTION BEGIN → INSERT → COMMIT
    Bắt đầu transaction để insert dữ liệu mới vào dim_product
    """
    with engine.begin() as conn:  # WORKFLOW STEP 25: Transaction BEGIN
        if rows_to_insert:
            """
            WORKFLOW STEP 26-30: CHUẨN BỊ VÀ INSERT DỮ LIỆU
            - Bước 26: Tạo insert_df từ rows_to_insert
            - Bước 27: DROP cột cũ nếu có ('Tên sản phẩm', 'Giá', 'Nguồn')
            - Bước 28: Thêm cột thiếu = None
            - Bước 29: Sắp xếp cột theo dim_columns_order
            - Bước 30: INSERT vào dim_product (if_exists='append', chunksize=500)
            """
            insert_df = pd.DataFrame(rows_to_insert)  # WORKFLOW STEP 26: Tạo insert_df
            
            # Loại bỏ các cột cũ nếu có trong DataFrame
            columns_to_exclude = {'Tên sản phẩm', 'Giá', 'Nguồn'}  # WORKFLOW STEP 27: DROP cột cũ
            for col in columns_to_exclude:
                if col in insert_df.columns:
                    insert_df = insert_df.drop(columns=[col])

            for col in dim_columns_order:
                if col not in insert_df.columns:
                    insert_df[col] = None  # WORKFLOW STEP 28: Thêm cột thiếu = None
            insert_df = insert_df[dim_columns_order]  # WORKFLOW STEP 29: Sắp xếp cột
            
            insert_df.to_sql('dim_product', engine, if_exists='append', index=False, chunksize=500)  # WORKFLOW STEP 30: INSERT to dim_product
    
    # WORKFLOW STEP 31: Transaction COMMIT (tự động khi exit context manager)

    """
    WORKFLOW STEP 32: KẾT QUẢ
    Tính toán số lượng bản ghi:
    - inserted_count: Số bản ghi mới được insert
    - updated_count: Số bản ghi expired (SCD Type 2, nhưng hiện tại = 0)
    - unchanged_rows: Số bản ghi không thay đổi
    """
    inserted_count = len(rows_to_insert)  # WORKFLOW STEP 32: Tính inserted_count
    updated_count = len(rows_to_expire)  # WORKFLOW STEP 32: Tính updated_count
    print(
        f" ✅ Đã áp dụng SCD Type 2 cho dim_product – inserted: {inserted_count}, expired: {updated_count}, unchanged: {unchanged_rows}")  # WORKFLOW STEP 32: Log kết quả
    return inserted_count, updated_count


def compare_staging_with_dim(target_date=None, sample_size=5):
    """
    WORKFLOW STEP 17: SO SÁNH STAGING VỚI DIM_PRODUCT
    So sánh dữ liệu mới nhất tại stg_products với dim_product để xem có thay đổi gì:
    - Build staging snapshot
    - Fetch current dim lookup
    - Detect dim changes
    - Hiển thị summary: total_stg, dim_current, new_records, unchanged
    """
    print("\n" + "=" * 60)
    print("SO SÁNH STG_PRODUCTS ↔ DIM_PRODUCT")
    print("=" * 60)
    engine = create_mysql_engine()
    stg_snapshot = build_staging_snapshot(engine, target_date=target_date)  # WORKFLOW STEP 17: Build staging snapshot
    if stg_snapshot.empty:
        print(" ⚠️ Không có dữ liệu trong stg_products với điều kiện yêu cầu.")
        return {"total_stg": 0, "new_records": 0, "changed_records": 0}

    current_lookup, dim_current = fetch_current_dim_lookup(engine)  # WORKFLOW STEP 17: Fetch current dim lookup
    rows_to_insert, rows_to_expire, unchanged_rows = detect_dim_changes(stg_snapshot, current_lookup)  # WORKFLOW STEP 17: Detect changes

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
    WORKFLOW STEP 19: SYNC DATE KEY AND DIM
    Đơn giản hóa: chỉ load vào dim_product, không còn sync date_key nữa.
    - Nếu rebuild_dim = True → gọi load_to_dim()
    - Nếu rebuild_dim = False → return 0, 0
    """
    if rebuild_dim:
        return load_to_dim()  # WORKFLOW STEP 19: Gọi load_to_dim() → thực hiện STEP 20-33
    return 0, 0


# ============================================
# MAIN ETL PROCESS
# ============================================
def run_etl(simulated_date=None, stage_only=False, auto_compare=False):
    """
    WORKFLOW STEP 1: KHỞI TẠO ETL PROCESS
    - Tạo batch_id từ timestamp
    - Khởi tạo control_logs dictionary để theo dõi các process
    """
    print("BẮT ĐẦU QUY TRÌNH ETL: GENERAL → STG_PRODUCTS → DIM_PRODUCT")
    batch_id = f"batch_{datetime.now().strftime('%Y%m%d%H%M%S')}"  # WORKFLOW STEP 1.1: Tạo batch_id
    control_logs = {  # WORKFLOW STEP 1.2: Khởi tạo control_logs dict
        "transform": None,
        "load_staging": None,
        "load_dwh": None,
    }
    
    """
    WORKFLOW STEP 2: KIỂM TRA ĐIỀU KIỆN TIÊN QUYẾT
    - Kiểm tra log load JSON → general
    - Đảm bảo lần load gần nhất = success
    - Nếu thất bại → RuntimeError và dừng ETL
    """
    try:
        ensure_general_load_success()  # WORKFLOW STEP 2: Kiểm tra điều kiện tiên quyết
    except RuntimeError as blocker:
        print(f"❌ Dừng ETL: {blocker}")  # WORKFLOW STEP 2.1: Dừng ETL nếu điều kiện không thỏa
        return
    
    try:
        """
        WORKFLOW STEP 3: BẮT ĐẦU TRANSFORM PROCESS
        - Ghi log bắt đầu process transform vào control.etl_log
        - source_table = "general"
        - target_table = "pandas_dataframe"
        """
        control_logs["transform"] = control_log_start(  # WORKFLOW STEP 3: Bắt đầu Transform process
            "transform",
            batch_id,
            source_table="general",
            target_table="pandas_dataframe",
        )
        
        """
        WORKFLOW STEP 4: EXTRACT FROM GENERAL
        - Query: SELECT * FROM general
        - Đọc toàn bộ dữ liệu từ bảng general vào pandas DataFrame
        """
        df = extract_from_general()  # WORKFLOW STEP 4: Extract từ bảng general
        
        """
        WORKFLOW STEP 4.1: KIỂM TRA CÓ DỮ LIỆU?
        - Nếu DataFrame rỗng → log success với skipped=1 và dừng ETL
        - Nếu có dữ liệu → tiếp tục transform
        """
        if len(df) == 0:  # WORKFLOW STEP 4.1: Kiểm tra có dữ liệu?
            control_log_finish(control_logs["transform"], "success", skipped=1)
            print("  Không có dữ liệu để xử lý!")
            return
        
        """
        WORKFLOW STEP 5-12: TRANSFORM DATA
        - Bước 5.1: Lọc dữ liệu rác (dropna, loại 'Không tìm thấy', chuỗi rỗng)
        - Bước 5.2: Rename columns (Tên sản phẩm → ten_san_pham, Giá → sale_price_vnd, Nguồn → nguon)
        - Bước 6: Trích xuất Brand từ tên sản phẩm (17 brands)
        - Bước 7: Phân loại Category (Foldable/Tablet/Smartphone)
        - Bước 8: Xử lý nguồn (fillna, chuyển string)
        - Bước 9: Parse Price (chuyển string → decimal)
        - Bước 10: Chuẩn hóa kiểu dữ liệu (tất cả → string, xử lý NULL)
        - Bước 11: Sắp xếp cột (URL đầu tiên)
        - Bước 12: Kết thúc Transform, log success với số dòng inserted
        """
        df_clean = transform_data(df, simulated_date=simulated_date)  # WORKFLOW STEP 5-12: Transform data
        control_log_finish(control_logs["transform"], "success", inserted=len(df_clean))  # WORKFLOW STEP 12: Kết thúc Transform
        control_logs["transform"] = None

        """
        WORKFLOW STEP 13: BẮT ĐẦU LOAD STAGING
        - Ghi log bắt đầu process load_staging vào control.etl_log
        - source_table = "general"
        - target_table = "stg_products"
        """
        control_logs["load_staging"] = control_log_start(  # WORKFLOW STEP 13: Bắt đầu Load Staging
            "load_staging",
            batch_id,
            source_table="general",
            target_table="stg_products",
        )
        
        """
        WORKFLOW STEP 14-16: LOAD TO STAGING
        - Bước 14.1: Sao chép DataFrame
        - Bước 14.2: pandas to_sql() load vào stg_products (if_exists=replace, chunksize=1000)
        - Bước 15: ALTER TABLE chuyển đổi kiểu dữ liệu MySQL
          - Bước 15.1: sale_price_vnd → DECIMAL(15,2)
          - Bước 15.2: VARCHAR columns (nguon, brand, category, ten_san_pham, ...)
          - Bước 15.3: TEXT columns giữ nguyên (URL, mô tả dài, thông số kỹ thuật)
        - Bước 16: Kết thúc Load Staging, log success với số dòng inserted
        """
        inserted_stg = load_to_staging(df_clean)  # WORKFLOW STEP 14-16: Load to Staging
        control_log_finish(control_logs["load_staging"], "success", inserted=inserted_stg)  # WORKFLOW STEP 16: Kết thúc Load Staging
        control_logs["load_staging"] = None

        """
        WORKFLOW STEP 17: KIỂM TRA STAGE_ONLY FLAG
        - Nếu stage_only = True → dừng sau staging
        - Nếu auto_compare = True → so sánh staging với dim_product
        - Nếu stage_only = False → tiếp tục load dim_product
        """
        if stage_only:  # WORKFLOW STEP 17: Kiểm tra stage_only flag
            print(" ⏸️ Đã dừng theo yêu cầu sau bước load stg_products.")
            if auto_compare:  # WORKFLOW STEP 17.1: So sánh dữ liệu nếu auto_compare = True
                compare_staging_with_dim(target_date=simulated_date)  # WORKFLOW STEP 17: So sánh staging với dim
            return

        """
        WORKFLOW STEP 18: BẮT ĐẦU LOAD DWH
        - Ghi log bắt đầu process load_dwh vào control.etl_log
        - source_table = "stg_products"
        - target_table = "dim_product"
        """
        control_logs["load_dwh"] = control_log_start(  # WORKFLOW STEP 18: Bắt đầu Load DWH
            "load_dwh",
            batch_id,
            source_table="stg_products",
            target_table="dim_product",
        )
        
        """
        WORKFLOW STEP 19-33: SYNC DATE KEY AND DIM → LOAD TO DIM
        - Bước 19: Gọi sync_date_key_and_dim() → load_to_dim()
        - Bước 20: Lấy schema stg_products từ INFORMATION_SCHEMA.COLUMNS
        - Bước 21: Đảm bảo cấu trúc dim_product (CREATE TABLE, ADD/DROP columns, indexes)
        - Bước 22: Build staging snapshot (chuẩn hóa, sort, deduplicate)
        - Bước 23: Fetch current dim lookup (lấy dim_product hiện tại)
        - Bước 24: Detect dim changes (so sánh stg với dim → xác định insert/update)
        - Bước 25-31: Transaction BEGIN → INSERT → COMMIT
        - Bước 32: Kết quả (inserted_count, updated_count, unchanged_rows)
        - Bước 33: Kết thúc Load DWH, log success
        """
        inserted_dim, updated_dim = sync_date_key_and_dim(rebuild_dim=True)  # WORKFLOW STEP 19-33: Load to Dim
        control_log_finish(  # WORKFLOW STEP 33: Kết thúc Load DWH
            control_logs["load_dwh"],
            "success",
            inserted=inserted_dim,
            updated=updated_dim,
        )
        control_logs["load_dwh"] = None
        
        # ETL HOÀN TẤT THÀNH CÔNG
        print("\nETL HOÀN TẤT THÀNH CÔNG!")
        print(
            f"  • Batch ID: {batch_id}"
            f"\n  • Dòng đã xử lý (staging): {inserted_stg}"
            f"\n  • Dim_product - bản ghi mới: {inserted_dim}, bản ghi đóng: {updated_dim}"
            f"\n  • Trạng thái: SUCCESS"
        )
    except Exception as e:
        # Xử lý lỗi: đánh dấu tất cả process đang dang dở là failed
        print("❌ ETL THẤT BẠI!")
        print(f" Lỗi: {e}")
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