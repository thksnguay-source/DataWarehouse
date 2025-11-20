import json
import re
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text

# ============================================
# MYSQL CONNECTION
# ============================================
def get_mysql_url():
    return "mysql+pymysql://root:@localhost:3306/datawarehouse?charset=utf8mb4"

def create_mysql_engine():
    return create_engine(get_mysql_url(), pool_pre_ping=True)

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

def update_success_log(etl_id, inserted_count):
    if not etl_id:
        return
    engine = create_mysql_engine()
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE etl_log
            SET status='success',
                records_inserted=:cnt,
                end_time=NOW()
            WHERE etl_id = :id
        """), {"cnt": inserted_count, "id": etl_id})
    print(f" Đã cập nhật Log: Success (Inserted: {inserted_count})")

# ============================================
# EXTRACT
# ============================================
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
def transform_data(df):
    print("\n" + "="*60)
    print("BƯỚC 2: TRANSFORM - Làm sạch và chuẩn hóa dữ liệu")
    print("="*60)
    df = df.copy()

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

    # Metadata - Thêm thông tin ngày crawl (DATETIME)
    df['ngay_crawl'] = datetime.now()
    df['date_key'] = datetime.now().strftime("%Y%m%d")
    
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

# ============================================
# LOAD TO DIMENSION TABLE
# ============================================
def load_to_dim():
    print("\n" + "="*60)
    print("BƯỚC 4: LOAD - Nạp dữ liệu vào dim_product")
    print("="*60)
    engine = create_mysql_engine()
    
    with engine.begin() as conn:
        # Lấy danh sách tất cả các cột từ stg_products
        result = conn.execute(text("""
            SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'datawarehouse' 
            AND TABLE_NAME = 'stg_products'
            ORDER BY ORDINAL_POSITION
        """))
        
        columns_info = result.fetchall()
        if not columns_info:
            print(" ❌ Không tìm thấy bảng stg_products")
            return 0
        
        # Xây dựng câu lệnh CREATE TABLE với tất cả các cột từ stg_products
        # Thêm product_id làm PRIMARY KEY
        column_definitions = ["product_id INT AUTO_INCREMENT PRIMARY KEY"]
        
        for col_name, data_type, max_length in columns_info:
            # Luôn escape tên cột với backtick để tránh lỗi với tên đặc biệt
            col_name_escaped = f"`{col_name}`"
            
            # Chuyển đổi kiểu dữ liệu phù hợp
            if data_type == 'text':
                col_def = f"{col_name_escaped} TEXT"
            elif data_type == 'varchar':
                length = f"({max_length})" if max_length else "(255)"
                col_def = f"{col_name_escaped} VARCHAR{length}"
            elif data_type == 'datetime':
                col_def = f"{col_name_escaped} DATETIME"
            elif data_type == 'int':
                col_def = f"{col_name_escaped} INT"
            elif data_type == 'decimal':
                col_def = f"{col_name_escaped} DECIMAL(10,2)"
            else:
                col_def = f"{col_name_escaped} {data_type.upper()}"
            
            column_definitions.append(col_def)
        
        # Drop và tạo lại bảng để đảm bảo cấu trúc đúng
        # (Vì CREATE TABLE IF NOT EXISTS không thay đổi cấu trúc nếu bảng đã tồn tại)
        conn.execute(text("DROP TABLE IF EXISTS dim_product"))
        
        # Tạo bảng dim_product với tất cả các cột giống stg_products
        create_table_sql = f"""
            CREATE TABLE dim_product (
                {', '.join(column_definitions)}
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci
        """
        
        conn.execute(text(create_table_sql))
        print("   ✓ Đã tạo bảng dim_product với tất cả các cột từ stg_products")
        
        # Không cần TRUNCATE vì đã DROP và tạo lại bảng
        
        # Lấy danh sách tất cả các cột (trừ product_id vì là AUTO_INCREMENT)
        # Luôn escape tất cả tên cột với backtick
        all_columns = [f"`{col[0]}`" for col in columns_info]
        columns_str = ', '.join(all_columns)
        
        # Insert toàn bộ dữ liệu từ stg_products sang dim_product
        insert_sql = f"""
            INSERT INTO dim_product ({columns_str})
            SELECT {columns_str}
            FROM stg_products
        """
        
        result = conn.execute(text(insert_sql))
        inserted_count = result.rowcount
        
        # Thêm UNIQUE constraint cho ten_san_pham nếu chưa có
        try:
            # Kiểm tra xem constraint đã tồn tại chưa
            check_constraint = conn.execute(text("""
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS 
                WHERE TABLE_SCHEMA = 'datawarehouse' 
                AND TABLE_NAME = 'dim_product' 
                AND CONSTRAINT_NAME = 'unique_product'
            """))
            
            if check_constraint.scalar() == 0:
                conn.execute(text("""
                    ALTER TABLE dim_product 
                    ADD UNIQUE KEY unique_product (ten_san_pham)
                """))
                print("   ✓ Đã thêm UNIQUE constraint cho ten_san_pham")
        except Exception as e:
            # Nếu constraint đã tồn tại hoặc có lỗi, bỏ qua
            print(f"   ⚠️  Không thể thêm UNIQUE constraint: {e}")
    
    print(f" ✅ Đã load {inserted_count} dòng vào dim_product (toàn bộ dữ liệu từ stg_products)")
    return inserted_count

# ============================================
# MAIN ETL PROCESS
# ============================================
def run_etl():
    print("BẮT ĐẦU QUY TRÌNH ETL: GENERAL → STG_PRODUCTS → DIM_PRODUCT")
    etl_id, batch_id = start_etl_log()
    try:
        df = extract_from_general()
        if len(df) == 0:
            print("  Không có dữ liệu để xử lý!")
            return
        df_clean = transform_data(df)
        inserted_stg = load_to_staging(df_clean)
        inserted_dim = load_to_dim()
        update_success_log(etl_id, inserted_stg)
        print("\nETL HOÀN TẤT THÀNH CÔNG!")
        print(f"  • Batch ID: {batch_id}\n  • Dòng đã xử lý (staging): {inserted_stg}\n  • Dòng nạp vào dim: {inserted_dim}\n  • Trạng thái: SUCCESS")
    except Exception as e:
        print("❌ ETL THẤT BẠI!")
        print(f" Lỗi: {e}")
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
