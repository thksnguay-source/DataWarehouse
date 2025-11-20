import json
import re
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
from config.db_config import get_mysql_url


# ============================================
# MYSQL ENGINE
# ============================================
def create_mysql_engine():
    """Tạo MySQL engine với UTF-8 encoding"""
    url = get_mysql_url()
    # Thêm charset vào URL
    if '?' in url:
        url += '&charset=utf8mb4'
    else:
        url += '?charset=utf8mb4'
    return create_engine(url, pool_pre_ping=True, connect_args={"charset": "utf8mb4"})


# ============================================
# ETL LOG FUNCTIONS
# ============================================
def check_running_etl():
    """Kiểm tra ETL đang chạy"""
    engine = create_mysql_engine()
    with engine.connect() as conn:
        result = conn.execute(text("SELECT * FROM etl_log WHERE status='running' LIMIT 1"))
        row = result.fetchone()
        if row:
            print(f"🔄 Found running ETL batch: {row[0]}. Reusing it.")
            return row[0]
        return None


def start_etl_log():
    """Bắt đầu ETL log mới"""
    engine = create_mysql_engine()
    with engine.begin() as conn:
        result = conn.execute(text("""
                                   INSERT INTO etl_log (batch_id, source_table, target_table, status)
                                   VALUES (:batch_id, :source_table, :target_table, 'running')
                                   """), {
                                  "batch_id": f"batch_{datetime.now().strftime('%Y%m%d%H%M%S')}",
                                  "source_table": "stg_products",
                                  "target_table": "dim_*"
                              })
        etl_id = result.lastrowid
    print(f" Started new ETL batch: {etl_id}")
    return etl_id


def update_etl_log(etl_id, inserted=0, updated=0, skipped=0, status="running", error_msg=None):
    """Cập nhật ETL log"""
    engine = create_mysql_engine()
    with engine.begin() as conn:
        conn.execute(text("""
                          UPDATE etl_log
                          SET records_inserted=:inserted,
                              records_updated=:updated,
                              records_skipped=:skipped,
                              status=:status,
                              end_time=IF(:status <> 'running', NOW(), NULL)
                          WHERE etl_id = :etl_id
                          """), {
                         "inserted": inserted,
                         "updated": updated,
                         "skipped": skipped,
                         "status": status,
                         "etl_id": etl_id
                     })


# ============================================
# EXTRACT - Load JSON
# ============================================
def load_json(path="cellphoneS.json"):
    """Load dữ liệu từ JSON file"""
    print("\n" + "=" * 60)
    print("BƯỚC 1: EXTRACT - Load JSON Data")
    print("=" * 60)

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    df = pd.DataFrame(data)
    print(f" Loaded {len(df)} records from {path}")

    return df


# ============================================
# TRANSFORM - Data Cleaning & Normalization
# ============================================
def transform_data(df):
    """Transform dữ liệu theo chuẩn ETL"""
    print("\n" + "=" * 60)
    print("BƯỚC 2: TRANSFORM - Clean & Normalize Data")
    print("=" * 60)

    df = df.copy()
    initial_count = len(df)

    # 2.1: DATA QUALITY - Loại bỏ records không hợp lệ
    invalid_mask = (
            (df['Tên sản phẩm'] == 'Không tìm thấy') |
            (df['Giá'] == 'Không tìm thấy') |
            (df['Tên sản phẩm'].isna())
    )
    invalid_count = invalid_mask.sum()
    df = df[~invalid_mask].copy()
    print(f" Removed {invalid_count} invalid records")
    print(f" Valid records: {len(df)}")

    # 2.2: Loại bỏ duplicates
    before_dedup = len(df)
    df = df.drop_duplicates(subset=["URL"], keep="first")
    dup_count = before_dedup - len(df)
    print(f" Removed {dup_count} duplicate URLs")
    print(f" Unique records: {len(df)}")

    # 2.3: EXTRACT BRAND

    def extract_brand(product_name):
        if pd.isna(product_name):
            return 'Unknown'

        brands = [
            'Samsung', 'iPhone', 'Xiaomi', 'OPPO', 'Realme',
            'Vivo', 'Nokia', 'TECNO', 'Itel', 'Masstel',
            'HONOR', 'Nothing', 'Sony', 'Google', 'OnePlus',
            'ASUS', 'Nubia', 'ZTE', 'Motorola', 'Lenovo',
            'Huawei', 'POCO', 'Tecno'
        ]

        product_upper = str(product_name).upper()
        for brand in brands:
            if brand.upper() in product_upper:
                return brand

        return 'Unknown'

    df['Brand'] = df['Tên sản phẩm'].apply(extract_brand)
    brand_counts = df['Brand'].value_counts()
    print(f" Extracted brands: {df['Brand'].nunique()} unique brands")
    print(f" Top brands: {', '.join(brand_counts.head(5).index.tolist())}")

    # 2.4: EXTRACT CATEGORY
    print("\n Step 2.4: Extract Category")

    def categorize_product(row):
        name = str(row.get('Tên sản phẩm', '')).upper()

        # Foldable phones
        if any(x in name for x in ['FOLD', 'FLIP', 'MAGIC V']):
            return 'Foldable'

        # Gaming phones
        if any(x in name for x in ['ROG', 'BLACK SHARK', 'LEGION', 'RED MAGIC']):
            return 'Gaming'

        # Default category
        return 'Smartphone'

    df['Category'] = df.apply(categorize_product, axis=1)
    cat_counts = df['Category'].value_counts()
    print(f" Categorized: {df['Category'].nunique()} categories")
    print(f" Categories: {cat_counts.to_dict()}")

    # 2.5: NORMALIZE PRICE

    def parse_price(price_text):
        if pd.isna(price_text):
            return None

        price_text = str(price_text).strip()

        # Case: "Liên hệ để báo giá"
        if 'liên hệ' in price_text.lower():
            return None

        # Extract số: "7.990.000đ" → 7990000
        numbers = re.findall(r'[\d.]+', price_text)
        if numbers:
            price_str = numbers[0].replace('.', '')
            try:
                return float(price_str)
            except:
                return None

        return None

    df['sale_price_vnd'] = df['Giá'].apply(parse_price)
    valid_prices = df['sale_price_vnd'].notna().sum()
    print(f" Parsed {valid_prices}/{len(df)} prices")
    if valid_prices > 0:
        print(f" Price range: {df['sale_price_vnd'].min():,.0f} - {df['sale_price_vnd'].max():,.0f} VND")
        print(f" Average price: {df['sale_price_vnd'].mean():,.0f} VND")

    # 2.6: ADD METADATA
    df['Nguồn'] = df.get('Nguồn', 'CellphoneS').fillna('CellphoneS')
    df['Ngày_crawl'] = pd.to_datetime(df.get('Ngày_crawl', datetime.now()))

    # Generate product_key
    df['product_key'] = range(1, len(df) + 1)

    print(f" Added metadata columns")
    print(f" Crawl date: {df['Ngày_crawl'].iloc[0]}")

    # 2.7: BUILD DIMENSION TABLES
    print("\n Step 2.7: Build Dimension Tables")

    # dim_date
    dim_date = pd.DataFrame({
        "date_key": df["Ngày_crawl"].dt.strftime("%Y%m%d").astype(int),
        "date": df["Ngày_crawl"],
        "year": df["Ngày_crawl"].dt.year,
        "month": df["Ngày_crawl"].dt.month,
        "day": df["Ngày_crawl"].dt.day
    }).drop_duplicates()
    print(f" dim_date: {len(dim_date)} records")

    # dim_source
    dim_source = pd.DataFrame({
        "source_key": range(1, df["Nguồn"].nunique() + 1),
        "source_name": df["Nguồn"].unique()
    })
    print(f" dim_source: {len(dim_source)} records")

    # dim_category
    dim_category = pd.DataFrame({
        "category_key": range(1, df["Category"].nunique() + 1),
        "category_name": df["Category"].unique()
    })
    print(f" dim_category: {len(dim_category)} records")

    # dim_brand
    dim_brand = pd.DataFrame({
        "brand_key": range(1, df["Brand"].nunique() + 1),
        "brand_name": df["Brand"].unique()
    })
    print(f" dim_brand: {len(dim_brand)} records")

    print("\n" + "=" * 60)
    print(" TRANSFORM COMPLETED!")
    print(f" Summary: {initial_count} → {len(df)} records (removed {initial_count - len(df)})")
    print("=" * 60)

    return df, dim_date, dim_source, dim_category, dim_brand


# ============================================
# LOAD - Load to MySQL
# ============================================
def load_to_mysql(df, table_name, if_exists='replace'):
    """Load DataFrame vào MySQL với UTF-8 encoding"""
    print(f"\n Loading {len(df)} records to table: {table_name}")

    engine = create_mysql_engine()

    try:
        # Đảm bảo tất cả string columns là UTF-8
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].astype(str)

        # Load to MySQL
        df.to_sql(table_name, engine, if_exists=if_exists, index=False)
        print(f" Successfully loaded to {table_name}")

    except Exception as e:
        print(f" Error loading to {table_name}: {e}")
        raise


# ============================================
# MAIN ETL PIPELINE
# ============================================
def run_etl(json_file="cellphoneS.json"):
    """Main ETL pipeline"""

    print("\n" + "=" * 70)
    print(" " * 20 + "🚀 ETL PIPELINE START")
    print("=" * 70)

    # Check running ETL
    run_id = check_running_etl()
    if not run_id:
        run_id = start_etl_log()

    try:
        # ========================================
        # EXTRACT
        # ========================================
        df_raw = load_json(json_file)

        # ========================================
        # TRANSFORM
        # ========================================
        df_product, dim_date, dim_source, dim_category, dim_brand = transform_data(df_raw)

        # ========================================
        # LOAD
        # ========================================
        print("\n" + "=" * 60)
        print("BƯỚC 3: LOAD - Load to MySQL Database")
        print("=" * 60)

        # Load staging table
        load_to_mysql(df_product, "stg_products", if_exists='replace')

        # Load dimension tables
        load_to_mysql(df_product, "dim_product", if_exists='replace')
        load_to_mysql(dim_date, "dim_date", if_exists='replace')
        load_to_mysql(dim_source, "dim_source", if_exists='replace')
        load_to_mysql(dim_category, "dim_category", if_exists='replace')
        load_to_mysql(dim_brand, "dim_brand", if_exists='replace')

        # ========================================
        # UPDATE ETL LOG
        # ========================================
        update_etl_log(
            run_id,
            inserted=len(df_product),
            updated=0,
            skipped=0,
            status="success"
        )

        print("\n" + "=" * 70)
        print(" " * 20 + " ETL PIPELINE COMPLETED!")
        print("=" * 70)
        print(f"\n📊 ETL Summary:")
        print(f"   - Batch ID: {run_id}")
        print(f"   - Records loaded: {len(df_product)}")
        print(f"   - Brands: {dim_brand['brand_name'].nunique()}")
        print(f"   - Categories: {dim_category['category_name'].nunique()}")
        print(f"   - Sources: {dim_source['source_name'].nunique()}")
        print(f"   - Status: SUCCESS ")

    except Exception as e:
        # Update log as failed
        update_etl_log(run_id, status="failed")

        print("\n" + "=" * 70)
        print(" " * 20 + " ETL PIPELINE FAILED!")
        print("=" * 70)
        print(f"Error: {e}")

        import traceback
        print("Traceback:")
        print(traceback.format_exc())

        raise


# ============================================
# USAGE
# ============================================
if __name__ == "__main__":
    # Chạy ETL với file JSON
    run_etl("../crawed/cellphoneS.json")

    # Hoặc với file khác:
    # run_etl("path/to/your/products.json")