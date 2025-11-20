import pandas as pd
import pymysql
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
from datetime import datetime

# =========================
# ⚙️ CẤU HÌNH KẾT NỐI DATABASE
# =========================
DB_USER = "root"
DB_PASS = ""
DB_HOST = "localhost"
DB_PORT = 3306

PRODUCT_MART_DB = "data_mart_prod"
PRICE_MART_DB = "data_mart_price"

# Tạo Engines (Giữ nguyên)
try:
    engine_prod = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{PRODUCT_MART_DB}")
    engine_price = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{PRICE_MART_DB}")
    print(f"✅ Kết nối thành công tới cả hai Data Marts.")
except Exception as e:
    print(f"⛔ LỖI KẾT NỐI DB. Lỗi: {e}")
    exit()

# --------------------------------------------------------------------
# A. PHÂN TÍCH VÀ VẼ CHART 1: GIÁ TRUNG BÌNH THEO BRAND (TỪ PRODUCT MART)
# --------------------------------------------------------------------

print("\n--- Bắt đầu Chart 1: Giá Trung bình theo Brand (Product Mart) ---")

try:
    # 1. Đọc dữ liệu từ data_mart_prod (Giữ nguyên)
    fact_product_df = pd.read_sql("SELECT brand_key, price FROM fact_product", engine_prod)
    dim_brand_df = pd.read_sql("SELECT brand_key, brand_name FROM dim_brand", engine_prod)

    # 2. Kết hợp và tính toán (Giữ nguyên)
    price_comparison_df = pd.merge(fact_product_df, dim_brand_df, on='brand_key', how='left')
    average_price_by_brand = price_comparison_df.groupby('brand_name')['price'].mean().reset_index()
    average_price_by_brand = average_price_by_brand.sort_values(by='price', ascending=False)

    # 3. Tạo biểu đồ cột (Bar Chart) - ĐÃ SỬA CẢNH BÁO
    plt.figure(figsize=(12, 6))
    sns.barplot(
        x='brand_name',
        y='price',
        # 🚨 THÊM HUE VÀ ẨN LEGEND ĐỂ KHẮC PHỤC CẢNH BÁO
        hue='brand_name',
        legend=False,
        data=average_price_by_brand,
        palette='Set1'
    )

    # Thêm Giá trị vào cột (Giữ nguyên)
    for index, row in average_price_by_brand.iterrows():
        price_label = f'{row["price"]:,.0f}'
        plt.text(index, row['price'] + 1000, price_label, color='black', ha="center", fontsize=9)

    plt.title('Chart 1: Giá Trung Bình Sản Phẩm Theo Thương Hiệu', fontsize=16)
    plt.xlabel('Thương Hiệu', fontsize=12)
    plt.ylabel('Giá Trung Bình (VND)', fontsize=12)
    plt.xticks(rotation=45, ha='right')
    plt.grid(axis='y', linestyle='--', alpha=0.5)
    plt.tight_layout()
    print("✅ Đã tạo Biểu đồ 1.")

except Exception as e:
    print(f"⛔ LỖI XỬ LÝ CHART 1: {e}")

# --------------------------------------------------------------------
# B. PHÂN TÍCH VÀ VẼ CHART 2: BIẾN ĐỘNG GIÁ TRUNG BÌNH THEO NGÀY (TỪ PRICING MART)
# --------------------------------------------------------------------

print("\n--- Bắt đầu Chart 2: Biến động Giá Trung bình theo Ngày (Pricing Mart) ---")

try:
    # 1. Đọc dữ liệu từ data_mart_price (Giữ nguyên)
    fact_price_df = pd.read_sql("SELECT time_key, price, change_rate FROM fact_price", engine_price)
    dim_time_df = pd.read_sql("SELECT time_key, year, month, day_of_month FROM dim_time", engine_price)

    # 2. Kết hợp và tính toán (Giữ nguyên)
    fact_price_df['load_date'] = pd.to_datetime(fact_price_df['time_key'].astype(str), format='%Y%m%d')
    daily_analysis_df = fact_price_df.groupby('load_date').agg(
        avg_change_rate=('change_rate', 'mean'),
        avg_price=('price', 'mean')
    ).reset_index()
    daily_analysis_df = daily_analysis_df.sort_values('load_date')

    # 3. Tạo biểu đồ đường (Line Chart) cho Tỷ lệ thay đổi giá trung bình - ĐÃ SỬA CẢNH BÁO
    plt.figure(figsize=(12, 6))

    sns.lineplot(
        x='load_date',
        y='avg_change_rate',
        data=daily_analysis_df,
        marker='o',
        # 🚨 DÙNG COLOR THAY CHO PALETTE ĐỂ KHẮC PHỤC CẢNH BÁO
        color='mediumblue',
        linewidth=2
    )

    # Định dạng và hiển thị (Giữ nguyên)
    plt.title('Chart 2: Tỷ Lệ Thay Đổi Giá Trung Bình Hàng Ngày', fontsize=16)
    plt.xlabel('Ngày (Date)', fontsize=12)
    plt.ylabel('Tỷ lệ Thay đổi giá TB (%)', fontsize=12)
    plt.xticks(rotation=45)
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.tight_layout()
    print("✅ Đã tạo Biểu đồ 2.")

except Exception as e:
    print(f"⛔ LỖI XỬ LÝ CHART 2: {e}")

# --------------------------------------------------------------------
# C. HIỂN THỊ TẤT CẢ BIỂU ĐỒ
# --------------------------------------------------------------------
plt.show()