"""
Script đồng bộ date_key trong stg_products và tự động rebuild dim_product.
Chạy mỗi khi cần đối chiếu date_key để đảm bảo dim_product luôn theo sát stg_products.
"""

import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

from etl_transform import load_to_dim

# MySQL connection
engine = create_engine("mysql+pymysql://root:@localhost:3306/datawarehouse?charset=utf8mb4", pool_pre_ping=True)

def sync_date_key_and_dim():
    # 1. Load dữ liệu từ staging và date_dims
    stg_df = pd.read_sql("SELECT * FROM stg_products", engine)
    date_df = pd.read_sql("SELECT date_sk, full_date FROM date_dims", engine)

    if stg_df.empty:
        print("⚠️ stg_products đang trống, bỏ qua cập nhật.")
        return

    if date_df.empty:
        raise ValueError("Bảng date_dims không có dữ liệu, không thể map date_key.")

    # 2. Xác định đúng tên cột ngày trong stg (ưu tiên snake_case mới)
    if 'ngay_crawl' in stg_df.columns:
        ngay_col = 'ngay_crawl'
    elif 'Ngày_crawl' in stg_df.columns:
        ngay_col = 'Ngày_crawl'
    else:
        raise KeyError("Không tìm thấy cột ngày crawl trong stg_products (mong đợi 'ngay_crawl').")

    # 3. Chuẩn hóa kiểu dữ liệu ngày
    stg_df[ngay_col] = pd.to_datetime(stg_df[ngay_col], errors='coerce').dt.date
    date_df['full_date'] = pd.to_datetime(date_df['full_date'], errors='coerce').dt.date

    # 4. Map date_sk dựa vào full_date
    date_map = dict(zip(date_df['full_date'], date_df['date_sk']))
    stg_df['date_key'] = stg_df[ngay_col].map(date_map)

    # 5. Kiểm tra những dòng chưa match được date_key
    missing = stg_df['date_key'].isna().sum()
    if missing:
        print(f"⚠️ Có {missing} dòng không match được date_key. Kiểm tra giá trị tại cột {ngay_col}.")
    else:
        print("✅ Tất cả dòng đã match được date_key.")

    print(stg_df[[ngay_col, 'date_key']].head())

    # 6. Ghi trở lại vào stg_products
    stg_df.to_sql('stg_products', engine, if_exists='replace', index=False, chunksize=1000)
    print("Đã cập nhật date_key dựa trên full_date thành công!")

    # 7. Tự động rebuild dim_product để phản ánh thay đổi từ stg_products
    print("Đang đồng bộ dim_product từ stg_products...")
    load_to_dim()
    print("Hoàn tất đồng bộ dim_product.")


if __name__ == "__main__":
    sync_date_key_and_dim()
