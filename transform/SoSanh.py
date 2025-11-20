import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime

# MySQL connection
engine = create_engine("mysql+pymysql://root:@localhost:3306/datawarehouse?charset=utf8mb4", pool_pre_ping=True)

# 1. Load dữ liệu từ staging và date_dims
stg_df = pd.read_sql("SELECT * FROM stg_products", engine)
date_df = pd.read_sql("SELECT date_sk, full_date FROM date_dims", engine)

# 2. Chuẩn hóa kiểu dữ liệu ngày
stg_df['Ngày_crawl'] = pd.to_datetime(stg_df['Ngày_crawl']).dt.date
date_df['full_date'] = pd.to_datetime(date_df['full_date']).dt.date

# 3. Map date_sk dựa vào full_date
# Tạo dictionary từ full_date -> date_sk
date_map = dict(zip(date_df['full_date'], date_df['date_sk']))

# Áp dụng map để thay date_key
stg_df['date_key'] = stg_df['Ngày_crawl'].map(date_map)

# Kiểm tra kết quả
print(stg_df[['Ngày_crawl', 'date_key']].head())

# 4. Ghi trở lại vào stg_products
# Nếu muốn update toàn bộ bảng
stg_df.to_sql('stg_products', engine, if_exists='replace', index=False, chunksize=1000)
print("Đã cập nhật date_key dựa trên full_date thành công!")
