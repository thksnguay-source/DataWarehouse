## Data Warehouse Transform (Hub-and-Spoke)

### 1) Yêu cầu môi trường
- Python 3.10+
- Cài đặt thư viện:
```bash
pip install -r requirement.txt
pip install pymysql
```

### 2) Cấu hình MySQL (ENV)
Thiết lập biến môi trường trước khi chạy:
- `MYSQL_HOST` (vd: localhost)
- `MYSQL_PORT` (vd: 3306)
- `MYSQL_USER` (vd: root)
- `MYSQL_PASSWORD`
- `MYSQL_DB` (vd: datawh)

### 3) Chạy transform
```bash
python etl_transform.py
```
Kết quả:
- Thư mục `staging/normalized_products.csv`
- Thư mục `hub/` chứa `DIM_Product.csv`, `DIM_Source.csv`, `DIM_Date.csv`, `DIM_Price_History.csv`
- Thư mục `logs/` ghi log run json

Nếu cấu hình ENV MySQL hợp lệ, script sẽ tạo/ghi các bảng:
- `DIM_Product`
- `DIM_Source`
- `DIM_Date`
- `DIM_Price_History`
- `ETL_Log`

### 4) Ghi chú thiết kế
- Khóa tự nhiên `product_id` sinh từ URL/tên (slug) để map sản phẩm.
- `brand` được suy luận từ URL/tên; có thể mở rộng mapping.
- `DIM_Date` tạo theo ngày chạy (`date_key = YYYYMMDD`).
- `DIM_Price_History` lưu giá bán tại thời điểm load; có thể mở rộng thêm `original_price`, `discount_percent` khi có dữ liệu.


