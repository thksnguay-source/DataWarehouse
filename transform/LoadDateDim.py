import csv
import pymysql

# Thông tin kết nối MySQL
HOST = "127.0.0.1"
USER = "ngan"
PASSWORD = "123"
DB = "datawarehouse"

CSV_FILE = "date_dim_without_quarter.csv"

try:
    # Kết nối MySQL
    conn = pymysql.connect(
        host=HOST,
        user=USER,
        password=PASSWORD,
        db=DB,
        charset='utf8mb4'
    )
    cursor = conn.cursor()

    # Tạo bảng nếu chưa có
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS date_dims (
        date_sk INT PRIMARY KEY,
        full_date DATE,
        day_since_2005 INT,
        month_since_2005 INT,
        day_of_week VARCHAR(20),
        calendar_month VARCHAR(20),
        calendar_year VARCHAR(4),
        calendar_year_month VARCHAR(10),
        day_of_month INT,
        day_of_year INT,
        week_of_year_sunday INT,
        year_week_sunday VARCHAR(10),
        week_sunday_start DATE,
        week_of_year_monday INT,
        year_week_monday VARCHAR(10),
        week_monday_start DATE,
        holiday VARCHAR(50),
        day_type VARCHAR(20)
    )
    """)
    conn.commit()
    print("Bảng `date_dims` đã sẵn sàng!")

    # Load dữ liệu từ CSV
    with open(CSV_FILE, encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            cursor.execute("""
                INSERT INTO date_dims (
                    date_sk, full_date, day_since_2005, month_since_2005, day_of_week,
                    calendar_month, calendar_year, calendar_year_month, day_of_month,
                    day_of_year, week_of_year_sunday, year_week_sunday, week_sunday_start,
                    week_of_year_monday, year_week_monday, week_monday_start, holiday, day_type
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                int(row['date_sk']),
                row['full_date'],
                int(row['day_since_2005']),
                int(row['month_since_2005']),
                row['day_of_week'],
                row['calendar_month'],
                row['calendar_year'],
                row['calendar_year_month'],
                int(row['day_of_month']),
                int(row['day_of_year']),
                int(row['week_of_year_sunday']),
                row['year_week_sunday'],
                row['week_sunday_start'],
                int(row['week_of_year_monday']),
                row['year_week_monday'],
                row['week_monday_start'],
                row['holiday'],
                row['day_type']
            ))
    conn.commit()
    print("Đã load dữ liệu CSV vào bảng `date_dims` thành công!")

except Exception as e:
    print("Lỗi:", e)

finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()
