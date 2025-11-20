import json
import pymysql

try:
    # Kết nối MySQL
    conn = pymysql.connect(
        host='127.0.0.1',
        user='ngan',
        password='123',
        db='datawarehouse',
        charset='utf8mb4'
    )
    cursor = conn.cursor()

    # Tạo bảng nếu chưa có
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS general (
        id INT AUTO_INCREMENT PRIMARY KEY,
        json_data LONGTEXT
    )
    """)
    conn.commit()

    # Xóa dữ liệu cũ
    cursor.execute("DELETE FROM general")
    conn.commit()

    # Load JSON từ file
    with open('../crawed/cellphoneS.json', encoding='utf-8') as f:
        data = json.load(f)

    # Nếu JSON là dict, chuyển thành list
    if isinstance(data, dict):
        data = [data]

    # Debug: in 2 bản ghi đầu tiên
    print("Preview JSON data:", data[:2])

    # Insert dữ liệu JSON vào bảng
    for item in data:
        # Ép JSON thành chuỗi, escape dấu nháy kép
        json_str = json.dumps(item, ensure_ascii=False)
        cursor.execute("INSERT INTO general (json_data) VALUES (%s)", (json_str,))

    conn.commit()
    print("Đã load JSON thành công!")

    # Kiểm tra dữ liệu vừa load
    cursor.execute("SELECT id, json_data FROM general LIMIT 5")
    rows = cursor.fetchall()
    for row in rows:
        print(f"id: {row[0]}, URL: {json.loads(row[1]).get('URL')}")  # Lấy thuộc tính URL

except Exception as e:
    print("Lỗi:", e)

finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()
