import json
from sqlalchemy import create_engine, text
from config.db_config import get_mysql_url

field_map = {
    "Tên sản phẩm": "Tên sản phẩm",
    "Giá": "Giá",
    "Công nghệ màn hình": "Công nghệ màn hình",
    "Cam sau": "Cam sau",
    "Cam trước": "Cam trước",
    "Chip": "Chip",
    "Sim": "Sim",
    "Hỗ trợ mạng": "Hỗ trợ mạng",
    "RAM": "RAM",
    "ROM": "Rom",
    "Pin": "Pin",
    "Hệ điều hành": "Hệ điều hành",
    "Kháng nước bụi": "Kháng nước bụi",
    "URL": "URL",
    "Nguồn": "Nguồn"
}


# --- Load JSON ---
def load_json(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_multiple_json(file_list):
    all_data = []
    for file in file_list:
        print(f"Đang đọc file: {file}")
        data = load_json(file)
        all_data.extend(data)
    print(f"Tổng số bản ghi đọc được: {len(all_data)}")
    return all_data


# --- Xóa dữ liệu cũ ---
def clear_table():
    engine = create_engine(get_mysql_url(), echo=True)
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE `general`;"))
        conn.commit()
    print("Đã xóa toàn bộ dữ liệu cũ trong bảng general!")


# --- Insert dữ liệu ---
def insert_data_simple(data):
    engine = create_engine(get_mysql_url(), echo=True)
    with engine.connect() as conn:
        for item in data:
            columns = ", ".join([f"`{col}`" for col in field_map.values()])
            values = ", ".join([f"'{item.get(json_key, '').replace('\'','\\\'')}'" for json_key in field_map.keys()])
            sql = f"INSERT INTO `general` ({columns}) VALUES ({values})"
            conn.execute(text(sql))
        conn.commit()


# --- ETL process chính ---
def etl_process(file_list):
    clear_table()
    data = load_multiple_json(file_list)
    insert_data_simple(data)

# --- Main ---
if __name__ == "__main__":
    files = ["cellphones.json", "thegioididong.json"]
    etl_process(files)
