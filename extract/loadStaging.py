import json
from sqlalchemy import create_engine, text
# from config.db_loadStaging import get_mysql_load_staging_url, get_mysql_control_url
from datetime import datetime
from transform.etl_transforms import get_mysql_url,get_control_mysql_url



            # các trường trong general
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
    "ROM": "ROM",
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

                # --- Xóa dữ liệu cũ ---
def clear_table():
    engine = create_engine(get_mysql_url(), echo=True)
    with engine.begin() as conn:
        conn.execute(text("CALL sp_clear_general();"))
    print("Đã xóa toàn bộ dữ liệu cũ trong bảng general!")

                # --- Insert nhiều bản ghi ---
def insert_one(item, conn):
    columns = []
    values = []
    for json_key, col_name in field_map.items():
        columns.append(f"`{col_name}`")
        value = str(item.get(json_key, ''))  # nếu thiếu thì để ''
        value = value.replace("'", "''")     # escape nháy đơn
        values.append(f"'{value}'")
    sql = f"INSERT INTO general ({', '.join(columns)}) VALUES ({', '.join(values)})"
    try:
        result= conn.execute(text(sql))
        return result.lastrowid
    except Exception as e:
        print(f"Lỗi khi chèn bản ghi: {e}")
        return False

                # --- ETL process chính ---
def etl_log(file_list):
    staging_engine = create_engine(get_mysql_url())
    control_engine = create_engine(get_control_mysql_url())

    clear_table()

    for file in file_list:
        with staging_engine.begin() as stg_conn, control_engine.begin() as ctl_conn:

            # 1) GHI LOG BẮT ĐẦU
            batch_id = f"batch_{datetime.now().strftime('%Y%m%d%H%M%S')}"
            start_time = datetime.now()

            sql_insert_log = text("""
                INSERT INTO etl_log (
                    batch_id, process_id, source_table, target_table,
                    records_inserted, records_updated, records_skipped,
                    status, start_time
                ) VALUES (
                    :batch_id, :process_id, :source_table, :target_table,
                    0, 0, 0,
                    'RUNNING', :start_time
                )
            """)

            result = ctl_conn.execute(sql_insert_log, {
                "batch_id": batch_id,
                "process_id": 2,  # Load Staging
                "source_table": file,
                "target_table": "general",
                "start_time": start_time
            })

            etl_id = result.lastrowid

            # 2) XỬ LÝ FILE JSON
            try:
                data = load_json(file)
                records_inserted = 0
                records_skipped = 0
                records_updated = 0

                for item in data:
                    success = insert_one(item, stg_conn)
                    if success:
                        records_inserted += 1
                    else:
                        records_skipped += 1

                status = "SUCCESS"

            except Exception as e:
                print(f"Lỗi khi xử lý {file}: {e}")
                records_inserted = 0
                records_skipped = 0
                records_updated = 0
                status = "FAILED"

            # 3) UPDATE LOG KẾT THÚC
            end_time = datetime.now()
            sql_update_log = text("""
                UPDATE etl_log
                SET 
                    status = :status,
                    records_inserted = :records_inserted,
                    records_updated = :records_updated,
                    records_skipped = :records_skipped,
                    end_time = :end_time
                WHERE etl_id = :etl_id
            """)
            ctl_conn.execute(sql_update_log, {
                "status": status,
                "records_inserted": records_inserted,
                "records_updated": records_updated,
                "records_skipped": records_skipped,
                "end_time": end_time,
                "etl_id": etl_id
            })

            print(f"✔ {file} → {status} ({records_inserted} inserted)")

                    # --- Main ---
if __name__ == "__main__":
    files = ["cellphones.json", "thegioididong.json"]
    etl_log(files)
