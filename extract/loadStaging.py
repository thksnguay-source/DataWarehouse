import json
import sys
import os
from sqlalchemy import create_engine, text
from datetime import datetime

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.join(current_dir, '..')
sys.path.append(project_root)

from config.db_config import get_mysql_url, get_mysql_url_control

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

#Bước 2-2 Dùng batch_id mới của crawl
def get_latest_crawl_batch():
    engine = create_engine(get_mysql_url_control())
    sql = text("""
        SELECT batch_id
        FROM etl_log
        WHERE process_id = 1 AND status = 'success'
        ORDER BY start_time DESC
        LIMIT 1
    """)
    with engine.connect() as conn:
        row = conn.execute(sql).fetchone()
    if row:
        return row[0]
    else:
        raise Exception("Không tìm thấy batch Crawl thành công nào!")

#Bước 3-1 lấy JSON từ batch_id loadStaging
def get_files_from_staging_batch(batch_id):
    engine = create_engine(get_mysql_url_control())
    sql = text("""
        SELECT source_table
        FROM etl_log
        WHERE batch_id = :batch_id AND process_id = 2 AND status = 'success'
        ORDER BY start_time DESC
        LIMIT 2
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, {"batch_id": batch_id}).fetchall()
    return [r[0] for r in rows]

# Bước 3-2. Lấy file JSON từ batch_id crawl
def get_files_from_crawl_batch(batch_id):
    engine = create_engine(get_mysql_url_control())
    sql = text("""
        SELECT target_table
        FROM etl_log
        WHERE batch_id = :batch_id AND process_id = 1 AND status = 'success'
        ORDER BY start_time DESC
        LIMIT 2
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, {"batch_id": batch_id}).fetchall()
    return [r[0] for r in rows]

# Bước 4 clear bảng dữ liệu của general clear_table()
def clear_table():
    engine = create_engine(get_mysql_url(), echo=True)
    with engine.begin() as conn:
        conn.execute(text("CALL sp_clear_general();"))
    print("Đã xóa toàn bộ dữ liệu trong bảng general!")


# Bước 6 load dữ liệu file JSON
def load_json(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)



#Bước 8 insert_one() vào bảng general;
def insert_one(item, conn):
    columns = []
    values = []
    for json_key, col_name in field_map.items():
        columns.append(f"`{col_name}`")
        value = str(item.get(json_key, ''))
        value = value.replace("'", "''")
        values.append(f"'{value}'")

    sql = f"INSERT INTO general ({', '.join(columns)}) VALUES ({', '.join(values)})"

    try:
        result = conn.execute(text(sql))
        return result.lastrowid
    except Exception as e:
        print(f"Lỗi khi chèn bản ghi: {e}")
        return False


def etl_log(file_list):

    staging_engine = create_engine(get_mysql_url())
    control_engine = create_engine(get_mysql_url_control())

    clear_table()

    for file in file_list:
        with staging_engine.begin() as stg_conn, control_engine.begin() as ctl_conn:

            batch_id = f"batch_{datetime.now().strftime('%Y%m%d%H%M%S')}"
            start_time = datetime.now()

            #Bước 5 insert log vào etl_log
            sql_insert_log = text("""
                INSERT INTO etl_log (
                    batch_id, process_id, source_table, target_table,
                    records_inserted, records_updated, records_skipped,
                    error_message, status, start_time
                ) VALUES (
                    :batch_id, :process_id, :source_table, :target_table,
                    0, 0, 0, NULL,
                    'RUNNING', :start_time
                )
            """)

            result = ctl_conn.execute(sql_insert_log, {
                "batch_id": batch_id,
                "process_id": 2,
                "source_table": file,
                "target_table": "general",
                "start_time": start_time
            })

            etl_id = result.lastrowid

            try:
                data = load_json(file)
                inserted = skipped = updated = 0

                #Bước 7 Duyệt từng bản ghi JSON

                for item in data:
                    ok = insert_one(item, stg_conn)
                #Bước 9 Cập nhật counters (inserted/skipped);
                    if ok:
                        inserted += 1
                    else:
                        skipped += 1

                status = "success"
                error_message = None

            except Exception as e:
                status = "failed"
                inserted = skipped = updated = 0
                error_message = str(e)
                print(f"Lỗi file {file}: {e}")

            #Bước 10 Cập nhật log với status và số bản ghi;
            end_time = datetime.now()

            sql_update_log = text("""
                UPDATE etl_log
                SET status = :status,
                    records_inserted = :ins,
                    records_skipped = :skip,
                    records_updated = :updated,
                    end_time = :end
                WHERE etl_id = :id
            """)

            ctl_conn.execute(sql_update_log, {
                "status": status,
                "ins": inserted,
                "skip": skipped,
                "updated": updated,
                "end": end_time,
                "id": etl_id
            })
            # Bước 11: Hoàn tất ETL, in kết quả
            print(f"✔ {file} → {inserted} inserted")

if __name__ == "__main__":
    # Bước 1: Kiểm tra batch_id
    if len(sys.argv) == 1:

        batch_id = get_latest_crawl_batch()
        print(f"→ Dùng batch Crawl mới nhất: {batch_id}")

        files = get_files_from_crawl_batch(batch_id)
    else:

        batch_id = sys.argv[1]
        print(f"→ Chạy lại LoadStaging của batch_id: {batch_id}")
        # Bước 2-1: dùng batch_id cũ của Staging
        files = get_files_from_staging_batch(batch_id)
    print("→ File JSON:", files)

    etl_log(files)
