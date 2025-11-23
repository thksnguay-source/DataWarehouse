# **********************************************************************
# FILE: loadDataWarehouse.py
# MỤC ĐÍCH: Chạy quá trình ETL từ Staging sang Data Warehouse.
# LOGIC GHI LOG: Được thực hiện hoàn toàn trong Python, ghi vào DB_CONTROL.
# LƯU Ý: Phải đảm bảo SP_RUN_ETL_DIM đã loại bỏ logic ghi log.
# **********************************************************************

import mysql.connector
from datetime import datetime
import sys

# Import các thành phần cần thiết để subclass CMySQLCursor
try:
    from mysql.connector.cursor_cext import CMySQLCursor
except ImportError:
    # Dự phòng nếu C extension không có sẵn
    from mysql.connector.cursor import MySQLCursor as CMySQLCursor

# --- IMPORT CẤU HÌNH TỪ FILE RIÊNG (GIẢ ĐỊNH) ---
try:
    # Đảm bảo các biến DB_CONFIG, DB_DATAWH, DB_CONTROL, DIMENSION_CONFIGS, DB_STAGING có sẵn
    # Ví dụ: from config.dtwh_config import DB_CONFIG, DB_DATAWH, DB_CONTROL, DIMENSION_CONFIGS, DB_STAGING
    from config.dtwh_config import DB_CONFIG, DB_DATAWH, DB_CONTROL, DIMENSION_CONFIGS, DB_STAGING
except ImportError:
    print("LỖI: Không tìm thấy file config/dtwh_config.py hoặc các biến cần thiết.")
    sys.exit(1)

# Process ID cho LoadDataWarehouse (Process hiện tại đang ghi log)
PROCESS_ID_DATAWH = 4
# Process ID cho Load_Staging (để tìm batch_id mới nhất)
PROCESS_ID_STAGING = 3

# ======================================================================
# LỚP CURSOR MỚI: Hỗ trợ next_result()
# ======================================================================
class CustomMultiResultCursor(CMySQLCursor):
    """
    Kế thừa từ CMySQLCursor để cho phép next_result() hoạt động,
    giúp xử lý kết quả trả về từ Stored Procedure.
    """

    def __init__(self, connection, *args, **kwargs):
        super(CustomMultiResultCursor, self).__init__(connection, *args, **kwargs)

    def next_result(self):
        return self._connection._cmysql.next_result()


# ======================================================================
# HÀM GHI LOG VÀO DB_CONTROL
# ======================================================================

def execute_log_query(conn_config, sql, data=None):
    """Thực thi một câu lệnh SQL (INSERT/UPDATE) trên DB_CONTROL."""
    conn_log = None
    try:
        conn_log = mysql.connector.connect(**conn_config, database=DB_CONTROL)
        cursor = conn_log.cursor()
        cursor.execute(sql, data)
        conn_log.commit()
        last_id = cursor.lastrowid
        cursor.close()
        return last_id
    except mysql.connector.Error as err:
        print(f"LỖI GHI LOG VÀO DB_CONTROL: {err}")
        return None
    finally:
        if conn_log and conn_log.is_connected():
            conn_log.close()


def log_start(conn_config, batch_id, target_table):
    """Ghi log bắt đầu với PROCESS_ID_DATAWH = 4 và trả về etl_id."""
    sql = """
          INSERT INTO etl_log
              (batch_id, process_id, target_table, status, start_time)
          VALUES (%s, %s, %s, 'started', NOW()) \
          """
    data = (batch_id, PROCESS_ID_DATAWH, target_table)
    etl_id = execute_log_query(conn_config, sql, data)
    return etl_id


def log_end(conn_config, etl_id, status, inserted=0, updated=0, error_message=None):
    """Ghi log kết thúc (success/failed) và cập nhật số liệu."""
    sql = """
          UPDATE etl_log
          SET end_time         = NOW(),
              status           = %s,
              records_inserted = %s,
              records_updated  = %s,
              source_table     = %s -- Tạm dùng source_table để ghi error_message nếu có
          WHERE etl_id = %s \
          """
    if error_message is None:
        error_message = ''

    data = (status, inserted, updated, error_message, etl_id)
    execute_log_query(conn_config, sql, data)


# ======================================================================
# 1. Khởi tạo & Kiểm tra Nguồn (KIỂM TRA CHẶT CHẼ)
# ======================================================================

def get_latest_staging_batch(conn_config):
    # 1.1 Tìm Batch Staging mới nhất (Bất kể trạng thái)
    conn_staging = None
    try:
        conn_staging = mysql.connector.connect(**conn_config, database=DB_CONTROL)
        cursor = conn_staging.cursor()

        # Dò bản ghi mới nhất (LIMIT 1) của Process Staging (PROCESS_ID_STAGING = 2)
        sql_check = f"""
            SELECT batch_id, status, start_time
            FROM etl_log
            WHERE process_id = {PROCESS_ID_STAGING} 
            ORDER BY start_time DESC
            LIMIT 1
        """
        cursor.execute(sql_check)
        result = cursor.fetchone()
        cursor.close()

        if result:
            batch_id, status, start_time = result
            print(
                f"[{datetime.now().strftime('%H:%M:%S')}] Tìm thấy Batch Staging mới nhất: **{batch_id}** (Trạng thái: {status}, Thời gian: {start_time})")

            # 1.2 Kiểm tra trạng thái của batch mới nhất
            if status.lower() == 'success':
                return batch_id
            else:
                # Nếu trạng thái là 'failed' hoặc 'started', dừng lại
                print(
                    f"[{datetime.now().strftime('%H:%M:%S')}] LỖI: Batch Staging mới nhất có trạng thái **{status}**. Vui lòng kiểm tra lỗi Staging trước khi tiếp tục.")
                return None
        else:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] KHÔNG tìm thấy batch Staging nào.")
            return None

    except mysql.connector.Error as err:
        print(f"LỖI KHI CHECK LOG STAGING: {err}")
        return None
    finally:
        if conn_staging and conn_staging.is_connected():
            conn_staging.close()


# ======================================================================
# 2. Xử lý ETL cho từng Dimension
# ======================================================================

def run_etl_dimension(conn_datawh, dim_name, source_db, pk_column, batch_id, db_config):
    # 2.0 Ghi Log START với process_id = 4
    etl_id = log_start(db_config, batch_id, dim_name)
    if etl_id is None:
        print(f"   -> LỖI: Không thể ghi log START cho {dim_name}. Bỏ qua ETL.")
        return

    cursor = None
    inserted, updated = 0, 0
    # Gọi SP_RUN_ETL_DIM (đã được dọn dẹp log)
    sql_call = f"CALL {DB_DATAWH}.SP_RUN_ETL_DIM('{batch_id}', '{dim_name}', '{source_db}', '{pk_column}')"

    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Đang chạy ETL cho: **{dim_name}**")
    print(f"SQL: {sql_call}")

    try:
        cursor = conn_datawh.cursor(cursor_class=CustomMultiResultCursor)
        cursor.execute(sql_call)

        results = None

        # 2.1 Đọc kết quả Result Set (inserted_count, updated_count)
        try:
            results = cursor.fetchone()
        except mysql.connector.Error as err:
            print(f"   -> Cảnh báo: Lỗi khi fetchone Result Set thống kê: {err}")

        # 2.2 Vòng lặp dọn dẹp các Result Set còn lại
        while cursor.next_result():
            try:
                cursor.fetchall()
            except mysql.connector.Error:
                continue

        cursor.close()
        conn_datawh.commit()  # Commit ETL thành công

        # 2.3 Ghi nhận kết quả
        if results and len(results) == 2:
            inserted, updated = results
            print(f"   -> Kết quả: **INSERTED={inserted}**, **UPDATED={updated}**")
        else:
            print("   -> Cảnh báo: Không nhận được kết quả thống kê (inserted/updated) từ SP.")

        # 2.4 Ghi Log END - Thành công
        log_end(db_config, etl_id, 'success', inserted, updated)

    except mysql.connector.Error as err:
        # Lỗi SQL
        error_msg = f"LỖI SQL KHI GỌI SP: {err}"
        print(f"   -> LỖI ETL cho {dim_name}: **{error_msg}**")
        if conn_datawh and conn_datawh.is_connected():
            conn_datawh.rollback()

        # Ghi Log END - Thất bại
        log_end(db_config, etl_id, 'failed', inserted, updated, error_msg)

    except Exception as e:
        # Lỗi Python/Khác
        error_msg = f"LỖI PYTHON/KHÁC: {e}"
        print(f"   -> LỖI ETL cho {dim_name}: **{error_msg}**")
        if conn_datawh and conn_datawh.is_connected():
            conn_datawh.rollback()

        # Ghi Log END - Thất bại
        log_end(db_config, etl_id, 'failed', inserted, updated, error_msg)

    finally:
        if cursor is not None:
            try:
                cursor.close()
            except:
                pass


def main_etl_process():
    """
    Chức năng chính để chạy toàn bộ quá trình ETL cho các Dimension.
    """
    staging_batch_id = get_latest_staging_batch(DB_CONFIG)

    if not staging_batch_id:
        print("\nKHÔNG THỂ TIẾN HÀNH ETL DATA WAREHOUSE. KẾT THÚC QUÁ TRÌNH.")
        return

    dw_batch_id = staging_batch_id
    print(f"\n--- BẮT ĐẦU QUÁ TRÌNH ETL VÀO DATA WAREHOUSE (DW Batch: {dw_batch_id}) ---")

    conn_datawh = None
    try:
        conn_datawh = mysql.connector.connect(**DB_CONFIG, database=DB_DATAWH)

        for dim_name, source_db, pk_column in DIMENSION_CONFIGS:
            run_etl_dimension(conn_datawh, dim_name, source_db, pk_column, dw_batch_id, DB_CONFIG)

    except mysql.connector.Error as err:
        print(f"LỖI KẾT NỐI TỔNG QUAN: {err}")
    finally:
        if conn_datawh and conn_datawh.is_connected():
            conn_datawh.close()
            print("\n--- KẾT THÚC QUÁ TRÌNH ETL ---")

if __name__ == "__main__":
    main_etl_process()