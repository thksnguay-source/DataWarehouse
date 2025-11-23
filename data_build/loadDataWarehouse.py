# FILE: loadDataWarehouse.py
import mysql.connector
from datetime import datetime
import sys

# Import các thành phần cần thiết để subclass CMySQLCursor
try:
    from mysql.connector.cursor_cext import CMySQLCursor
except ImportError:
    # Dự phòng nếu C extension không có sẵn
    from mysql.connector.cursor import MySQLCursor as CMySQLCursor

# Import cấu hình từ file dtwh_config
try:
    from config.dtwh_config import DB_CONFIG, DB_DATAWH, DB_CONTROL, DIMENSION_CONFIGS, DB_STAGING
except ImportError:
    print("LỖI: Không tìm thấy file config/dtwh_config.py hoặc các biến cần thiết.")
    sys.exit(1)

# Process ID cho LoadDataWarehouse (Process hiện tại đang ghi log)
PROCESS_ID_DATAWH = 4
# Process ID cho Load_Staging (để tìm batch_id mới nhất)
PROCESS_ID_STAGING = 3

# LỚP CURSOR: Hỗ trợ next_result()
class CustomMultiResultCursor(CMySQLCursor):

    def __init__(self, connection, *args, **kwargs):
        super(CustomMultiResultCursor, self).__init__(connection, *args, **kwargs)

    def next_result(self):
        # Gọi phương thức C API để chuyển sang Result Set tiếp theo
        return self._connection._cmysql.next_result()

# HÀM GHI LOG VÀO DB_CONTROL
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

# 1. KHỞI TẠO VÀ KIỂM TRA NGUỒN

def get_latest_staging_batch(conn_config):
    # 1.1 Tìm Batch Staging Mới nhất (status=success)
    conn_staging = None
    try:
        conn_staging = mysql.connector.connect(**conn_config, database=DB_CONTROL)
        cursor = conn_staging.cursor()

        # Dò bản ghi mới nhất (LIMIT 1) của Process Staging (PROCESS_ID_STAGING = 3)
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

            # 1.2 Kiểm tra có tìm thấy Batch Staging Thành công?
            if status.lower() == 'success':
                return batch_id
            else:
                # 1.3.2 LỖI: Không có Batch failed' hoặc 'started
                print(
                    f"[{datetime.now().strftime('%H:%M:%S')}] LỖI: Batch Staging mới nhất có trạng thái **{status}**. Vui lòng kiểm tra lỗi Staging trước khi tiếp tục.")
                return None
        # 1.3.2 LỖI: Không có Batch
        else:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] KHÔNG tìm thấy batch Staging nào.")
            return None

    except mysql.connector.Error as err:
        print(f"LỖI KHI CHECK LOG STAGING: {err}")
        return None
    finally:
        if conn_staging and conn_staging.is_connected():
            conn_staging.close()

def log_start(conn_config, batch_id, source_table, target_table):
    # Ghi log bắt đầu với PROCESS_ID_DATAWH = 4, lưu cả source_table và trả về etl_id.
    sql = """
          INSERT INTO etl_log
              (batch_id, process_id, source_table, target_table, status, start_time)
          VALUES (%s, %s, %s, %s, 'started', NOW())
          """
    # Thêm source_table vào dữ liệu
    data = (batch_id, PROCESS_ID_DATAWH, source_table, target_table)
    etl_id = execute_log_query(conn_config, sql, data)
    return etl_id

def log_end(conn_config, etl_id, status, inserted=0, updated=0, error_message=None):
    # Ghi log kết thúc (success/failed) và cập nhật số liệu.
    sql = """
          UPDATE etl_log
          SET end_time         = NOW(),
              status           = %s,
              records_inserted = %s,
              records_updated  = %s,
              error_message     = %s -- Tạm dùng source_table để ghi error_message nếu có
          WHERE etl_id = %s \
          """
    if error_message is None:
        error_message = ''

    data = (status, inserted, updated, error_message, etl_id)
    execute_log_query(conn_config, sql, data)

# 2. XỬ LÝ ELT CHO TỪNG DIMENSION

def run_etl_dimension(conn_datawh, dim_name, source_db, pk_column, batch_id, db_config):

    # 2.1 Ghi Log START
    etl_id = log_start(db_config, batch_id, dim_name, dim_name)
    if etl_id is None:
        print(f"   -> LỖI: Không thể ghi log START cho {dim_name}. Bỏ qua ETL.")
        return
    cursor_standard = None
    cursor_multi = None
    inserted, updated, extracted = 0, 0, 0
    temp_table_name = f"temp_{dim_name}"

    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Đang chạy ETL cho: **{dim_name}**")

    try:
        cursor_standard = conn_datawh.cursor()

        # 2.2 CALL PROCEDURE SP_TRUNCATE_TEMP -> xóa sạch dữ liệu của bảng tạm
        print(f"[{datetime.now().strftime('%H:%M:%S')}]   -> BƯỚC 1: Gọi SP_TRUNCATE_TEMP...")
        sql_step1 = f"CALL {DB_DATAWH}.SP_TRUNCATE_TEMP('{temp_table_name}')"

        try:
            cursor_standard.execute(sql_step1)
            conn_datawh.commit()
        except mysql.connector.Error as err:
            error_msg = f"LỖI SQL BƯỚC 1 (TRUNCATE): {err}"
            raise Exception(error_msg)

        # 2.3 CALL PROCEDURE SP_LOAD_STAGING_TO_TEMP -> load dữ liệu mới từ staging vào bảng tạm
        print(f"[{datetime.now().strftime('%H:%M:%S')}]   -> BƯỚC 2: Gọi SP_LOAD_STAGING_TO_TEMP...")

        try:
            sql_step2_call = f"CALL {DB_DATAWH}.SP_LOAD_STAGING_TO_TEMP('{dim_name}', '{source_db}')"
            cursor_standard.execute(sql_step2_call)

            conn_datawh.commit()
            print(f"[{datetime.now().strftime('%H:%M:%S')}]   -> BƯỚC 2: Hoàn thành.")
            cursor_standard.close()

        except mysql.connector.Error as err:
            error_msg = f"LỖI SQL BƯỚC 2 (LOAD STAGING): {err}"
            raise Exception(error_msg)

        # 2.4 CALL PROCEDURE SP_RUN_ETL_DIM -> insert dữ liệu theo SCD Logic
        print(f"[{datetime.now().strftime('%H:%M:%S')}]   -> BƯỚC 3: Gọi SP_RUN_ETL_DIM (SCD/Insert)...")
        sql_step3_call = f"CALL {DB_DATAWH}.SP_RUN_ETL_DIM('{batch_id}', '{dim_name}', '{source_db}', '{pk_column}')"

        try:
            # Sử dụng CustomMultiResultCursor để đọc kết quả SELECT cuối cùng
            cursor_multi = conn_datawh.cursor(cursor_class=CustomMultiResultCursor)
            cursor_multi.execute(sql_step3_call)

            # 2.5 Đọc Result Set (inserted, updated, skipped) & Dọn dẹp Cursor
            results = cursor_multi.fetchone()

            # Vòng lặp dọn dẹp các Result Set còn lại
            while cursor_multi.next_result():
                try:
                    cursor_multi.fetchall()
                except mysql.connector.Error:
                    continue

        # Có Exception SQL/Python?
        # 2.5.1. SUCESS

            conn_datawh.commit()

            if results and len(results) == 2:
                inserted, updated = results

        except mysql.connector.Error as err:
            error_msg = f"LỖI SQL BƯỚC 3 (SCD/INSERT): {err}"
            raise Exception(error_msg)

        print(f"   -> Kết quả tổng: **INSERTED={inserted}**, **UPDATED={updated}**, **EXTRACTED={extracted}**")
        # Ghi Log END - Thành công
        log_end(db_config, etl_id, 'success', inserted, updated)

    # 2.5.2. FAILURE
    except Exception as e:
        # Xử lý khi có bất kỳ Exception nào (SQL hoặc Python) từ các khối try lồng nhau
        error_msg = str(e)
        if "LỖI SQL BƯỚC" not in error_msg:
            # Nếu không phải lỗi SQL đã được đánh dấu, đây là lỗi Python/khác
            error_msg = f"LỖI PYTHON/KHÁC: {e}"

        print(f"   -> LỖI ETL cho {dim_name}: **{error_msg}**")

        # Rollback transaction nếu kết nối còn hoạt động
        if conn_datawh and conn_datawh.is_connected():
            conn_datawh.rollback()

        # Ghi Log END - Thất bại
        log_end(db_config, etl_id, 'failed', inserted, updated, error_msg)

    finally:
        # Đảm bảo đóng tất cả các cursor
        if 'cursor_standard' in locals() and cursor_standard is not None:
            try:
                cursor_standard.close()
            except:
                pass
        if 'cursor_multi' in locals() and cursor_multi is not None:
            try:
                cursor_multi.close()
            except:
                pass

def main_etl_process():
    """
    Function chính để chạy toàn bộ quá trình ETL cho các Dimension.
    """
    staging_batch_id = get_latest_staging_batch(DB_CONFIG)
    # Không tìm thấy batch_id
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