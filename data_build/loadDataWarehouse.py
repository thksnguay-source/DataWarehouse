# **********************************************************************
# FILE: loadDataWarehouse.py
# MỤC ĐÍCH: Chạy quá trình ETL từ Staging sang Data Warehouse.
# FIX: SỬ DỤNG CUSTOM CURSOR ĐỂ XỬ LÝ MULTI-RESULT SET (Lỗi Commands out of sync)
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

# --- IMPORT CẤU HÌNH TỪ FILE RIÊNG ---
try:
    from config.dtwh_config import DB_CONFIG, DB_DATAWH, DIMENSION_CONFIGS, DB_STAGING
except ImportError:
    print("LỖI: Không tìm thấy file config/dtwh_config.py hoặc các biến cần thiết.")
    sys.exit(1)


# ======================================================================
# LỚP CURSOR MỚI: Hỗ trợ next_result() trên C Extension (FIX lỗi _cmysql)
# ======================================================================
class CustomMultiResultCursor(CMySQLCursor):
    """
    Kế thừa từ CMySQLCursor để cho phép next_result() hoạt động.
    """

    def __init__(self, connection, *args, **kwargs):
        # FIX: Sử dụng *args, **kwargs để tránh lỗi 'unexpected keyword argument' (buffered, raw,...)
        super(CustomMultiResultCursor, self).__init__(connection, *args, **kwargs)

    def next_result(self):
        """
        Sửa lỗi AttributeError: 'CustomMultiResultCursor' object has no attribute '_cmysql'.
        Truy cập next_result() thông qua đối tượng connection (nơi chứa giao diện C).
        """
        # FIX: Truy cập _cmysql thông qua _connection
        return self._connection._cmysql.next_result()


def get_latest_staging_batch(conn_config):
    """Tìm Batch Staging mới nhất đã hoàn thành."""
    conn_staging = None
    try:
        conn_staging = mysql.connector.connect(**conn_config, database=DB_STAGING)
        cursor = conn_staging.cursor()

        sql_check = f"""
            SELECT batch_id, start_time
            FROM etl_log
            WHERE status = 'success'
            ORDER BY start_time DESC
            LIMIT 1
        """
        cursor.execute(sql_check)
        result = cursor.fetchone()
        cursor.close()

        if result:
            batch_id, start_time = result
            print(
                f"[{datetime.now().strftime('%H:%M:%S')}] Tìm thấy Batch Staging mới nhất: **{batch_id}** (Thời gian: {start_time})")
            return batch_id
        else:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] KHÔNG tìm thấy batch Staging thành công.")
            return None

    except mysql.connector.Error as err:
        print(f"LỖI KHI CHECK LOG STAGING: {err}")
        return None
    finally:
        if conn_staging and conn_staging.is_connected():
            conn_staging.close()


def run_etl_dimension(conn_datawh, dim_name, source_db, pk_column, batch_id):
    """
    Thực thi Stored Procedure SP_RUN_ETL_DIM.
    Sử dụng CustomMultiResultCursor để xử lý đa bộ kết quả.
    """
    cursor = None
    sql_call = f"CALL {DB_DATAWH}.SP_RUN_ETL_DIM('{batch_id}', '{dim_name}', '{source_db}', '{pk_column}')"

    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Đang chạy ETL cho: **{dim_name}**")
    print(f"SQL: {sql_call}")

    try:
        # 1. SỬ DỤNG CUSTOM CURSOR ĐÃ SỬA ĐỔI
        cursor = conn_datawh.cursor(cursor_class=CustomMultiResultCursor)
        cursor.execute(sql_call)

        results = None

        # 2. Đọc Result Set thống kê cuối cùng
        try:
            results = cursor.fetchone()
        except mysql.connector.Error as err:
            print(f"   -> Cảnh báo: Lỗi khi fetchone Result Set thống kê: {err}")

        # 3. Vòng lặp dọn dẹp TẤT CẢ các Result Set còn lại
        while cursor.next_result():
            try:
                cursor.fetchall()
            except mysql.connector.Error:
                continue

        # 4. Đóng Cursor
        cursor.close()

        # 5. Thực hiện COMMIT (Đã an toàn)
        conn_datawh.commit()

        # 6. Xử lý kết quả thống kê
        if results and len(results) == 2:
            inserted, updated = results
            print(f"   -> Kết quả: **INSERTED={inserted}**, **UPDATED={updated}**")
        else:
            print("   -> Cảnh báo: Không nhận được kết quả thống kê (inserted/updated) từ SP.")


    except mysql.connector.Error as err:
        print(f"   -> ❌ LỖI ETL cho {dim_name}: **{err}**")
        if conn_datawh and conn_datawh.is_connected():
            conn_datawh.rollback()
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
        print("\nKHÔNG CÓ DỮ LIỆU MỚI ĐỂ XỬ LÝ. KẾT THÚC QUÁ TRÌNH.")
        return

    dw_batch_id = staging_batch_id
    print(f"\n--- BẮT ĐẦU QUÁ TRÌNH ETL VÀO DATA WAREHOUSE (DW Batch: {dw_batch_id}) ---")

    conn_datawh = None
    try:
        # Sử dụng kết nối mặc định (C Extension)
        conn_datawh = mysql.connector.connect(**DB_CONFIG, database=DB_DATAWH)

        # Lặp qua các cấu hình, giả định DIMENSION_CONFIGS đã được sửa để dùng tên khóa đúng
        # (brand_key, category_key, source_key, product_key)
        for dim_name, source_db, pk_column in DIMENSION_CONFIGS:
            run_etl_dimension(conn_datawh, dim_name, source_db, pk_column, dw_batch_id)

    except mysql.connector.Error as err:
        print(f"LỖI KẾT NỐI TỔNG QUAN: {err}")
    finally:
        if conn_datawh and conn_datawh.is_connected():
            conn_datawh.close()
            print("\n--- KẾT THÚC QUÁ TRÌNH ETL ---")


if __name__ == "__main__":
    main_etl_process()