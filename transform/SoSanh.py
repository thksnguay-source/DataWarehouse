"""
Wrapper tiện ích: chạy sync_date_key_and_dim() từ etl_transform.
Giữ file này để ai quen chạy SoSanh.py vẫn sử dụng được.
"""

from etl_transform import sync_date_key_and_dim


if __name__ == "__main__":
    try:
        sync_date_key_and_dim()
    except Exception as exc:
        print(f"Chương trình kết thúc với lỗi: {exc}")
        raise
