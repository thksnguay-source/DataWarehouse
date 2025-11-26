# **********************************************************************
# FILE: dtwh_config.py
# MỤC ĐÍCH: Chứa thông tin cấu hình kết nối DB
# **********************************************************************

# Cấu hình kết nối MySQL
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'port': 3306
}

# Tên các database
DB_DATAWH = 'datawh'
DB_STAGING = 'staging'
DB_CONTROL = 'control'

# Cấu hình các Dimension: Key là Surrogate Key (SK) mới
# (Tên DIM, DB Nguồn (Staging), Surrogate Key (SK))
DIMENSION_CONFIGS = [
    ('dim_brand', DB_STAGING, 'brand_key'),
     ('dim_category', DB_STAGING, 'category_key'),
     ('dim_source', DB_STAGING, 'source_key'),
     ('dim_product',DB_STAGING, 'product_id'),
     ('date_dims',DB_STAGING, 'date_key')
]