
import os

def get_mysql_url() -> str:
    host = "172.16.17.10"
    port = "3306"
    user = "linh"
    password = "123"
    db = "datawarehouse"

    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}"

