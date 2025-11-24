import os

def get_mysql_url() -> str:
    host = "localhost"
    port = "3306"
    user = "root"
    password = ""
    db = "staging"

    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}?charset=utf8mb4"

def get_mysql_url_control() -> str:
    host = "localhost"
    port = "3306"
    user = "root"
    password = ""
    db = "control"

    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}?charset=utf8mb4"

