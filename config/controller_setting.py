import os

def get_db_controller_url() -> str:
    host = "localhost"
    port = "3306"
    user = "root"
    password = ""
    db = "crawl_controller"

    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}"