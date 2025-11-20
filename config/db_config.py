"""
MySQL configuration helper for SQLAlchemy engine creation.

Environment variables (recommended):
- MYSQL_HOST
- MYSQL_PORT
- MYSQL_USER
- MYSQL_PASSWORD
- MYSQL_DB

Fallback: you can hardcode defaults below if needed.
"""
import os

def get_mysql_url() -> str:
    host = "localhost"
    port = "3306"
    user = "root"
    password = ""
    db = "dw"

    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}?charset=utf8mb4"

