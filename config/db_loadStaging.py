
import os
    #connect db staging
def get_mysql_load_staging_url() -> str:
    host = "localhost"
    port = "3306"
    user = "root"
    password = ""
    db = "staging"

    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}"
    # connect db control
def get_mysql_control_url():
    host_control = "localhost"
    port_control = "3306"
    user_control = "root"
    password_control = ""
    db_control = "control"

    return f"mysql+pymysql://{user_control}:{password_control}@{host_control}:{port_control}/{db_control}"