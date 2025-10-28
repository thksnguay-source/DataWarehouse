import sys

print("Python:", sys.executable)
print("Python arch:", sys.maxsize > 2**32 and "64-bit" or "32-bit")

modules = ["selenium", "pandas", "sqlalchemy"]
for m in modules:
    try:
        __import__(m)
        print(f"{m} OK")
    except Exception as e:
        print(f"{m} lỗi: {e}")
