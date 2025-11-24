@echo off
REM --- SETTINGS: Đặt đường dẫn cho Python và các script ---
SET PYTHON_EXE="D:\datawh\.venv\Scripts\python.exe"
SET CRAWLER_SCRIPT="D:\datawh\crawed\crawed.py"
SET TRANSFORM_SCRIPT="D:\datawh\transform\etl_transform.py"
SET LOG_FILE="D:\datawh\log\etl_log.txt"

REM --- KIỂM TRA & LOG KHỞI TẠO ---
REM Đảm bảo thư mục log tồn tại
if not exist "D:\datawh\log" mkdir "D:\datawh\log"

REM Bắt đầu ghi log
echo. >> %LOG_FILE%
echo ========================================================================= >> %LOG_FILE%
echo %date% %time% - ETL Process Started >> %LOG_FILE%
echo ========================================================================= >> %LOG_FILE%

REM --- BƯỚC 1: CHẠY CRAWLER SCRIPT (crawed.py) ---
echo Running crawled.py... >> %LOG_FILE%
REM Lệnh chạy Python: %PYTHON_EXE% là interpreter, %CRAWLER_SCRIPT% là file
%PYTHON_EXE% %CRAWLER_SCRIPT% >> %LOG_FILE% 2>&1

REM Kiểm tra mã thoát của crawler (errorlevel 1 nghĩa là có lỗi)
if errorlevel 1 (
    echo ERROR: crawled.py failed! HỦY TOÀN BỘ QUY TRÌNH ETL. >> %LOG_FILE%
    goto :end
)

REM --- BƯỚC 2: CHẠY TRANSFORM SCRIPT (etl_transform.py) ---
echo Running etl_transform.py... >> %LOG_FILE%
REM Lệnh chạy Python
%PYTHON_EXE% %TRANSFORM_SCRIPT% >> %LOG_FILE% 2>&1

REM Kiểm tra mã thoát của transformer
if errorlevel 1 (
    echo ERROR: etl_transform.py failed! HỦY TOÀN BỘ QUY TRÌNH ETL. >> %LOG_FILE%
    goto :end
)

:end
REM --- KẾT THÚC QUÁ TRÌNH ---
echo %date% %time% - ETL Process Finished >> %LOG_FILE%