import csv
from datetime import datetime, timedelta
import calendar

OUT_FILE = "date_dim_without_quarter.csv"
NUMBER_OF_RECORD = 7670
START_DATE = datetime(2004, 12, 31)  # Start date
HOLIDAY = "Non-Holiday"

def is_weekend(day_name):
    return "Weekend" if day_name in ["Saturday", "Sunday"] else "Weekday"

def get_quarter(month):
    quarter = (month - 1) // 3 + 1
    return f"Q{quarter}"

def last_day_of_last_week(date):
    # Tìm ngày cuối tuần trước (sunday)
    weekday = date.weekday()  # Monday=0..Sunday=6
    start_of_week = date - timedelta(days=weekday+1)
    return start_of_week

def main():
    date_sk = 0
    month_since_2005 = 1
    day_since_2005 = 0
    quarter_since_2005_temp = 0
    quarter_temp = 1

    start_date = START_DATE
    start_date_for_month = start_date + timedelta(days=1)

    with open(OUT_FILE, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        # Header
        writer.writerow([
            "date_sk", "full_date", "day_since_2005", "month_since_2005", "day_of_week",
            "calendar_month", "calendar_year", "calendar_year_month", "day_of_month",
            "day_of_year", "week_of_year_sunday", "year_week_sunday", "week_sunday_start",
            "week_of_year_monday", "year_week_monday", "week_monday_start", "holiday", "day_type"
        ])

        for count in range(NUMBER_OF_RECORD):
            start_date += timedelta(days=1)
            date_sk += 1
            day_since_2005 += 1

            # Full Date
            full_date = start_date.strftime("%Y-%m-%d")

            # Month since 2005
            delta_months = (start_date.year - start_date_for_month.year) * 12 + (start_date.month - start_date_for_month.month)
            month_since_2005 = delta_months + 1

            # Day of Week
            day_of_week = start_date.strftime("%A")

            # Calendar Month
            calendar_month = start_date.strftime("%B")

            # Calendar Year
            calendar_year = start_date.strftime("%Y")

            # Calendar Year Month (short month)
            calendar_year_month = f"{calendar_year}-{start_date.strftime('%b')}"

            # Day of Month
            day_of_month = start_date.day

            # Day of Year
            day_of_year = start_date.timetuple().tm_yday

            # Week of Year Sunday
            week_of_year_sunday = int(start_date.strftime("%U")) + 1  # %U: week number starting Sunday
            year_sunday = start_date.year
            year_week_sunday = f"{year_sunday}-W{week_of_year_sunday:02d}"

            # Week Sunday Start
            week_sunday_start = (start_date - timedelta(days=start_date.weekday() + 1)).strftime("%Y-%m-%d")

            # Week of Year Monday
            week_of_year_monday = int(start_date.strftime("%W")) + 1  # %W: week number starting Monday
            year_week_monday = f"{start_date.year}-W{week_of_year_monday:02d}"

            # Week Monday Start
            monday_start = start_date - timedelta(days=start_date.weekday())
            week_monday_start = monday_start.strftime("%Y-%m-%d")

            # Quarter Since 2005
            month = start_date.month
            quarter = get_quarter(month)
            if int(quarter[1]) == quarter_temp:
                quarter_since_2005_temp += 1
                quarter_temp += 1
                if quarter_temp > 4:
                    quarter_temp = 1
            quarter_since_2005 = quarter_since_2005_temp

            # Day Type
            day_type = is_weekend(day_of_week)

            # Write row
            writer.writerow([
                date_sk, full_date, day_since_2005, month_since_2005, day_of_week,
                calendar_month, calendar_year, calendar_year_month, day_of_month,
                day_of_year, week_of_year_sunday, year_week_sunday, week_sunday_start,
                week_of_year_monday, year_week_monday, week_monday_start, HOLIDAY, day_type
            ])

    print(f"Đã tạo file {OUT_FILE} với {NUMBER_OF_RECORD} bản ghi.")

if __name__ == "__main__":
    main()
