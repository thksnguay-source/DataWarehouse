#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from config.datamart_cofig import connect_db


# =========================
# 🔹 Load dữ liệu
# =========================
def get_data():
    # ... (giữ nguyên phần kết nối DB và truy vấn SQL) ...
    conn = connect_db("data_mart_prod")
    query = """
            SELECT p.product_id,
               p.product_name, 
               p.price,         
               p.cpu,           
               p.ram,           
               p.storage,       
               p.os,
               b.brand_name,
               d.full_date   
        FROM dim_product p
        LEFT JOIN dim_brand b ON p.brand_key = b.brand_key
        LEFT JOIN date_dims d ON p.date_key = d.date_sk
            """
    df = pd.read_sql(query, conn)
    conn.close()

    # --- BƯỚC CHUẨN HÓA DỮ LIỆU ---

    # 1. Chuẩn hóa brand_name (để an toàn, loại bỏ khoảng trắng, đổi sang chữ hoa đầu câu)
    if 'brand_name' in df.columns:
        df['brand_name'] = df['brand_name'].str.title().str.strip()

    # 2. Chuẩn hóa os: Gộp các phiên bản vào tên hệ điều hành chính
    if 'os' in df.columns:
        df['os'] = df['os'].str.lower().fillna('')

        def standardize_os(os_value):
            if 'android' in os_value:
                return 'Android'
            elif 'ios' in os_value or 'iphone' in os_value:
                return 'iOS'
            elif 'harmony' in os_value:
                return 'HarmonyOS'
            # Thêm các hệ điều hành khác nếu có
            return os_value.title()  # Trả về giá trị gốc nếu không xác định

        df['os_clean'] = df['os'].apply(standardize_os)

        # Thay thế cột 'os' gốc bằng cột đã làm sạch
        df['os'] = df['os_clean']
        df.drop(columns=['os_clean'], inplace=True)

    # --- KẾT THÚC CHUẨN HÓA ---

    return df


# =========================
# 🔹 Biểu đồ số lượng sản phẩm theo thương hiệu
# =========================
def plot_products_by_brand(df):
    plt.figure(figsize=(10, 6))


    sns.countplot(data=df, x='brand_name',
                  order=df['brand_name'].value_counts().index)

    plt.title('Số lượng sản phẩm theo thương hiệu', fontsize=16)
    plt.xlabel('Thương hiệu')
    plt.ylabel('Số lượng sản phẩm')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()


# =========================
# 🔹 Biểu đồ phân bố sản phẩm theo hệ điều hành
# =========================
def plot_os_distribution(df):
    os_counts = df['os'].value_counts()

    plt.figure(figsize=(8, 8))
    plt.pie(os_counts,
            labels=os_counts.index,
            autopct='%1.1f%%',
            startangle=140)

    plt.title('Phân bố sản phẩm theo hệ điều hành', fontsize=16)
    plt.tight_layout()
    plt.show()


# =========================
# 🔹 Chạy script
# =========================
if __name__ == "__main__":
    df = get_data()
    print("🚀 Dữ liệu đã load, tổng số dòng:", len(df))

    print("\n--- KIỂM TRA PHÂN BỐ THƯƠNG HIỆU ---")
    if 'brand_name' in df.columns:
        brand_counts = df['brand_name'].value_counts()
        print(brand_counts)
        if len(brand_counts) > 1:
            plot_products_by_brand(df)
        else:
            print("🚨 CẢNH BÁO: Chỉ có một thương hiệu được tìm thấy. Không thể vẽ biểu đồ đa dạng.")

    print("\n--- KIỂM TRA PHÂN BỐ HỆ ĐIỀU HÀNH ---")
    if 'os' in df.columns:
        os_counts = df['os'].value_counts()
        print(os_counts)
        if len(os_counts) > 1:
            plot_os_distribution(df)
        else:
            print(
                "🚨 CẢNH BÁO: Chỉ có một Hệ điều hành được tìm thấy (sau khi chuẩn hóa). Không thể vẽ biểu đồ đa dạng.")
            # In ra các giá trị OS gốc trước khi chuẩn hóa (để debug thêm)
            # print("\nGiá trị OS Gốc trong tập dữ liệu:")
            # print(pd.read_sql('SELECT DISTINCT os FROM dim_product', connect_db("data_mart_prod")).to_string(index=False))