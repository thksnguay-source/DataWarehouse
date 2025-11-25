#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from config.datamart_cofig import connect_db


# =========================
# 🔹 Lấy dữ liệu từ Data Mart
# =========================
def get_data():
    conn = connect_db("data_mart_prod")
    query = """
            SELECT p.product_id, \
                   p.product_name, \
                   p.price, \
                   p.cpu, \
                   p.ram, \
                   p.storage, \
                   p.os,
                   b.brand_name, \
                   d.full_date
            FROM dim_product p
                     LEFT JOIN dim_brand b ON p.brand_key = b.brand_key
                     LEFT JOIN date_dims d ON p.date_key = d.date_sk \
            """
    df = pd.read_sql(query, conn)
    conn.close()
    return df


# =========================
# 🔹 Biểu đồ số lượng sản phẩm theo thương hiệu
# =========================
def plot_products_by_brand(df):
    plt.figure(figsize=(10, 6))
    sns.countplot(data=df, x='brand_name', order=df['brand_name'].value_counts().index, palette='Set2')
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
    os_counts.plot(kind='pie', autopct='%1.1f%%', startangle=140, colors=sns.color_palette('pastel'))
    plt.title('Phân bố sản phẩm theo hệ điều hành', fontsize=16)
    plt.ylabel('')
    plt.tight_layout()
    plt.show()


# =========================
# 🔹 Chạy script
# =========================
if __name__ == "__main__":
    df = get_data()
    print("🚀 Dữ liệu đã load, tổng số dòng:", len(df))

    plot_products_by_brand(df)
    plot_os_distribution(df)
