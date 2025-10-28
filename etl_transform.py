import os
import re
import json
import uuid
from datetime import datetime
from urllib.parse import urlparse

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from db_config import get_mysql_url


def ensure_dirs(paths):
    for p in paths:
        os.makedirs(p, exist_ok=True)


def parse_vnd_to_int(value: str) -> int | None:
    if value is None:
        return None
    s = str(value)
    if not s or s.strip().lower() in {"n/a", "không tìm thấy", "liên hệ để báo giá"}:
        return None
    s = s.replace(".", "").replace(",", "").replace("đ", "").replace("VND", "").strip()
    digits = re.findall(r"\d+", s)
    if not digits:
        return None
    try:
        return int("".join(digits))
    except Exception:
        return None


def slugify(text: str) -> str:
    if not text:
        return ""
    s = re.sub(r"[^\w\s-]", "", str(text), flags=re.UNICODE)
    s = re.sub(r"[\s_-]+", "-", s).strip("-").lower()
    return s[:200]


def extract_brand_from_url(url: str) -> str | None:
    if not url:
        return None
    lowered = url.lower()
    known = [
        "apple",
        "iphone",
        "samsung",
        "xiaomi",
        "oppo",
        "tecno",
        "honor",
        "nubia",
        "sony",
        "nokia",
        "poco",
    ]
    for k in known:
        if k in lowered:
            if k == "iphone":
                return "Apple"
            if k == "poco":
                return "Xiaomi POCO"
            return k.capitalize()
    return None


def infer_brand(name: str, url: str) -> str | None:
    brand = extract_brand_from_url(url)
    if brand:
        return brand
    if not name:
        return None
    name_l = name.lower()
    candidates = {
        "iphone": "Apple",
        "apple": "Apple",
        "samsung": "Samsung",
        "xiaomi": "Xiaomi",
        "poco": "Xiaomi POCO",
        "redmi": "Xiaomi Redmi",
        "oppo": "OPPO",
        "tecno": "TECNO",
        "honor": "HONOR",
        "nubia": "Nubia",
        "sony": "Sony",
        "nokia": "Nokia",
        "nothing": "Nothing",
    }
    for kw, br in candidates.items():
        if kw in name_l:
            return br
    return None


def build_dim_source(df: pd.DataFrame) -> pd.DataFrame:
    sources = []
    for _, row in df.iterrows():
        url = row.get("URL") or row.get("url")
        src_name = row.get("Nguồn") or "Unknown"
        netloc = urlparse(url).netloc if url else None
        sources.append((src_name, netloc))
    distinct = sorted({(s, n) for s, n in sources})
    records = []
    for i, (s, n) in enumerate(distinct, start=1):
        records.append(
            {
                "source_key": i,
                "source_id": slugify(s or n or f"src-{i}"),
                "source_name": s,
                "source_url": ("https://" + n) if n else None,
                "active_flag": 1,
            }
        )
    return pd.DataFrame(records)


def build_dim_date(load_date: datetime) -> pd.DataFrame:
    date_key = int(load_date.strftime("%Y%m%d"))
    return pd.DataFrame(
        [
            {
                "date_key": date_key,
                "full_date": load_date.strftime("%Y-%m-%d"),
                "day": load_date.day,
                "month": load_date.month,
                "year": load_date.year,
                "quarter": (load_date.month - 1) // 3 + 1,
                "week": int(load_date.strftime("%U")),
                "is_weekend": 1 if load_date.weekday() >= 5 else 0,
            }
        ]
    )


def build_dim_product(df: pd.DataFrame) -> pd.DataFrame:
    products = []
    seen = set()
    for _, row in df.iterrows():
        url = row.get("URL") or row.get("url")
        name = row.get("Tên sản phẩm") or row.get("ten_san_pham")
        if not url and not name:
            continue
        product_id = slugify(url or name)
        if product_id in seen:
            continue
        seen.add(product_id)
        brand = infer_brand(name, url)
        products.append(
            {
                "product_key": len(seen),
                "product_id": product_id,
                "name": name,
                "brand": brand,
                "model": None,
                "category": "phone",
                "subcategory": None,
                "specifications": None,
            }
        )
    return pd.DataFrame(products)


def build_dim_price_history(df_raw: pd.DataFrame, dim_product: pd.DataFrame, dim_source: pd.DataFrame, date_key: int) -> pd.DataFrame:
    pid_to_key = {r["product_id"]: int(r["product_key"]) for _, r in dim_product.iterrows()}
    sid_to_key = {r["source_id"]: int(r["source_key"]) for _, r in dim_source.iterrows()}

    records = []
    seq = 1
    for _, row in df_raw.iterrows():
        url = row.get("URL") or row.get("url")
        src_name = row.get("Nguồn") or "Unknown"
        source_id = slugify(src_name)
        product_id = slugify(url or (row.get("Tên sản phẩm") or row.get("ten_san_pham")))
        product_key = pid_to_key.get(product_id)
        source_key = sid_to_key.get(source_id)
        price_raw = row.get("Giá") or row.get("gia")
        sale_price = parse_vnd_to_int(price_raw)
        if product_key is None or source_key is None:
            continue
        records.append(
            {
                "price_key": seq,
                "product_key": product_key,
                "source_key": source_key,
                "date_key": date_key,
                "original_price": None,
                "sale_price": sale_price,
                "discount_percent": None,
            }
        )
        seq += 1
    return pd.DataFrame(records)


def _clean_and_standardize(df: pd.DataFrame) -> pd.DataFrame:
    # 1) Cleaning: strip whitespace, drop fully empty rows, normalize
    df = df.copy()
    df = df.replace({"\u00a0": " ", "\u200b": ""}, regex=True)
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].astype(str).str.strip()
    # 2) Standardize types
    # Price to int
    if "Giá" in df.columns:
        df["sale_price_vnd"] = df["Giá"].apply(parse_vnd_to_int)
    # Normalize null-ish
    df.replace({"None": None, "N/A": None, "nan": None}, inplace=True)
    # remove obvious duplicates by URL or Name
    if "URL" in df.columns:
        df = df.drop_duplicates(subset=["URL"], keep="first")
    elif "Tên sản phẩm" in df.columns:
        df = df.drop_duplicates(subset=["Tên sản phẩm"], keep="first")
    return df


def _enrich_and_conform(df: pd.DataFrame) -> pd.DataFrame:
    # 3) Enrichment + 4) Conformation
    df = df.copy()
    df["brand_inferred"] = df.apply(lambda r: infer_brand(r.get("Tên sản phẩm"), r.get("URL")), axis=1)
    df["product_id"] = df.apply(lambda r: slugify(r.get("URL") or r.get("Tên sản phẩm")), axis=1)
    df["source_id"] = df["Nguồn"].apply(lambda x: slugify(x or "Unknown"))
    df["category"] = "phone"
    # 7) Business rule: optional VND->USD conversion via env rate
    try:
        rate = float(os.getenv("VND_USD_RATE", "25000"))
        df["sale_price_usd"] = df["sale_price_vnd"].apply(lambda v: round((v or 0) / rate, 2) if v else None)
    except Exception:
        df["sale_price_usd"] = None
    return df


def transform(input_json_path: str = "simple_crawled_products.json") -> dict:
    ensure_dirs(["staging", "hub", "logs"])
    with open(input_json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError("Dữ liệu crawl không phải danh sách JSON")
    df = pd.DataFrame(data)

    df["URL"] = df.get("URL", df.get("url"))
    df["Nguồn"] = df.get("Nguồn", "Unknown")
    df["Tên sản phẩm"] = df.get("Tên sản phẩm", df.get("ten_san_pham"))
    df["Giá"] = df.get("Giá", df.get("gia"))

    df_staging = _clean_and_standardize(df)
    df_staging = _enrich_and_conform(df_staging)

    staging_path = os.path.join("staging", "normalized_products.csv")
    df_staging.to_csv(staging_path, index=False, encoding="utf-8-sig")

    load_date = datetime.now()
    dim_source = build_dim_source(df_staging)
    dim_date = build_dim_date(load_date)
    dim_product = build_dim_product(df_staging)
    price_history = build_dim_price_history(
        df_staging, dim_product=dim_product, dim_source=dim_source, date_key=int(dim_date.loc[0, "date_key"])
    )

    dim_source.to_csv(os.path.join("hub", "DIM_Source.csv"), index=False, encoding="utf-8-sig")
    dim_date.to_csv(os.path.join("hub", "DIM_Date.csv"), index=False, encoding="utf-8-sig")
    dim_product.to_csv(os.path.join("hub", "DIM_Product.csv"), index=False, encoding="utf-8-sig")
    price_history.to_csv(os.path.join("hub", "DIM_Price_History.csv"), index=False, encoding="utf-8-sig")

    run_id = uuid.uuid4().hex[:8]
    log_msg = {
        "run_id": run_id,
        "timestamp": datetime.now().isoformat(),
        "input_records": len(df_staging),
        "dim_source": len(dim_source),
        "dim_product": len(dim_product),
        "price_history": len(price_history),
    }
    with open(os.path.join("logs", f"transform_{run_id}.json"), "w", encoding="utf-8") as f:
        json.dump(log_msg, f, ensure_ascii=False, indent=2)

    return {"log": log_msg, "frames": {
        "dim_source": dim_source,
        "dim_date": dim_date,
        "dim_product": dim_product,
        "dim_price_history": price_history,
        "staging": df_staging,
    }}


def create_mysql_engine() -> Engine:
    url = get_mysql_url()
    return create_engine(url, pool_pre_ping=True)


def _ensure_dim_date(engine: Engine, dim_date: pd.DataFrame) -> None:
    with engine.begin() as conn:
        for _, r in dim_date.iterrows():
            conn.execute(
                text(
                    """
                    INSERT INTO dim_date (date_key, full_date, day, month, year, quarter, week, is_weekend)
                    VALUES (:date_key, :full_date, :day, :month, :year, :quarter, :week, :is_weekend)
                    ON DUPLICATE KEY UPDATE full_date=VALUES(full_date), day=VALUES(day), month=VALUES(month),
                    year=VALUES(year), quarter=VALUES(quarter), week=VALUES(week), is_weekend=VALUES(is_weekend)
                    """
                ),
                r.to_dict(),
            )


def _get_existing_maps(engine: Engine) -> tuple[dict, dict]:
    pid_map = {}
    sid_map = {}
    with engine.begin() as conn:
        # product_id -> product_key
        res = conn.execute(text("SELECT product_key, product_id FROM dim_product"))
        for row in res:
            pid_map[row.product_id] = row.product_key
        # source_id -> source_key
        res2 = conn.execute(text("SELECT source_key, source_id FROM dim_source"))
        for row in res2:
            sid_map[row.source_id] = row.source_key
    return pid_map, sid_map


def _upsert_dim_source(engine: Engine, df_source: pd.DataFrame) -> dict:
    # returns updated map source_id->source_key
    with engine.begin() as conn:
        for _, r in df_source.iterrows():
            conn.execute(
                text(
                    """
                    INSERT INTO dim_source (source_key, source_id, source_name, source_url, active_flag)
                    VALUES (:source_key, :source_id, :source_name, :source_url, :active_flag)
                    ON DUPLICATE KEY UPDATE source_name=VALUES(source_name), source_url=VALUES(source_url), active_flag=VALUES(active_flag)
                    """
                ),
                r.to_dict(),
            )
    _, sid_map = _get_existing_maps(engine)
    return sid_map


def _scd1_upsert_dim_product(engine: Engine, df_product: pd.DataFrame) -> dict:
    with engine.begin() as conn:
        # Insert new by checking existing product_id
        existing = {pid for pid, _ in conn.execute(text("SELECT product_id, 1 FROM dim_product"))}
        for _, r in df_product.iterrows():
            pid = r["product_id"]
            if pid in existing:
                conn.execute(
                    text(
                        """
                        UPDATE dim_product
                        SET name=:name, brand=:brand, model=:model, category=:category, subcategory=:subcategory, specifications=:specifications
                        WHERE product_id=:product_id
                        """
                    ),
                    r.to_dict(),
                )
            else:
                conn.execute(
                    text(
                        """
                        INSERT INTO dim_product (product_key, product_id, name, brand, model, category, subcategory, specifications)
                        VALUES (:product_key, :product_id, :name, :brand, :model, :category, :subcategory, :specifications)
                        """
                    ),
                    r.to_dict(),
                )
    pid_map, _ = _get_existing_maps(engine)
    return pid_map


def _write_staging(engine: Engine, df_staging: pd.DataFrame) -> None:
    # 9) Restructuring: store staging snapshot
    df_out = df_staging[[
        "URL", "Nguồn", "Tên sản phẩm", "Giá", "sale_price_vnd", "sale_price_usd", "brand_inferred", "product_id", "source_id"
    ]].copy()
    df_out["load_ts"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df_out.rename(columns={"Nguồn": "source_name", "Tên sản phẩm": "product_name", "Giá": "price_raw"}, inplace=True)
    df_out.to_sql("stg_products", engine, if_exists="replace", index=False)


def load_to_mysql(frames: dict, if_exists: str = "replace") -> None:
    engine = create_mysql_engine()
    with engine.begin() as conn:
        conn.execute(text("SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci"))

    # Write staging snapshot
    if "staging" in frames:
        _write_staging(engine, frames["staging"])

    # Ensure date dim
    _ensure_dim_date(engine, frames["dim_date"])

    # Upsert source dim, then product dim with SCD1
    _ = _upsert_dim_source(engine, frames["dim_source"].rename(columns=str.lower))
    pid_map = _scd1_upsert_dim_product(engine, frames["dim_product"].rename(columns=str.lower))

    # Build foreign keys for price history based on current maps from DB
    # Recompute maps from DB to ensure we have surrogate keys
    pid_map, sid_map = _get_existing_maps(engine)
    price_df = frames["staging"][["product_id", "source_id", "sale_price_vnd"]].copy()
    date_key = int(frames["dim_date"].iloc[0]["date_key"]) if not frames["dim_date"].empty else int(datetime.now().strftime("%Y%m%d"))
    out_records = []
    for _, r in price_df.iterrows():
        pk = pid_map.get(r["product_id"])  # product_key
        sk = sid_map.get(r["source_id"])  # source_key
        if pk and sk:
            out_records.append({
                "product_key": int(pk),
                "source_key": int(sk),
                "date_key": date_key,
                "original_price": None,
                "sale_price": int(r["sale_price_vnd"]) if pd.notna(r["sale_price_vnd"]) else None,
                "discount_percent": None,
            })
    df_price = pd.DataFrame(out_records)
    if not df_price.empty:
        df_price.to_sql("dim_price_history", engine, if_exists="append", index=False)

    # Write ETL log
    etl_log_df = pd.DataFrame([{
        "run_id": uuid.uuid4().hex[:8],
        "run_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source_file": "simple_crawled_products.json",
        "records_input": int(frames["staging"].shape[0]) if "staging" in frames else int(frames["dim_product"].shape[0]),
        "records_price": int(df_price.shape[0]) if 'df_price' in locals() and not df_price.empty else 0,
        "status": "SUCCESS",
    }])
    etl_log_df.to_sql("etl_log", engine, if_exists="append", index=False)


if __name__ == "__main__":
    info = transform()
    load_to_mysql(info["frames"], if_exists="replace")
    print("✅ ETL hoàn tất & dữ liệu đã nạp vào MySQL (XAMPP)")
