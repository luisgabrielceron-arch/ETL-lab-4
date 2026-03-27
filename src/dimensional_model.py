from pathlib import Path
from typing import Dict
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
REPORTS_DIR = PROJECT_ROOT / "reports"
INPUT_TRANSFORMED_CSV = PROCESSED_DIR / "retail_transformed.csv"

def ensure_directories() -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

def load_transformed(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path, parse_dates=["invoice_date"])
    for col in ["invoice_id", "customer_id", "quantity"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    for col in ["price", "total_revenue"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df["country"] = df["country"].astype("string")
    df["product"] = df["product"].astype("string")
    return df

def build_dim_date() -> pd.DataFrame:
    dates = pd.date_range(start="2023-01-01", end="2023-12-31", freq="D")
    df = pd.DataFrame({"full_date": dates})
    df["date_id"] = (df["full_date"].dt.strftime("%Y%m%d")).astype(int)
    df["year"] = df["full_date"].dt.year.astype(int)
    df["month"] = df["full_date"].dt.month.astype(int)
    df["month_name"] = df["full_date"].dt.month_name()
    df["day_of_week"] = df["full_date"].dt.day_name()
    return df[["date_id", "full_date", "year", "month", "month_name", "day_of_week"]]

def build_dim_product(df: pd.DataFrame) -> pd.DataFrame:
    s = df["product"].dropna().drop_duplicates().sort_values()
    dim = pd.DataFrame({"product_name": s})
    dim["product_id"] = range(1, len(dim) + 1)
    return dim[["product_id", "product_name"]]

def build_dim_customer(df: pd.DataFrame) -> pd.DataFrame:
    s = df["customer_id"].dropna().drop_duplicates().sort_values()
    dim = pd.DataFrame({"customer_id": s})
    dim["customer_key"] = dim["customer_id"]
    return dim[["customer_id", "customer_key"]]

def build_dim_location(df: pd.DataFrame) -> pd.DataFrame:
    s = df["country"].dropna().drop_duplicates().sort_values()
    dim = pd.DataFrame({"country": s})
    dim["location_id"] = range(1, len(dim) + 1)
    return dim[["location_id", "country"]]

def build_fact_sales(df: pd.DataFrame, dim_product: pd.DataFrame, dim_customer: pd.DataFrame, dim_location: pd.DataFrame, dim_date: pd.DataFrame) -> pd.DataFrame:
    if df["product"].isna().any():
        raise ValueError("fact_sales build failed: null product values exist in transformed data.")
    if df["country"].isna().any():
        raise ValueError("fact_sales build failed: null country values exist in transformed data.")
    if df["invoice_date"].isna().any():
        raise ValueError("fact_sales build failed: null invoice_date values exist in transformed data.")
    if df["customer_id"].isna().any():
        raise ValueError("fact_sales build failed: null customer_id values exist in transformed data.")

    if dim_product["product_name"].duplicated().any():
        raise ValueError("dim_product has duplicate product_name values; cannot safely map product_id.")
    if dim_location["country"].duplicated().any():
        raise ValueError("dim_location has duplicate country values; cannot safely map location_id.")
    if dim_date["full_date"].duplicated().any():
        raise ValueError("dim_date has duplicate full_date values; cannot safely map date_id.")
    if dim_customer["customer_id"].duplicated().any():
        raise ValueError("dim_customer has duplicate customer_id values; cannot safely map customers.")

    original_row_count = int(len(df))

    p = dim_product.rename(columns={"product_name": "product"})
    x = df.merge(
        p[["product", "product_id"]],
        on="product",
        how="left",
        validate="many_to_one",
        indicator="_merge_product",
    )
    missing_product = int((x["_merge_product"] == "left_only").sum())
    if missing_product:
        sample = x.loc[x["_merge_product"] == "left_only", "product"].dropna().astype(str).head(10).tolist()
        raise ValueError(f"Unmatched product values: {missing_product}. Examples: {sample}")
    x = x.drop(columns=["_merge_product"])

    l = dim_location
    x = x.merge(
        l[["country", "location_id"]],
        on="country",
        how="left",
        validate="many_to_one",
        indicator="_merge_location",
    )
    missing_location = int((x["_merge_location"] == "left_only").sum())
    if missing_location:
        sample = x.loc[x["_merge_location"] == "left_only", "country"].dropna().astype(str).head(10).tolist()
        raise ValueError(f"Unmatched country values: {missing_location}. Examples: {sample}")
    x = x.drop(columns=["_merge_location"])

    d = dim_date.rename(columns={"full_date": "invoice_date"})
    x = x.merge(
        d[["invoice_date", "date_id"]],
        on="invoice_date",
        how="left",
        validate="many_to_one",
        indicator="_merge_date",
    )
    missing_date = int((x["_merge_date"] == "left_only").sum())
    if missing_date:
        sample = (
            x.loc[x["_merge_date"] == "left_only", "invoice_date"]
            .dropna()
            .astype(str)
            .head(10)
            .tolist()
        )
        raise ValueError(f"Unmatched invoice_date values: {missing_date}. Examples: {sample}")
    x = x.drop(columns=["_merge_date"])

    if int(len(x)) != original_row_count:
        raise ValueError(
            f"fact_sales build changed row count unexpectedly: before={original_row_count}, after={len(x)}"
        )

    required_fk_cols = ["product_id", "location_id", "date_id"]
    missing_fk_any = {col: int(x[col].isna().sum()) for col in required_fk_cols}
    if any(v > 0 for v in missing_fk_any.values()):
        raise ValueError(f"fact_sales has null foreign keys after mapping: {missing_fk_any}")

    x = x[["invoice_id", "product_id", "customer_id", "location_id", "date_id", "quantity", "price", "total_revenue"]].copy()
    x["sale_id"] = range(1, len(x) + 1)
    cols = ["sale_id", "invoice_id", "product_id", "customer_id", "location_id", "date_id", "quantity", "price", "total_revenue"]
    return x[cols]

def save_outputs(dim_product: pd.DataFrame, dim_customer: pd.DataFrame, dim_location: pd.DataFrame, dim_date: pd.DataFrame, fact_sales: pd.DataFrame) -> Dict[str, Path]:
    paths: Dict[str, Path] = {}
    paths["dim_product"] = PROCESSED_DIR / "dim_product.csv"
    paths["dim_customer"] = PROCESSED_DIR / "dim_customer.csv"
    paths["dim_location"] = PROCESSED_DIR / "dim_location.csv"
    paths["dim_date"] = PROCESSED_DIR / "dim_date.csv"
    paths["fact_sales"] = PROCESSED_DIR / "fact_sales.csv"
    dim_product.to_csv(paths["dim_product"], index=False, encoding="utf-8-sig")
    dim_customer.to_csv(paths["dim_customer"], index=False, encoding="utf-8-sig")
    dim_location.to_csv(paths["dim_location"], index=False, encoding="utf-8-sig")
    dim_date.to_csv(paths["dim_date"], index=False, encoding="utf-8-sig")
    fact_sales.to_csv(paths["fact_sales"], index=False, encoding="utf-8-sig")
    model_md = REPORTS_DIR / "model_description.md"
    with open(model_md, "w", encoding="utf-8") as f:
        f.write("# Star Schema Description\n\n")
        f.write("Tables: dim_product(product_id, product_name), dim_customer(customer_id, customer_key), dim_location(location_id, country), dim_date(date_id, full_date, year, month, month_name, day_of_week), fact_sales(sale_id, invoice_id, product_id, customer_id, location_id, date_id, quantity, price, total_revenue)\n")
        f.write("\nGranularity: one row per transaction line.\n")
        f.write("\nForeign keys in fact_sales reference surrogate keys in dimensions.\n")
    paths["model_md"] = model_md
    return paths

def main() -> None:
    ensure_directories()
    df = load_transformed(INPUT_TRANSFORMED_CSV)
    dim_date = build_dim_date()
    dim_product = build_dim_product(df)
    dim_customer = build_dim_customer(df)
    dim_location = build_dim_location(df)
    fact_sales = build_fact_sales(df, dim_product, dim_customer, dim_location, dim_date)
    save_outputs(dim_product, dim_customer, dim_location, dim_date, fact_sales)

if __name__ == "__main__":
    main()
