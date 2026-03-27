from pathlib import Path
from typing import Dict
import sqlite3
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
REPORTS_DIR = PROJECT_ROOT / "reports"
DB_PATH = PROCESSED_DIR / "data_warehouse.db"

def ensure_directories() -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

def load_csvs() -> Dict[str, pd.DataFrame]:
    dfs: Dict[str, pd.DataFrame] = {}
    dfs["dim_date"] = pd.read_csv(PROCESSED_DIR / "dim_date.csv", parse_dates=["full_date"])
    dfs["dim_product"] = pd.read_csv(PROCESSED_DIR / "dim_product.csv")
    dfs["dim_customer"] = pd.read_csv(PROCESSED_DIR / "dim_customer.csv")
    dfs["dim_location"] = pd.read_csv(PROCESSED_DIR / "dim_location.csv")
    dfs["fact_sales"] = pd.read_csv(PROCESSED_DIR / "fact_sales.csv")
    return dfs

def load_to_sqlite(dfs: Dict[str, pd.DataFrame]) -> None:
    conn = sqlite3.connect(DB_PATH)
    try:
        dfs["dim_date"].to_sql("dim_date", conn, if_exists="replace", index=False)
        dfs["dim_product"].to_sql("dim_product", conn, if_exists="replace", index=False)
        dfs["dim_customer"].to_sql("dim_customer", conn, if_exists="replace", index=False)
        dfs["dim_location"].to_sql("dim_location", conn, if_exists="replace", index=False)
        dfs["fact_sales"].to_sql("fact_sales", conn, if_exists="replace", index=False)
    finally:
        conn.close()

def build_referential_summary(dfs: Dict[str, pd.DataFrame]) -> Dict[str, int]:
    f = dfs["fact_sales"]
    ok_product = int(f["product_id"].isin(dfs["dim_product"]["product_id"]).sum())
    ok_customer = int(f["customer_id"].isin(dfs["dim_customer"]["customer_id"]).sum())
    ok_location = int(f["location_id"].isin(dfs["dim_location"]["location_id"]).sum())
    ok_date = int(f["date_id"].isin(dfs["dim_date"]["date_id"]).sum())
    total = int(len(f))
    return {
        "total_fact_rows": total,
        "fk_product_resolved": ok_product,
        "fk_customer_resolved": ok_customer,
        "fk_location_resolved": ok_location,
        "fk_date_resolved": ok_date,
    }

def save_summary(summary: Dict[str, int]) -> Path:
    md = REPORTS_DIR / "referential_integrity.md"
    with open(md, "w", encoding="utf-8") as f:
        f.write("# Referential Integrity Summary\n\n")
        for k, v in summary.items():
            f.write(f"- {k}: {v}\n")
    return md

def main() -> None:
    ensure_directories()
    dfs = load_csvs()
    load_to_sqlite(dfs)
    summary = build_referential_summary(dfs)
    save_summary(summary)

if __name__ == "__main__":
    main()
