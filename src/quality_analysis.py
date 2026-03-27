from pathlib import Path
from typing import Any, Dict, List
import json
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
REPORTS_DIR = PROJECT_ROOT / "reports"
GX_REPORT_INPUT_JSON = REPORTS_DIR / "input_validation_results.json"
RAW_CSV = RAW_DIR / "retail_etl_dataset.csv"

def ensure_directories() -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

def load_input_validation_results(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def load_raw_df(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    for col in df.columns:
        df[col] = df[col].astype("string").str.strip()
    numeric_cols = ["invoice_id", "customer_id", "quantity", "price", "total_revenue"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

def build_issues_table(df: pd.DataFrame, validation: Dict[str, Any]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    inv_results = validation.get("results", [])
    dupe_count = None
    for item in inv_results:
        cfg = item.get("expectation_config", {})
        t = cfg.get("type")
        kwargs = cfg.get("kwargs", {})
        if t == "expect_column_values_to_be_unique" and kwargs.get("column") == "invoice_id":
            dupe_count = int(item.get("result", {}).get("unexpected_count", 0))
            sample = item.get("result", {}).get("partial_unexpected_list", [])
            example = str(sample[0]) if sample else "duplicate id"
            rows.append({
                "Column": "invoice_id",
                "Issue": "Duplicate IDs",
                "Example": example,
                "Dimension": "Uniqueness",
                "Business Impact": "Revenue double counted in BO-1 KPIs",
                "Approx Count": dupe_count
            })
    null_customer = int(df["customer_id"].isna().sum()) if "customer_id" in df.columns else 0
    rows.append({
        "Column": "customer_id",
        "Issue": "NULL values",
        "Example": "NaN",
        "Dimension": "Completeness",
        "Business Impact": "Cannot link sales to customers for BO-3",
        "Approx Count": null_customer
    })
    neg_qty = int((pd.to_numeric(df["quantity"], errors="coerce") < 0).sum()) if "quantity" in df.columns else 0
    rows.append({
        "Column": "quantity",
        "Issue": "Negative values",
        "Example": "-3",
        "Dimension": "Validity",
        "Business Impact": "Negative units corrupt total_revenue (BO-1)",
        "Approx Count": neg_qty
    })
    neg_price = int((pd.to_numeric(df["price"], errors="coerce") < 0).sum()) if "price" in df.columns else 0
    rows.append({
        "Column": "price",
        "Issue": "Negative values",
        "Example": "-83.02",
        "Dimension": "Validity",
        "Business Impact": "Invalid pricing affects financial KPIs (BO-1)",
        "Approx Count": neg_price
    })
    expected_total = (pd.to_numeric(df["quantity"], errors="coerce") * pd.to_numeric(df["price"], errors="coerce")).round(2)
    inaccurate = int(((pd.to_numeric(df["total_revenue"], errors="coerce") - expected_total).abs() > 0.01).sum()) if "total_revenue" in df.columns else 0
    rows.append({
        "Column": "total_revenue",
        "Issue": "Does not equal quantity × price",
        "Example": "Offset error",
        "Dimension": "Accuracy",
        "Business Impact": "Misstated revenue (BO-1)",
        "Approx Count": inaccurate
    })
    country_variants = int(df["country"].nunique()) if "country" in df.columns else 0
    rows.append({
        "Column": "country",
        "Issue": "Multiple formats",
        "Example": "colombia, CO",
        "Dimension": "Consistency",
        "Business Impact": "Regional insights unreliable (BO-3)",
        "Approx Count": country_variants
    })
    raw_dates = df["invoice_date"].astype("string") if "invoice_date" in df.columns else pd.Series(dtype="string")
    null_like = raw_dates.str.upper().isin({"", "N/A", "NULL", "NONE", "NAN"}).sum()
    parsed = pd.to_datetime(raw_dates, errors="coerce")
    future = int((parsed > pd.Timestamp("2023-12-31")).sum())
    rows.append({
        "Column": "invoice_date",
        "Issue": "Null-like strings and future dates",
        "Example": "N/A, 2026-01-01",
        "Dimension": "Timeliness",
        "Business Impact": "Time series unreliable (BO-2)",
        "Approx Count": int(null_like) + int(future)
    })
    return pd.DataFrame(rows)

def build_policy_table() -> pd.DataFrame:
    rows = [
        {"Policy Statement": "invoice_id must be unique across the entire dataset", "GE Expectation": "expect_column_values_to_be_unique(invoice_id)", "Severity": "Critical", "Addresses (BO)": "BO-1, BO-4"},
        {"Policy Statement": "quantity must be a positive integer (≥ 1)", "GE Expectation": "expect_column_values_to_be_between(quantity, min=1)", "Severity": "Critical", "Addresses (BO)": "BO-1"},
        {"Policy Statement": "price must be greater than zero", "GE Expectation": "expect_column_values_to_be_between(price, min=0.01)", "Severity": "Critical", "Addresses (BO)": "BO-1"},
        {"Policy Statement": "total_revenue must equal quantity × price (±0.01)", "GE Expectation": "expect_column_values_to_be_between(_revenue_diff_abs, min=0.0, max=0.01)", "Severity": "Critical", "Addresses (BO)": "BO-1, BO-4"},
        {"Policy Statement": "country must be one of {Colombia, Ecuador, Peru, Chile}", "GE Expectation": "expect_column_values_to_be_in_set(country, {Colombia,Ecuador,Peru,Chile})", "Severity": "High", "Addresses (BO)": "BO-3"},
        {"Policy Statement": "invoice_date must follow YYYY-MM-DD and fall within 2023-01-01 to 2023-12-31", "GE Expectation": "expect_column_values_to_match_regex(invoice_date, ^\\d{4}-\\d{2}-\\d{2}$), expect_column_values_to_be_between(year, 2023, 2023)", "Severity": "High", "Addresses (BO)": "BO-2"},
        {"Policy Statement": "customer_id must be non-null positive integer", "GE Expectation": "expect_column_values_to_not_be_null(customer_id), expect_column_values_to_be_between(customer_id, min=1)", "Severity": "High", "Addresses (BO)": "BO-3"},
        {"Policy Statement": "product must belong to the allowed catalog", "GE Expectation": "expect_column_values_to_be_in_set(product, catalog)", "Severity": "Medium", "Addresses (BO)": "BO-3"},
    ]
    return pd.DataFrame(rows)

def save_outputs(issues_df: pd.DataFrame, policies_df: pd.DataFrame) -> Dict[str, Path]:
    issues_csv = REPORTS_DIR / "dq_issues_table.csv"
    policies_csv = REPORTS_DIR / "dq_policy_proposal.csv"
    report_md = REPORTS_DIR / "quality_report.md"
    issues_df.to_csv(issues_csv, index=False, encoding="utf-8-sig")
    policies_df.to_csv(policies_csv, index=False, encoding="utf-8-sig")
    with open(report_md, "w", encoding="utf-8") as f:
        f.write("# Data Quality Issues and Policy Proposal\n\n")
        try:
            f.write("## Issues\n\n")
            f.write(issues_df.to_markdown(index=False))
            f.write("\n\n## Policies\n\n")
            f.write(policies_df.to_markdown(index=False))
        except Exception:
            f.write("## Issues\n\n")
            f.write(issues_df.to_string(index=False))
            f.write("\n\n## Policies\n\n")
            f.write(policies_df.to_string(index=False))
    return {"issues_csv": issues_csv, "policies_csv": policies_csv, "report_md": report_md}

def main() -> None:
    ensure_directories()
    validation = load_input_validation_results(GX_REPORT_INPUT_JSON)
    df = load_raw_df(RAW_CSV)
    issues_df = build_issues_table(df, validation)
    policies_df = build_policy_table()
    save_outputs(issues_df, policies_df)

if __name__ == "__main__":
    main()
