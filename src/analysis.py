from pathlib import Path
import json
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
REPORTS_DIR = PROJECT_ROOT / "reports"
INPUT_TRANSFORMED_CSV = PROCESSED_DIR / "retail_transformed.csv"
DQ_SCORES_JSON = REPORTS_DIR / "dq_scores.json"
OUTPUT_DASHBOARD_HTML = REPORTS_DIR / "kpi_dashboard.html"

def ensure_directories() -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

def load_data() -> pd.DataFrame:
    df = pd.read_csv(INPUT_TRANSFORMED_CSV, parse_dates=["invoice_date"])
    df["country"] = df["country"].astype("string")
    df["product"] = df["product"].astype("string")
    df["revenue_bin"] = df["revenue_bin"].astype("string")
    return df

def save_plot(fig: plt.Figure, filename: str) -> Path:
    path = REPORTS_DIR / filename
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    return path

def kpi_total_revenue_by_country(df: pd.DataFrame) -> Path:
    agg = df.groupby("country", dropna=False)["total_revenue"].sum().reset_index()
    fig, ax = plt.subplots(figsize=(8, 5))
    sns.barplot(data=agg, x="country", y="total_revenue", ax=ax)
    ax.set_title("Total revenue per country")
    ax.set_xlabel("Country")
    ax.set_ylabel("Total revenue")
    return save_plot(fig, "bo1_total_revenue_per_country.png")

def kpi_avg_transaction_box_by_product(df: pd.DataFrame) -> Path:
    fig, ax = plt.subplots(figsize=(10, 6))
    sns.boxplot(data=df, x="product", y="total_revenue", ax=ax)
    ax.set_title("Transaction value distribution by product")
    ax.set_xlabel("Product")
    ax.set_ylabel("Total revenue")
    ax.tick_params(axis="x", rotation=45)
    return save_plot(fig, "bo1_transaction_value_box_by_product.png")

def kpi_monthly_revenue_trend(df: pd.DataFrame) -> Path:
    agg = df.groupby("month", dropna=False)["total_revenue"].sum().reset_index()
    fig, ax = plt.subplots(figsize=(8, 5))
    sns.lineplot(data=agg, x="month", y="total_revenue", marker="o", ax=ax)
    ax.set_title("Monthly revenue trend (2023)")
    ax.set_xlabel("Month")
    ax.set_ylabel("Total revenue")
    ax.set_xticks(range(1, 13))
    return save_plot(fig, "bo2_monthly_revenue_trend.png")

def kpi_peak_day_of_week_volume(df: pd.DataFrame) -> Path:
    agg = df.groupby("day_of_week", dropna=False)["invoice_id"].count().reset_index(name="transaction_count")
    order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
    agg["day_of_week"] = pd.Categorical(agg["day_of_week"], categories=order, ordered=True)
    agg = agg.sort_values("day_of_week")
    fig, ax = plt.subplots(figsize=(8, 5))
    sns.barplot(data=agg, x="day_of_week", y="transaction_count", ax=ax)
    ax.set_title("Transaction count by day of week")
    ax.set_xlabel("Day of week")
    ax.set_ylabel("Transactions")
    return save_plot(fig, "bo2_transactions_by_day_of_week.png")

def kpi_top3_products_by_revenue(df: pd.DataFrame) -> Path:
    agg = df.groupby("product", dropna=False)["total_revenue"].sum().reset_index()
    top3 = agg.sort_values("total_revenue", ascending=False).head(3)
    fig, ax = plt.subplots(figsize=(8, 5))
    sns.barplot(data=top3, y="product", x="total_revenue", ax=ax, orient="h")
    ax.set_title("Top 3 products by total revenue")
    ax.set_xlabel("Total revenue")
    ax.set_ylabel("Product")
    return save_plot(fig, "bo3_top3_products_by_revenue.png")

def kpi_sales_distribution_by_country(df: pd.DataFrame) -> Path:
    agg = df.groupby("country", dropna=False)["total_revenue"].sum().reset_index()
    fig, ax = plt.subplots(figsize=(6, 6))
    ax.pie(agg["total_revenue"], labels=agg["country"], autopct="%1.1f%%", startangle=90)
    ax.set_title("Revenue share by country")
    return save_plot(fig, "bo3_revenue_share_by_country.png")

def kpi_dq_scores_bar() -> Path:
    if not DQ_SCORES_JSON.exists():
        payload = {"data_quality_score_input": 0.0, "data_quality_score_output": 0.0}
    else:
        with open(DQ_SCORES_JSON, "r", encoding="utf-8") as f:
            payload = json.load(f)
    scores = pd.DataFrame({
        "Stage": ["Input", "Output"],
        "DQ Score": [payload.get("data_quality_score_input", 0.0), payload.get("data_quality_score_output", 0.0)]
    })
    fig, ax = plt.subplots(figsize=(6, 5))
    sns.barplot(data=scores, x="Stage", y="DQ Score", ax=ax)
    ax.set_title("Data Quality Score: Input vs Output")
    ax.set_xlabel("Stage")
    ax.set_ylabel("DQ Score (%)")
    ax.set_ylim(0, 100)
    return save_plot(fig, "bo4_dq_score_input_vs_output.png")

def load_dq_scores() -> dict:
    if not DQ_SCORES_JSON.exists():
        return {"data_quality_score_input": 0.0, "data_quality_score_output": 0.0}
    with open(DQ_SCORES_JSON, "r", encoding="utf-8") as f:
        return json.load(f)

def write_dashboard(df: pd.DataFrame) -> Path:
    dq = load_dq_scores()
    total_revenue = float(df["total_revenue"].sum())
    transaction_count = int(len(df))
    customer_count = int(df["customer_id"].nunique(dropna=True)) if "customer_id" in df.columns else 0
    product_count = int(df["product"].nunique(dropna=True)) if "product" in df.columns else 0
    country_count = int(df["country"].nunique(dropna=True)) if "country" in df.columns else 0

    charts = [
        ("BO-1 — Financial Integrity", [
            ("Total revenue per country", "bo1_total_revenue_per_country.png"),
            ("Transaction value distribution by product", "bo1_transaction_value_box_by_product.png"),
        ]),
        ("BO-2 — Reliable Trends", [
            ("Monthly revenue trend (2023)", "bo2_monthly_revenue_trend.png"),
            ("Transaction count by day of week", "bo2_transactions_by_day_of_week.png"),
        ]),
        ("BO-3 — Product and Regional Insights", [
            ("Top 3 products by total revenue", "bo3_top3_products_by_revenue.png"),
            ("Revenue share by country", "bo3_revenue_share_by_country.png"),
        ]),
        ("BO-4 — Transparent Reporting", [
            ("Data Quality Score: Input vs Output", "bo4_dq_score_input_vs_output.png"),
        ]),
    ]

    data_docs_rel = "../gx_context/uncommitted/data_docs/local_site/index.html"
    validation_comparison_rel = "output_validation_comparison_table.html"
    input_failure_summary_rel = "input_validation_failure_summary.html"

    html_parts = []
    html_parts.append("<!doctype html><html><head><meta charset='utf-8'>")
    html_parts.append("<meta name='viewport' content='width=device-width, initial-scale=1'>")
    html_parts.append("<title>Retail KPIs Dashboard</title>")
    html_parts.append(
        "<style>"
        "body{font-family:Arial,sans-serif;margin:24px;background:#0b1020;color:#e8ecff;}"
        "h1{margin:0 0 8px 0;font-size:26px;}"
        "h2{margin:24px 0 12px 0;font-size:18px;}"
        ".sub{color:#b8c1ff;margin:0 0 18px 0;}"
        ".links{display:flex;gap:12px;flex-wrap:wrap;margin:10px 0 18px 0;}"
        ".links a{color:#9ad0ff;text-decoration:none;border:1px solid #2b3a7a;padding:8px 10px;border-radius:8px;background:#10183a;}"
        ".grid{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:10px;margin:12px 0 18px 0;}"
        ".card{background:#10183a;border:1px solid #2b3a7a;border-radius:10px;padding:12px;}"
        ".label{color:#b8c1ff;font-size:12px;margin-bottom:6px;}"
        ".value{font-size:18px;font-weight:700;}"
        ".section{border-top:1px solid #25306b;padding-top:14px;}"
        ".charts{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px;}"
        ".chart{background:#0f1736;border:1px solid #2b3a7a;border-radius:10px;padding:10px;}"
        ".chart img{width:100%;height:auto;border-radius:8px;background:#fff;}"
        ".caption{margin:8px 0 0 0;color:#cbd2ff;font-size:13px;}"
        "@media (max-width:1100px){.grid{grid-template-columns:repeat(2,minmax(0,1fr));}.charts{grid-template-columns:1fr;}}"
        "</style>"
    )
    html_parts.append("</head><body>")
    html_parts.append("<h1>Retail KPIs Dashboard</h1>")
    html_parts.append("<p class='sub'>ETL Lab 4 — Cleaned, validated, and transformed dataset (2023).</p>")
    html_parts.append("<div class='links'>")
    html_parts.append(f"<a href='{data_docs_rel}'>Great Expectations Data Docs</a>")
    html_parts.append(f"<a href='{validation_comparison_rel}'>Output Validation Comparison</a>")
    html_parts.append(f"<a href='{input_failure_summary_rel}'>Input Validation Failure Summary</a>")
    html_parts.append("</div>")

    html_parts.append("<div class='grid'>")
    html_parts.append(f"<div class='card'><div class='label'>Total Revenue</div><div class='value'>{total_revenue:,.2f}</div></div>")
    html_parts.append(f"<div class='card'><div class='label'>Transactions</div><div class='value'>{transaction_count:,}</div></div>")
    html_parts.append(f"<div class='card'><div class='label'>Unique Customers</div><div class='value'>{customer_count:,}</div></div>")
    html_parts.append(f"<div class='card'><div class='label'>Products</div><div class='value'>{product_count:,}</div></div>")
    html_parts.append(f"<div class='card'><div class='label'>Countries</div><div class='value'>{country_count:,}</div></div>")
    html_parts.append("</div>")

    html_parts.append("<div class='grid'>")
    html_parts.append(f"<div class='card'><div class='label'>DQ Score (Input)</div><div class='value'>{dq.get('data_quality_score_input', 0.0)}%</div></div>")
    html_parts.append(f"<div class='card'><div class='label'>DQ Score (Output)</div><div class='value'>{dq.get('data_quality_score_output', 0.0)}%</div></div>")
    html_parts.append("</div>")

    for title, items in charts:
        html_parts.append("<div class='section'>")
        html_parts.append(f"<h2>{title}</h2>")
        html_parts.append("<div class='charts'>")
        for caption, filename in items:
            html_parts.append("<div class='chart'>")
            html_parts.append(f"<img src='{filename}' alt='{caption}'>")
            html_parts.append(f"<p class='caption'>{caption}</p>")
            html_parts.append("</div>")
        html_parts.append("</div>")
        html_parts.append("</div>")

    html_parts.append("</body></html>")
    html = "".join(html_parts)
    OUTPUT_DASHBOARD_HTML.write_text(html, encoding="utf-8")
    return OUTPUT_DASHBOARD_HTML

def main() -> None:
    ensure_directories()
    df = load_data()
    kpi_total_revenue_by_country(df)
    kpi_avg_transaction_box_by_product(df)
    kpi_monthly_revenue_trend(df)
    kpi_peak_day_of_week_volume(df)
    kpi_top3_products_by_revenue(df)
    kpi_sales_distribution_by_country(df)
    kpi_dq_scores_bar()
    write_dashboard(df)

if __name__ == "__main__":
    main()
