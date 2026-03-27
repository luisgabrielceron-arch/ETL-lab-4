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

if __name__ == "__main__":
    main()
