from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import pandas as pd


# =========================================================
# CONFIGURACIÓN
# =========================================================
PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
REPORTS_DIR = PROJECT_ROOT / "reports"

INPUT_CLEAN_CSV = PROCESSED_DIR / "retail_clean.csv"
OUTPUT_TRANSFORMED_CSV = PROCESSED_DIR / "retail_transformed.csv"

VALID_COUNTRY_LOOKUP = {
    "colombia": "Colombia",
    "co": "Colombia",
    "ecuador": "Ecuador",
    "ecuador ": "Ecuador",
    "peru": "Peru",
    "péru": "Peru",
    "pe": "Peru",
    "chile": "Chile",
    "cl": "Chile",
}

DAY_NAME_MAP = {
    "Monday": "Monday",
    "Tuesday": "Tuesday",
    "Wednesday": "Wednesday",
    "Thursday": "Thursday",
    "Friday": "Friday",
    "Saturday": "Saturday",
    "Sunday": "Sunday",
}

EXPECTED_COLUMNS = [
    "invoice_id",
    "customer_id",
    "product",
    "quantity",
    "price",
    "total_revenue",
    "country",
    "invoice_date",
]


# =========================================================
# UTILIDADES
# =========================================================
def ensure_directories() -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)


def load_clean_dataframe(csv_path: Path) -> pd.DataFrame:
    if not csv_path.exists():
        raise FileNotFoundError(
            f"No se encontró el archivo limpio esperado: {csv_path}"
        )

    df = pd.read_csv(csv_path)

    missing_cols = [col for col in EXPECTED_COLUMNS if col not in df.columns]
    if missing_cols:
        raise ValueError(
            f"El dataset limpio no contiene estas columnas esperadas: {missing_cols}"
        )

    return df[EXPECTED_COLUMNS].copy()


def parse_single_date(value: Any) -> pd.Timestamp:
    if pd.isna(value):
        return pd.NaT

    text = str(value).strip()
    if not text:
        return pd.NaT

    known_formats = [
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d-%m-%Y",
    ]

    for fmt in known_formats:
        try:
            return pd.to_datetime(text, format=fmt, errors="raise")
        except Exception:
            continue

    try:
        return pd.to_datetime(text, errors="coerce")
    except Exception:
        return pd.NaT


def standardize_country(value: Any) -> str:
    if pd.isna(value):
        return ""

    text = str(value).strip()
    key = text.casefold()

    if key in VALID_COUNTRY_LOOKUP:
        return VALID_COUNTRY_LOOKUP[key]

    return text.title()


def normalize_product(value: Any) -> str:
    if pd.isna(value):
        return ""

    text = str(value).strip()
    return text.title()


def build_revenue_bin(series: pd.Series) -> pd.Series:
    """
    Crea Low / Medium / High con cuantiles.
    Si hay problemas por valores repetidos en cortes, usa rank para estabilizar.
    """
    ranked = series.rank(method="first")
    return pd.qcut(
        ranked,
        q=3,
        labels=["Low", "Medium", "High"],
    ).astype("string")


def json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): json_ready(v) for k, v in value.items()}
    if isinstance(value, list):
        return [json_ready(v) for v in value]
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


# =========================================================
# TRANSFORMACIÓN
# =========================================================
def apply_transformation(df: pd.DataFrame) -> pd.DataFrame:
    transformed = df.copy()

    # 1) Standardize country
    transformed["country"] = transformed["country"].apply(standardize_country)

    # 2) Parse invoice_date and derive calendar columns
    transformed["invoice_date"] = transformed["invoice_date"].apply(parse_single_date)
    transformed["year"] = transformed["invoice_date"].dt.year.astype("Int64")
    transformed["month"] = transformed["invoice_date"].dt.month.astype("Int64")
    transformed["day_of_week"] = (
        transformed["invoice_date"]
        .dt.day_name()
        .map(DAY_NAME_MAP)
        .astype("string")
    )

    # 3) Cast customer_id to Int64
    transformed["customer_id"] = (
        pd.to_numeric(transformed["customer_id"], errors="coerce")
        .round()
        .astype("Int64")
    )

    # 4) Normalize product
    transformed["product"] = transformed["product"].apply(normalize_product).astype("string")

    # 5) Revenue bin
    transformed["revenue_bin"] = build_revenue_bin(
        pd.to_numeric(transformed["total_revenue"], errors="coerce")
    )

    # Asegurar tipos esperados en columnas base
    transformed["invoice_id"] = pd.to_numeric(
        transformed["invoice_id"], errors="coerce"
    ).astype("Int64")
    transformed["quantity"] = pd.to_numeric(
        transformed["quantity"], errors="coerce"
    ).astype("Int64")
    transformed["price"] = pd.to_numeric(
        transformed["price"], errors="coerce"
    )
    transformed["total_revenue"] = pd.to_numeric(
        transformed["total_revenue"], errors="coerce"
    )
    transformed["country"] = transformed["country"].astype("string")

    return transformed


# =========================================================
# REPORTES
# =========================================================
def build_transformation_summary(df_before: pd.DataFrame, df_after: pd.DataFrame) -> Dict[str, Any]:
    return {
        "generated_at": datetime.now().isoformat(),
        "row_count_before": int(len(df_before)),
        "row_count_after": int(len(df_after)),
        "columns_before": list(df_before.columns),
        "columns_after": list(df_after.columns),
        "dtypes_after": {col: str(dtype) for col, dtype in df_after.dtypes.items()},
        "country_values_after": sorted(
            [str(v) for v in df_after["country"].dropna().unique().tolist()]
        ),
        "revenue_bin_distribution": {
            str(k): int(v)
            for k, v in df_after["revenue_bin"].value_counts(dropna=False).to_dict().items()
        },
        "month_distribution": {
            str(k): int(v)
            for k, v in df_after["month"].value_counts(dropna=False).sort_index().to_dict().items()
        },
    }


def save_outputs(df_after: pd.DataFrame, summary: Dict[str, Any]) -> Dict[str, Path]:
    df_after.to_csv(OUTPUT_TRANSFORMED_CSV, index=False, encoding="utf-8-sig")

    summary_json = REPORTS_DIR / "transformation_summary.json"
    summary_md = REPORTS_DIR / "transformation_summary.md"

    with open(summary_json, "w", encoding="utf-8") as f:
        json.dump(json_ready(summary), f, indent=4, ensure_ascii=False)

    with open(summary_md, "w", encoding="utf-8") as f:
        f.write("# Transformation Summary\n\n")
        f.write(f"- Row count before: **{summary['row_count_before']}**\n")
        f.write(f"- Row count after: **{summary['row_count_after']}**\n")
        f.write(f"- Columns before: **{len(summary['columns_before'])}**\n")
        f.write(f"- Columns after: **{len(summary['columns_after'])}**\n\n")

        f.write("## New / final columns\n")
        for col in summary["columns_after"]:
            f.write(f"- `{col}` ({summary['dtypes_after'].get(col)})\n")

        f.write("\n## Country values after standardization\n")
        for value in summary["country_values_after"]:
            f.write(f"- {value}\n")

        f.write("\n## Revenue bin distribution\n")
        for key, value in summary["revenue_bin_distribution"].items():
            f.write(f"- {key}: **{value}**\n")

        f.write("\n## Month distribution\n")
        for key, value in summary["month_distribution"].items():
            f.write(f"- {key}: **{value}**\n")

    return {
        "transformed_csv": OUTPUT_TRANSFORMED_CSV,
        "summary_json": summary_json,
        "summary_md": summary_md,
    }


# =========================================================
# CONSOLA
# =========================================================
def print_console_summary(summary: Dict[str, Any], output_paths: Dict[str, Path]) -> None:
    print("\n" + "=" * 72)
    print("TRANSFORMATION")
    print("=" * 72)
    print(f"Row count before: {summary['row_count_before']}")
    print(f"Row count after : {summary['row_count_after']}")

    print("\nColumns after transformation:")
    for col in summary["columns_after"]:
        print(f"  - {col} ({summary['dtypes_after'].get(col)})")

    print("\nCountry values after standardization:")
    for value in summary["country_values_after"]:
        print(f"  - {value}")

    print("\nRevenue bin distribution:")
    for key, value in summary["revenue_bin_distribution"].items():
        print(f"  - {key}: {value}")

    print("\nMonth distribution:")
    for key, value in summary["month_distribution"].items():
        print(f"  - {key}: {value}")

    print("\nArchivos generados:")
    for key, path in output_paths.items():
        print(f"  - {key}: {path}")

    print("=" * 72 + "\n")


# =========================================================
# MAIN
# =========================================================
def main() -> None:
    ensure_directories()

    df_before = load_clean_dataframe(INPUT_CLEAN_CSV)
    df_after = apply_transformation(df_before)

    summary = build_transformation_summary(df_before, df_after)
    output_paths = save_outputs(df_after, summary)

    print_console_summary(summary, output_paths)


if __name__ == "__main__":
    main()