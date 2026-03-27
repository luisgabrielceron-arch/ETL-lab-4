from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


# =========================================================
# CONFIGURACIÓN
# =========================================================
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
REPORTS_DIR = PROJECT_ROOT / "reports"

EXPECTED_CSV_NAMES = [
    "retail_etl_dataset.csv",
    "retail_etl_dataset_enriched.csv",
]

OUTPUT_CLEAN_CSV = PROCESSED_DIR / "retail_clean.csv"
REVENUE_TOLERANCE = 0.01
MAX_VALID_DATE = pd.Timestamp("2023-12-31")
NULL_LIKE_DATE_VALUES = {"", "N/A", "NULL", "NONE", "NAN"}


# =========================================================
# UTILIDADES
# =========================================================
def ensure_directories() -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)


def find_raw_csv(raw_dir: Path) -> Path:
    for name in EXPECTED_CSV_NAMES:
        candidate = raw_dir / name
        if candidate.exists():
            return candidate

    csv_files = sorted(raw_dir.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(
            f"No se encontró ningún archivo CSV dentro de: {raw_dir}"
        )
    return csv_files[0]


def load_raw_dataframe(csv_path: Path) -> pd.DataFrame:
    """
    Carga el CSV preservando strings crudos para detectar null-like invoice_date,
    y luego convierte columnas numéricas.
    """
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)

    # Normalización básica de texto
    for col in df.columns:
        df[col] = df[col].astype("string").str.strip()

    # Convertir numéricos esperados
    numeric_cols = ["invoice_id", "customer_id", "quantity", "price", "total_revenue"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Convertir invoice_date null-like a nulo real para que el before/after los refleje
    if "invoice_date" in df.columns:
        raw_upper = df["invoice_date"].astype("string").str.upper()
        null_like_mask = raw_upper.isin(NULL_LIKE_DATE_VALUES)
        df.loc[null_like_mask, "invoice_date"] = pd.NA

    return df


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


def parse_date_series(series: pd.Series) -> pd.Series:
    return series.apply(parse_single_date)


def null_counts_dict(df: pd.DataFrame) -> Dict[str, int]:
    return {col: int(df[col].isna().sum()) for col in df.columns}


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
# CLEANING
# =========================================================
def log_action(
    logs: List[Dict[str, Any]],
    issue: str,
    strategy: str,
    justification: str,
    action_type: str,
    rows_affected: int,
    log_requirement: str,
) -> None:
    logs.append(
        {
            "issue": issue,
            "cleaning_strategy": strategy,
            "justification": justification,
            "action_type": action_type,
            "rows_affected": int(rows_affected),
            "log_requirement": log_requirement,
        }
    )


def apply_cleaning(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Aplica la lógica de limpieza definida para Task d.
    """
    logs: List[Dict[str, Any]] = []

    # 1) Duplicate invoice_id -> remove duplicates; keep first occurrence
    duplicate_mask = df.duplicated(subset=["invoice_id"], keep="first")
    duplicate_removed = int(duplicate_mask.sum())
    df = df.loc[~duplicate_mask].copy()
    log_action(
        logs=logs,
        issue="duplicate_invoice_id",
        strategy="Eliminar duplicados y conservar la primera ocurrencia",
        justification="Las filas duplicadas inflan ingresos y afectan BO-1",
        action_type="drop",
        rows_affected=duplicate_removed,
        log_requirement="Count removed",
    )

    # 2) Negative / invalid quantity -> drop rows with quantity < 1
    invalid_quantity_mask = df["quantity"].isna() | (df["quantity"] < 1)
    invalid_quantity_removed = int(invalid_quantity_mask.sum())
    df = df.loc[~invalid_quantity_mask].copy()
    log_action(
        logs=logs,
        issue="negative_or_invalid_quantity",
        strategy="Eliminar filas con quantity < 1 o nulo",
        justification="No es posible interpretar unidades vendidas negativas o faltantes",
        action_type="drop",
        rows_affected=invalid_quantity_removed,
        log_requirement="Count dropped",
    )

    # 3) Negative / invalid price -> drop rows with price <= 0
    invalid_price_mask = df["price"].isna() | (df["price"] <= 0)
    invalid_price_removed = int(invalid_price_mask.sum())
    df = df.loc[~invalid_price_mask].copy()
    log_action(
        logs=logs,
        issue="negative_or_invalid_price",
        strategy="Eliminar filas con price <= 0 o nulo",
        justification="No es válido analizar ventas con precio cero, negativo o faltante",
        action_type="drop",
        rows_affected=invalid_price_removed,
        log_requirement="Count dropped",
    )

    # 4) NULL customer_id -> drop rows
    null_customer_mask = df["customer_id"].isna()
    null_customer_removed = int(null_customer_mask.sum())
    df = df.loc[~null_customer_mask].copy()
    log_action(
        logs=logs,
        issue="null_customer_id",
        strategy="Eliminar filas con customer_id nulo",
        justification="No se puede asociar la transacción a un cliente, afectando BO-3 y BO-4",
        action_type="drop",
        rows_affected=null_customer_removed,
        log_requirement="Count dropped",
    )

    # 5) Null-like / missing invoice_date -> drop rows
    null_invoice_date_mask = df["invoice_date"].isna()
    null_invoice_date_removed = int(null_invoice_date_mask.sum())
    df = df.loc[~null_invoice_date_mask].copy()
    log_action(
        logs=logs,
        issue="null_like_invoice_date",
        strategy="Eliminar filas con invoice_date nulo o null-like",
        justification="Sin fecha válida no se puede hacer análisis temporal confiable (BO-2)",
        action_type="drop",
        rows_affected=null_invoice_date_removed,
        log_requirement="Count dropped",
    )

    # 6) Unparseable invoice_date -> drop rows
    parsed_dates = parse_date_series(df["invoice_date"])
    unparseable_mask = parsed_dates.isna()
    unparseable_removed = int(unparseable_mask.sum())
    df = df.loc[~unparseable_mask].copy()
    parsed_dates = parsed_dates.loc[~unparseable_mask].copy()
    log_action(
        logs=logs,
        issue="unparseable_invoice_date",
        strategy="Eliminar filas cuya invoice_date no puede convertirse a fecha",
        justification="Una fecha no interpretable no puede entrar de forma segura a transformación",
        action_type="drop",
        rows_affected=unparseable_removed,
        log_requirement="Count dropped",
    )

    # 7) Future invoice_date -> drop rows
    future_mask = parsed_dates > MAX_VALID_DATE
    future_removed = int(future_mask.sum())
    df = df.loc[~future_mask].copy()
    parsed_dates = parsed_dates.loc[~future_mask].copy()
    log_action(
        logs=logs,
        issue="future_invoice_date",
        strategy="Eliminar filas con invoice_date > 2023-12-31",
        justification="La guía define que invoice_date debe estar dentro de 2023 (BO-2)",
        action_type="drop",
        rows_affected=future_removed,
        log_requirement="Count dropped",
    )

    # 8) Inaccurate total_revenue -> correct derived field
    expected_total = (df["quantity"] * df["price"]).round(2)
    inaccurate_mask = (df["total_revenue"] - expected_total).abs() > REVENUE_TOLERANCE
    inaccurate_corrected = int(inaccurate_mask.sum())
    df.loc[inaccurate_mask, "total_revenue"] = expected_total.loc[inaccurate_mask]
    log_action(
        logs=logs,
        issue="inaccurate_total_revenue",
        strategy="Recalcular total_revenue = quantity * price",
        justification="Es un campo derivado; si quantity y price ya son válidos, se puede corregir sin perder la transacción",
        action_type="correct",
        rows_affected=inaccurate_corrected,
        log_requirement="Count corrected",
    )

    # Reset index para dejar dataset limpio y ordenado
    df = df.reset_index(drop=True)

    logs_df = pd.DataFrame(logs)
    return df, logs_df


# =========================================================
# REPORTES
# =========================================================
def build_before_after_summary(df_before: pd.DataFrame, df_after: pd.DataFrame, logs_df: pd.DataFrame) -> Dict[str, Any]:
    total_dropped = int(logs_df.loc[logs_df["action_type"] == "drop", "rows_affected"].sum())
    total_corrected = int(logs_df.loc[logs_df["action_type"] == "correct", "rows_affected"].sum())

    return {
        "generated_at": datetime.now().isoformat(),
        "row_count_before": int(len(df_before)),
        "row_count_after": int(len(df_after)),
        "rows_dropped_total": total_dropped,
        "rows_corrected_total": total_corrected,
        "null_counts_before": null_counts_dict(df_before),
        "null_counts_after": null_counts_dict(df_after),
        "rows_affected_per_reason": logs_df.to_dict(orient="records"),
    }


def save_outputs(df_after: pd.DataFrame, logs_df: pd.DataFrame, summary: Dict[str, Any]) -> Dict[str, Path]:
    OUTPUT_CLEAN_CSV.parent.mkdir(parents=True, exist_ok=True)
    df_after.to_csv(OUTPUT_CLEAN_CSV, index=False, encoding="utf-8-sig")

    actions_csv = REPORTS_DIR / "cleaning_actions.csv"
    actions_md = REPORTS_DIR / "cleaning_actions.md"
    summary_json = REPORTS_DIR / "cleaning_before_after_summary.json"
    summary_md = REPORTS_DIR / "cleaning_before_after_summary.md"

    logs_df.to_csv(actions_csv, index=False, encoding="utf-8-sig")

    with open(summary_json, "w", encoding="utf-8") as f:
        json.dump(json_ready(summary), f, indent=4, ensure_ascii=False)

    with open(actions_md, "w", encoding="utf-8") as f:
        f.write("# Cleaning Actions\n\n")
        try:
            f.write(logs_df.to_markdown(index=False))
        except Exception:
            f.write(logs_df.to_string(index=False))

    with open(summary_md, "w", encoding="utf-8") as f:
        f.write("# Cleaning Before/After Summary\n\n")
        f.write(f"- Row count before: **{summary['row_count_before']}**\n")
        f.write(f"- Row count after: **{summary['row_count_after']}**\n")
        f.write(f"- Rows dropped total: **{summary['rows_dropped_total']}**\n")
        f.write(f"- Rows corrected total: **{summary['rows_corrected_total']}**\n\n")

        f.write("## Null counts before\n")
        for col, value in summary["null_counts_before"].items():
            f.write(f"- {col}: **{value}**\n")

        f.write("\n## Null counts after\n")
        for col, value in summary["null_counts_after"].items():
            f.write(f"- {col}: **{value}**\n")

        f.write("\n## Rows affected per reason\n")
        try:
            f.write(logs_df.to_markdown(index=False))
        except Exception:
            f.write(logs_df.to_string(index=False))

    return {
        "clean_csv": OUTPUT_CLEAN_CSV,
        "actions_csv": actions_csv,
        "actions_md": actions_md,
        "summary_json": summary_json,
        "summary_md": summary_md,
    }


# =========================================================
# CONSOLA
# =========================================================
def print_console_summary(summary: Dict[str, Any], logs_df: pd.DataFrame, output_paths: Dict[str, Path]) -> None:
    print("\n" + "=" * 72)
    print("CLEANING")
    print("=" * 72)
    print(f"Row count before: {summary['row_count_before']}")
    print(f"Row count after : {summary['row_count_after']}")
    print(f"Rows dropped total: {summary['rows_dropped_total']}")
    print(f"Rows corrected total: {summary['rows_corrected_total']}")

    print("\nNull counts before:")
    for col, value in summary["null_counts_before"].items():
        print(f"  - {col}: {value}")

    print("\nNull counts after:")
    for col, value in summary["null_counts_after"].items():
        print(f"  - {col}: {value}")

    print("\nRows affected per reason:")
    for _, row in logs_df.iterrows():
        print(
            f"  - {row['issue']}: {row['rows_affected']} "
            f"({row['action_type']})"
        )

    print("\nArchivos generados:")
    for key, path in output_paths.items():
        print(f"  - {key}: {path}")

    print("=" * 72 + "\n")


# =========================================================
# MAIN
# =========================================================
def main() -> None:
    ensure_directories()

    csv_path = find_raw_csv(RAW_DIR)
    df_before = load_raw_dataframe(csv_path)

    original_columns = [
        "invoice_id",
        "customer_id",
        "product",
        "quantity",
        "price",
        "total_revenue",
        "country",
        "invoice_date",
    ]
    df_before = df_before[original_columns].copy()

    df_after, logs_df = apply_cleaning(df_before.copy())
    summary = build_before_after_summary(df_before, df_after, logs_df)
    output_paths = save_outputs(df_after, logs_df, summary)

    print_console_summary(summary, logs_df, output_paths)


if __name__ == "__main__":
    main()

    