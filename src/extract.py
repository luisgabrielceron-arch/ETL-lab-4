from __future__ import annotations

import json
import math
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


# =========================================================
# CONFIGURACIÓN GENERAL
# =========================================================
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
REPORTS_DIR = PROJECT_ROOT / "reports"

# Nombres esperados según guía / variaciones comunes
EXPECTED_CSV_NAMES = [
    "retail_etl_dataset.csv",
    "retail_etl_dataset_enriched.csv",
]

# Valores tipo nulo para analizar invoice_date como texto crudo
NULL_LIKE_DATE_VALUES = {"", "N/A", "NULL", "NONE", "NAN"}

# Tolerancia para comparar total_revenue vs quantity * price
REVENUE_TOLERANCE = 0.01


# =========================================================
# UTILIDADES
# =========================================================
def ensure_directories() -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)


def find_raw_csv(raw_dir: Path) -> Path:
    """
    Busca el CSV principal dentro de data/raw.
    Prioriza nombres esperados; si no existen, toma el primer CSV encontrado.
    """
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


def load_main_dataframe(csv_path: Path) -> pd.DataFrame:
    """
    Carga principal del dataset.
    Se deja a pandas inferir tipos para que el profiling de dtypes/missing
    sea representativo del ingreso real del archivo.
    """
    return pd.read_csv(csv_path)


def load_raw_strings_dataframe(csv_path: Path) -> pd.DataFrame:
    """
    Carga auxiliar como texto crudo para analizar formatos exactos de invoice_date
    y null-like strings sin que pandas los convierta automáticamente a NaN.
    """
    return pd.read_csv(csv_path, dtype=str, keep_default_na=False)


def to_numeric_series(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def normalize_for_json(value: Any) -> Any:
    """
    Convierte tipos de numpy/pandas a tipos serializables por json.
    """
    if pd.isna(value):
        return None
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        if math.isnan(float(value)):
            return None
        return float(value)
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    if isinstance(value, np.ndarray):
        return value.tolist()
    return value


def json_ready(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {str(k): json_ready(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [json_ready(v) for v in obj]
    return normalize_for_json(obj)


# =========================================================
# GREAT EXPECTATIONS
# =========================================================
def register_with_great_expectations(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Registra el DataFrame como datasource en memoria para Great Expectations.
    Incluye compatibilidad básica con APIs recientes y antiguas.
    """
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    datasource_name = f"etl_lab_pandas_{run_id}"
    asset_name = f"retail_raw_asset_{run_id}"

    try:
        import great_expectations as gx  # noqa: WPS433
    except Exception as exc:
        return {
            "registered": False,
            "error": f"No se pudo importar great_expectations: {exc}",
            "datasource_name": None,
            "asset_name": None,
            "api_mode": None,
        }

    try:
        context = gx.get_context()

        # API nueva (fluent)
        try:
            datasource = context.data_sources.add_pandas(name=datasource_name)
            asset = datasource.add_dataframe_asset(name=asset_name)
            batch_definition = asset.add_batch_definition_whole_dataframe(
                name=f"{asset_name}_batch_definition"
            )
            _ = batch_definition.get_batch(
                batch_parameters={"dataframe": df}
            )

            return {
                "registered": True,
                "error": None,
                "datasource_name": datasource_name,
                "asset_name": asset_name,
                "api_mode": "fluent_data_sources",
            }
        except Exception as fluent_error:
            # API antigua / alternativa
            try:
                datasource = context.sources.add_pandas(name=datasource_name)
                asset = datasource.add_dataframe_asset(name=asset_name)
                _ = asset.build_batch_request(dataframe=df)

                return {
                    "registered": True,
                    "error": None,
                    "datasource_name": datasource_name,
                    "asset_name": asset_name,
                    "api_mode": "legacy_sources",
                }
            except Exception as legacy_error:
                return {
                    "registered": False,
                    "error": (
                        "No se pudo registrar el DataFrame en Great Expectations. "
                        f"Fluent API error: {fluent_error} | "
                        f"Legacy API error: {legacy_error}"
                    ),
                    "datasource_name": datasource_name,
                    "asset_name": asset_name,
                    "api_mode": None,
                }

    except Exception as exc:
        return {
            "registered": False,
            "error": f"Error general creando contexto GE: {exc}",
            "datasource_name": datasource_name,
            "asset_name": asset_name,
            "api_mode": None,
        }


# =========================================================
# ANÁLISIS DE FECHAS
# =========================================================
def clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def is_null_like_date(value: Any) -> bool:
    return clean_text(value).upper() in NULL_LIKE_DATE_VALUES


def classify_invoice_date_format(value: Any) -> str:
    """
    Clasifica formatos:
    - YYYY-MM-DD
    - YYYY/MM/DD
    - DD-MM-YYYY
    - null_like
    - other
    """
    text = clean_text(value)

    if is_null_like_date(text):
        return "null_like"

    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return "YYYY-MM-DD"

    if re.fullmatch(r"\d{4}/\d{2}/\d{2}", text):
        return "YYYY/MM/DD"

    if re.fullmatch(r"\d{2}-\d{2}-\d{4}", text):
        return "DD-MM-YYYY"

    return "other"


def parse_mixed_invoice_date(value: Any) -> pd.Timestamp:
    """
    Intenta parsear los formatos esperados del laboratorio.
    Si falla, retorna NaT.
    """
    text = clean_text(value)

    if is_null_like_date(text):
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

    # Fallback conservador
    try:
        return pd.to_datetime(text, errors="coerce")
    except Exception:
        return pd.NaT


# =========================================================
# PROFILING
# =========================================================
def get_memory_usage_mb(df: pd.DataFrame) -> float:
    return float(df.memory_usage(deep=True).sum() / (1024 ** 2))


def get_missing_summary(df: pd.DataFrame) -> pd.DataFrame:
    rows = len(df)
    summary = pd.DataFrame({
        "column": df.columns,
        "missing_count": [int(df[col].isna().sum()) for col in df.columns],
    })
    summary["missing_pct"] = summary["missing_count"].apply(
        lambda x: round((x / rows) * 100, 4) if rows > 0 else 0.0
    )
    return summary


def get_numeric_stats(df: pd.DataFrame) -> Dict[str, Dict[str, Optional[float]]]:
    numeric_columns = ["quantity", "price", "total_revenue"]
    stats: Dict[str, Dict[str, Optional[float]]] = {}

    for col in numeric_columns:
        if col not in df.columns:
            stats[col] = {
                "min": None,
                "max": None,
                "mean": None,
                "median": None,
                "std": None,
            }
            continue

        series = to_numeric_series(df[col])
        stats[col] = {
            "min": normalize_for_json(series.min()),
            "max": normalize_for_json(series.max()),
            "mean": normalize_for_json(series.mean()),
            "median": normalize_for_json(series.median()),
            "std": normalize_for_json(series.std()),
        }

    return stats


def get_duplicate_invoice_metrics(df: pd.DataFrame) -> Dict[str, Optional[int]]:
    if "invoice_id" not in df.columns:
        return {
            "duplicate_invoice_id_rows_including_first": None,
            "duplicate_invoice_id_excess_rows": None,
            "duplicate_distinct_invoice_ids": None,
        }

    invoice_numeric = to_numeric_series(df["invoice_id"])

    duplicate_rows_including_first = int(invoice_numeric.duplicated(keep=False).sum())
    duplicate_excess_rows = int(invoice_numeric.duplicated(keep="first").sum())

    counts = invoice_numeric.value_counts(dropna=True)
    duplicate_distinct_ids = int((counts > 1).sum())

    return {
        "duplicate_invoice_id_rows_including_first": duplicate_rows_including_first,
        "duplicate_invoice_id_excess_rows": duplicate_excess_rows,
        "duplicate_distinct_invoice_ids": duplicate_distinct_ids,
    }


def get_revenue_mismatch_count(df: pd.DataFrame) -> int:
    required = {"quantity", "price", "total_revenue"}
    if not required.issubset(df.columns):
        return 0

    quantity = to_numeric_series(df["quantity"])
    price = to_numeric_series(df["price"])
    total_revenue = to_numeric_series(df["total_revenue"])

    expected_total = quantity * price

    comparable_mask = quantity.notna() & price.notna() & total_revenue.notna()
    mismatches = (
        comparable_mask
        & ((total_revenue - expected_total).abs() > REVENUE_TOLERANCE)
    )

    return int(mismatches.sum())


def get_invoice_date_format_distribution(raw_strings_df: pd.DataFrame) -> Dict[str, int]:
    if "invoice_date" not in raw_strings_df.columns:
        return {}

    format_series = raw_strings_df["invoice_date"].apply(classify_invoice_date_format)
    distribution = format_series.value_counts(dropna=False).to_dict()

    return {str(k): int(v) for k, v in distribution.items()}


def get_future_and_null_like_date_counts(
    raw_strings_df: pd.DataFrame,
) -> Dict[str, int]:
    if "invoice_date" not in raw_strings_df.columns:
        return {
            "future_invoice_date_count": 0,
            "null_like_invoice_date_count": 0,
        }

    raw_dates = raw_strings_df["invoice_date"].fillna("").astype(str)

    null_like_count = int(raw_dates.apply(is_null_like_date).sum())

    parsed_dates = raw_dates.apply(parse_mixed_invoice_date)
    future_count = int((parsed_dates > pd.Timestamp("2023-12-31")).sum())

    return {
        "future_invoice_date_count": future_count,
        "null_like_invoice_date_count": null_like_count,
    }


def get_categorical_cardinality(
    df: pd.DataFrame,
    raw_strings_df: pd.DataFrame,
) -> Dict[str, Optional[int]]:
    result: Dict[str, Optional[int]] = {}

    for col in ["product", "country"]:
        result[col] = int(df[col].nunique(dropna=True)) if col in df.columns else None

    if "invoice_date" in raw_strings_df.columns:
        format_labels = raw_strings_df["invoice_date"].apply(classify_invoice_date_format)
        result["invoice_date_formats"] = int(format_labels.nunique(dropna=True))
    else:
        result["invoice_date_formats"] = None

    return result


def top_values_as_string(series: pd.Series, limit: int = 5) -> str:
    vc = (
        series.astype("string")
        .replace("<NA>", pd.NA)
        .dropna()
        .value_counts()
        .head(limit)
    )
    if vc.empty:
        return ""

    parts = [f"{idx} ({cnt})" for idx, cnt in vc.items()]
    return "; ".join(parts)


def build_profiling_summary_table(
    df: pd.DataFrame,
    raw_strings_df: pd.DataFrame,
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    numeric_stats = get_numeric_stats(df)
    total_rows = len(df)

    for col in df.columns:
        series = df[col]
        raw_series = raw_strings_df[col] if col in raw_strings_df.columns else series.astype(str)

        row: Dict[str, Any] = {
            "column": col,
            "dtype": str(series.dtype),
            "rows": int(total_rows),
            "non_null_count": int(series.notna().sum()),
            "null_count": int(series.isna().sum()),
            "null_pct": round((series.isna().sum() / total_rows) * 100, 4) if total_rows > 0 else 0.0,
            "unique_non_null_count": int(series.nunique(dropna=True)),
            "memory_bytes": int(series.memory_usage(deep=True)),
            "top_5_values": top_values_as_string(raw_series),
            "min": None,
            "max": None,
            "mean": None,
            "median": None,
            "std": None,
            "invoice_date_format_cardinality": None,
            "invoice_date_format_distribution": None,
        }

        if col in numeric_stats:
            row.update(numeric_stats[col])

        if col == "invoice_date":
            format_distribution = get_invoice_date_format_distribution(raw_strings_df)
            row["invoice_date_format_cardinality"] = int(len(format_distribution))
            row["invoice_date_format_distribution"] = json.dumps(
                format_distribution,
                ensure_ascii=False
            )

        rows.append(row)

    summary_df = pd.DataFrame(rows)
    return summary_df


def build_overview_payload(
    csv_path: Path,
    df: pd.DataFrame,
    raw_strings_df: pd.DataFrame,
    ge_status: Dict[str, Any],
) -> Dict[str, Any]:
    overview = {
        "dataset_path": str(csv_path),
        "generated_at": datetime.now().isoformat(),
        "shape": {
            "rows": int(df.shape[0]),
            "columns": int(df.shape[1]),
        },
        "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
        "dataset_memory_mb": round(get_memory_usage_mb(df), 4),
        "missing_summary": get_missing_summary(df).to_dict(orient="records"),
        "categorical_cardinality": get_categorical_cardinality(df, raw_strings_df),
        "numeric_stats": get_numeric_stats(df),
        "duplicate_invoice_metrics": get_duplicate_invoice_metrics(df),
        "revenue_mismatch_count": get_revenue_mismatch_count(df),
        "invoice_date_format_distribution": get_invoice_date_format_distribution(raw_strings_df),
        "invoice_date_timeliness": get_future_and_null_like_date_counts(raw_strings_df),
        "great_expectations_registration": ge_status,
    }
    return json_ready(overview)


# =========================================================
# SALIDAS
# =========================================================
def save_outputs(
    profiling_summary_df: pd.DataFrame,
    overview_payload: Dict[str, Any],
) -> None:
    profiling_csv = REPORTS_DIR / "profiling_summary.csv"
    overview_json = REPORTS_DIR / "profiling_overview.json"
    overview_md = REPORTS_DIR / "profiling_overview.md"

    profiling_summary_df.to_csv(profiling_csv, index=False, encoding="utf-8-sig")

    with open(overview_json, "w", encoding="utf-8") as f:
        json.dump(overview_payload, f, indent=4, ensure_ascii=False)

    with open(overview_md, "w", encoding="utf-8") as f:
        f.write("# Profiling Overview\n\n")
        f.write(f"- Dataset: `{overview_payload['dataset_path']}`\n")
        f.write(f"- Generated at: `{overview_payload['generated_at']}`\n")
        f.write(f"- Rows: **{overview_payload['shape']['rows']}**\n")
        f.write(f"- Columns: **{overview_payload['shape']['columns']}**\n")
        f.write(f"- Memory (MB): **{overview_payload['dataset_memory_mb']}**\n\n")

        f.write("## Duplicate invoice_id metrics\n")
        dup = overview_payload["duplicate_invoice_metrics"]
        for key, value in dup.items():
            f.write(f"- {key}: **{value}**\n")

        f.write("\n## Revenue mismatch\n")
        f.write(
            f"- Rows where `total_revenue != quantity * price` "
            f"(tol ±{REVENUE_TOLERANCE}): "
            f"**{overview_payload['revenue_mismatch_count']}**\n"
        )

        f.write("\n## Invoice date format distribution\n")
        for key, value in overview_payload["invoice_date_format_distribution"].items():
            f.write(f"- {key}: **{value}**\n")

        f.write("\n## Invoice date timeliness\n")
        for key, value in overview_payload["invoice_date_timeliness"].items():
            f.write(f"- {key}: **{value}**\n")

        f.write("\n## Great Expectations registration\n")
        ge = overview_payload["great_expectations_registration"]
        for key, value in ge.items():
            f.write(f"- {key}: **{value}**\n")


# =========================================================
# CONSOLA
# =========================================================
def print_console_summary(
    csv_path: Path,
    df: pd.DataFrame,
    overview_payload: Dict[str, Any],
) -> None:
    print("\n" + "=" * 72)
    print("EXTRACT & PROFILING")
    print("=" * 72)
    print(f"Archivo cargado: {csv_path}")
    print(f"Shape: {df.shape}")
    print(f"Memoria total (MB): {overview_payload['dataset_memory_mb']}")
    print("\nDtypes:")
    for col, dtype in overview_payload["dtypes"].items():
        print(f"  - {col}: {dtype}")

    print("\nCardinalidad categórica:")
    for key, value in overview_payload["categorical_cardinality"].items():
        print(f"  - {key}: {value}")

    print("\nEstadísticas numéricas:")
    for col, stats in overview_payload["numeric_stats"].items():
        print(f"  - {col}: {stats}")

    print("\nDuplicados invoice_id:")
    for key, value in overview_payload["duplicate_invoice_metrics"].items():
        print(f"  - {key}: {value}")

    print(
        "\nRows con total_revenue != quantity * price "
        f"(tol ±{REVENUE_TOLERANCE}): "
        f"{overview_payload['revenue_mismatch_count']}"
    )

    print("\nDistribución formatos invoice_date:")
    for key, value in overview_payload["invoice_date_format_distribution"].items():
        print(f"  - {key}: {value}")

    print("\nFechas futuras / null-like:")
    for key, value in overview_payload["invoice_date_timeliness"].items():
        print(f"  - {key}: {value}")

    print("\nGreat Expectations:")
    for key, value in overview_payload["great_expectations_registration"].items():
        print(f"  - {key}: {value}")

    print("\nArchivos generados:")
    print(f"  - {REPORTS_DIR / 'profiling_summary.csv'}")
    print(f"  - {REPORTS_DIR / 'profiling_overview.json'}")
    print(f"  - {REPORTS_DIR / 'profiling_overview.md'}")
    print("=" * 72 + "\n")


# =========================================================
# MAIN
# =========================================================
def main() -> None:
    ensure_directories()

    csv_path = find_raw_csv(RAW_DIR)
    df = load_main_dataframe(csv_path)
    raw_strings_df = load_raw_strings_dataframe(csv_path)

    ge_status = register_with_great_expectations(df)

    profiling_summary_df = build_profiling_summary_table(df, raw_strings_df)
    overview_payload = build_overview_payload(csv_path, df, raw_strings_df, ge_status)

    save_outputs(profiling_summary_df, overview_payload)
    print_console_summary(csv_path, df, overview_payload)


if __name__ == "__main__":
    main()