from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd
import great_expectations as gx


# =========================================================
# CONFIGURACIÓN
# =========================================================
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
REPORTS_DIR = PROJECT_ROOT / "reports"

# IMPORTANTE:
# Esta ruta asume que renombraste la carpeta local de contexto
# de "great_expectations" a "gx_context" para evitar conflicto
# con la librería instalada.
GE_DIR = PROJECT_ROOT / "gx_context"

EXPECTED_CSV_NAMES = [
    "retail_etl_dataset.csv",
    "retail_etl_dataset_enriched.csv",
]

SUITE_NAME = "retail_input_validation_suite"
RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
VALIDATION_NAME = f"retail_input_validation_definition_{RUN_ID}"
REVENUE_TOLERANCE = 0.01

# Ajusta esta lista si tu catálogo real usa nombres distintos
VALID_PRODUCTS = [
    "Laptop",
    "Tablet",
    "Smartphone",
    "Headphones",
    "Keyboard",
    "Mouse",
    "Monitor",
    "Printer",
]

VALID_COUNTRIES = [
    "Colombia",
    "Ecuador",
    "Peru",
    "Chile",
]


# =========================================================
# UTILIDADES
# =========================================================
def ensure_directories() -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    GE_DIR.mkdir(parents=True, exist_ok=True)
    (GE_DIR / "expectations").mkdir(parents=True, exist_ok=True)
    (GE_DIR / "uncommitted").mkdir(parents=True, exist_ok=True)


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
    df = pd.read_csv(csv_path)

    # Normalización mínima de tipos para validar
    numeric_cols = ["invoice_id", "customer_id", "quantity", "price", "total_revenue"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    text_cols = ["product", "country", "invoice_date"]
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].astype("string")

    # Columnas helper para reglas de calidad
    df["_revenue_expected"] = df["quantity"] * df["price"]
    df["_revenue_diff_abs"] = (df["total_revenue"] - df["_revenue_expected"]).abs()

    parsed_dates = pd.to_datetime(
        df["invoice_date"],
        errors="coerce",
        format="mixed",
        dayfirst=True,
    )
    df["_invoice_date_iso"] = parsed_dates.dt.strftime("%Y-%m-%d")

    return df


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
# CONTEXTO GX
# =========================================================
def get_context() -> Any:
    try:
        return gx.get_context(context_root_dir=str(GE_DIR))
    except TypeError:
        try:
            return gx.get_context(project_root_dir=str(GE_DIR))
        except Exception:
            return gx.get_context()
    except Exception:
        return gx.get_context()


def build_batch_definition(context: Any) -> Tuple[Any, str, str, str]:
    datasource_name = f"retail_input_ds_{RUN_ID}"
    asset_name = f"retail_raw_asset_{RUN_ID}"
    batch_definition_name = f"retail_raw_batch_{RUN_ID}"

    datasource = context.data_sources.add_pandas(name=datasource_name)
    asset = datasource.add_dataframe_asset(name=asset_name)
    batch_definition = asset.add_batch_definition_whole_dataframe(
        batch_definition_name
    )

    return batch_definition, datasource_name, asset_name, batch_definition_name


# =========================================================
# SUITE Y EXPECTATIONS
# =========================================================
def build_expectation_suite(context: Any) -> Any:
    suite = gx.ExpectationSuite(name=SUITE_NAME)

    # Completeness
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="invoice_date")
    )

    # Uniqueness
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeUnique(column="invoice_id")
    )

    # Validity
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="quantity",
            min_value=1,
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="price",
            min_value=0.01,
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="product",
            value_set=VALID_PRODUCTS,
        )
    )

    # Accuracy
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="_revenue_diff_abs",
            min_value=0.0,
            max_value=REVENUE_TOLERANCE,
        )
    )

    # Consistency
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="country",
            value_set=VALID_COUNTRIES,
        )
    )

    # Timeliness / date quality
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToMatchRegex(
            column="invoice_date",
            regex=r"^\d{4}-\d{2}-\d{2}$",
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="_invoice_date_iso",
            min_value="2023-01-01",
            max_value="2023-12-31",
        )
    )

    # Guarda o actualiza la suite
    context.suites.add_or_update(suite)
    return suite


def save_suite_json(suite: Any) -> Path:
    suite_path = GE_DIR / "expectations" / f"{SUITE_NAME}.json"
    suite_dict = suite.to_json_dict()

    with open(suite_path, "w", encoding="utf-8") as f:
        json.dump(json_ready(suite_dict), f, indent=4, ensure_ascii=False)

    return suite_path


# =========================================================
# VALIDACIÓN
# =========================================================
def run_validation(context: Any, batch_definition: Any, suite: Any, df: pd.DataFrame) -> Any:
    validation = gx.ValidationDefinition(
        name=VALIDATION_NAME,
        data=batch_definition,
        suite=suite,
    )

    # Si la API permite guardar/actualizar, lo hace.
    # Si no, igual corre con nombre único por ejecución y evita conflicto.
    try:
        validation = context.validation_definitions.add_or_update(validation)
    except Exception:
        pass

    validation_result = validation.run(
        batch_parameters={"dataframe": df},
        result_format="SUMMARY",
    )

    return validation_result


def validation_result_to_dict(validation_result: Any) -> Dict[str, Any]:
    if hasattr(validation_result, "to_json_dict"):
        return validation_result.to_json_dict()
    if isinstance(validation_result, dict):
        return validation_result
    return json.loads(json.dumps(validation_result, default=str))


# =========================================================
# RESUMEN DE REGLAS
# =========================================================
def build_rule_catalog() -> Dict[Tuple[str, str], Dict[str, str]]:
    return {
        ("expect_column_values_to_not_be_null", "customer_id"): {
            "label": "customer_id not null",
            "dimension": "Completeness",
            "expected_result": "FAIL",
        },
        ("expect_column_values_to_not_be_null", "invoice_date"): {
            "label": "invoice_date not null",
            "dimension": "Completeness",
            "expected_result": "FAIL",
        },
        ("expect_column_values_to_be_unique", "invoice_id"): {
            "label": "invoice_id unique",
            "dimension": "Uniqueness",
            "expected_result": "FAIL",
        },
        ("expect_column_values_to_be_between", "quantity"): {
            "label": "quantity >= 1",
            "dimension": "Validity",
            "expected_result": "FAIL",
        },
        ("expect_column_values_to_be_between", "price"): {
            "label": "price >= 0.01",
            "dimension": "Validity",
            "expected_result": "FAIL",
        },
        ("expect_column_values_to_be_in_set", "product"): {
            "label": "product in allowed catalog",
            "dimension": "Validity",
            "expected_result": "FAIL",
        },
        ("expect_column_values_to_be_between", "_revenue_diff_abs"): {
            "label": "Custom: total_revenue == quantity * price (tol ±0.01)",
            "dimension": "Accuracy",
            "expected_result": "FAIL",
        },
        ("expect_column_values_to_be_in_set", "country"): {
            "label": "country in {Colombia, Ecuador, Peru, Chile}",
            "dimension": "Consistency",
            "expected_result": "FAIL",
        },
        ("expect_column_values_to_match_regex", "invoice_date"): {
            "label": "invoice_date matches YYYY-MM-DD",
            "dimension": "Timeliness",
            "expected_result": "FAIL",
        },
        ("expect_column_values_to_be_between", "_invoice_date_iso"): {
            "label": "invoice_date within 2023",
            "dimension": "Timeliness",
            "expected_result": "FAIL",
        },
    }


def build_failure_summary(validation_result_dict: Dict[str, Any]) -> pd.DataFrame:
    rules = build_rule_catalog()
    rows: List[Dict[str, Any]] = []

    results = validation_result_dict.get("results", [])

    for item in results:
        expectation_config = item.get("expectation_config", {})
        kwargs = expectation_config.get("kwargs", {})
        expectation_type = (
            expectation_config.get("type")
            or expectation_config.get("expectation_type")
        )
        column = kwargs.get("column", "")

        meta = rules.get(
            (expectation_type, column),
            {
                "label": f"{expectation_type}({column})",
                "dimension": "Unknown",
                "expected_result": "FAIL",
            },
        )

        result_payload = item.get("result", {})
        element_count = result_payload.get("element_count")
        unexpected_count = result_payload.get("unexpected_count")
        unexpected_percent = result_payload.get("unexpected_percent")

        pass_percent = None
        failure_percent = None

        if unexpected_percent is not None:
            failure_percent = float(unexpected_percent)
            pass_percent = round(100.0 - float(unexpected_percent), 4)
        elif (
            element_count is not None
            and unexpected_count is not None
            and element_count != 0
        ):
            failure_percent = round((unexpected_count / element_count) * 100, 4)
            pass_percent = round(100.0 - failure_percent, 4)

        rows.append(
            {
                "expectation_label": meta["label"],
                "expectation_type": expectation_type,
                "column": column,
                "dimension": meta["dimension"],
                "expected_result_raw": meta["expected_result"],
                "actual_result_raw": "PASS" if item.get("success") else "FAIL",
                "success": item.get("success"),
                "element_count": element_count,
                "unexpected_count": unexpected_count,
                "pass_percent": pass_percent,
                "failure_percent": failure_percent,
            }
        )

    summary_df = pd.DataFrame(rows)

    if not summary_df.empty:
        summary_df = summary_df.sort_values(
            by=["dimension", "expectation_label"],
            ascending=[True, True],
        ).reset_index(drop=True)

    return summary_df


def build_dq_score(summary_df: pd.DataFrame) -> float:
    if summary_df.empty:
        return 0.0
    passed = int(summary_df["success"].fillna(False).sum())
    total = int(len(summary_df))
    return round((passed / total) * 100, 4) if total > 0 else 0.0


# =========================================================
# SALIDAS
# =========================================================
def save_validation_outputs(
    validation_result_dict: Dict[str, Any],
    summary_df: pd.DataFrame,
    suite_path: Path,
) -> Dict[str, Path]:
    validation_json_path = REPORTS_DIR / "input_validation_results.json"
    summary_csv_path = REPORTS_DIR / "input_validation_failure_summary.csv"
    summary_md_path = REPORTS_DIR / "input_validation_failure_summary.md"
    summary_html_path = REPORTS_DIR / "input_validation_failure_summary.html"

    with open(validation_json_path, "w", encoding="utf-8") as f:
        json.dump(json_ready(validation_result_dict), f, indent=4, ensure_ascii=False)

    summary_df.to_csv(summary_csv_path, index=False, encoding="utf-8-sig")

    dq_score = build_dq_score(summary_df)

    with open(summary_md_path, "w", encoding="utf-8") as f:
        f.write("# Input Validation Failure Summary\n\n")
        f.write(f"- Expectation Suite JSON: `{suite_path}`\n")
        f.write(f"- Validation name: `{VALIDATION_NAME}`\n")
        f.write(f"- Data Quality Score (input): **{dq_score}%**\n\n")
        try:
            f.write(summary_df.to_markdown(index=False))
        except Exception:
            f.write(summary_df.to_string(index=False))

    html_table = summary_df.to_html(index=False, border=0)
    with open(summary_html_path, "w", encoding="utf-8") as f:
        f.write("<html><head><meta charset='utf-8'>")
        f.write("<title>Input Validation Summary</title>")
        f.write(
            "<style>"
            "body{font-family:Arial,sans-serif;margin:32px;}"
            "h1,h2{color:#222;}"
            "table{border-collapse:collapse;width:100%;font-size:14px;}"
            "th,td{border:1px solid #ccc;padding:8px;text-align:left;}"
            "th{background:#f4f4f4;}"
            "</style>"
        )
        f.write("</head><body>")
        f.write("<h1>Input Validation Summary</h1>")
        f.write(f"<p><strong>Validation name:</strong> {VALIDATION_NAME}</p>")
        f.write(f"<p><strong>Data Quality Score (input):</strong> {dq_score}%</p>")
        f.write("<h2>Failure Rates by Expectation</h2>")
        f.write(html_table)
        f.write("</body></html>")

    return {
        "validation_json": validation_json_path,
        "summary_csv": summary_csv_path,
        "summary_md": summary_md_path,
        "summary_html": summary_html_path,
    }


def try_build_data_docs(context: Any) -> List[str]:
    urls: List[str] = []
    try:
        docs_result = context.build_data_docs()
        if isinstance(docs_result, dict):
            urls = [str(v) for v in docs_result.values()]
    except Exception:
        pass
    return urls


# =========================================================
# CONSOLA
# =========================================================
def print_console_summary(
    csv_path: Path,
    suite_path: Path,
    summary_df: pd.DataFrame,
    output_paths: Dict[str, Path],
    data_docs_urls: List[str],
) -> None:
    dq_score = build_dq_score(summary_df)

    print("\n" + "=" * 72)
    print("INPUT DATA VALIDATION (GREAT EXPECTATIONS)")
    print("=" * 72)
    print(f"Archivo validado: {csv_path}")
    print(f"Expectation suite: {suite_path}")
    print(f"Validation name: {VALIDATION_NAME}")
    print(f"Número de expectations: {len(summary_df)}")
    print(f"Data Quality Score (input): {dq_score}%")

    print("\nResumen por expectation:")
    for _, row in summary_df.iterrows():
        print(
            f"  - [{row['dimension']}] {row['expectation_label']} "
            f"-> {row['actual_result_raw']} "
            f"(failure %: {row['failure_percent']})"
        )

    print("\nArchivos generados:")
    for key, path in output_paths.items():
        print(f"  - {key}: {path}")

    if data_docs_urls:
        print("\nData Docs:")
        for url in data_docs_urls:
            print(f"  - {url}")
    else:
        print("\nData Docs:")
        print("  - No se pudo obtener ruta automática; usa el HTML de reports como respaldo.")

    print("=" * 72 + "\n")


# =========================================================
# MAIN
# =========================================================
def main() -> None:
    ensure_directories()

    csv_path = find_raw_csv(RAW_DIR)
    df = load_raw_dataframe(csv_path)

    context = get_context()
    batch_definition, datasource_name, asset_name, batch_definition_name = build_batch_definition(
        context
    )
    suite = build_expectation_suite(context)
    suite_path = save_suite_json(suite)

    validation_result = run_validation(context, batch_definition, suite, df)
    validation_result_dict = validation_result_to_dict(validation_result)

    summary_df = build_failure_summary(validation_result_dict)
    output_paths = save_validation_outputs(validation_result_dict, summary_df, suite_path)
    data_docs_urls = try_build_data_docs(context)

    print_console_summary(
        csv_path=csv_path,
        suite_path=suite_path,
        summary_df=summary_df,
        output_paths=output_paths,
        data_docs_urls=data_docs_urls,
    )


if __name__ == "__main__":
    main()