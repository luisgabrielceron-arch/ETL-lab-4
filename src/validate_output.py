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
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
REPORTS_DIR = PROJECT_ROOT / "reports"
GE_DIR = PROJECT_ROOT / "gx_context"

INPUT_TRANSFORMED_CSV = PROCESSED_DIR / "retail_transformed.csv"
INPUT_VALIDATION_SUMMARY_CSV = REPORTS_DIR / "input_validation_failure_summary.csv"

SUITE_NAME = "retail_input_validation_suite"
RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
VALIDATION_NAME = f"retail_output_validation_definition_{RUN_ID}"

REVENUE_TOLERANCE = 0.01
VALID_COUNTRIES = ["Colombia", "Ecuador", "Peru", "Chile"]
VALID_REVENUE_BINS = ["Low", "Medium", "High"]


# =========================================================
# UTILIDADES
# =========================================================
def ensure_directories() -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    GE_DIR.mkdir(parents=True, exist_ok=True)
    (GE_DIR / "expectations").mkdir(parents=True, exist_ok=True)
    (GE_DIR / "uncommitted").mkdir(parents=True, exist_ok=True)


def load_transformed_dataframe(csv_path: Path) -> pd.DataFrame:
    if not csv_path.exists():
        raise FileNotFoundError(
            f"No se encontró el archivo transformado esperado: {csv_path}"
        )

    df = pd.read_csv(csv_path, parse_dates=["invoice_date"])

    # Normalización mínima de tipos
    for col in ["invoice_id", "customer_id", "quantity", "year", "month"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in ["price", "total_revenue"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in ["product", "country", "day_of_week", "revenue_bin"]:
        if col in df.columns:
            df[col] = df[col].astype("string")

    # Helpers para expectativas
    df["_revenue_expected"] = (df["quantity"] * df["price"]).round(2)
    df["_revenue_diff_abs"] = (df["total_revenue"] - df["_revenue_expected"]).abs()
    df["_invoice_date_str"] = df["invoice_date"].dt.strftime("%Y-%m-%d").astype("string")

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
    datasource_name = f"retail_output_ds_{RUN_ID}"
    asset_name = f"retail_transformed_asset_{RUN_ID}"
    batch_definition_name = f"retail_transformed_batch_{RUN_ID}"

    datasource = context.data_sources.add_pandas(name=datasource_name)
    asset = datasource.add_dataframe_asset(name=asset_name)
    batch_definition = asset.add_batch_definition_whole_dataframe(
        batch_definition_name
    )

    return batch_definition, datasource_name, asset_name, batch_definition_name


# =========================================================
# SUITE Y EXPECTATIONS
# =========================================================
def build_expectation_suite(context: Any, valid_products: List[str]) -> Any:
    """
    Reutiliza el nombre de la suite del Task b y la actualiza con:
    - expectativas base
    - expectativas específicas del dataset transformado
    """
    suite = gx.ExpectationSuite(name=SUITE_NAME)

    # -----------------------------
    # Expectativas base
    # -----------------------------
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="invoice_date")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeUnique(column="invoice_id")
    )
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
            value_set=valid_products,
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="_revenue_diff_abs",
            min_value=0.0,
            max_value=REVENUE_TOLERANCE,
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="country",
            value_set=VALID_COUNTRIES,
        )
    )

    # -----------------------------
    # Expectativas específicas de output
    # -----------------------------
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToMatchRegex(
            column="_invoice_date_str",
            regex=r"^\d{4}-\d{2}-\d{2}$",
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="year",
            min_value=2023,
            max_value=2023,
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="month",
            min_value=1,
            max_value=12,
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="revenue_bin",
            value_set=VALID_REVENUE_BINS,
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="total_revenue",
            min_value=0.01,
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="invoice_id")
    )

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
# RESUMEN / COMPARACIÓN
# =========================================================
def build_rule_catalog() -> Dict[Tuple[str, str], Dict[str, str]]:
    return {
        ("expect_column_values_to_not_be_null", "customer_id"): {
            "label": "customer_id not null",
            "dimension": "Completeness",
        },
        ("expect_column_values_to_not_be_null", "invoice_date"): {
            "label": "invoice_date not null",
            "dimension": "Completeness",
        },
        ("expect_column_values_to_be_unique", "invoice_id"): {
            "label": "invoice_id unique",
            "dimension": "Uniqueness",
        },
        ("expect_column_values_to_be_between", "quantity"): {
            "label": "quantity >= 1",
            "dimension": "Validity",
        },
        ("expect_column_values_to_be_between", "price"): {
            "label": "price >= 0.01",
            "dimension": "Validity",
        },
        ("expect_column_values_to_be_in_set", "product"): {
            "label": "product in allowed catalog",
            "dimension": "Validity",
        },
        ("expect_column_values_to_be_between", "_revenue_diff_abs"): {
            "label": "Custom: total_revenue == quantity * price (tol ±0.01)",
            "dimension": "Accuracy",
        },
        ("expect_column_values_to_be_in_set", "country"): {
            "label": "country in {Colombia, Ecuador, Peru, Chile}",
            "dimension": "Consistency",
        },
        ("expect_column_values_to_match_regex", "_invoice_date_str"): {
            "label": "invoice_date matches YYYY-MM-DD",
            "dimension": "Timeliness",
        },
        ("expect_column_values_to_be_between", "year"): {
            "label": "invoice_date within 2023",
            "dimension": "Timeliness",
        },
        ("expect_column_values_to_be_between", "month"): {
            "label": "month in [1, 12]",
            "dimension": "Timeliness",
        },
        ("expect_column_values_to_be_in_set", "revenue_bin"): {
            "label": "revenue_bin in {Low, Medium, High}",
            "dimension": "Validity",
        },
        ("expect_column_values_to_be_between", "total_revenue"): {
            "label": "total_revenue > 0",
            "dimension": "Validity",
        },
        ("expect_column_values_to_not_be_null", "invoice_id"): {
            "label": "invoice_id not null",
            "dimension": "Completeness",
        },
    }


def build_output_summary(validation_result_dict: Dict[str, Any], df: pd.DataFrame) -> pd.DataFrame:
    rules = build_rule_catalog()
    rows: List[Dict[str, Any]] = []

    # Fila manual para documentar que invoice_date sí quedó parseada como datetime
    is_datetime = pd.api.types.is_datetime64_any_dtype(df["invoice_date"])
    rows.append(
        {
            "expectation_label": "invoice_date parsed as datetime type",
            "expectation_type": "manual_dtype_check",
            "column": "invoice_date",
            "dimension": "Timeliness",
            "actual_result_output": "PASS" if is_datetime else "FAIL",
            "success_output": bool(is_datetime),
            "pass_percent_output": 100.0 if is_datetime else 0.0,
            "failure_percent_output": 0.0 if is_datetime else 100.0,
        }
    )

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
        else:
            pass_percent = 100.0 if item.get("success") else 0.0
            failure_percent = 0.0 if item.get("success") else 100.0

        rows.append(
            {
                "expectation_label": meta["label"],
                "expectation_type": expectation_type,
                "column": column,
                "dimension": meta["dimension"],
                "actual_result_output": "PASS" if item.get("success") else "FAIL",
                "success_output": item.get("success"),
                "pass_percent_output": pass_percent,
                "failure_percent_output": failure_percent,
            }
        )

    summary_df = pd.DataFrame(rows)

    if not summary_df.empty:
        summary_df = summary_df.sort_values(
            by=["dimension", "expectation_label"],
            ascending=[True, True],
        ).reset_index(drop=True)

    return summary_df


def load_input_summary(csv_path: Path) -> pd.DataFrame:
    if not csv_path.exists():
        return pd.DataFrame(
            columns=[
                "expectation_label",
                "pass_percent",
                "failure_percent",
                "success",
                "dimension",
            ]
        )

    df = pd.read_csv(csv_path)
    return df


def build_comparison_table(input_summary_df: pd.DataFrame, output_summary_df: pd.DataFrame) -> pd.DataFrame:
    raw_df = input_summary_df.rename(
        columns={
            "pass_percent": "raw_pass_percent",
            "failure_percent": "raw_failure_percent",
            "success": "success_raw",
            "dimension": "dimension_raw",
        }
    )

    out_df = output_summary_df.rename(
        columns={
            "pass_percent_output": "output_pass_percent",
            "failure_percent_output": "output_failure_percent",
            "success_output": "success_output",
        }
    )

    comparison_df = out_df.merge(
        raw_df[["expectation_label", "raw_pass_percent", "raw_failure_percent", "success_raw", "dimension_raw"]],
        on="expectation_label",
        how="left",
    )

    comparison_df["dimension"] = comparison_df["dimension"].fillna(comparison_df["dimension_raw"])

    def resolve_status(row: pd.Series) -> str:
        raw_pass = row.get("raw_pass_percent")
        out_pass = row.get("output_pass_percent")

        if pd.isna(raw_pass):
            return "NEW"
        if out_pass == 100.0 and raw_pass < 100.0:
            return "RESOLVED"
        if out_pass == 100.0 and raw_pass == 100.0:
            return "MAINTAINED"
        return "PERSISTS"

    comparison_df["status"] = comparison_df.apply(resolve_status, axis=1)

    comparison_df = comparison_df[
        [
            "expectation_label",
            "raw_pass_percent",
            "output_pass_percent",
            "status",
            "dimension",
            "actual_result_output",
        ]
    ].rename(
        columns={
            "expectation_label": "Expectation",
            "raw_pass_percent": "Raw (pass %)",
            "output_pass_percent": "Clean (pass %)",
            "status": "Status",
            "dimension": "Dimension",
            "actual_result_output": "Output Result",
        }
    )

    return comparison_df


def build_dq_score(summary_df: pd.DataFrame, success_col: str) -> float:
    if summary_df.empty:
        return 0.0
    passed = int(summary_df[success_col].fillna(False).sum())
    total = int(len(summary_df))
    return round((passed / total) * 100, 4) if total > 0 else 0.0


# =========================================================
# SALIDAS
# =========================================================
def save_outputs(
    validation_result_dict: Dict[str, Any],
    output_summary_df: pd.DataFrame,
    comparison_df: pd.DataFrame,
    suite_path: Path,
    dq_input: float,
    dq_output: float,
) -> Dict[str, Path]:
    validation_json_path = REPORTS_DIR / "output_validation_results.json"
    output_summary_csv_path = REPORTS_DIR / "output_validation_summary.csv"
    comparison_csv_path = REPORTS_DIR / "output_validation_comparison_table.csv"
    comparison_md_path = REPORTS_DIR / "output_validation_comparison_table.md"
    comparison_html_path = REPORTS_DIR / "output_validation_comparison_table.html"
    dq_scores_json_path = REPORTS_DIR / "dq_scores.json"
    dq_scores_md_path = REPORTS_DIR / "dq_scores.md"

    with open(validation_json_path, "w", encoding="utf-8") as f:
        json.dump(json_ready(validation_result_dict), f, indent=4, ensure_ascii=False)

    output_summary_df.to_csv(output_summary_csv_path, index=False, encoding="utf-8-sig")
    comparison_df.to_csv(comparison_csv_path, index=False, encoding="utf-8-sig")

    with open(comparison_md_path, "w", encoding="utf-8") as f:
        f.write("# Output Validation Comparison Table\n\n")
        f.write(f"- Expectation Suite JSON: `{suite_path}`\n")
        f.write(f"- Validation name: `{VALIDATION_NAME}`\n")
        f.write(f"- Data Quality Score (input): **{dq_input}%**\n")
        f.write(f"- Data Quality Score (output): **{dq_output}%**\n\n")
        try:
            f.write(comparison_df.to_markdown(index=False))
        except Exception:
            f.write(comparison_df.to_string(index=False))

    html_table = comparison_df.to_html(index=False, border=0)
    with open(comparison_html_path, "w", encoding="utf-8") as f:
        f.write("<html><head><meta charset='utf-8'>")
        f.write("<title>Output Validation Comparison</title>")
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
        f.write("<h1>Output Validation Comparison Table</h1>")
        f.write(f"<p><strong>Data Quality Score (input):</strong> {dq_input}%</p>")
        f.write(f"<p><strong>Data Quality Score (output):</strong> {dq_output}%</p>")
        f.write(html_table)
        f.write("</body></html>")

    dq_scores_payload = {
        "generated_at": datetime.now().isoformat(),
        "data_quality_score_input": dq_input,
        "data_quality_score_output": dq_output,
    }
    with open(dq_scores_json_path, "w", encoding="utf-8") as f:
        json.dump(dq_scores_payload, f, indent=4, ensure_ascii=False)

    with open(dq_scores_md_path, "w", encoding="utf-8") as f:
        f.write("# Data Quality Scores\n\n")
        f.write(f"- Data Quality Score (input): **{dq_input}%**\n")
        f.write(f"- Data Quality Score (output): **{dq_output}%**\n")

    return {
        "validation_json": validation_json_path,
        "output_summary_csv": output_summary_csv_path,
        "comparison_csv": comparison_csv_path,
        "comparison_md": comparison_md_path,
        "comparison_html": comparison_html_path,
        "dq_scores_json": dq_scores_json_path,
        "dq_scores_md": dq_scores_md_path,
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
    suite_path: Path,
    comparison_df: pd.DataFrame,
    dq_input: float,
    dq_output: float,
    output_paths: Dict[str, Path],
    data_docs_urls: List[str],
) -> None:
    print("\n" + "=" * 72)
    print("POST-TRANSFORMATION VALIDATION (GREAT EXPECTATIONS)")
    print("=" * 72)
    print(f"Expectation suite: {suite_path}")
    print(f"Validation name: {VALIDATION_NAME}")
    print(f"Número de expectations comparadas: {len(comparison_df)}")
    print(f"Data Quality Score (input): {dq_input}%")
    print(f"Data Quality Score (output): {dq_output}%")

    print("\nComparison table:")
    for _, row in comparison_df.iterrows():
        print(
            f"  - {row['Expectation']} | "
            f"Raw: {row['Raw (pass %)']} | "
            f"Clean: {row['Clean (pass %)']} | "
            f"Status: {row['Status']}"
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

    df = load_transformed_dataframe(INPUT_TRANSFORMED_CSV)
    valid_products = sorted([str(v) for v in df["product"].dropna().unique().tolist()])

    context = get_context()
    batch_definition, datasource_name, asset_name, batch_definition_name = build_batch_definition(
        context
    )

    suite = build_expectation_suite(context, valid_products)
    suite_path = save_suite_json(suite)

    validation_result = run_validation(context, batch_definition, suite, df)
    validation_result_dict = validation_result_to_dict(validation_result)

    output_summary_df = build_output_summary(validation_result_dict, df)
    input_summary_df = load_input_summary(INPUT_VALIDATION_SUMMARY_CSV)
    comparison_df = build_comparison_table(input_summary_df, output_summary_df)

    dq_input = build_dq_score(input_summary_df, "success") if not input_summary_df.empty else 0.0
    dq_output = build_dq_score(output_summary_df, "success_output")

    output_paths = save_outputs(
        validation_result_dict=validation_result_dict,
        output_summary_df=output_summary_df,
        comparison_df=comparison_df,
        suite_path=suite_path,
        dq_input=dq_input,
        dq_output=dq_output,
    )
    data_docs_urls = try_build_data_docs(context)

    print_console_summary(
        suite_path=suite_path,
        comparison_df=comparison_df,
        dq_input=dq_input,
        dq_output=dq_output,
        output_paths=output_paths,
        data_docs_urls=data_docs_urls,
    )


if __name__ == "__main__":
    main()