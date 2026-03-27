# Profiling Overview

- Dataset: `C:\Users\LENOVO\OneDrive\Documentos\ETL\ETL-lab-4\data\raw\retail_etl_dataset.csv`
- Generated at: `2026-03-27T13:17:35.224243`
- Rows: **5100**
- Columns: **8**
- Memory (MB): **1.0192**

## Duplicate invoice_id metrics
- duplicate_invoice_id_rows_including_first: **2131**
- duplicate_invoice_id_excess_rows: **1159**
- duplicate_distinct_invoice_ids: **972**

## Revenue mismatch
- Rows where `total_revenue != quantity * price` (tol ±0.01): **148**

## Invoice date format distribution
- YYYY-MM-DD: **4909**
- YYYY/MM/DD: **98**
- DD-MM-YYYY: **78**
- null_like: **13**
- other: **2**

## Invoice date timeliness
- future_invoice_date_count: **128**
- null_like_invoice_date_count: **13**

## Great Expectations registration
- registered: **True**
- error: **None**
- datasource_name: **etl_lab_pandas_20260327_131734**
- asset_name: **retail_raw_asset_20260327_131734**
- api_mode: **fluent_data_sources**
