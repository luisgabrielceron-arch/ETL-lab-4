# Incomplete Tasks and Deliverables

- Task C — Data Quality Analysis and Policy Proposal
  - Implement src/quality_analysis.py to generate the Data Quality Issues table and the Policy Proposal table into reports/.
  - Ensure artifacts are saved in structured formats (CSV/MD) under reports/.
- Task G — Dimensional Modeling
  - Implement src/dimensional_model.py to build dim_product, dim_customer, dim_location, dim_date (full 2023), and fact_sales from data/processed/retail_transformed.csv.
  - Save dimension and fact tables to data/processed/ and populate reports/model_description.md with the schema description.
- Task H — Load to SQLite Data Warehouse
  - Implement src/load_dw.py to load tables into data/processed/data_warehouse.db in the correct order and produce a referential integrity summary in reports/.
- Task I — Business Analysis and Visualizations
  - Implement src/analysis.py to compute KPIs and produce required PNG charts under reports/ for BO-1 to BO-4.
- Great Expectations Data Docs
  - Ensure Data Docs for validations are generated and accessible; include paths or copies within the repository when feasible.
- Orchestrator
  - Implement src/main.py to run the pipeline end-to-end (a→i) using the existing scripts.
- Requirements
  - Populate requirements.txt with pinned versions for all dependencies used across the pipeline.
