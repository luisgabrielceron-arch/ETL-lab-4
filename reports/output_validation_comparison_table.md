# Output Validation Comparison Table

- Expectation Suite JSON: `C:\Users\LENOVO\OneDrive\Documentos\ETL\ETL-lab-4\gx_context\expectations\retail_input_validation_suite.json`
- Validation name: `retail_output_validation_definition_20260327_173605_987353`
- Data Quality Score (input): **0.0%**
- Data Quality Score (output): **100.0%**

                                          Expectation  Raw (pass %)  Clean (pass %)   Status    Dimension Output Result
Custom: total_revenue == quantity * price (tol ±0.01)       97.0980           100.0 RESOLVED     Accuracy          PASS
                                 customer_id not null       96.0392           100.0 RESOLVED Completeness          PASS
                                invoice_date not null       99.7451           100.0 RESOLVED Completeness          PASS
                                  invoice_id not null           NaN           100.0      NEW Completeness          PASS
          country in {Colombia, Ecuador, Peru, Chile}       56.6471           100.0 RESOLVED  Consistency          PASS
                      invoice_date matches YYYY-MM-DD       96.5009           100.0 RESOLVED   Timeliness          PASS
                 invoice_date parsed as datetime type           NaN           100.0      NEW   Timeliness          PASS
                             invoice_date within 2023           NaN           100.0      NEW   Timeliness          PASS
                                     month in [1, 12]           NaN           100.0      NEW   Timeliness          PASS
                                    invoice_id unique       58.2157           100.0 RESOLVED   Uniqueness          PASS
                                        price >= 0.01       98.0196           100.0 RESOLVED     Validity          PASS
                           product in allowed catalog       87.3529           100.0 RESOLVED     Validity          PASS
                                        quantity >= 1       97.0784           100.0 RESOLVED     Validity          PASS
                   revenue_bin in {Low, Medium, High}           NaN           100.0      NEW     Validity          PASS
                                    total_revenue > 0           NaN           100.0      NEW     Validity          PASS