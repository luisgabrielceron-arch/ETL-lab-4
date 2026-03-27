# Input Validation Failure Summary

- Expectation Suite JSON: `C:\Users\Pasoseguro\OneDrive - mipasoseguro.com.co\Escritorio\UAO- INTELIGENCIA ARTIFICIAL\6 SEMESTRE\ETL\ETL_Lab_4\gx_context\expectations\retail_input_validation_suite.json`
- Validation name: `retail_input_validation_definition_20260325_123805_290622`
- Data Quality Score (input): **0.0%**

                                    expectation_label                    expectation_type            column    dimension expected_result_raw actual_result_raw  success  element_count  unexpected_count  pass_percent  failure_percent
Custom: total_revenue == quantity * price (tol ±0.01)  expect_column_values_to_be_between _revenue_diff_abs     Accuracy                FAIL              FAIL    False         5100.0             148.0       97.0980         2.901961
                                 customer_id not null expect_column_values_to_not_be_null       customer_id Completeness                FAIL              FAIL    False         5100.0             202.0       96.0392         3.960784
                                invoice_date not null expect_column_values_to_not_be_null      invoice_date Completeness                FAIL              FAIL    False         5100.0              13.0       99.7451         0.254902
          country in {Colombia, Ecuador, Peru, Chile}   expect_column_values_to_be_in_set           country  Consistency                FAIL              FAIL    False         5100.0            2211.0       56.6471        43.352941
                      invoice_date matches YYYY-MM-DD expect_column_values_to_match_regex      invoice_date   Timeliness                FAIL              FAIL    False         5100.0             178.0       96.5009         3.499115
                             invoice_date within 2023  expect_column_values_to_be_between _invoice_date_iso   Timeliness                FAIL              FAIL    False            NaN               NaN           NaN              NaN
                                    invoice_id unique   expect_column_values_to_be_unique        invoice_id   Uniqueness                FAIL              FAIL    False         5100.0            2131.0       58.2157        41.784314
                                        price >= 0.01  expect_column_values_to_be_between             price     Validity                FAIL              FAIL    False         5100.0             101.0       98.0196         1.980392
                           product in allowed catalog   expect_column_values_to_be_in_set           product     Validity                FAIL              FAIL    False         5100.0             645.0       87.3529        12.647059
                                        quantity >= 1  expect_column_values_to_be_between          quantity     Validity                FAIL              FAIL    False         5100.0             149.0       97.0784         2.921569