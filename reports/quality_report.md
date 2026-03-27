# Data Quality Issues and Policy Proposal

## Issues

## Issues

       Column                              Issue         Example    Dimension                               Business Impact  Approx Count
   invoice_id                      Duplicate IDs           21238   Uniqueness           Revenue double counted in BO-1 KPIs          2131
  customer_id                        NULL values             NaN Completeness       Cannot link sales to customers for BO-3           202
     quantity                    Negative values              -3     Validity   Negative units corrupt total_revenue (BO-1)           149
        price                    Negative values          -83.02     Validity Invalid pricing affects financial KPIs (BO-1)           101
total_revenue    Does not equal quantity × price    Offset error     Accuracy                      Misstated revenue (BO-1)           148
      country                   Multiple formats    colombia, CO  Consistency           Regional insights unreliable (BO-3)             7
 invoice_date Null-like strings and future dates N/A, 2026-01-01   Timeliness                 Time series unreliable (BO-2)           139

## Policies

                                                            Policy Statement                                                                                                               GE Expectation Severity Addresses (BO)
                         invoice_id must be unique across the entire dataset                                                                                expect_column_values_to_be_unique(invoice_id) Critical     BO-1, BO-4
                                   quantity must be a positive integer (≥ 1)                                                                          expect_column_values_to_be_between(quantity, min=1) Critical           BO-1
                                             price must be greater than zero                                                                          expect_column_values_to_be_between(price, min=0.01) Critical           BO-1
                           total_revenue must equal quantity × price (±0.01)                                                     expect_column_values_to_be_between(_revenue_diff_abs, min=0.0, max=0.01) Critical     BO-1, BO-4
                     country must be one of {Colombia, Ecuador, Peru, Chile}                                                    expect_column_values_to_be_in_set(country, {Colombia,Ecuador,Peru,Chile})     High           BO-3
invoice_date must follow YYYY-MM-DD and fall within 2023-01-01 to 2023-12-31 expect_column_values_to_match_regex(invoice_date, ^\d{4}-\d{2}-\d{2}$), expect_column_values_to_be_between(year, 2023, 2023)     High           BO-2
                               customer_id must be non-null positive integer                     expect_column_values_to_not_be_null(customer_id), expect_column_values_to_be_between(customer_id, min=1)     High           BO-3
                                  product must belong to the allowed catalog                                                                          expect_column_values_to_be_in_set(product, catalog)   Medium           BO-3