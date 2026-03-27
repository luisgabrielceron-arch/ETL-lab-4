# Star Schema Description

Tables: dim_product(product_id, product_name), dim_customer(customer_id, customer_key), dim_location(location_id, country), dim_date(date_id, full_date, year, month, month_name, day_of_week), fact_sales(sale_id, invoice_id, product_id, customer_id, location_id, date_id, quantity, price, total_revenue)

Granularity: one row per transaction line.

Foreign keys in fact_sales reference surrogate keys in dimensions.
