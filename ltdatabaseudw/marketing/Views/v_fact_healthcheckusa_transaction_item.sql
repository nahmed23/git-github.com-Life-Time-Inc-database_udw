﻿CREATE VIEW [marketing].[v_fact_healthcheckusa_transaction_item]
AS select fact_healthcheckusa_allocated_transaction_item.allocated_dim_club_key allocated_dim_club_key,
       fact_healthcheckusa_allocated_transaction_item.allocated_month_starting_dim_date_key allocated_month_starting_dim_date_key,
       fact_healthcheckusa_allocated_transaction_item.allocated_recalculate_through_datetime allocated_recalculate_through_datetime,
       fact_healthcheckusa_allocated_transaction_item.allocated_recalculate_through_dim_date_key allocated_recalculate_through_dim_date_key,
       fact_healthcheckusa_allocated_transaction_item.dim_club_key dim_club_key,
       fact_healthcheckusa_allocated_transaction_item.dim_healthcheckusa_product_key dim_healthcheckusa_product_key,
       fact_healthcheckusa_allocated_transaction_item.discount_amount discount_amount,
       fact_healthcheckusa_allocated_transaction_item.fact_healthcheckusa_allocated_transaction_item_key fact_healthcheckusa_allocated_transaction_item_key,
       fact_healthcheckusa_allocated_transaction_item.gl_club_id gl_club_id,
       fact_healthcheckusa_allocated_transaction_item.order_for_employee_flag order_for_employee_flag,
       fact_healthcheckusa_allocated_transaction_item.order_number order_number,
       fact_healthcheckusa_allocated_transaction_item.original_currency_code original_currency_code,
       fact_healthcheckusa_allocated_transaction_item.product_sku product_sku,
       fact_healthcheckusa_allocated_transaction_item.refund_flag refund_flag,
       fact_healthcheckusa_allocated_transaction_item.sales_amount sales_amount,
       fact_healthcheckusa_allocated_transaction_item.sales_dim_employee_key sales_dim_employee_key,
       fact_healthcheckusa_allocated_transaction_item.sales_quantity sales_quantity,
       fact_healthcheckusa_allocated_transaction_item.transaction_date transaction_date,
       fact_healthcheckusa_allocated_transaction_item.transaction_post_dim_date_key transaction_post_dim_date_key,
       fact_healthcheckusa_allocated_transaction_item.transaction_reporting_dim_club_key transaction_reporting_dim_club_key,
       fact_healthcheckusa_allocated_transaction_item.transaction_reporting_local_currency_dim_plan_exchange_rate_key transaction_reporting_local_currency_dim_plan_exchange_rate_key,
       fact_healthcheckusa_allocated_transaction_item.transaction_reporting_local_currency_monthly_average_dim_exchange_rate_key transaction_reporting_local_currency_monthly_average_dim_exchange_rate_key,
       fact_healthcheckusa_allocated_transaction_item.transaction_type transaction_type,
       fact_healthcheckusa_allocated_transaction_item.udw_inserted_dim_date_key udw_inserted_dim_date_key,
       fact_healthcheckusa_allocated_transaction_item.usd_dim_plan_exchange_rate_key usd_dim_plan_exchange_rate_key,
       fact_healthcheckusa_allocated_transaction_item.usd_monthly_average_dim_exchange_rate_key usd_monthly_average_dim_exchange_rate_key
  from dbo.fact_healthcheckusa_allocated_transaction_item;