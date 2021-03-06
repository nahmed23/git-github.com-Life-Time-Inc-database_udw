﻿CREATE VIEW [marketing].[v_fact_cafe_tender_by_check_type_summary] AS select fact_cafe_tender_by_check_type_summary.check_type_dim_id check_type_dim_id,
       fact_cafe_tender_by_check_type_summary.credit_type_id credit_type_id,
       fact_cafe_tender_by_check_type_summary.dim_cafe_payment_type_key dim_cafe_payment_type_key,
       fact_cafe_tender_by_check_type_summary.dim_cafe_profit_center_key dim_cafe_profit_center_key,
       fact_cafe_tender_by_check_type_summary.dim_club_currency_code_key dim_club_currency_code_key,
       fact_cafe_tender_by_check_type_summary.dim_club_key dim_club_key,
       fact_cafe_tender_by_check_type_summary.dim_merchant_number_key dim_merchant_number_key,
       fact_cafe_tender_by_check_type_summary.event_dim_id event_dim_id,
       fact_cafe_tender_by_check_type_summary.fact_cafe_tender_by_check_type_summary_key fact_cafe_tender_by_check_type_summary_key,
       fact_cafe_tender_by_check_type_summary.meal_period_dim_id meal_period_dim_id,
       fact_cafe_tender_by_check_type_summary.original_currency_code original_currency_code,
       fact_cafe_tender_by_check_type_summary.posted_business_period_dim_id posted_business_period_dim_id,
       fact_cafe_tender_by_check_type_summary.posted_business_period_end_dim_date_key posted_business_period_end_dim_date_key,
       fact_cafe_tender_by_check_type_summary.posted_business_period_start_dim_date_key posted_business_period_start_dim_date_key,
       fact_cafe_tender_by_check_type_summary.profit_center_dim_id profit_center_dim_id,
       fact_cafe_tender_by_check_type_summary.tender_dim_id tender_dim_id,
       fact_cafe_tender_by_check_type_summary.tender_net_amount tender_net_amount,
       fact_cafe_tender_by_check_type_summary.tendered_business_period_dim_id tendered_business_period_dim_id,
       fact_cafe_tender_by_check_type_summary.tendered_business_period_end_dim_date_key tendered_business_period_end_dim_date_key,
       fact_cafe_tender_by_check_type_summary.tendered_business_period_start_dim_date_key tendered_business_period_start_dim_date_key,
       fact_cafe_tender_by_check_type_summary.usd_dim_plan_exchange_rate_key usd_dim_plan_exchange_rate_key,
       fact_cafe_tender_by_check_type_summary.usd_monthly_average_dim_exchange_rate_key usd_monthly_average_dim_exchange_rate_key
  from dbo.fact_cafe_tender_by_check_type_summary;