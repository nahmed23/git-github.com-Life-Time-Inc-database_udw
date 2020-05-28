﻿CREATE VIEW [marketing].[v_fact_exerp_subscription_participation]
AS select fact_exerp_payroll.club_id club_id,
       fact_exerp_payroll.delivered_dim_date_key delivered_dim_date_key,
       fact_exerp_payroll.delivered_dim_time_key delivered_dim_time_key,
       fact_exerp_payroll.dim_club_key dim_club_key,
       fact_exerp_payroll.dim_employee_key dim_employee_key,
       fact_exerp_payroll.dim_exerp_booking_key dim_exerp_booking_key,
       fact_exerp_payroll.dim_exerp_product_key dim_exerp_product_key,
       fact_exerp_payroll.dim_exerp_subscription_key dim_exerp_subscription_key,
       fact_exerp_payroll.dim_exerp_subscription_period_key dim_exerp_subscription_period_key,
       fact_exerp_payroll.dim_mms_member_key dim_mms_member_key,
       fact_exerp_payroll.dim_mms_product_key dim_mms_product_key,
       fact_exerp_payroll.employee_id employee_id,
       fact_exerp_payroll.fact_exerp_participation_key fact_exerp_participation_key,
       fact_exerp_payroll.member_id member_id,
       fact_exerp_payroll.participation_id participation_id,
       fact_exerp_payroll.pay_period_first_day_dim_date_key pay_period_first_day_dim_date_key,
       fact_exerp_payroll.payroll_description payroll_description,
       fact_exerp_payroll.payroll_group_description payroll_group_description,
       fact_exerp_payroll.payroll_region_type payroll_region_type,
       fact_exerp_payroll.payroll_service_amount_flag payroll_service_amount_flag,
       fact_exerp_payroll.payroll_service_quantity_flag payroll_service_quantity_flag,
       fact_exerp_payroll.payroll_unique_key payroll_unique_key,
       fact_exerp_payroll.price_per_booking price_per_booking,
       fact_exerp_payroll.price_per_booking_less_lt_bucks price_per_booking_less_lt_bucks,
       fact_exerp_payroll.product_id product_id,
       fact_exerp_payroll.refund_amount refund_amount,
       fact_exerp_payroll.revenue_dim_date_key revenue_dim_date_key
  from dbo.fact_exerp_payroll;