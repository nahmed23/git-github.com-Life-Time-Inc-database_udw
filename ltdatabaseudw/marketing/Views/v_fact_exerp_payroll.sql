CREATE VIEW [marketing].[v_fact_exerp_payroll]
AS select dim_mms_member.customer_name customer_name,
       fact_exerp_payroll.employee_id employee_id,
       fact_exerp_payroll.member_id member_id,
       fact_exerp_payroll.pay_period_first_day_dim_date_key pay_period_first_day_dim_date_key,
       fact_exerp_payroll.payroll_description payroll_description,
       fact_exerp_payroll.payroll_group_description payroll_group_description,
       fact_exerp_payroll.price_per_booking price_per_booking,
       fact_exerp_payroll.price_per_booking_less_lt_bucks price_per_booking_less_lt_bucks,
       dim_mms_product.product_description product_description,
       fact_exerp_payroll.product_id product_id,
       1 quantity,
       'Service' record_type,
       dim_date.calendar_date transaction_date,
       '7'+fact_exerp_payroll.fact_exerp_participation_key unique_key,
       dim_club.workday_region workday_region
from fact_exerp_payroll
join marketing.v_dim_mms_member dim_mms_member on fact_exerp_payroll.dim_mms_member_key = dim_mms_member.dim_mms_member_key
join marketing.v_dim_club dim_club on fact_exerp_payroll.dim_club_key = dim_club.dim_club_key
join marketing.v_dim_date dim_date on fact_exerp_payroll.delivered_dim_date_key = dim_date.dim_date_key
join marketing.v_dim_mms_product dim_mms_product on fact_exerp_payroll.dim_mms_product_key = dim_mms_product.dim_mms_product_key;