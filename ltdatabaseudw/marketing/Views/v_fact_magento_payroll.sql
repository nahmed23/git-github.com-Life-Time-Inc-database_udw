CREATE VIEW [marketing].[v_fact_magento_payroll]
AS with pay_dim_date (dim_date_key, pay_week_first_day_dim_date_key, pay_week_last_day_dim_date_key) as
(
	select d1.dim_date_key,
		   max(ps.dim_date_key) pay_week_first_day_dim_date_key,
		   min(pe.dim_date_key) pay_week_last_day_dim_date_keyx
	  from marketing.v_dim_date d1 --"LTFDW day"
	  join marketing.v_dim_date ps --week start
		on ps.dim_date_key <= d1.dim_date_key 
	   and ps.day_number_in_week = 1 
	   and d1.bi_weekly_pay_period_code = ps.bi_weekly_pay_period_code --optimization join clause
	  join marketing.v_dim_date pe --week end
		on pe.dim_date_key >= d1.dim_date_key 
	   and pe.day_number_in_week = 7 
	   and d1.bi_weekly_pay_period_code = pe.bi_weekly_pay_period_code --optimization join clause
	where d1.calendar_date < dateadd(dd,1,getdate())
	group by d1.dim_date_key
)
select '8a'+f.unique_key unique_key,
       f.order_item_id,
       f.payroll_amount,
	   f.transaction_bucks_amount bucks_amount,
       f.transaction_quantity quantity,
       f.udw_inserted_datetime,
       f.order_datetime,
       case when f.refund_flag = 'Y' then f.allocated_datetime else null end refund_datetime,
	   null canceled_datetime,
       f.refund_flag,
       e.employee_id,
       e.employee_name,
       m.member_id,
       m.customer_name member_name,
       c.workday_region,
       c.club_id,
       'sale' record_type,
       mag_p.product_id,
       mag_p.sku,
       mag_p.product_name,
       mag_p.payroll_description,
       mag_p.payroll_standard_group_description payroll_group_description,
       dd.pay_week_first_day_dim_date_key,
	   'N' manual_cancel_record
from fact_magento_tran_item f
join marketing.v_dim_magento_product_history mag_p
  on f.dim_magento_product_key = mag_p.dim_magento_product_key
 and mag_p.effective_date_time <= f.order_datetime
 and mag_p.expiration_date_time > f.order_datetime
join marketing.v_dim_employee e
  on f.dim_employee_key = e.dim_employee_key
join marketing.v_dim_mms_member m
  on f.dim_mms_member_key = m.dim_mms_member_key
join marketing.v_dim_club c
  on f.payroll_dim_club_key = c.dim_club_key
join pay_dim_date dd
  on f.udw_inserted_dim_date_key = dd.dim_date_key
where e.dim_employee_key not in ('-998','-997','-999')
and mag_p.payroll_standard_sales_amount_flag = 'y'
union
select '8b'+f.unique_key unique_key,
       f.order_item_id,
       -1 * f.payroll_amount,
       -1 * f.transaction_bucks_amount bucks_amount,
       -1 * f.transaction_quantity payroll_quantity,
       f.udw_inserted_datetime,
       f.order_datetime,	   
       case when f.refund_flag = 'Y' then f.allocated_datetime else null end refund_datetime,
	   f.canceled_datetime canceled_datetime,
       f.refund_flag,
       e.employee_id,
       e.employee_name,
       m.member_id,
       m.customer_name member_name,
       c.workday_region,
       c.club_id,
       'sale' record_type,
       mag_p.product_id,
       mag_p.sku,
       mag_p.product_name,
       mag_p.payroll_description,
       mag_p.payroll_standard_group_description payroll_group_description,
       dd.pay_week_first_day_dim_date_key,
	   'Y' manual_cancel_record
from fact_magento_tran_item f
join marketing.v_dim_magento_product_history mag_p
  on f.dim_magento_product_key = mag_p.dim_magento_product_key
 and mag_p.effective_date_time <= f.order_datetime
 and mag_p.expiration_date_time > f.order_datetime
join marketing.v_dim_employee e
  on f.dim_employee_key = e.dim_employee_key
join marketing.v_dim_mms_member m
  on f.dim_mms_member_key = m.dim_mms_member_key
join marketing.v_dim_club c
  on f.payroll_dim_club_key = c.dim_club_key
join pay_dim_date dd
  on f.canceled_dim_date_key = dd.dim_date_key
where e.dim_employee_key not in ('-998','-997','-999')
and mag_p.payroll_standard_sales_amount_flag = 'y'
and f.canceled_datetime is not null;