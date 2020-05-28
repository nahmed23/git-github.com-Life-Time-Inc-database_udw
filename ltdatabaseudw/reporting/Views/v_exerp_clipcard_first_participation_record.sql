CREATE VIEW [reporting].[v_exerp_clipcard_first_participation_record]
AS select a.dim_exerp_clipcard_key,dim_exerp_booking_key as first_booking_key,service_employee_key  from
(
select par.dim_exerp_clipcard_key,par.dim_exerp_booking_key, su.dim_employee_key service_employee_key, rank() over (partition by par.dim_exerp_clipcard_key order by bo.start_dim_date_key,bo.start_dim_time_key) rnk 
from marketing.v_fact_exerp_participation par
INNER JOIN marketing.v_dim_exerp_booking bo on par.dim_exerp_booking_key = bo.dim_exerp_booking_key
INNER JOIN [marketing].[v_dim_exerp_staff_usage] su on bo.dim_exerp_booking_key = su.dim_exerp_booking_key and su.staff_usage_state ='ACTIVE'
--where par.dim_exerp_clipcard_key = '12E990D49CFB148CB5A872E9A98199B8'
)
a where a.rnk = 1
group by a.dim_exerp_clipcard_key,dim_exerp_booking_key,service_employee_key;