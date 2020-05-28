CREATE VIEW [sandbox_ebi].[v_booking]
AS select b.dim_exerp_booking_key,
       b.booking_id,
	   b.booking_name,
	   b.class_capacity,
	   b.start_dim_date_key booking_dim_date_key,
	   b.start_dim_time_key booking_dim_time_key,
	   b.booking_state,
	   a.activity_group_name,
	   a.activity_name,
	   a.department
from dim_exerp_booking b
join dim_exerp_activity a on b.dim_exerp_activity_key = a.dim_exerp_activity_key;