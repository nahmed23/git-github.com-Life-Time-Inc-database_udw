CREATE VIEW [reporting].[v_exerp_participation]
AS select 
v_dim_exerp_booking.dim_exerp_booking_recurrence_key,
v_dim_exerp_booking.dim_exerp_booking_key as reservation_key,
v_dim_exerp_booking.booking_id,
v_dim_exerp_booking.booking_state,
v_dim_exerp_booking.comment as booking_comment,
'NULL' instructor_type, /*Open Question - How Exerp determines this ?*/
v_dim_exerp_booking.start_dim_date_key as booking_dim_date_key,
v_fact_exerp_participation.show_up_dim_date_key,
v_dim_exerp_staff_usage.dim_employee_key,
sum(case when v_fact_exerp_participation.participation_state ='PARTICIPATION' then 1 else 0 end ) number_of_participants
from
marketing.v_dim_exerp_booking
LEFT JOIN marketing.v_fact_exerp_participation on v_fact_exerp_participation.dim_exerp_booking_key = v_dim_exerp_booking.dim_exerp_booking_key and participation_state <> 'CANCELLED' and v_fact_exerp_participation.dim_mms_member_key = '-998'
LEFT JOIN marketing.v_dim_exerp_staff_usage on v_dim_exerp_staff_usage.dim_exerp_booking_key = v_dim_exerp_booking.dim_exerp_booking_key and v_dim_exerp_staff_usage.staff_usage_state = 'ACTIVE'
Group by 
v_dim_exerp_booking.dim_exerp_booking_recurrence_key,
v_dim_exerp_booking.dim_exerp_booking_key,
v_dim_exerp_booking.booking_id,
v_dim_exerp_booking.booking_state,
v_dim_exerp_booking.comment ,
v_dim_exerp_booking.start_dim_date_key,
v_fact_exerp_participation.show_up_dim_date_key,
v_dim_exerp_staff_usage.dim_employee_key;