CREATE VIEW [reporting].[v_exerp_boss_participation] AS select
'BOSS' data_source,
fbp.dim_boss_reservation_key as reservation_key,
fbp.instructor_type,
fbp.participation_dim_date_key,
fbp.primary_dim_employee_key,
fbp.secondary_dim_employee_key,
fbp.number_of_participants
from marketing.v_fact_boss_participation fbp
UNION
select 
'EXERP' data_source,
mb.dim_exerp_booking_recurrence_key as reservation_key,
'NULL' Instructor_type, /*Open Question - How Exerp determines this ?*/
fep.show_up_dim_date_key,
esu.dim_employee_key,
'-998' secondary_dim_employee_key,
count(fep.participation_id) number_of_participants
from
marketing.v_fact_exerp_participation fep
INNER JOIN marketing.v_dim_exerp_booking mb on fep.d_exerp_booking_bk_hash = mb.dim_exerp_booking_key
LEFT JOIN marketing.v_dim_exerp_staff_usage esu on esu.dim_exerp_booking_key = mb.dim_exerp_booking_key and esu.staff_usage_state = 'ACTIVE'
WHERE participation_state <>'CANCELLED'
Group by 
mb.dim_exerp_booking_recurrence_key,
fep.show_up_dim_date_key,
esu.dim_employee_key;