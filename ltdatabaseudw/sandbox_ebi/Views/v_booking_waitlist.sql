CREATE VIEW [sandbox_ebi].[v_booking_waitlist]
AS select p.dim_club_key,
       l.club_id,
	   l.club_code,
	   l.club_name,
       b.dim_exerp_booking_key,
       b.booking_id,
	   b.booking_name,
	   b.booking_dim_date_key,
	   b.booking_dim_time_key,
	   b.class_capacity,
	   b.activity_name,
	   b.activity_group_name,
	   b.department,
	   sum(case when waitlist_flag = 'N' and p.participation_state <> 'cancelled' and dim_mms_member_key <> '-998' then 1 else 0 end) roster_count,
	   sum(case when waitlist_flag = 'Y' then 1 else 0 end) waitlist_count
from dbo.fact_exerp_participation p
join sandbox_ebi.v_booking b on p.dim_exerp_booking_key = b.dim_exerp_booking_key
join sandbox_ebi.v_location l on p.dim_club_key = l.dim_club_key
where p.booking_dim_date_key >= convert(varchar,getdate(),112)
group by p.dim_club_key,
       l.club_id,
	   l.club_code,
	   l.club_name,
       b.dim_exerp_booking_key,
       b.booking_id,
	   b.booking_name,
	   b.booking_dim_date_key,
	   b.booking_dim_time_key,
	   b.class_capacity,
	   b.activity_name,
	   b.activity_group_name,
	   b.department
having sum(case when waitlist_flag = 'Y' then 1 else 0 end) > 1;