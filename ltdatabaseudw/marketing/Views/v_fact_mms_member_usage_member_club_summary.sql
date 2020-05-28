CREATE VIEW [marketing].[v_fact_mms_member_usage_member_club_summary]
AS select member.member_id,
club.club_id,
usage.all_checkins,
usage.current_month_checkins,
usage.current_week_checkins,
usage.today_checkins,
usage.last_checkin_dim_date_key,
usage.last_checkin_date_time,
case when today_dd.day_number_in_month = 2 and month(usage.last_checkin_date_time) = month(today_dd.calendar_date)
then usage.udw_inserted_date_time
when today_dd.day_number_in_month = 2 and month(usage.last_checkin_date_time) <> month(today_dd.calendar_date)
then getdate() 
else usage.udw_inserted_date_time end as udw_inserted_date_time
from
(
select fmu.checkin_dim_mms_member_key,
fmu.dim_club_key,
sum(1) all_checkins,
sum(case when dd.month_starting_dim_date_key = today_dd.month_starting_dim_date_key then 1 else 0 end) current_month_checkins,
sum(case when dd.year = today_dd.year and dd.week_number_in_year = today_dd.week_number_in_year then 1 else 0 end) current_week_checkins,
sum(case when dd.dim_date_key = today_dd.dim_date_key then 1 else 0 end) today_checkins,
max(dd.dim_date_key) last_checkin_dim_date_key,
max(fmu.checkin_date_time) last_checkin_date_time,
max(fmu.dv_inserted_date_time) udw_inserted_date_time
from marketing.v_fact_mms_member_usage fmu
join marketing.v_dim_date dd on convert(datetime,convert(Varchar,fmu.checkin_date_time,110),110) = dd.calendar_date
cross join v_get_date get_date
join marketing.v_dim_date today_dd on today_dd.calendar_date = get_date.get_date
group by fmu.checkin_dim_mms_member_key,
fmu.dim_club_key
) usage
join marketing.v_dim_mms_member member on usage.checkin_dim_mms_member_key = member.dim_mms_member_key
join marketing.v_dim_club club on usage.dim_club_key = club.dim_club_key
join marketing.v_dim_date cur_date on cur_date.dim_date_key = usage.last_checkin_dim_date_key
join marketing.v_dim_date today_dd on today_dd.dim_date_key = convert(varchar, getdate(), 112)
where member.customer_name not like '%House Account'
and usage.last_checkin_dim_date_key >= getdate() -31;