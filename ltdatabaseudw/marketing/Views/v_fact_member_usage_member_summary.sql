CREATE VIEW [marketing].[v_fact_member_usage_member_summary]
AS select /*--fmu.checkin_dim_mms_member_key,*/
member.member_id,
/*-fmu.dim_club_key,*/
sum(case when dd.month_starting_dim_date_key = today_dd.month_starting_dim_date_key then 1 else 0 end) current_month_checkins,
sum(fmu.TotalCheckinCount) all_checkins,
sum(case when dd.year = today_dd.year and dd.week_number_in_year = today_dd.week_number_in_year then 1 else 0 end) current_week_checkins,
sum(case when dd.dim_date_key = today_dd.dim_date_key then 1 else 0 end) today_checkins,
max(dd.dim_date_key) last_checkin_dim_date_key,
max(fmu.last_checkin_date_time) last_checkin_date_time,
max( case when today_dd.day_number_in_month = 2 and month(fmu.last_checkin_date_time) = month(today_dd.calendar_date)
		then fmu.udw_inserted_date_time
		when today_dd.day_number_in_month = 2 and month(fmu.last_checkin_date_time) <> month(today_dd.calendar_date)
			then getdate() 
		else fmu.udw_inserted_date_time end ) as udw_inserted_date_time
from
(
select temp.checkin_dim_mms_member_key,
temp.check_in_date_key,
sum(temp.checkin_count) as TotalCheckinCount,
/*-case when count(distinct temp.dim_club_key) > 1 then 1 else count(distinct temp.dim_club_key) end as ActualCheckinCount,*/
max(temp.checkin_date_time) as last_checkin_date_time,
max(temp.dv_inserted_date_time) as udw_inserted_date_time
from
(
select checkin_dim_mms_member_key, 
dim_club_key,
count(checkin_date_time) as checkin_count, 
convert(char(8),checkin_date_time,112) as check_in_date_key,
max(checkin_date_time) as checkin_date_time, 
max(dv_inserted_date_time) as dv_inserted_date_time
from marketing.v_fact_mms_member_usage
/*-where checkin_dim_mms_member_key in ('00009998A6778075BFEAA7F15AF90B49', '2F248B49D360FA5424AFF3E89904DA2C')*/
group by checkin_dim_mms_member_key, dim_club_key, convert(char(8),checkin_date_time,112)
) temp
group by temp.checkin_dim_mms_member_key,
temp.check_in_date_key
) fmu
join marketing.v_dim_mms_member member on fmu.checkin_dim_mms_member_key = member.dim_mms_member_key
join marketing.v_dim_date dd on fmu.check_in_date_key = dd.dim_date_key
join marketing.v_dim_date get_date on get_date.dim_date_key = convert(varchar, getdate(), 112)
join marketing.v_dim_date today_dd on today_dd.calendar_date = get_date.calendar_date
where member.customer_name not like '%House Account'
group by member.member_id;