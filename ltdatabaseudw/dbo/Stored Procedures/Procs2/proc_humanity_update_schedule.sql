CREATE PROC [dbo].[proc_humanity_update_schedule] AS
begin

if object_id('tempdb..#shift_id_needed') is not null
drop table #shift_id_needed

--exec proc_humanity_update_schedule
--update file arrive date from file_name column


---------Get latest records --from schedule full file process 
select shift_id,
max(file_arrive_date) as file_arrive_date
into #shift_id_needed
from dbo.d_humanity_schedule
where bk_hash not in ('-997', '-998','-999')
group by shift_id
-----------------------------------------------


------------------------------------------------
truncate table dbo.fact_Humanity_schedule
------------------------------------------------

insert into dbo.fact_Humanity_schedule
(
shift_id	,
company_id	,
company_name	,
employee_id	,
employee_eid	,
employee_name	,
location_id	,
location_name	,
position_id	,
workday_position_id	,
position_name	,
shift_start_date_utc	,
shift_start_time	,
shift_end_date_utc	,
shift_end_time	,
hours	,
wage	,
published	,
published_datetime_utc	,
shift_type	,
employees_needed	,
employees_working	,
recurring_shift	,
created_by_id	,
created_by_eid	,
created_by_name	,
created_datetime_utc	,
updated_at_utc	,
notes	,
is_deleted	,
dv_inserted_date_time	,
dv_insert_USER	,
file_arrive_date	)

select
stage_Humanity_schedule_data.shift_id,
stage_Humanity_schedule_data.company_id,
stage_Humanity_schedule_data.company_name,
stage_Humanity_schedule_data.employee_id,
stage_Humanity_schedule_data.employee_eid,
stage_Humanity_schedule_data.employee_name,
stage_Humanity_schedule_data.location_id,
stage_Humanity_schedule_data.location_name,
stage_Humanity_schedule_data.position_id,
stage_Humanity_schedule_data.workday_position_id,
stage_Humanity_schedule_data.position_name,
stage_Humanity_schedule_data.shift_start_date_utc,
stage_Humanity_schedule_data.shift_start_time,
stage_Humanity_schedule_data.shift_end_date_utc,
stage_Humanity_schedule_data.shift_end_time,
stage_Humanity_schedule_data.hours,
stage_Humanity_schedule_data.wage,
stage_Humanity_schedule_data.published,
stage_Humanity_schedule_data.published_datetime_utc,
stage_Humanity_schedule_data.shift_type,
stage_Humanity_schedule_data.employees_needed,
stage_Humanity_schedule_data.employees_working,
stage_Humanity_schedule_data.recurring_shift,
stage_Humanity_schedule_data.created_by_id,
stage_Humanity_schedule_data.created_by_eid,
stage_Humanity_schedule_data.created_by_name,
stage_Humanity_schedule_data.created_datetime_utc,
stage_Humanity_schedule_data.updated_at_utc,
stage_Humanity_schedule_data.notes,
stage_Humanity_schedule_data.is_deleted,
stage_Humanity_schedule_data.DV_Inserted_date_time,
stage_Humanity_schedule_data.dv_insert_user,
stage_Humanity_schedule_data.file_arrive_date
from dbo.d_humanity_schedule stage_Humanity_schedule_data
join #shift_id_needed shift_id_needed
on stage_Humanity_schedule_data.file_arrive_date=shift_id_needed.file_arrive_date
and stage_Humanity_schedule_data.shift_id=shift_id_needed.shift_id
where stage_Humanity_schedule_data.bk_hash not in ('-997', '-998','-999')

if object_id('tempdb..#shift_id_needed') is not null
drop table #shift_id_needed

END
-------------------------------------------

SET QUOTED_IDENTIFIER ON
