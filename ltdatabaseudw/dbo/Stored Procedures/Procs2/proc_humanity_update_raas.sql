CREATE PROC [dbo].[proc_humanity_update_raas] AS
begin
--exec [dbo].[proc_humanity_update_raas]
---------------------------------------------------------------------------------------------------------
---- Combine Humanity schedule, Humanity Employees, workday employees to create raas file
---- Step 1 getting timezone from humanity employees for schedule file where every employee match
---------------------------------------------------------------------------------------------------------

If object_id( 'tempdb..#fact_Humanity_schedule_utc_conv') is not null
Drop table #fact_Humanity_schedule_utc_conv
select
shift_id,
fact_Humanity_schedule.company_id,
fact_Humanity_schedule.company_name,
fact_Humanity_schedule.employee_id,
fact_Humanity_schedule.employee_eid,
fact_Humanity_schedule.employee_name,
location_id,
location_name,
position_id,
workday_position_id,
position_name,
cast(shift_start_date_utc as date) as shift_start_date_utc ,
cast(shift_start_time as time) as shift_start_time ,
cast(shift_end_date_utc as date) as shift_end_date_utc ,
cast(shift_end_time as time) as shift_end_time ,
hours,
wage,
published,
cast(published_datetime_utc as datetime) as published_datetime_utc ,
shift_type,
employees_needed,
employees_working,
recurring_shift,
created_by_id,
created_by_eid,
created_by_name,
created_datetime_utc,
updated_at_utc,
is_deleted,
notes,
fact_Humanity_schedule.dv_Inserted_date_time,
fact_Humanity_schedule.dv_Insert_user,

cast (dateadd("hh",cast(Lkp_Timezone_to_UTC.hours_diff as int),CAST(shift_start_date_utc as DATETIME) + CAST(shift_start_time as DATETIME)) as date) as employee_start_date,
cast (dateadd("hh",cast(Lkp_Timezone_to_UTC.hours_diff as int),CAST(shift_start_date_utc as DATETIME) + CAST(shift_start_time as DATETIME)) as time) as employee_start_time,
cast (dateadd("hh",cast(Lkp_Timezone_to_UTC.hours_diff as int),CAST(shift_end_date_utc as DATETIME) + CAST(shift_end_time as DATETIME)) as date) as employee_end_date,
cast (dateadd("hh",cast(Lkp_Timezone_to_UTC.hours_diff as int),CAST(shift_end_date_utc as DATETIME) + CAST(shift_end_time as DATETIME)) as time) as employee_end_time
into #fact_Humanity_schedule_utc_conv
--cast (dateadd("hh",cast(Lkp_Timezone_to_UTC.hours_diff as int),CAST(shift_end_date_utc as DATETIME) + CAST(shift_end_time as DATETIME)) as time) as employee_end_time1
from dbo.fact_Humanity_schedule fact_Humanity_schedule
left join (select distinct employee_id,user_timezone,company_id from fact_Humanity_employees) fact_Humanity_employees
on fact_Humanity_employees.employee_id=fact_Humanity_schedule.employee_id
AND fact_Humanity_employees.employee_id<>0
left join Lkp_Timezone_to_UTC Lkp_Timezone_to_UTC
on fact_Humanity_employees.user_timezone=Lkp_Timezone_to_UTC.location
---------------------------------------------------------------------------------------------------------
---- Step 2 getting timezone from humanity employees for schedule file where every employee does not match fails 
---- and company id match
---------------------------------------------------------------------------------------------------------
If object_id( 'tempdb..#fact_Humanity_schedule_utc_conv_1') is not null
Drop table #fact_Humanity_schedule_utc_conv_1
select
fact_Humanity_schedule.shift_id,
fact_Humanity_schedule.company_id,
fact_Humanity_schedule.company_name,
fact_Humanity_schedule.employee_id,
fact_Humanity_schedule.employee_eid,
fact_Humanity_schedule.employee_name,
fact_Humanity_schedule.location_id,
fact_Humanity_schedule.location_name,
fact_Humanity_schedule.position_id,
fact_Humanity_schedule.workday_position_id,
fact_Humanity_schedule.position_name,
cast(fact_Humanity_schedule.shift_start_date_utc as date) as shift_start_date_utc ,
cast(fact_Humanity_schedule.shift_start_time as time) as shift_start_time ,
cast(fact_Humanity_schedule.shift_end_date_utc as date) as shift_end_date_utc ,
cast(fact_Humanity_schedule.shift_end_time as time) as shift_end_time ,
fact_Humanity_schedule.hours,
fact_Humanity_schedule.wage,
fact_Humanity_schedule.published,
cast(fact_Humanity_schedule.published_datetime_utc as datetime) as published_datetime_utc ,
fact_Humanity_schedule.shift_type,
fact_Humanity_schedule.employees_needed,
fact_Humanity_schedule.employees_working,
fact_Humanity_schedule.recurring_shift,
fact_Humanity_schedule.created_by_id,
fact_Humanity_schedule.created_by_eid,
fact_Humanity_schedule.created_by_name,
fact_Humanity_schedule.created_datetime_utc ,
fact_Humanity_schedule.updated_at_utc ,
fact_Humanity_schedule.is_deleted,
fact_Humanity_schedule.notes,
fact_Humanity_schedule.dv_Inserted_date_time,
fact_Humanity_schedule.dv_Insert_user,
isnull
(
fact_Humanity_schedule.employee_start_date,
cast (dateadd("hh",cast(Lkp_Timezone_to_UTC.hours_diff as int),CAST(fact_Humanity_schedule.shift_start_date_utc as DATETIME) + CAST(fact_Humanity_schedule.shift_start_time as DATETIME)) as date)
)
as employee_start_date,
isnull
(
fact_Humanity_schedule.employee_start_time,
cast (dateadd("hh",cast(Lkp_Timezone_to_UTC.hours_diff as int),CAST(fact_Humanity_schedule.shift_start_date_utc as DATETIME) + CAST(fact_Humanity_schedule.shift_start_time as DATETIME)) as time)
)
as employee_start_time,
isnull
(
fact_Humanity_schedule.employee_end_date,
cast (dateadd("hh",cast(Lkp_Timezone_to_UTC.hours_diff as int),CAST(fact_Humanity_schedule.shift_end_date_utc as DATETIME) + CAST(fact_Humanity_schedule.shift_end_time as DATETIME)) as date)
)
as employee_end_date,
isnull
(
fact_Humanity_schedule.employee_end_time,
cast (dateadd("hh",cast(Lkp_Timezone_to_UTC.hours_diff as int),CAST(fact_Humanity_schedule.shift_end_date_utc as DATETIME) + CAST(fact_Humanity_schedule.shift_end_time as DATETIME)) as time)
)
as employee_end_time
INTO #fact_Humanity_schedule_utc_conv_1
from #fact_Humanity_schedule_utc_conv fact_Humanity_schedule
left join (select distinct company_id,user_timezone from fact_Humanity_employees WHERE USER_TIMEZONE<>''
AND  employee_rolE='EMPLOYEE') fact_Humanity_employees
on  fact_Humanity_employees.company_id=fact_Humanity_schedule.COMPANY_ID
and fact_Humanity_schedule.employee_id=0
left join Lkp_Timezone_to_UTC Lkp_Timezone_to_UTC
on fact_Humanity_employees.user_timezone=Lkp_Timezone_to_UTC.location
---------------------------------------------------------------------------------------------------------
---- Step 3 getting timezone from humanity employees company id match from step 2 and  
---- Combine multiple timezone into one
---------------------------------------------------------------------------------------------------------
If object_id( 'tempdb..#fact_Humanity_schedule_utc_conv_2') is not null
Drop table #fact_Humanity_schedule_utc_conv_2

select
shift_id
,company_id
,company_name
,employee_id
,employee_eid
,employee_name
,location_id
,location_name
,position_id
,workday_position_id
,position_name
,shift_start_date_utc
,shift_start_time
,shift_end_date_utc
,shift_end_time
,hours
,wage
,published
,published_datetime_utc
,shift_type
,employees_needed
,employees_working
,recurring_shift
,created_by_id
,created_by_eid
,created_by_name
,created_datetime_utc
,updated_at_utc
,is_deleted
,notes
,dv_Inserted_date_time
,dv_Insert_user
,max(employee_start_date) as adjusted_start_date
,max(employee_start_time) as adjusted_start_time
,max(employee_end_date) as adjusted_end_date
,max(employee_end_time) as adjusted_end_time
into #fact_Humanity_schedule_utc_conv_2
from #fact_Humanity_schedule_utc_conv_1
where isnumeric(employee_eid) = 1 or employee_eid is null 
group by
shift_id
,company_id
,company_name
,employee_id
,employee_eid
,employee_name
,location_id
,location_name
,position_id
,workday_position_id
,position_name
,shift_start_date_utc
,shift_start_time
,shift_end_date_utc
,shift_end_time
,hours
,wage
,published
,published_datetime_utc
,shift_type
,employees_needed
,employees_working
,recurring_shift
,created_by_id
,created_by_eid
,created_by_name
,created_datetime_utc
,updated_at_utc
,is_deleted
,notes
,dv_Inserted_date_time
,dv_Insert_user

---------------------------------------------------------------------------------------------------------
---- Step 4 getting workday data and merging into schedule data  
---------------------------------------------------------------------------------------------------------

truncate table dbo.fact_Humanity_schedule_raas

insert into dbo.fact_Humanity_schedule_raas
(
shift_id	
,company_id	
,company_name
,employee_id	
,employee_eid	
,employee_name	
,location_id	
,location_name
,position_id	
,workday_position_id	
,position_name	
,shift_start_date_utc
,shift_start_time
,shift_end_date_utc
,shift_end_time
,hours	
,wage	
,published	
,published_datetime_utc	
,shift_type	
,employees_needed	
,employees_working	
,recurring_shift	
,created_by_id	
,created_by_eid	
,created_by_name	
,created_datetime_utc	
,updated_at_utc	
,is_deleted	
,notes	
,Inserted_date_time	
,Inserted_user	
,adjusted_start_date
,adjusted_start_time
,adjusted_end_date
,adjusted_end_time
,cost_center
,Hourly_Amount
,Job_code
,Offering
,Region
,Effective_date_begin
,Effective_date_end
)


select
Humanity_LifeTime_Schedule.shift_id
,Humanity_LifeTime_Schedule.company_id
,Humanity_LifeTime_Schedule.company_name
,Humanity_LifeTime_Schedule.employee_id
,Humanity_LifeTime_Schedule.employee_eid
,Humanity_LifeTime_Schedule.employee_name
,Humanity_LifeTime_Schedule.location_id
,Humanity_LifeTime_Schedule.location_name
,Humanity_LifeTime_Schedule.position_id
,Humanity_LifeTime_Schedule.workday_position_id
,Humanity_LifeTime_Schedule.position_name
,Humanity_LifeTime_Schedule.shift_start_date_utc
,Humanity_LifeTime_Schedule.shift_start_time
,Humanity_LifeTime_Schedule.shift_end_date_utc
,Humanity_LifeTime_Schedule.shift_end_time
,cast(Humanity_LifeTime_Schedule.hours as decimal(7,2)) as hours
,Humanity_LifeTime_Schedule.wage
,Humanity_LifeTime_Schedule.published
,Humanity_LifeTime_Schedule.published_datetime_utc
,Humanity_LifeTime_Schedule.shift_type
,Humanity_LifeTime_Schedule.employees_needed
,Humanity_LifeTime_Schedule.employees_working
,Humanity_LifeTime_Schedule.recurring_shift
,Humanity_LifeTime_Schedule.created_by_id
,Humanity_LifeTime_Schedule.created_by_eid
,Humanity_LifeTime_Schedule.created_by_name
,Humanity_LifeTime_Schedule.created_datetime_utc
,Humanity_LifeTime_Schedule.updated_at_utc
,Humanity_LifeTime_Schedule.is_deleted
,Humanity_LifeTime_Schedule.notes
,Humanity_LifeTime_Schedule.dv_Inserted_date_time
,Humanity_LifeTime_Schedule.dv_Insert_user
,Humanity_LifeTime_Schedule.adjusted_start_date
,Humanity_LifeTime_Schedule.adjusted_start_time
,Humanity_LifeTime_Schedule.adjusted_end_date
,Humanity_LifeTime_Schedule.adjusted_end_time
,isnull(Humanity_Employee_RaaS.cost_center,Humanity_Employee_RaaS1.cost_center) as cost_center
,isnull(Humanity_Employee_RaaS.Hourly_Amount,Humanity_Employee_RaaS1.Hourly_Amount) as Hourly_Amount
,isnull(Humanity_Employee_RaaS.Job_Code,Humanity_Employee_RaaS1.Job_Code) as Job_code
,isnull(Humanity_Employee_RaaS.Offering,Humanity_Employee_RaaS1.Offering) as Offering
,isnull(Humanity_Employee_RaaS.Region,Humanity_Employee_RaaS1.Region) as Region
,isnull(Humanity_Employee_RaaS.Effective_date_begin,Humanity_Employee_RaaS1.Effective_date_begin) as Effective_date_begin
,isnull(Humanity_Employee_RaaS.Effective_date_end,Humanity_Employee_RaaS1.Effective_date_end) as Effective_date_end
--into #T
from #fact_Humanity_schedule_utc_conv_2 Humanity_LifeTime_Schedule
left join dbo.fact_humanity_workday_Employees Humanity_Employee_RaaS
on Humanity_Employee_RaaS.employee_id = CAST(CAST(Humanity_LifeTime_Schedule.employee_eid AS bigINT) AS VARCHAR(10))
and Humanity_Employee_RaaS.position_id = Humanity_LifeTime_Schedule.workday_position_id
and Humanity_LifeTime_Schedule.shift_start_date_utc >= Humanity_Employee_RaaS.Effective_date_begin
and Humanity_LifeTime_Schedule.shift_start_date_utc <= Humanity_Employee_RaaS.Effective_date_end
left join
(select Cost_Center,Hourly_Amount,Job_Code,Offering,Region,Position_ID,Primary_Job,Employee_ID,Effective_date_begin,Effective_date_end,File_arrive_date,Employee_position_hashkey,Cost_Hour_Job_Offer_Region_hashkey from(select Cost_Center,Hourly_Amount,Job_Code,Offering,Region,Position_ID,Primary_Job,Employee_ID,Effective_date_begin,Effective_date_end,File_arrive_date,Employee_position_hashkey,Cost_Hour_Job_Offer_Region_hashkey,rank() over(partition by Employee_ID order by Effective_date_begin desc) r from dbo.fact_humanity_workday_Employees where primary_job='1') A where A.r=1
) Humanity_Employee_RaaS1
on Humanity_Employee_RaaS1.employee_id = CAST(CAST(Humanity_LifeTime_Schedule.employee_eid AS bigINT) AS VARCHAR(10))

-------------------------------------------------------------------------------------------------------
--Remove temp files 
-------------------------------------------------------------------------------------------------------
If object_id( 'tempdb..#fact_Humanity_schedule_utc_conv') is not null
Drop table #fact_Humanity_schedule_utc_conv
If object_id( 'tempdb..#fact_Humanity_schedule_utc_conv_1') is not null
Drop table #fact_Humanity_schedule_utc_conv_1
If object_id( 'tempdb..#fact_Humanity_schedule_utc_conv_2') is not null
Drop table #fact_Humanity_schedule_utc_conv_2

end