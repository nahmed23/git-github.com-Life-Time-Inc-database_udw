CREATE PROC [dbo].[proc_humanity_update_employees] AS
begin

/*exec proc_humanity_update_employees */
/*select * from dbo.fact_humanity_employees*/
/*select top 1 * from stage_humanity_employees*/

update dbo.d_humanity_employees
set file_arrive_date=cast(SUBSTRING(ltf_file_name,CHARINDEX('.csv',(ltf_file_name))-8,8) as date)
from dbo.d_humanity_employees
where bk_hash not in ('-997', '-998','-999')
and ltf_file_name is not null and file_arrive_date is null

truncate table dbo.fact_humanity_employees
insert into dbo.fact_humanity_employees
(
	[employee_id] ,
	[employee_eid],
	[employee_name] ,
	[employee_email],
	[company_id],
	[company_name],
	[deleted_flg],
	[employee_status],
	[employee_role],
	[position_name],
	[location_name],
	[employee_to_see_wages],
	[last_active_date_utc] ,
	[user_timezone],
	[Inserted_date_time],
	[Inserted_user],
	[file_arrive_date]
	 
)
select 
	[employee_id] ,
	[employee_eid],
	[employee_name] ,
	[employee_email],
	[company_id],
	[company_name],
	[deleted_flg],
	[employee_status],
	[employee_role],
	[position_name],
	[location_name],
	[employee_to_see_wages],
	[last_active_date_utc] ,
	[user_timezone],
    [dv_inserted_date_time],
    [dv_insert_user],
	[file_arrive_date]
from dbo.d_humanity_employees
Where 
file_arrive_date=(select max(file_arrive_date) from dbo.d_humanity_employees where bk_hash not in ('-997', '-998','-999'))
and bk_hash not in ('-997', '-998','-999')


end


