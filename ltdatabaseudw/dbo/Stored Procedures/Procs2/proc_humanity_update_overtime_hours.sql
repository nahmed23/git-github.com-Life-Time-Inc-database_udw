CREATE PROC [dbo].[proc_humanity_update_overtime_hours] AS
begin

--exec proc_humanity_update_overtime_hours
--select count(*) from dbo.fact_humanity_overtime_hours
--select count(*) from dbo.d_humanity_overtime_hours
--select top 1 * from stage_humanity_overtime_hours

if object_id('tempdb..#employee_id_needed') is not null
drop table #employee_id_needed
---------Get latest records --from employee id  
select employee_id,
ot_date_formatted_dim_date_key as ot_date_formatted_dim_date_key,
max(file_arrive_date) as file_arrive_date
into #employee_id_needed
from dbo.d_humanity_overtime_hours
where bk_hash not in ('-997', '-998','-999')
group by employee_id,ot_date_formatted_dim_date_key
-----------------------------------------------
truncate table dbo.fact_humanity_overtime_hours
insert into dbo.fact_humanity_overtime_hours
(
    fact_humanity_overtime_hours_key,
	userid,
	company_id,
	employee_name,
	employee_id,
	date_formatted,
	hours_regular,
	hours_overtime,
	hours_d_overtime,
	hours_position_id,
	hours_location_id,
	start_time,
	end_time,
	file_arrive_date,
	deleted_flag,
	ot_date_formatted_dim_date_key,
	ot_start_time_dim_time_key,
	ot_end_time_dim_time_key,
	dv_load_date_time,
	dv_load_end_date_time,
	dv_batch_id,
	dv_inserted_date_time,
	dv_insert_user,
	dv_updated_date_time,
	dv_update_user
 
)
select 
    bk_hash,
	userid,
	company_id,
	employee_name,
	d_humanity_overtime_hours.employee_id,
	date_formatted,
	hours_regular,
	hours_overtime,
	hours_d_overtime,
	hours_position_id,
	hours_location_id,
	start_time,
	end_time,
	d_humanity_overtime_hours.file_arrive_date,
	deleted_flag,
	d_humanity_overtime_hours.ot_date_formatted_dim_date_key,
	ot_start_time_dim_time_key,
	ot_end_time_dim_time_key,
    dv_load_date_time,
	dv_load_end_date_time,
	dv_batch_id,
	dv_inserted_date_time,
	dv_insert_user,
	dv_updated_date_time,
	dv_update_user
from dbo.d_humanity_overtime_hours d_humanity_overtime_hours
join #employee_id_needed employee_id_needed
on employee_id_needed.employee_id=d_humanity_overtime_hours.employee_id
and employee_id_needed.ot_date_formatted_dim_date_key=d_humanity_overtime_hours.ot_date_formatted_dim_date_key
and employee_id_needed.file_arrive_date=d_humanity_overtime_hours.file_arrive_date
where d_humanity_overtime_hours.bk_hash not in ('-997', '-998','-999')

if object_id('tempdb..#employee_id_needed') is not null
drop table #employee_id_needed

end