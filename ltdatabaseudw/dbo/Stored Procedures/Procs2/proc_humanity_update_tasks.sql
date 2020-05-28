CREATE PROC [dbo].[proc_humanity_update_tasks] AS
begin

if object_id('tempdb..#shift_id_task_id_needed') is not null
drop table #shift_id_task_id_needed

--exec proc_humanity_update_tasks 
--select * from dbo.fact_humanity_tasks
--select * from d_humanity_tasks
--select top 1 * from stage_humanity_tasks
---------Get latest records --from tasks full file process 
select shift_id,
task_id,
max(file_arrive_date) as file_arrive_date
into #shift_id_task_id_needed
from dbo.d_humanity_tasks
where bk_hash not in ('-997', '-998','-999')
group by shift_id,task_id



truncate table dbo.fact_humanity_tasks
insert into dbo.fact_humanity_tasks
(
    [fact_humanity_tasks_key],
 	[task_id],
	[shift_id],
	[company_id] ,
	[task_name] ,
	[created_at] ,
	[created_by] ,
	[deleted] ,
	[load_dttm] ,
	[file_arrive_date],	
	[deleted_flag],
	dv_load_date_time,
	dv_load_end_date_time,
	dv_batch_id,
	dv_inserted_date_time,
	dv_insert_user,
	dv_updated_date_time,
	dv_update_user	 
)
select 
    d_humanity_tasks.[bk_hash],
   	d_humanity_tasks.[task_id],
	d_humanity_tasks.[shift_id],
	d_humanity_tasks.[company_id] ,
	d_humanity_tasks.[task_name] ,
	d_humanity_tasks.[created_at] ,
	d_humanity_tasks.[created_by] ,
	d_humanity_tasks.[deleted] ,
	d_humanity_tasks.[load_dttm] ,
	d_humanity_tasks.[file_arrive_date],	
	d_humanity_tasks.[deleted_flag],
	d_humanity_tasks.dv_load_date_time,
	d_humanity_tasks.dv_load_end_date_time,
	d_humanity_tasks.dv_batch_id,
	d_humanity_tasks.dv_inserted_date_time,
	d_humanity_tasks.dv_insert_user,
	d_humanity_tasks.dv_updated_date_time,
	d_humanity_tasks.dv_update_user
from dbo.d_humanity_tasks d_humanity_tasks
join #shift_id_task_id_needed shift_id_task_id_needed
on 
    shift_id_task_id_needed.shift_id=d_humanity_tasks.shift_id
and shift_id_task_id_needed.task_id=d_humanity_tasks.task_id
and shift_id_task_id_needed.file_arrive_date=d_humanity_tasks.file_arrive_date
Where 
d_humanity_tasks.bk_hash not in ('-997', '-998','-999')


end


