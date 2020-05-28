﻿CREATE PROC [dbo].[proc_p_mms_department] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

declare @wf_name varchar(500) = 'wf_dv_mms_department'
declare @last_successful_dv_batch_id bigint = (select isnull(max(dv_batch_id) + 1,-3) from dbo.dv_job_status_history where job_name = @wf_name and job_status = 'Complete')
declare @process_dv_batch_id bigint = case when @current_dv_batch_id <= @last_successful_dv_batch_id then @current_dv_batch_id else @last_successful_dv_batch_id end

if object_id('tempdb..#process') is not null drop table #process
create table dbo.#process with(distribution=hash(bk_hash),location= user_db, clustered index (bk_hash)) as
select bk_hash
  from s_mms_department
 where dv_batch_id >= @process_dv_batch_id

delete from p_mms_department where bk_hash in (select bk_hash from #process)

insert into dbo.p_mms_department(
        bk_hash,
        department_id,
        s_mms_department_id,
        dv_inserted_date_time,
        dv_insert_user,
        dv_load_date_time,
        dv_load_end_date_time,
        dv_batch_id)
select h.bk_hash,
       h.department_id,
       s_mms_department.s_mms_department_id,
       getdate(),
       suser_sname(),
       s_mms_department.dv_load_date_time dv_load_date_time,
       'Dec 31, 9999' dv_load_end_date_time,
       s_mms_department.dv_batch_id dv_batch_id
  from h_mms_department h
  join (select bk_hash, s_mms_department_id, dv_batch_id, dv_load_date_time
          from (select bk_hash, s_mms_department_id, dv_batch_id, dv_load_date_time, rank() over (partition by bk_hash order by dv_load_date_time desc) r from s_mms_department where bk_hash in (select bk_hash from #process)) x
         where r = 1) s_mms_department
    on h.bk_hash = s_mms_department.bk_hash
 where h.bk_hash in (select bk_hash from #process)
end