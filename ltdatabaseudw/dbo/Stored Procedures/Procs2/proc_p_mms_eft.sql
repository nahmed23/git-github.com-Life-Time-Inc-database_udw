﻿CREATE PROC [dbo].[proc_p_mms_eft] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

declare @wf_name varchar(500) = 'wf_dv_mms_eft'
declare @last_successful_dv_batch_id bigint = (select isnull(max(dv_batch_id) + 1,-3) from dbo.dv_job_status_history where job_name = @wf_name and job_status = 'Complete')
declare @process_dv_batch_id bigint = case when @current_dv_batch_id <= @last_successful_dv_batch_id then @current_dv_batch_id else @last_successful_dv_batch_id end

if object_id('tempdb..#process') is not null drop table #process
create table dbo.#process with(distribution=hash(bk_hash),location= user_db, clustered index (bk_hash)) as
select bk_hash
  from s_mms_eft_1
 where dv_batch_id >= @process_dv_batch_id
union
select bk_hash
  from l_mms_eft_1
 where dv_batch_id >= @process_dv_batch_id
union
select bk_hash
  from l_mms_eft
 where dv_batch_id >= @process_dv_batch_id
union
select bk_hash
  from s_mms_eft
 where dv_batch_id >= @process_dv_batch_id

delete from p_mms_eft where bk_hash in (select bk_hash from #process)

insert into dbo.p_mms_eft(
        bk_hash,
        eft_id,
        s_mms_eft_1_id,
        l_mms_eft_1_id,
        l_mms_eft_id,
        s_mms_eft_id,
        dv_inserted_date_time,
        dv_insert_user,
        dv_load_date_time,
        dv_load_end_date_time,
        dv_batch_id)
select h.bk_hash,
       h.eft_id,
       s_mms_eft_1.s_mms_eft_1_id,
       l_mms_eft_1.l_mms_eft_1_id,
       l_mms_eft.l_mms_eft_id,
       s_mms_eft.s_mms_eft_id,
       getdate(),
       suser_sname(),
       case when s_mms_eft_1.dv_load_date_time >= l_mms_eft_1.dv_load_date_time and s_mms_eft_1.dv_load_date_time >= l_mms_eft.dv_load_date_time and s_mms_eft_1.dv_load_date_time >= s_mms_eft.dv_load_date_time then s_mms_eft_1.dv_load_date_time
            when l_mms_eft_1.dv_load_date_time >= l_mms_eft.dv_load_date_time and l_mms_eft_1.dv_load_date_time >= s_mms_eft.dv_load_date_time then l_mms_eft_1.dv_load_date_time
            when l_mms_eft.dv_load_date_time >= s_mms_eft.dv_load_date_time then l_mms_eft.dv_load_date_time
            else s_mms_eft.dv_load_date_time end dv_load_date_time,
       'Dec 31, 9999' dv_load_end_date_time,
       case when s_mms_eft_1.dv_batch_id >= l_mms_eft_1.dv_batch_id and s_mms_eft_1.dv_batch_id >= l_mms_eft.dv_batch_id and s_mms_eft_1.dv_batch_id >= s_mms_eft.dv_batch_id then s_mms_eft_1.dv_batch_id
            when l_mms_eft_1.dv_batch_id >= l_mms_eft.dv_batch_id and l_mms_eft_1.dv_batch_id >= s_mms_eft.dv_batch_id then l_mms_eft_1.dv_batch_id
            when l_mms_eft.dv_batch_id >= s_mms_eft.dv_batch_id then l_mms_eft.dv_batch_id
            else s_mms_eft.dv_batch_id end dv_batch_id
  from h_mms_eft h
  join (select bk_hash, s_mms_eft_1_id, dv_batch_id, dv_load_date_time
          from (select bk_hash, s_mms_eft_1_id, dv_batch_id, dv_load_date_time, rank() over (partition by bk_hash order by dv_load_date_time desc,s_mms_eft_1_id desc) r from s_mms_eft_1 where bk_hash in (select bk_hash from #process)) x
         where r = 1) s_mms_eft_1
    on h.bk_hash = s_mms_eft_1.bk_hash
  join (select bk_hash, l_mms_eft_1_id, dv_batch_id, dv_load_date_time
          from (select bk_hash, l_mms_eft_1_id, dv_batch_id, dv_load_date_time, rank() over (partition by bk_hash order by dv_load_date_time desc,l_mms_eft_1_id desc) r from l_mms_eft_1 where bk_hash in (select bk_hash from #process)) x
         where r = 1) l_mms_eft_1
    on h.bk_hash = l_mms_eft_1.bk_hash
  join (select bk_hash, l_mms_eft_id, dv_batch_id, dv_load_date_time
          from (select bk_hash, l_mms_eft_id, dv_batch_id, dv_load_date_time, rank() over (partition by bk_hash order by dv_load_date_time desc,l_mms_eft_id desc) r from l_mms_eft where bk_hash in (select bk_hash from #process)) x
         where r = 1) l_mms_eft
    on h.bk_hash = l_mms_eft.bk_hash
  join (select bk_hash, s_mms_eft_id, dv_batch_id, dv_load_date_time
          from (select bk_hash, s_mms_eft_id, dv_batch_id, dv_load_date_time, rank() over (partition by bk_hash order by dv_load_date_time desc,s_mms_eft_id desc) r from s_mms_eft where bk_hash in (select bk_hash from #process)) x
         where r = 1) s_mms_eft
    on h.bk_hash = s_mms_eft.bk_hash
 where h.bk_hash in (select bk_hash from #process)
end