CREATE PROC [dbo].[proc_p_exacttarget_bounces] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

declare @wf_name varchar(500) = 'wf_dv_exacttarget_bounces'
declare @last_successful_dv_batch_id bigint = (select isnull(max(dv_batch_id) + 1,-3) from dbo.dv_job_status_history where job_name = @wf_name and job_status = 'Complete')
declare @process_dv_batch_id bigint = case when @current_dv_batch_id <= @last_successful_dv_batch_id then @current_dv_batch_id else @last_successful_dv_batch_id end

if object_id('tempdb..#process') is not null drop table #process
create table dbo.#process with(distribution=hash(bk_hash),location= user_db, clustered index (bk_hash)) as
select bk_hash
  from s_exacttarget_bounces
 where dv_batch_id >= @process_dv_batch_id

delete from p_exacttarget_bounces where bk_hash in (select bk_hash from #process)

insert into dbo.p_exacttarget_bounces(
        bk_hash,
        stage_exacttarget_bounces_id,
        client_id,
        send_id,
        subscriber_id,
        list_id,
        batch_id,
        s_exacttarget_bounces_id,
        dv_inserted_date_time,
        dv_insert_user,
        dv_load_date_time,
        dv_load_end_date_time,
        dv_batch_id)
select h.bk_hash,
       h.stage_exacttarget_bounces_id,
       h.client_id,
       h.send_id,
       h.subscriber_id,
       h.list_id,
       h.batch_id,
       s_exacttarget_bounces.s_exacttarget_bounces_id,
       getdate(),
       suser_sname(),
       s_exacttarget_bounces.dv_load_date_time dv_load_date_time,
       'Dec 31, 9999' dv_load_end_date_time,
       s_exacttarget_bounces.dv_batch_id dv_batch_id
  from h_exacttarget_bounces h
  join (select bk_hash, s_exacttarget_bounces_id, dv_batch_id, dv_load_date_time
          from (select bk_hash, s_exacttarget_bounces_id, dv_batch_id, dv_load_date_time, rank() over (partition by bk_hash order by dv_load_date_time desc,s_exacttarget_bounces_id desc) r from s_exacttarget_bounces where bk_hash in (select bk_hash from #process)) x
         where r = 1) s_exacttarget_bounces
    on h.bk_hash = s_exacttarget_bounces.bk_hash
 where h.bk_hash in (select bk_hash from #process)
end