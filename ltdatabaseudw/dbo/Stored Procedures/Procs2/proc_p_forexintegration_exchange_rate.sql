CREATE PROC [dbo].[proc_p_forexintegration_exchange_rate] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

declare @wf_name varchar(500) = 'wf_dv_forexintegration_exchange_rate'
declare @last_successful_dv_batch_id bigint = (select isnull(max(dv_batch_id) + 1,-3) from dbo.dv_job_status_history where job_name = @wf_name and job_status = 'Complete')
declare @process_dv_batch_id bigint = case when @current_dv_batch_id <= @last_successful_dv_batch_id then @current_dv_batch_id else @last_successful_dv_batch_id end

if object_id('tempdb..#process') is not null drop table #process
create table dbo.#process with(distribution=hash(bk_hash),location= user_db, clustered index (bk_hash)) as
select bk_hash
  from l_forexintegration_exchange_rate
 where dv_batch_id >= @process_dv_batch_id
union
select bk_hash
  from s_forexintegration_exchange_rate
 where dv_batch_id >= @process_dv_batch_id

delete from p_forexintegration_exchange_rate where bk_hash in (select bk_hash from #process)

insert into dbo.p_forexintegration_exchange_rate(
        bk_hash,
        exchange_rate_id,
        l_forexintegration_exchange_rate_id,
        s_forexintegration_exchange_rate_id,
        dv_inserted_date_time,
        dv_insert_user,
        dv_load_date_time,
        dv_load_end_date_time,
        dv_batch_id)
select h.bk_hash,
       h.exchange_rate_id,
       l_forexintegration_exchange_rate.l_forexintegration_exchange_rate_id,
       s_forexintegration_exchange_rate.s_forexintegration_exchange_rate_id,
       getdate(),
       suser_sname(),
       case when l_forexintegration_exchange_rate.dv_load_date_time >= s_forexintegration_exchange_rate.dv_load_date_time then l_forexintegration_exchange_rate.dv_load_date_time
            else s_forexintegration_exchange_rate.dv_load_date_time end dv_load_date_time,
       'Dec 31, 9999' dv_load_end_date_time,
       case when l_forexintegration_exchange_rate.dv_batch_id >= s_forexintegration_exchange_rate.dv_batch_id then l_forexintegration_exchange_rate.dv_batch_id
            else s_forexintegration_exchange_rate.dv_batch_id end dv_batch_id
  from h_forexintegration_exchange_rate h
  join (select bk_hash, l_forexintegration_exchange_rate_id, dv_batch_id, dv_load_date_time
          from (select bk_hash, l_forexintegration_exchange_rate_id, dv_batch_id, dv_load_date_time, rank() over (partition by bk_hash order by dv_load_date_time desc) r from l_forexintegration_exchange_rate where bk_hash in (select bk_hash from #process)) x
         where r = 1) l_forexintegration_exchange_rate
    on h.bk_hash = l_forexintegration_exchange_rate.bk_hash
  join (select bk_hash, s_forexintegration_exchange_rate_id, dv_batch_id, dv_load_date_time
          from (select bk_hash, s_forexintegration_exchange_rate_id, dv_batch_id, dv_load_date_time, rank() over (partition by bk_hash order by dv_load_date_time desc) r from s_forexintegration_exchange_rate where bk_hash in (select bk_hash from #process)) x
         where r = 1) s_forexintegration_exchange_rate
    on h.bk_hash = s_forexintegration_exchange_rate.bk_hash
 where h.bk_hash in (select bk_hash from #process)
end