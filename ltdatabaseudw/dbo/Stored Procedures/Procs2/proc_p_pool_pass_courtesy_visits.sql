CREATE PROC [dbo].[proc_p_pool_pass_courtesy_visits] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

declare @wf_name varchar(500) = 'wf_dv_pool_pass_courtesy_visits'
declare @last_successful_dv_batch_id bigint = (select isnull(max(dv_batch_id) + 1,-3) from dbo.dv_job_status_history where job_name = @wf_name and job_status = 'Complete')
declare @process_dv_batch_id bigint = case when @current_dv_batch_id <= @last_successful_dv_batch_id then @current_dv_batch_id else @last_successful_dv_batch_id end

if object_id('tempdb..#process') is not null drop table #process
create table dbo.#process with(distribution=hash(bk_hash),location= user_db, clustered index (bk_hash)) as
select bk_hash
  from h_pool_pass_courtesy_visits
 where dv_batch_id >= @process_dv_batch_id
 union
select bk_hash
  from l_pool_pass_courtesy_visits
 where dv_batch_id >= @process_dv_batch_id
 union
select bk_hash
  from s_pool_pass_courtesy_visits
 where dv_batch_id >= @process_dv_batch_id

delete from p_pool_pass_courtesy_visits where bk_hash in (select bk_hash from #process)

insert into dbo.p_pool_pass_courtesy_visits(
        bk_hash,
        courtesy_visits_id,
        l_pool_pass_courtesy_visits_id,
        s_pool_pass_courtesy_visits_id,
        dv_inserted_date_time,
        dv_insert_user,
        dv_load_date_time,
        dv_load_end_date_time,
        dv_batch_id)
select h.bk_hash,
       h.courtesy_visits_id,
       isnull(l_pool_pass_courtesy_visits.l_pool_pass_courtesy_visits_id,'-998'),
       isnull(s_pool_pass_courtesy_visits.s_pool_pass_courtesy_visits_id,'-998'),
       getdate(),
       suser_sname(),
       case when l_pool_pass_courtesy_visits.dv_load_date_time >= isnull(s_pool_pass_courtesy_visits.dv_load_date_time,'jan 1, 1763') then l_pool_pass_courtesy_visits.dv_load_date_time
            else isnull(s_pool_pass_courtesy_visits.dv_load_date_time,'jan 1, 1763') end dv_load_date_time,
       'Dec 31, 9999' dv_load_end_date_time,
       case when l_pool_pass_courtesy_visits.dv_batch_id >= isnull(s_pool_pass_courtesy_visits.dv_batch_id,-2) then l_pool_pass_courtesy_visits.dv_batch_id
            else isnull(s_pool_pass_courtesy_visits.dv_batch_id,-2) end dv_batch_id
  from h_pool_pass_courtesy_visits h
  left join (select bk_hash, l_pool_pass_courtesy_visits_id, dv_batch_id, dv_load_date_time
          from (select bk_hash, l_pool_pass_courtesy_visits_id, dv_batch_id, dv_load_date_time, rank() over (partition by bk_hash order by dv_load_date_time desc,l_pool_pass_courtesy_visits_id desc) r from l_pool_pass_courtesy_visits where bk_hash in (select bk_hash from #process)) x
         where r = 1) l_pool_pass_courtesy_visits
    on h.bk_hash = l_pool_pass_courtesy_visits.bk_hash
  left join (select bk_hash, s_pool_pass_courtesy_visits_id, dv_batch_id, dv_load_date_time
          from (select bk_hash, s_pool_pass_courtesy_visits_id, dv_batch_id, dv_load_date_time, rank() over (partition by bk_hash order by dv_load_date_time desc,s_pool_pass_courtesy_visits_id desc) r from s_pool_pass_courtesy_visits where bk_hash in (select bk_hash from #process)) x
         where r = 1) s_pool_pass_courtesy_visits
    on h.bk_hash = s_pool_pass_courtesy_visits.bk_hash
 where h.bk_hash in (select bk_hash from #process)
end