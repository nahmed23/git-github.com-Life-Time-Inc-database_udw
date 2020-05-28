CREATE PROC [dbo].[proc_p_fitmetrix_api_appointment_id_statistics] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

declare @wf_name varchar(500) = 'wf_dv_fitmetrix_api_appointment_id_statistics'
declare @last_successful_dv_batch_id bigint = (select isnull(max(dv_batch_id) + 1,-3) from dbo.dv_job_status_history where job_name = @wf_name and job_status = 'Complete')
declare @process_dv_batch_id bigint = case when @current_dv_batch_id <= @last_successful_dv_batch_id then @current_dv_batch_id else @last_successful_dv_batch_id end

if object_id('tempdb..#process') is not null drop table #process
create table dbo.#process with(distribution=hash(bk_hash),location= user_db, clustered index (bk_hash)) as
select bk_hash
  from l_fitmetrix_api_appointment_id_statistics
 where dv_batch_id >= @process_dv_batch_id
union
select bk_hash
  from s_fitmetrix_api_appointment_id_statistics
 where dv_batch_id >= @process_dv_batch_id

delete from p_fitmetrix_api_appointment_id_statistics where bk_hash in (select bk_hash from #process)

insert into dbo.p_fitmetrix_api_appointment_id_statistics(
        bk_hash,
        profile_appointment_id,
        l_fitmetrix_api_appointment_id_statistics_id,
        s_fitmetrix_api_appointment_id_statistics_id,
        dv_inserted_date_time,
        dv_insert_user,
        dv_load_date_time,
        dv_load_end_date_time,
        dv_batch_id)
select h.bk_hash,
       h.profile_appointment_id,
       isnull(l_fitmetrix_api_appointment_id_statistics.l_fitmetrix_api_appointment_id_statistics_id,'-998'),
       isnull(s_fitmetrix_api_appointment_id_statistics.s_fitmetrix_api_appointment_id_statistics_id,'-998'),
       getdate(),
       suser_sname(),
       case when l_fitmetrix_api_appointment_id_statistics.dv_load_date_time >= isnull(s_fitmetrix_api_appointment_id_statistics.dv_load_date_time,'jan 1, 1763') then l_fitmetrix_api_appointment_id_statistics.dv_load_date_time
            else isnull(s_fitmetrix_api_appointment_id_statistics.dv_load_date_time,'jan 1, 1763') end dv_load_date_time,
       'Dec 31, 9999' dv_load_end_date_time,
       case when l_fitmetrix_api_appointment_id_statistics.dv_batch_id >= isnull(s_fitmetrix_api_appointment_id_statistics.dv_batch_id,-2) then l_fitmetrix_api_appointment_id_statistics.dv_batch_id
            else isnull(s_fitmetrix_api_appointment_id_statistics.dv_batch_id,-2) end dv_batch_id
  from h_fitmetrix_api_appointment_id_statistics h
  left join (select bk_hash, l_fitmetrix_api_appointment_id_statistics_id, dv_batch_id, dv_load_date_time
          from (select bk_hash, l_fitmetrix_api_appointment_id_statistics_id, dv_batch_id, dv_load_date_time, rank() over (partition by bk_hash order by dv_load_date_time desc,l_fitmetrix_api_appointment_id_statistics_id desc) r from l_fitmetrix_api_appointment_id_statistics where bk_hash in (select bk_hash from #process)) x
         where r = 1) l_fitmetrix_api_appointment_id_statistics
    on h.bk_hash = l_fitmetrix_api_appointment_id_statistics.bk_hash
  left join (select bk_hash, s_fitmetrix_api_appointment_id_statistics_id, dv_batch_id, dv_load_date_time
          from (select bk_hash, s_fitmetrix_api_appointment_id_statistics_id, dv_batch_id, dv_load_date_time, rank() over (partition by bk_hash order by dv_load_date_time desc,s_fitmetrix_api_appointment_id_statistics_id desc) r from s_fitmetrix_api_appointment_id_statistics where bk_hash in (select bk_hash from #process)) x
         where r = 1) s_fitmetrix_api_appointment_id_statistics
    on h.bk_hash = s_fitmetrix_api_appointment_id_statistics.bk_hash
 where h.bk_hash in (select bk_hash from #process)
end