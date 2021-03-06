﻿CREATE PROC [dbo].[proc_p_athlinks_api_vw_athlete_non_member] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

declare @wf_name varchar(500) = 'wf_dv_athlinks_api_vw_athlete_non_member'
declare @last_successful_dv_batch_id bigint = (select isnull(max(dv_batch_id) + 1,-3) from dbo.dv_job_status_history where job_name = @wf_name and job_status = 'Complete')
declare @process_dv_batch_id bigint = case when @current_dv_batch_id <= @last_successful_dv_batch_id then @current_dv_batch_id else @last_successful_dv_batch_id end

if object_id('tempdb..#process') is not null drop table #process
create table dbo.#process with(distribution=hash(bk_hash),location= user_db, clustered index (bk_hash)) as
select bk_hash
  from s_athlinks_api_vw_athlete_non_member
 where dv_batch_id >= @process_dv_batch_id

delete from p_athlinks_api_vw_athlete_non_member where bk_hash in (select bk_hash from #process)

insert into dbo.p_athlinks_api_vw_athlete_non_member(
        bk_hash,
        racer_id,
        s_athlinks_api_vw_athlete_non_member_id,
        dv_inserted_date_time,
        dv_insert_user,
        dv_load_date_time,
        dv_load_end_date_time,
        dv_batch_id)
select h.bk_hash,
       h.racer_id,
       s_athlinks_api_vw_athlete_non_member.s_athlinks_api_vw_athlete_non_member_id,
       getdate(),
       suser_sname(),
       s_athlinks_api_vw_athlete_non_member.dv_load_date_time dv_load_date_time,
       'Dec 31, 9999' dv_load_end_date_time,
       s_athlinks_api_vw_athlete_non_member.dv_batch_id dv_batch_id
  from h_athlinks_api_vw_athlete_non_member h
  join (select bk_hash, s_athlinks_api_vw_athlete_non_member_id, dv_batch_id, dv_load_date_time
          from (select bk_hash, s_athlinks_api_vw_athlete_non_member_id, dv_batch_id, dv_load_date_time, rank() over (partition by bk_hash order by dv_load_date_time desc,s_athlinks_api_vw_athlete_non_member_id desc) r from s_athlinks_api_vw_athlete_non_member where bk_hash in (select bk_hash from #process)) x
         where r = 1) s_athlinks_api_vw_athlete_non_member
    on h.bk_hash = s_athlinks_api_vw_athlete_non_member.bk_hash
 where h.bk_hash in (select bk_hash from #process)
end