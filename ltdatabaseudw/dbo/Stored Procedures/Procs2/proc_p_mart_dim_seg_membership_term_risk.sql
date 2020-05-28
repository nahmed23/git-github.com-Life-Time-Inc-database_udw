﻿CREATE PROC [dbo].[proc_p_mart_dim_seg_membership_term_risk] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

declare @wf_name varchar(500) = 'wf_dv_mart_dim_seg_membership_term_risk'
declare @last_successful_dv_batch_id bigint = (select isnull(max(dv_batch_id) + 1,-3) from dbo.dv_job_status_history where job_name = @wf_name and job_status = 'Complete')
declare @process_dv_batch_id bigint = case when @current_dv_batch_id <= @last_successful_dv_batch_id then @current_dv_batch_id else @last_successful_dv_batch_id end

if object_id('tempdb..#process') is not null drop table #process
create table dbo.#process with(distribution=hash(bk_hash),location= user_db, clustered index (bk_hash)) as
select bk_hash
  from s_mart_dim_seg_membership_term_risk
 where dv_batch_id >= @process_dv_batch_id

delete from p_mart_dim_seg_membership_term_risk where bk_hash in (select bk_hash from #process)

insert into dbo.p_mart_dim_seg_membership_term_risk(
        bk_hash,
        dim_seg_term_risk_id,
        s_mart_dim_seg_membership_term_risk_id,
        dv_inserted_date_time,
        dv_insert_user,
        dv_load_date_time,
        dv_load_end_date_time,
        dv_batch_id)
select h.bk_hash,
       h.dim_seg_term_risk_id,
       s_mart_dim_seg_membership_term_risk.s_mart_dim_seg_membership_term_risk_id,
       getdate(),
       suser_sname(),
       s_mart_dim_seg_membership_term_risk.dv_load_date_time dv_load_date_time,
       'Dec 31, 9999' dv_load_end_date_time,
       s_mart_dim_seg_membership_term_risk.dv_batch_id dv_batch_id
  from h_mart_dim_seg_membership_term_risk h
  join (select bk_hash, s_mart_dim_seg_membership_term_risk_id, dv_batch_id, dv_load_date_time
          from (select bk_hash, s_mart_dim_seg_membership_term_risk_id, dv_batch_id, dv_load_date_time, rank() over (partition by bk_hash order by dv_load_date_time desc,s_mart_dim_seg_membership_term_risk_id desc) r from s_mart_dim_seg_membership_term_risk where bk_hash in (select bk_hash from #process)) x
         where r = 1) s_mart_dim_seg_membership_term_risk
    on h.bk_hash = s_mart_dim_seg_membership_term_risk.bk_hash
 where h.bk_hash in (select bk_hash from #process)
end