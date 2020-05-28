CREATE PROC [dbo].[proc_p_ig_it_cfg_profit_center_group_join] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

declare @wf_name varchar(500) = 'wf_dv_ig_it_cfg_profit_center_group_join'
declare @last_successful_dv_batch_id bigint = (select isnull(max(dv_batch_id) + 1,-3) from dbo.dv_job_status_history where job_name = @wf_name and job_status = 'Complete')
declare @process_dv_batch_id bigint = case when @current_dv_batch_id <= @last_successful_dv_batch_id then @current_dv_batch_id else @last_successful_dv_batch_id end

if object_id('tempdb..#process') is not null drop table #process
create table dbo.#process with(distribution=hash(bk_hash),location= user_db, clustered index (bk_hash)) as
select bk_hash
  from s_ig_it_cfg_profit_center_group_join
 where dv_batch_id >= @process_dv_batch_id

delete from p_ig_it_cfg_profit_center_group_join where bk_hash in (select bk_hash from #process)

insert into dbo.p_ig_it_cfg_profit_center_group_join(
        bk_hash,
        ent_id,
        profit_ctr_grp_id,
        profit_center_id,
        s_ig_it_cfg_profit_center_group_join_id,
        dv_inserted_date_time,
        dv_insert_user,
        dv_load_date_time,
        dv_load_end_date_time,
        dv_batch_id)
select h.bk_hash,
       h.ent_id,
       h.profit_ctr_grp_id,
       h.profit_center_id,
       s_ig_it_cfg_profit_center_group_join.s_ig_it_cfg_profit_center_group_join_id,
       getdate(),
       suser_sname(),
       s_ig_it_cfg_profit_center_group_join.dv_load_date_time dv_load_date_time,
       'Dec 31, 9999' dv_load_end_date_time,
       s_ig_it_cfg_profit_center_group_join.dv_batch_id dv_batch_id
  from h_ig_it_cfg_profit_center_group_join h
  join (select bk_hash, s_ig_it_cfg_profit_center_group_join_id, dv_batch_id, dv_load_date_time
          from (select bk_hash, s_ig_it_cfg_profit_center_group_join_id, dv_batch_id, dv_load_date_time, rank() over (partition by bk_hash order by dv_load_date_time desc) r from s_ig_it_cfg_profit_center_group_join where bk_hash in (select bk_hash from #process)) x
         where r = 1) s_ig_it_cfg_profit_center_group_join
    on h.bk_hash = s_ig_it_cfg_profit_center_group_join.bk_hash
 where h.bk_hash in (select bk_hash from #process)
end