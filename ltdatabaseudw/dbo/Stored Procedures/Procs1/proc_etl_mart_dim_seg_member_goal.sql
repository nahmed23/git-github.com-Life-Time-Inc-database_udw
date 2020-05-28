CREATE PROC [dbo].[proc_etl_mart_dim_seg_member_goal] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mart_dim_seg_member_goal

set @insert_date_time = getdate()
insert into dbo.stage_hash_mart_dim_seg_member_goal (
       bk_hash,
       dim_seg_member_goal_id,
       goal_segment,
       goal,
       row_add_date,
       active_flag,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(dim_seg_member_goal_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       dim_seg_member_goal_id,
       goal_segment,
       goal,
       row_add_date,
       active_flag,
       isnull(cast(stage_mart_dim_seg_member_goal.row_add_date as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_mart_dim_seg_member_goal
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mart_dim_seg_member_goal @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mart_dim_seg_member_goal (
       bk_hash,
       dim_seg_member_goal_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_mart_dim_seg_member_goal.bk_hash,
       stage_hash_mart_dim_seg_member_goal.dim_seg_member_goal_id dim_seg_member_goal_id,
       isnull(cast(stage_hash_mart_dim_seg_member_goal.row_add_date as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       42,
       @insert_date_time,
       @user
  from stage_hash_mart_dim_seg_member_goal
  left join h_mart_dim_seg_member_goal
    on stage_hash_mart_dim_seg_member_goal.bk_hash = h_mart_dim_seg_member_goal.bk_hash
 where h_mart_dim_seg_member_goal_id is null
   and stage_hash_mart_dim_seg_member_goal.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current s_mart_dim_seg_member_goal
if object_id('tempdb..#s_mart_dim_seg_member_goal_inserts') is not null drop table #s_mart_dim_seg_member_goal_inserts
create table #s_mart_dim_seg_member_goal_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mart_dim_seg_member_goal.bk_hash,
       stage_hash_mart_dim_seg_member_goal.dim_seg_member_goal_id dim_seg_member_goal_id,
       stage_hash_mart_dim_seg_member_goal.goal_segment goal_segment,
       stage_hash_mart_dim_seg_member_goal.goal goal,
       stage_hash_mart_dim_seg_member_goal.row_add_date row_add_date,
       stage_hash_mart_dim_seg_member_goal.active_flag active_flag,
       isnull(cast(stage_hash_mart_dim_seg_member_goal.row_add_date as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mart_dim_seg_member_goal.dim_seg_member_goal_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mart_dim_seg_member_goal.goal_segment as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mart_dim_seg_member_goal.goal,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mart_dim_seg_member_goal.row_add_date,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mart_dim_seg_member_goal.active_flag as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mart_dim_seg_member_goal
 where stage_hash_mart_dim_seg_member_goal.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mart_dim_seg_member_goal records
set @insert_date_time = getdate()
insert into s_mart_dim_seg_member_goal (
       bk_hash,
       dim_seg_member_goal_id,
       goal_segment,
       goal,
       row_add_date,
       active_flag,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mart_dim_seg_member_goal_inserts.bk_hash,
       #s_mart_dim_seg_member_goal_inserts.dim_seg_member_goal_id,
       #s_mart_dim_seg_member_goal_inserts.goal_segment,
       #s_mart_dim_seg_member_goal_inserts.goal,
       #s_mart_dim_seg_member_goal_inserts.row_add_date,
       #s_mart_dim_seg_member_goal_inserts.active_flag,
       case when s_mart_dim_seg_member_goal.s_mart_dim_seg_member_goal_id is null then isnull(#s_mart_dim_seg_member_goal_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       42,
       #s_mart_dim_seg_member_goal_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mart_dim_seg_member_goal_inserts
  left join p_mart_dim_seg_member_goal
    on #s_mart_dim_seg_member_goal_inserts.bk_hash = p_mart_dim_seg_member_goal.bk_hash
   and p_mart_dim_seg_member_goal.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mart_dim_seg_member_goal
    on p_mart_dim_seg_member_goal.bk_hash = s_mart_dim_seg_member_goal.bk_hash
   and p_mart_dim_seg_member_goal.s_mart_dim_seg_member_goal_id = s_mart_dim_seg_member_goal.s_mart_dim_seg_member_goal_id
 where s_mart_dim_seg_member_goal.s_mart_dim_seg_member_goal_id is null
    or (s_mart_dim_seg_member_goal.s_mart_dim_seg_member_goal_id is not null
        and s_mart_dim_seg_member_goal.dv_hash <> #s_mart_dim_seg_member_goal_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mart_dim_seg_member_goal @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mart_dim_seg_member_goal @current_dv_batch_id

end
