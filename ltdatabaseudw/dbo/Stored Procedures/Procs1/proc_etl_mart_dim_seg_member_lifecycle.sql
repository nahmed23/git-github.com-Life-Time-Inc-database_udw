CREATE PROC [dbo].[proc_etl_mart_dim_seg_member_lifecycle] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mart_dim_seg_member_lifecycle

set @insert_date_time = getdate()
insert into dbo.stage_hash_mart_dim_seg_member_lifecycle (
       bk_hash,
       dim_seg_member_lifecycle_id,
       lifecycle_segment,
       lifecycle,
       row_add_date,
       active_flag,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(dim_seg_member_lifecycle_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       dim_seg_member_lifecycle_id,
       lifecycle_segment,
       lifecycle,
       row_add_date,
       active_flag,
       isnull(cast(stage_mart_dim_seg_member_lifecycle.row_add_date as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_mart_dim_seg_member_lifecycle
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mart_dim_seg_member_lifecycle @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mart_dim_seg_member_lifecycle (
       bk_hash,
       dim_seg_member_lifecycle_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_mart_dim_seg_member_lifecycle.bk_hash,
       stage_hash_mart_dim_seg_member_lifecycle.dim_seg_member_lifecycle_id dim_seg_member_lifecycle_id,
       isnull(cast(stage_hash_mart_dim_seg_member_lifecycle.row_add_date as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       42,
       @insert_date_time,
       @user
  from stage_hash_mart_dim_seg_member_lifecycle
  left join h_mart_dim_seg_member_lifecycle
    on stage_hash_mart_dim_seg_member_lifecycle.bk_hash = h_mart_dim_seg_member_lifecycle.bk_hash
 where h_mart_dim_seg_member_lifecycle_id is null
   and stage_hash_mart_dim_seg_member_lifecycle.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current s_mart_dim_seg_member_lifecycle
if object_id('tempdb..#s_mart_dim_seg_member_lifecycle_inserts') is not null drop table #s_mart_dim_seg_member_lifecycle_inserts
create table #s_mart_dim_seg_member_lifecycle_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mart_dim_seg_member_lifecycle.bk_hash,
       stage_hash_mart_dim_seg_member_lifecycle.dim_seg_member_lifecycle_id dim_seg_member_lifecycle_id,
       stage_hash_mart_dim_seg_member_lifecycle.lifecycle_segment lifecycle_segment,
       stage_hash_mart_dim_seg_member_lifecycle.lifecycle lifecycle,
       stage_hash_mart_dim_seg_member_lifecycle.row_add_date row_add_date,
       stage_hash_mart_dim_seg_member_lifecycle.active_flag active_flag,
       isnull(cast(stage_hash_mart_dim_seg_member_lifecycle.row_add_date as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mart_dim_seg_member_lifecycle.dim_seg_member_lifecycle_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mart_dim_seg_member_lifecycle.lifecycle_segment as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mart_dim_seg_member_lifecycle.lifecycle,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mart_dim_seg_member_lifecycle.row_add_date,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mart_dim_seg_member_lifecycle.active_flag as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mart_dim_seg_member_lifecycle
 where stage_hash_mart_dim_seg_member_lifecycle.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mart_dim_seg_member_lifecycle records
set @insert_date_time = getdate()
insert into s_mart_dim_seg_member_lifecycle (
       bk_hash,
       dim_seg_member_lifecycle_id,
       lifecycle_segment,
       lifecycle,
       row_add_date,
       active_flag,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mart_dim_seg_member_lifecycle_inserts.bk_hash,
       #s_mart_dim_seg_member_lifecycle_inserts.dim_seg_member_lifecycle_id,
       #s_mart_dim_seg_member_lifecycle_inserts.lifecycle_segment,
       #s_mart_dim_seg_member_lifecycle_inserts.lifecycle,
       #s_mart_dim_seg_member_lifecycle_inserts.row_add_date,
       #s_mart_dim_seg_member_lifecycle_inserts.active_flag,
       case when s_mart_dim_seg_member_lifecycle.s_mart_dim_seg_member_lifecycle_id is null then isnull(#s_mart_dim_seg_member_lifecycle_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       42,
       #s_mart_dim_seg_member_lifecycle_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mart_dim_seg_member_lifecycle_inserts
  left join p_mart_dim_seg_member_lifecycle
    on #s_mart_dim_seg_member_lifecycle_inserts.bk_hash = p_mart_dim_seg_member_lifecycle.bk_hash
   and p_mart_dim_seg_member_lifecycle.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mart_dim_seg_member_lifecycle
    on p_mart_dim_seg_member_lifecycle.bk_hash = s_mart_dim_seg_member_lifecycle.bk_hash
   and p_mart_dim_seg_member_lifecycle.s_mart_dim_seg_member_lifecycle_id = s_mart_dim_seg_member_lifecycle.s_mart_dim_seg_member_lifecycle_id
 where s_mart_dim_seg_member_lifecycle.s_mart_dim_seg_member_lifecycle_id is null
    or (s_mart_dim_seg_member_lifecycle.s_mart_dim_seg_member_lifecycle_id is not null
        and s_mart_dim_seg_member_lifecycle.dv_hash <> #s_mart_dim_seg_member_lifecycle_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mart_dim_seg_member_lifecycle @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mart_dim_seg_member_lifecycle @current_dv_batch_id

end
