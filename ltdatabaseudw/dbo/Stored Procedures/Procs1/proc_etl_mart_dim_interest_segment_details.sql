CREATE PROC [dbo].[proc_etl_mart_dim_interest_segment_details] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mart_dim_interest_segment_details

set @insert_date_time = getdate()
insert into dbo.stage_hash_mart_dim_interest_segment_details (
       bk_hash,
       dim_interest_segment_details_id,
       interest_id,
       interest_name,
       row_add_date,
       active_flag,
       interest_display_name,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(interest_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       dim_interest_segment_details_id,
       interest_id,
       interest_name,
       row_add_date,
       active_flag,
       interest_display_name,
       isnull(cast(stage_mart_dim_interest_segment_details.row_add_date as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_mart_dim_interest_segment_details
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mart_dim_interest_segment_details @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mart_dim_interest_segment_details (
       bk_hash,
       interest_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_mart_dim_interest_segment_details.bk_hash,
       stage_hash_mart_dim_interest_segment_details.interest_id interest_id,
       isnull(cast(stage_hash_mart_dim_interest_segment_details.row_add_date as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       42,
       @insert_date_time,
       @user
  from stage_hash_mart_dim_interest_segment_details
  left join h_mart_dim_interest_segment_details
    on stage_hash_mart_dim_interest_segment_details.bk_hash = h_mart_dim_interest_segment_details.bk_hash
 where h_mart_dim_interest_segment_details_id is null
   and stage_hash_mart_dim_interest_segment_details.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current s_mart_dim_interest_segment_details
if object_id('tempdb..#s_mart_dim_interest_segment_details_inserts') is not null drop table #s_mart_dim_interest_segment_details_inserts
create table #s_mart_dim_interest_segment_details_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mart_dim_interest_segment_details.bk_hash,
       stage_hash_mart_dim_interest_segment_details.dim_interest_segment_details_id dim_interest_segment_details_id,
       stage_hash_mart_dim_interest_segment_details.interest_id interest_id,
       stage_hash_mart_dim_interest_segment_details.interest_name interest_name ,
       stage_hash_mart_dim_interest_segment_details.row_add_date row_add_date,
       stage_hash_mart_dim_interest_segment_details.active_flag active_flag,
       stage_hash_mart_dim_interest_segment_details.interest_display_name interest_display_name,
       isnull(cast(stage_hash_mart_dim_interest_segment_details.row_add_date as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mart_dim_interest_segment_details.dim_interest_segment_details_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mart_dim_interest_segment_details.interest_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mart_dim_interest_segment_details.interest_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mart_dim_interest_segment_details.row_add_date,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mart_dim_interest_segment_details.active_flag as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_mart_dim_interest_segment_details.interest_display_name,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mart_dim_interest_segment_details
 where stage_hash_mart_dim_interest_segment_details.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mart_dim_interest_segment_details records
set @insert_date_time = getdate()
insert into s_mart_dim_interest_segment_details (
       bk_hash,
       dim_interest_segment_details_id,
       interest_id,
       interest_name ,
       row_add_date,
       active_flag,
       interest_display_name,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mart_dim_interest_segment_details_inserts.bk_hash,
       #s_mart_dim_interest_segment_details_inserts.dim_interest_segment_details_id,
       #s_mart_dim_interest_segment_details_inserts.interest_id,
       #s_mart_dim_interest_segment_details_inserts.interest_name ,
       #s_mart_dim_interest_segment_details_inserts.row_add_date,
       #s_mart_dim_interest_segment_details_inserts.active_flag,
       #s_mart_dim_interest_segment_details_inserts.interest_display_name,
       case when s_mart_dim_interest_segment_details.s_mart_dim_interest_segment_details_id is null then isnull(#s_mart_dim_interest_segment_details_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       42,
       #s_mart_dim_interest_segment_details_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mart_dim_interest_segment_details_inserts
  left join p_mart_dim_interest_segment_details
    on #s_mart_dim_interest_segment_details_inserts.bk_hash = p_mart_dim_interest_segment_details.bk_hash
   and p_mart_dim_interest_segment_details.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mart_dim_interest_segment_details
    on p_mart_dim_interest_segment_details.bk_hash = s_mart_dim_interest_segment_details.bk_hash
   and p_mart_dim_interest_segment_details.s_mart_dim_interest_segment_details_id = s_mart_dim_interest_segment_details.s_mart_dim_interest_segment_details_id
 where s_mart_dim_interest_segment_details.s_mart_dim_interest_segment_details_id is null
    or (s_mart_dim_interest_segment_details.s_mart_dim_interest_segment_details_id is not null
        and s_mart_dim_interest_segment_details.dv_hash <> #s_mart_dim_interest_segment_details_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mart_dim_interest_segment_details @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mart_dim_interest_segment_details @current_dv_batch_id

end
