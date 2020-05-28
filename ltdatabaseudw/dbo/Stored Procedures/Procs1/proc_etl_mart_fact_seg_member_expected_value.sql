CREATE PROC [dbo].[proc_etl_mart_fact_seg_member_expected_value] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mart_fact_seg_member_expected_value

set @insert_date_time = getdate()
insert into dbo.stage_hash_mart_fact_seg_member_expected_value (
       bk_hash,
       fact_seg_member_expected_value_id,
       member_id,
       expected_value_60_months,
       row_add_date,
       active_flag,
       row_deactivation_date ,
       past_spend_last_3_years ,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(fact_seg_member_expected_value_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       fact_seg_member_expected_value_id,
       member_id,
       expected_value_60_months,
       row_add_date,
       active_flag,
       row_deactivation_date ,
       past_spend_last_3_years ,
       isnull(cast(stage_mart_fact_seg_member_expected_value.row_add_date as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_mart_fact_seg_member_expected_value
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mart_fact_seg_member_expected_value @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mart_fact_seg_member_expected_value (
       bk_hash,
       fact_seg_member_expected_value_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_mart_fact_seg_member_expected_value.bk_hash,
       stage_hash_mart_fact_seg_member_expected_value.fact_seg_member_expected_value_id fact_seg_member_expected_value_id,
       isnull(cast(stage_hash_mart_fact_seg_member_expected_value.row_add_date as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       42,
       @insert_date_time,
       @user
  from stage_hash_mart_fact_seg_member_expected_value
  left join h_mart_fact_seg_member_expected_value
    on stage_hash_mart_fact_seg_member_expected_value.bk_hash = h_mart_fact_seg_member_expected_value.bk_hash
 where h_mart_fact_seg_member_expected_value_id is null
   and stage_hash_mart_fact_seg_member_expected_value.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mart_fact_seg_member_expected_value
if object_id('tempdb..#l_mart_fact_seg_member_expected_value_inserts') is not null drop table #l_mart_fact_seg_member_expected_value_inserts
create table #l_mart_fact_seg_member_expected_value_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mart_fact_seg_member_expected_value.bk_hash,
       stage_hash_mart_fact_seg_member_expected_value.fact_seg_member_expected_value_id fact_seg_member_expected_value_id,
       stage_hash_mart_fact_seg_member_expected_value.member_id member_id,
       isnull(cast(stage_hash_mart_fact_seg_member_expected_value.row_add_date as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mart_fact_seg_member_expected_value.fact_seg_member_expected_value_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mart_fact_seg_member_expected_value.member_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mart_fact_seg_member_expected_value
 where stage_hash_mart_fact_seg_member_expected_value.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mart_fact_seg_member_expected_value records
set @insert_date_time = getdate()
insert into l_mart_fact_seg_member_expected_value (
       bk_hash,
       fact_seg_member_expected_value_id,
       member_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mart_fact_seg_member_expected_value_inserts.bk_hash,
       #l_mart_fact_seg_member_expected_value_inserts.fact_seg_member_expected_value_id,
       #l_mart_fact_seg_member_expected_value_inserts.member_id,
       case when l_mart_fact_seg_member_expected_value.l_mart_fact_seg_member_expected_value_id is null then isnull(#l_mart_fact_seg_member_expected_value_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       42,
       #l_mart_fact_seg_member_expected_value_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mart_fact_seg_member_expected_value_inserts
  left join p_mart_fact_seg_member_expected_value
    on #l_mart_fact_seg_member_expected_value_inserts.bk_hash = p_mart_fact_seg_member_expected_value.bk_hash
   and p_mart_fact_seg_member_expected_value.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mart_fact_seg_member_expected_value
    on p_mart_fact_seg_member_expected_value.bk_hash = l_mart_fact_seg_member_expected_value.bk_hash
   and p_mart_fact_seg_member_expected_value.l_mart_fact_seg_member_expected_value_id = l_mart_fact_seg_member_expected_value.l_mart_fact_seg_member_expected_value_id
 where l_mart_fact_seg_member_expected_value.l_mart_fact_seg_member_expected_value_id is null
    or (l_mart_fact_seg_member_expected_value.l_mart_fact_seg_member_expected_value_id is not null
        and l_mart_fact_seg_member_expected_value.dv_hash <> #l_mart_fact_seg_member_expected_value_inserts.source_hash)

--calculate hash and lookup to current s_mart_fact_seg_member_expected_value
if object_id('tempdb..#s_mart_fact_seg_member_expected_value_inserts') is not null drop table #s_mart_fact_seg_member_expected_value_inserts
create table #s_mart_fact_seg_member_expected_value_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mart_fact_seg_member_expected_value.bk_hash,
       stage_hash_mart_fact_seg_member_expected_value.fact_seg_member_expected_value_id fact_seg_member_expected_value_id,
       stage_hash_mart_fact_seg_member_expected_value.expected_value_60_months expected_value_60_months,
       stage_hash_mart_fact_seg_member_expected_value.row_add_date row_add_date,
       stage_hash_mart_fact_seg_member_expected_value.active_flag active_flag,
       stage_hash_mart_fact_seg_member_expected_value.row_deactivation_date row_deactivation_date ,
       stage_hash_mart_fact_seg_member_expected_value.past_spend_last_3_years past_spend_last_3_years ,
       isnull(cast(stage_hash_mart_fact_seg_member_expected_value.row_add_date as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mart_fact_seg_member_expected_value.fact_seg_member_expected_value_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mart_fact_seg_member_expected_value.expected_value_60_months as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mart_fact_seg_member_expected_value.row_add_date,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mart_fact_seg_member_expected_value.active_flag as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mart_fact_seg_member_expected_value.row_deactivation_date,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mart_fact_seg_member_expected_value.past_spend_last_3_years as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mart_fact_seg_member_expected_value
 where stage_hash_mart_fact_seg_member_expected_value.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mart_fact_seg_member_expected_value records
set @insert_date_time = getdate()
insert into s_mart_fact_seg_member_expected_value (
       bk_hash,
       fact_seg_member_expected_value_id,
       expected_value_60_months,
       row_add_date,
       active_flag,
       row_deactivation_date ,
       past_spend_last_3_years ,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mart_fact_seg_member_expected_value_inserts.bk_hash,
       #s_mart_fact_seg_member_expected_value_inserts.fact_seg_member_expected_value_id,
       #s_mart_fact_seg_member_expected_value_inserts.expected_value_60_months,
       #s_mart_fact_seg_member_expected_value_inserts.row_add_date,
       #s_mart_fact_seg_member_expected_value_inserts.active_flag,
       #s_mart_fact_seg_member_expected_value_inserts.row_deactivation_date ,
       #s_mart_fact_seg_member_expected_value_inserts.past_spend_last_3_years ,
       case when s_mart_fact_seg_member_expected_value.s_mart_fact_seg_member_expected_value_id is null then isnull(#s_mart_fact_seg_member_expected_value_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       42,
       #s_mart_fact_seg_member_expected_value_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mart_fact_seg_member_expected_value_inserts
  left join p_mart_fact_seg_member_expected_value
    on #s_mart_fact_seg_member_expected_value_inserts.bk_hash = p_mart_fact_seg_member_expected_value.bk_hash
   and p_mart_fact_seg_member_expected_value.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mart_fact_seg_member_expected_value
    on p_mart_fact_seg_member_expected_value.bk_hash = s_mart_fact_seg_member_expected_value.bk_hash
   and p_mart_fact_seg_member_expected_value.s_mart_fact_seg_member_expected_value_id = s_mart_fact_seg_member_expected_value.s_mart_fact_seg_member_expected_value_id
 where s_mart_fact_seg_member_expected_value.s_mart_fact_seg_member_expected_value_id is null
    or (s_mart_fact_seg_member_expected_value.s_mart_fact_seg_member_expected_value_id is not null
        and s_mart_fact_seg_member_expected_value.dv_hash <> #s_mart_fact_seg_member_expected_value_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mart_fact_seg_member_expected_value @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mart_fact_seg_member_expected_value @current_dv_batch_id

end
