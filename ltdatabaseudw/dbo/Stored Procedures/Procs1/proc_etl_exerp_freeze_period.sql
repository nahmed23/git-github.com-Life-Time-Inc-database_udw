CREATE PROC [dbo].[proc_etl_exerp_freeze_period] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_exerp_freeze_period

set @insert_date_time = getdate()
insert into dbo.stage_hash_exerp_freeze_period (
       bk_hash,
       id,
       subscription_id,
       subscription_center_id,
       start_date,
       end_date,
       state,
       type,
       reason,
       entry_datetime,
       cancel_datetime,
       center_id,
       ets,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(convert(varchar,start_date,120),'z#@$k%&P'))),2) bk_hash,
       id,
       subscription_id,
       subscription_center_id,
       start_date,
       end_date,
       state,
       type,
       reason,
       entry_datetime,
       cancel_datetime,
       center_id,
       ets,
       dummy_modified_date_time,
       isnull(cast(stage_exerp_freeze_period.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_exerp_freeze_period
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_exerp_freeze_period @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_exerp_freeze_period (
       bk_hash,
       freeze_period_id,
       start_date,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_exerp_freeze_period.bk_hash,
       stage_hash_exerp_freeze_period.id freeze_period_id,
       stage_hash_exerp_freeze_period.start_date start_date,
       isnull(cast(stage_hash_exerp_freeze_period.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       33,
       @insert_date_time,
       @user
  from stage_hash_exerp_freeze_period
  left join h_exerp_freeze_period
    on stage_hash_exerp_freeze_period.bk_hash = h_exerp_freeze_period.bk_hash
 where h_exerp_freeze_period_id is null
   and stage_hash_exerp_freeze_period.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_exerp_freeze_period
if object_id('tempdb..#l_exerp_freeze_period_inserts') is not null drop table #l_exerp_freeze_period_inserts
create table #l_exerp_freeze_period_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_freeze_period.bk_hash,
       stage_hash_exerp_freeze_period.id freeze_period_id,
       stage_hash_exerp_freeze_period.subscription_id subscription_id,
       stage_hash_exerp_freeze_period.subscription_center_id subscription_center_id,
       stage_hash_exerp_freeze_period.start_date start_date,
       stage_hash_exerp_freeze_period.center_id center_id,
       isnull(cast(stage_hash_exerp_freeze_period.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_exerp_freeze_period.id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_freeze_period.subscription_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_freeze_period.subscription_center_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exerp_freeze_period.start_date,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_freeze_period.center_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_freeze_period
 where stage_hash_exerp_freeze_period.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_exerp_freeze_period records
set @insert_date_time = getdate()
insert into l_exerp_freeze_period (
       bk_hash,
       freeze_period_id,
       subscription_id,
       subscription_center_id,
       start_date,
       center_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_exerp_freeze_period_inserts.bk_hash,
       #l_exerp_freeze_period_inserts.freeze_period_id,
       #l_exerp_freeze_period_inserts.subscription_id,
       #l_exerp_freeze_period_inserts.subscription_center_id,
       #l_exerp_freeze_period_inserts.start_date,
       #l_exerp_freeze_period_inserts.center_id,
       case when l_exerp_freeze_period.l_exerp_freeze_period_id is null then isnull(#l_exerp_freeze_period_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #l_exerp_freeze_period_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_exerp_freeze_period_inserts
  left join p_exerp_freeze_period
    on #l_exerp_freeze_period_inserts.bk_hash = p_exerp_freeze_period.bk_hash
   and p_exerp_freeze_period.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_exerp_freeze_period
    on p_exerp_freeze_period.bk_hash = l_exerp_freeze_period.bk_hash
   and p_exerp_freeze_period.l_exerp_freeze_period_id = l_exerp_freeze_period.l_exerp_freeze_period_id
 where l_exerp_freeze_period.l_exerp_freeze_period_id is null
    or (l_exerp_freeze_period.l_exerp_freeze_period_id is not null
        and l_exerp_freeze_period.dv_hash <> #l_exerp_freeze_period_inserts.source_hash)

--calculate hash and lookup to current s_exerp_freeze_period
if object_id('tempdb..#s_exerp_freeze_period_inserts') is not null drop table #s_exerp_freeze_period_inserts
create table #s_exerp_freeze_period_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_freeze_period.bk_hash,
       stage_hash_exerp_freeze_period.id freeze_period_id,
       stage_hash_exerp_freeze_period.start_date start_date,
       stage_hash_exerp_freeze_period.end_date end_date,
       stage_hash_exerp_freeze_period.state state,
       stage_hash_exerp_freeze_period.type type,
       stage_hash_exerp_freeze_period.reason reason,
       stage_hash_exerp_freeze_period.entry_datetime entry_datetime,
       stage_hash_exerp_freeze_period.cancel_datetime cancel_datetime,
       stage_hash_exerp_freeze_period.ets ets,
       stage_hash_exerp_freeze_period.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_exerp_freeze_period.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_exerp_freeze_period.id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exerp_freeze_period.start_date,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exerp_freeze_period.end_date,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_freeze_period.state,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_freeze_period.type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_freeze_period.reason,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exerp_freeze_period.entry_datetime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exerp_freeze_period.cancel_datetime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_freeze_period.ets as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_freeze_period
 where stage_hash_exerp_freeze_period.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_exerp_freeze_period records
set @insert_date_time = getdate()
insert into s_exerp_freeze_period (
       bk_hash,
       freeze_period_id,
       start_date,
       end_date,
       state,
       type,
       reason,
       entry_datetime,
       cancel_datetime,
       ets,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_exerp_freeze_period_inserts.bk_hash,
       #s_exerp_freeze_period_inserts.freeze_period_id,
       #s_exerp_freeze_period_inserts.start_date,
       #s_exerp_freeze_period_inserts.end_date,
       #s_exerp_freeze_period_inserts.state,
       #s_exerp_freeze_period_inserts.type,
       #s_exerp_freeze_period_inserts.reason,
       #s_exerp_freeze_period_inserts.entry_datetime,
       #s_exerp_freeze_period_inserts.cancel_datetime,
       #s_exerp_freeze_period_inserts.ets,
       #s_exerp_freeze_period_inserts.dummy_modified_date_time,
       case when s_exerp_freeze_period.s_exerp_freeze_period_id is null then isnull(#s_exerp_freeze_period_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #s_exerp_freeze_period_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_exerp_freeze_period_inserts
  left join p_exerp_freeze_period
    on #s_exerp_freeze_period_inserts.bk_hash = p_exerp_freeze_period.bk_hash
   and p_exerp_freeze_period.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_exerp_freeze_period
    on p_exerp_freeze_period.bk_hash = s_exerp_freeze_period.bk_hash
   and p_exerp_freeze_period.s_exerp_freeze_period_id = s_exerp_freeze_period.s_exerp_freeze_period_id
 where s_exerp_freeze_period.s_exerp_freeze_period_id is null
    or (s_exerp_freeze_period.s_exerp_freeze_period_id is not null
        and s_exerp_freeze_period.dv_hash <> #s_exerp_freeze_period_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_exerp_freeze_period @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_exerp_freeze_period @current_dv_batch_id

end
