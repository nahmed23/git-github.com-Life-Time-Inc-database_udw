﻿CREATE PROC [dbo].[proc_etl_exerp_daily_member_state] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_exerp_daily_member_state

set @insert_date_time = getdate()
insert into dbo.stage_hash_exerp_daily_member_state (
       bk_hash,
       id,
       person_id,
       center_id,
       home_center_person_id,
       date,
       entry_datetime,
       change,
       member_number_delta,
       extra_number_delta,
       secondary_member_number_delta,
       cancel_datetime,
       ets,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       id,
       person_id,
       center_id,
       home_center_person_id,
       date,
       entry_datetime,
       change,
       member_number_delta,
       extra_number_delta,
       secondary_member_number_delta,
       cancel_datetime,
       ets,
       dummy_modified_date_time,
       isnull(cast(stage_exerp_daily_member_state.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_exerp_daily_member_state
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_exerp_daily_member_state @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_exerp_daily_member_state (
       bk_hash,
       daily_member_state_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_exerp_daily_member_state.bk_hash,
       stage_hash_exerp_daily_member_state.id daily_member_state_id,
       isnull(cast(stage_hash_exerp_daily_member_state.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       33,
       @insert_date_time,
       @user
  from stage_hash_exerp_daily_member_state
  left join h_exerp_daily_member_state
    on stage_hash_exerp_daily_member_state.bk_hash = h_exerp_daily_member_state.bk_hash
 where h_exerp_daily_member_state_id is null
   and stage_hash_exerp_daily_member_state.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_exerp_daily_member_state
if object_id('tempdb..#l_exerp_daily_member_state_inserts') is not null drop table #l_exerp_daily_member_state_inserts
create table #l_exerp_daily_member_state_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_daily_member_state.bk_hash,
       stage_hash_exerp_daily_member_state.id daily_member_state_id,
       stage_hash_exerp_daily_member_state.person_id person_id,
       stage_hash_exerp_daily_member_state.center_id center_id,
       stage_hash_exerp_daily_member_state.home_center_person_id home_center_person_id,
       isnull(cast(stage_hash_exerp_daily_member_state.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_exerp_daily_member_state.id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_daily_member_state.person_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_daily_member_state.center_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_daily_member_state.home_center_person_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_daily_member_state
 where stage_hash_exerp_daily_member_state.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_exerp_daily_member_state records
set @insert_date_time = getdate()
insert into l_exerp_daily_member_state (
       bk_hash,
       daily_member_state_id,
       person_id,
       center_id,
       home_center_person_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_exerp_daily_member_state_inserts.bk_hash,
       #l_exerp_daily_member_state_inserts.daily_member_state_id,
       #l_exerp_daily_member_state_inserts.person_id,
       #l_exerp_daily_member_state_inserts.center_id,
       #l_exerp_daily_member_state_inserts.home_center_person_id,
       case when l_exerp_daily_member_state.l_exerp_daily_member_state_id is null then isnull(#l_exerp_daily_member_state_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #l_exerp_daily_member_state_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_exerp_daily_member_state_inserts
  left join p_exerp_daily_member_state
    on #l_exerp_daily_member_state_inserts.bk_hash = p_exerp_daily_member_state.bk_hash
   and p_exerp_daily_member_state.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_exerp_daily_member_state
    on p_exerp_daily_member_state.bk_hash = l_exerp_daily_member_state.bk_hash
   and p_exerp_daily_member_state.l_exerp_daily_member_state_id = l_exerp_daily_member_state.l_exerp_daily_member_state_id
 where l_exerp_daily_member_state.l_exerp_daily_member_state_id is null
    or (l_exerp_daily_member_state.l_exerp_daily_member_state_id is not null
        and l_exerp_daily_member_state.dv_hash <> #l_exerp_daily_member_state_inserts.source_hash)

--calculate hash and lookup to current s_exerp_daily_member_state
if object_id('tempdb..#s_exerp_daily_member_state_inserts') is not null drop table #s_exerp_daily_member_state_inserts
create table #s_exerp_daily_member_state_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_daily_member_state.bk_hash,
       stage_hash_exerp_daily_member_state.id daily_member_state_id,
       stage_hash_exerp_daily_member_state.date date,
       stage_hash_exerp_daily_member_state.entry_datetime entry_datetime,
       stage_hash_exerp_daily_member_state.change change,
       stage_hash_exerp_daily_member_state.member_number_delta member_number_delta,
       stage_hash_exerp_daily_member_state.extra_number_delta extra_number_delta,
       stage_hash_exerp_daily_member_state.secondary_member_number_delta secondary_member_number_delta,
       stage_hash_exerp_daily_member_state.cancel_datetime cancel_datetime,
       stage_hash_exerp_daily_member_state.ets ets,
       stage_hash_exerp_daily_member_state.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_exerp_daily_member_state.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_exerp_daily_member_state.id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exerp_daily_member_state.date,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exerp_daily_member_state.entry_datetime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_daily_member_state.change,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_daily_member_state.member_number_delta as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_daily_member_state.extra_number_delta as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_daily_member_state.secondary_member_number_delta as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exerp_daily_member_state.cancel_datetime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_daily_member_state.ets as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_daily_member_state
 where stage_hash_exerp_daily_member_state.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_exerp_daily_member_state records
set @insert_date_time = getdate()
insert into s_exerp_daily_member_state (
       bk_hash,
       daily_member_state_id,
       date,
       entry_datetime,
       change,
       member_number_delta,
       extra_number_delta,
       secondary_member_number_delta,
       cancel_datetime,
       ets,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_exerp_daily_member_state_inserts.bk_hash,
       #s_exerp_daily_member_state_inserts.daily_member_state_id,
       #s_exerp_daily_member_state_inserts.date,
       #s_exerp_daily_member_state_inserts.entry_datetime,
       #s_exerp_daily_member_state_inserts.change,
       #s_exerp_daily_member_state_inserts.member_number_delta,
       #s_exerp_daily_member_state_inserts.extra_number_delta,
       #s_exerp_daily_member_state_inserts.secondary_member_number_delta,
       #s_exerp_daily_member_state_inserts.cancel_datetime,
       #s_exerp_daily_member_state_inserts.ets,
       #s_exerp_daily_member_state_inserts.dummy_modified_date_time,
       case when s_exerp_daily_member_state.s_exerp_daily_member_state_id is null then isnull(#s_exerp_daily_member_state_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #s_exerp_daily_member_state_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_exerp_daily_member_state_inserts
  left join p_exerp_daily_member_state
    on #s_exerp_daily_member_state_inserts.bk_hash = p_exerp_daily_member_state.bk_hash
   and p_exerp_daily_member_state.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_exerp_daily_member_state
    on p_exerp_daily_member_state.bk_hash = s_exerp_daily_member_state.bk_hash
   and p_exerp_daily_member_state.s_exerp_daily_member_state_id = s_exerp_daily_member_state.s_exerp_daily_member_state_id
 where s_exerp_daily_member_state.s_exerp_daily_member_state_id is null
    or (s_exerp_daily_member_state.s_exerp_daily_member_state_id is not null
        and s_exerp_daily_member_state.dv_hash <> #s_exerp_daily_member_state_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_exerp_daily_member_state @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_exerp_daily_member_state @current_dv_batch_id

end