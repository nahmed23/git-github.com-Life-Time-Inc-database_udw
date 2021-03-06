﻿CREATE PROC [dbo].[proc_etl_exerp_clipcard] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_exerp_clipcard

set @insert_date_time = getdate()
insert into dbo.stage_hash_exerp_clipcard (
       bk_hash,
       id,
       person_id,
       company_id,
       clips_left,
       clips_initial,
       sale_log_id,
       valid_from_datetime,
       valid_until_datetime,
       blocked,
       cancelled,
       cancel_datetime,
       assigned_person_id,
       center_id,
       ets,
       comment,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(id,'z#@$k%&P'))),2) bk_hash,
       id,
       person_id,
       company_id,
       clips_left,
       clips_initial,
       sale_log_id,
       valid_from_datetime,
       valid_until_datetime,
       blocked,
       cancelled,
       cancel_datetime,
       assigned_person_id,
       center_id,
       ets,
       comment,
       dummy_modified_date_time,
       isnull(cast(stage_exerp_clipcard.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_exerp_clipcard
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_exerp_clipcard @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_exerp_clipcard (
       bk_hash,
       clipcard_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_exerp_clipcard.bk_hash,
       stage_hash_exerp_clipcard.id clipcard_id,
       isnull(cast(stage_hash_exerp_clipcard.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       33,
       @insert_date_time,
       @user
  from stage_hash_exerp_clipcard
  left join h_exerp_clipcard
    on stage_hash_exerp_clipcard.bk_hash = h_exerp_clipcard.bk_hash
 where h_exerp_clipcard_id is null
   and stage_hash_exerp_clipcard.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_exerp_clipcard
if object_id('tempdb..#l_exerp_clipcard_inserts') is not null drop table #l_exerp_clipcard_inserts
create table #l_exerp_clipcard_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_clipcard.bk_hash,
       stage_hash_exerp_clipcard.id clipcard_id,
       stage_hash_exerp_clipcard.person_id person_id,
       stage_hash_exerp_clipcard.company_id company_id,
       stage_hash_exerp_clipcard.sale_log_id sale_log_id,
       stage_hash_exerp_clipcard.assigned_person_id assigned_person_id,
       stage_hash_exerp_clipcard.center_id center_id,
       isnull(cast(stage_hash_exerp_clipcard.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_exerp_clipcard.id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_clipcard.person_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_clipcard.company_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_clipcard.sale_log_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_clipcard.assigned_person_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_clipcard.center_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_clipcard
 where stage_hash_exerp_clipcard.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_exerp_clipcard records
set @insert_date_time = getdate()
insert into l_exerp_clipcard (
       bk_hash,
       clipcard_id,
       person_id,
       company_id,
       sale_log_id,
       assigned_person_id,
       center_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_exerp_clipcard_inserts.bk_hash,
       #l_exerp_clipcard_inserts.clipcard_id,
       #l_exerp_clipcard_inserts.person_id,
       #l_exerp_clipcard_inserts.company_id,
       #l_exerp_clipcard_inserts.sale_log_id,
       #l_exerp_clipcard_inserts.assigned_person_id,
       #l_exerp_clipcard_inserts.center_id,
       case when l_exerp_clipcard.l_exerp_clipcard_id is null then isnull(#l_exerp_clipcard_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #l_exerp_clipcard_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_exerp_clipcard_inserts
  left join p_exerp_clipcard
    on #l_exerp_clipcard_inserts.bk_hash = p_exerp_clipcard.bk_hash
   and p_exerp_clipcard.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_exerp_clipcard
    on p_exerp_clipcard.bk_hash = l_exerp_clipcard.bk_hash
   and p_exerp_clipcard.l_exerp_clipcard_id = l_exerp_clipcard.l_exerp_clipcard_id
 where l_exerp_clipcard.l_exerp_clipcard_id is null
    or (l_exerp_clipcard.l_exerp_clipcard_id is not null
        and l_exerp_clipcard.dv_hash <> #l_exerp_clipcard_inserts.source_hash)

--calculate hash and lookup to current l_exerp_clipcard_1
if object_id('tempdb..#l_exerp_clipcard_1_inserts') is not null drop table #l_exerp_clipcard_1_inserts
create table #l_exerp_clipcard_1_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_clipcard.bk_hash,
       stage_hash_exerp_clipcard.id clipcard_id,
       stage_hash_exerp_clipcard.comment comment,
       isnull(cast(stage_hash_exerp_clipcard.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_exerp_clipcard.id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_clipcard.comment,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_clipcard
 where stage_hash_exerp_clipcard.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_exerp_clipcard_1 records
set @insert_date_time = getdate()
insert into l_exerp_clipcard_1 (
       bk_hash,
       clipcard_id,
       comment,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_exerp_clipcard_1_inserts.bk_hash,
       #l_exerp_clipcard_1_inserts.clipcard_id,
       #l_exerp_clipcard_1_inserts.comment,
       case when l_exerp_clipcard_1.l_exerp_clipcard_1_id is null then isnull(#l_exerp_clipcard_1_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #l_exerp_clipcard_1_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_exerp_clipcard_1_inserts
  left join p_exerp_clipcard
    on #l_exerp_clipcard_1_inserts.bk_hash = p_exerp_clipcard.bk_hash
   and p_exerp_clipcard.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_exerp_clipcard_1
    on p_exerp_clipcard.bk_hash = l_exerp_clipcard_1.bk_hash
   and p_exerp_clipcard.l_exerp_clipcard_1_id = l_exerp_clipcard_1.l_exerp_clipcard_1_id
 where l_exerp_clipcard_1.l_exerp_clipcard_1_id is null
    or (l_exerp_clipcard_1.l_exerp_clipcard_1_id is not null
        and l_exerp_clipcard_1.dv_hash <> #l_exerp_clipcard_1_inserts.source_hash)

--calculate hash and lookup to current s_exerp_clipcard
if object_id('tempdb..#s_exerp_clipcard_inserts') is not null drop table #s_exerp_clipcard_inserts
create table #s_exerp_clipcard_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_clipcard.bk_hash,
       stage_hash_exerp_clipcard.id clipcard_id,
       stage_hash_exerp_clipcard.clips_left clips_left,
       stage_hash_exerp_clipcard.clips_initial clips_initial,
       stage_hash_exerp_clipcard.valid_from_datetime valid_from_datetime,
       stage_hash_exerp_clipcard.valid_until_datetime valid_until_datetime,
       stage_hash_exerp_clipcard.blocked blocked,
       stage_hash_exerp_clipcard.cancelled cancelled,
       stage_hash_exerp_clipcard.cancel_datetime cancel_datetime,
       stage_hash_exerp_clipcard.ets ets,
       stage_hash_exerp_clipcard.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_exerp_clipcard.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_exerp_clipcard.id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_clipcard.clips_left as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_clipcard.clips_initial as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exerp_clipcard.valid_from_datetime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exerp_clipcard.valid_until_datetime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_clipcard.blocked as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_clipcard.cancelled as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exerp_clipcard.cancel_datetime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_clipcard.ets as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_clipcard
 where stage_hash_exerp_clipcard.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_exerp_clipcard records
set @insert_date_time = getdate()
insert into s_exerp_clipcard (
       bk_hash,
       clipcard_id,
       clips_left,
       clips_initial,
       valid_from_datetime,
       valid_until_datetime,
       blocked,
       cancelled,
       cancel_datetime,
       ets,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_exerp_clipcard_inserts.bk_hash,
       #s_exerp_clipcard_inserts.clipcard_id,
       #s_exerp_clipcard_inserts.clips_left,
       #s_exerp_clipcard_inserts.clips_initial,
       #s_exerp_clipcard_inserts.valid_from_datetime,
       #s_exerp_clipcard_inserts.valid_until_datetime,
       #s_exerp_clipcard_inserts.blocked,
       #s_exerp_clipcard_inserts.cancelled,
       #s_exerp_clipcard_inserts.cancel_datetime,
       #s_exerp_clipcard_inserts.ets,
       #s_exerp_clipcard_inserts.dummy_modified_date_time,
       case when s_exerp_clipcard.s_exerp_clipcard_id is null then isnull(#s_exerp_clipcard_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #s_exerp_clipcard_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_exerp_clipcard_inserts
  left join p_exerp_clipcard
    on #s_exerp_clipcard_inserts.bk_hash = p_exerp_clipcard.bk_hash
   and p_exerp_clipcard.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_exerp_clipcard
    on p_exerp_clipcard.bk_hash = s_exerp_clipcard.bk_hash
   and p_exerp_clipcard.s_exerp_clipcard_id = s_exerp_clipcard.s_exerp_clipcard_id
 where s_exerp_clipcard.s_exerp_clipcard_id is null
    or (s_exerp_clipcard.s_exerp_clipcard_id is not null
        and s_exerp_clipcard.dv_hash <> #s_exerp_clipcard_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_exerp_clipcard @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_exerp_clipcard @current_dv_batch_id

end
