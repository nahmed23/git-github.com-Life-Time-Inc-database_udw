﻿CREATE PROC [dbo].[proc_etl_mart_fact_member_interests] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mart_fact_member_interests

set @insert_date_time = getdate()
insert into dbo.stage_hash_mart_fact_member_interests (
       bk_hash,
       fact_member_interests_id,
       member_id,
       interest_id,
       interest_confidence,
       row_add_date,
       active_flag,
       row_deactivation_date,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(fact_member_interests_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       fact_member_interests_id,
       member_id,
       interest_id,
       interest_confidence,
       row_add_date,
       active_flag,
       row_deactivation_date,
       isnull(cast(stage_mart_fact_member_interests.row_add_date as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_mart_fact_member_interests
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mart_fact_member_interests @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mart_fact_member_interests (
       bk_hash,
       fact_member_interests_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_mart_fact_member_interests.bk_hash,
       stage_hash_mart_fact_member_interests.fact_member_interests_id fact_member_interests_id,
       isnull(cast(stage_hash_mart_fact_member_interests.row_add_date as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       42,
       @insert_date_time,
       @user
  from stage_hash_mart_fact_member_interests
  left join h_mart_fact_member_interests
    on stage_hash_mart_fact_member_interests.bk_hash = h_mart_fact_member_interests.bk_hash
 where h_mart_fact_member_interests_id is null
   and stage_hash_mart_fact_member_interests.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mart_fact_member_interests
if object_id('tempdb..#l_mart_fact_member_interests_inserts') is not null drop table #l_mart_fact_member_interests_inserts
create table #l_mart_fact_member_interests_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mart_fact_member_interests.bk_hash,
       stage_hash_mart_fact_member_interests.fact_member_interests_id fact_member_interests_id,
       stage_hash_mart_fact_member_interests.member_id member_id,
       stage_hash_mart_fact_member_interests.interest_id interest_id,
       isnull(cast(stage_hash_mart_fact_member_interests.row_add_date as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mart_fact_member_interests.fact_member_interests_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mart_fact_member_interests.member_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mart_fact_member_interests.interest_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mart_fact_member_interests
 where stage_hash_mart_fact_member_interests.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mart_fact_member_interests records
set @insert_date_time = getdate()
insert into l_mart_fact_member_interests (
       bk_hash,
       fact_member_interests_id,
       member_id,
       interest_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mart_fact_member_interests_inserts.bk_hash,
       #l_mart_fact_member_interests_inserts.fact_member_interests_id,
       #l_mart_fact_member_interests_inserts.member_id,
       #l_mart_fact_member_interests_inserts.interest_id,
       case when l_mart_fact_member_interests.l_mart_fact_member_interests_id is null then isnull(#l_mart_fact_member_interests_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       42,
       #l_mart_fact_member_interests_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mart_fact_member_interests_inserts
  left join p_mart_fact_member_interests
    on #l_mart_fact_member_interests_inserts.bk_hash = p_mart_fact_member_interests.bk_hash
   and p_mart_fact_member_interests.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mart_fact_member_interests
    on p_mart_fact_member_interests.bk_hash = l_mart_fact_member_interests.bk_hash
   and p_mart_fact_member_interests.l_mart_fact_member_interests_id = l_mart_fact_member_interests.l_mart_fact_member_interests_id
 where l_mart_fact_member_interests.l_mart_fact_member_interests_id is null
    or (l_mart_fact_member_interests.l_mart_fact_member_interests_id is not null
        and l_mart_fact_member_interests.dv_hash <> #l_mart_fact_member_interests_inserts.source_hash)

--calculate hash and lookup to current s_mart_fact_member_interests
if object_id('tempdb..#s_mart_fact_member_interests_inserts') is not null drop table #s_mart_fact_member_interests_inserts
create table #s_mart_fact_member_interests_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mart_fact_member_interests.bk_hash,
       stage_hash_mart_fact_member_interests.fact_member_interests_id fact_member_interests_id,
       stage_hash_mart_fact_member_interests.interest_confidence interest_confidence ,
       stage_hash_mart_fact_member_interests.row_add_date row_add_date,
       stage_hash_mart_fact_member_interests.active_flag active_flag,
       stage_hash_mart_fact_member_interests.row_deactivation_date row_deactivation_date,
       isnull(cast(stage_hash_mart_fact_member_interests.row_add_date as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mart_fact_member_interests.fact_member_interests_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mart_fact_member_interests.interest_confidence as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mart_fact_member_interests.row_add_date,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mart_fact_member_interests.active_flag as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mart_fact_member_interests.row_deactivation_date,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mart_fact_member_interests
 where stage_hash_mart_fact_member_interests.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mart_fact_member_interests records
set @insert_date_time = getdate()
insert into s_mart_fact_member_interests (
       bk_hash,
       fact_member_interests_id,
       interest_confidence ,
       row_add_date,
       active_flag,
       row_deactivation_date,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mart_fact_member_interests_inserts.bk_hash,
       #s_mart_fact_member_interests_inserts.fact_member_interests_id,
       #s_mart_fact_member_interests_inserts.interest_confidence ,
       #s_mart_fact_member_interests_inserts.row_add_date,
       #s_mart_fact_member_interests_inserts.active_flag,
       #s_mart_fact_member_interests_inserts.row_deactivation_date,
       case when s_mart_fact_member_interests.s_mart_fact_member_interests_id is null then isnull(#s_mart_fact_member_interests_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       42,
       #s_mart_fact_member_interests_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mart_fact_member_interests_inserts
  left join p_mart_fact_member_interests
    on #s_mart_fact_member_interests_inserts.bk_hash = p_mart_fact_member_interests.bk_hash
   and p_mart_fact_member_interests.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mart_fact_member_interests
    on p_mart_fact_member_interests.bk_hash = s_mart_fact_member_interests.bk_hash
   and p_mart_fact_member_interests.s_mart_fact_member_interests_id = s_mart_fact_member_interests.s_mart_fact_member_interests_id
 where s_mart_fact_member_interests.s_mart_fact_member_interests_id is null
    or (s_mart_fact_member_interests.s_mart_fact_member_interests_id is not null
        and s_mart_fact_member_interests.dv_hash <> #s_mart_fact_member_interests_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mart_fact_member_interests @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mart_fact_member_interests @current_dv_batch_id

end
