﻿CREATE PROC [dbo].[proc_etl_exerp_privilege_grant] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_exerp_privilege_grant

set @insert_date_time = getdate()
insert into dbo.stage_hash_exerp_privilege_grant (
       bk_hash,
       id,
       source_type,
       source_id,
       privilege_set_id,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       id,
       source_type,
       source_id,
       privilege_set_id,
       dummy_modified_date_time,
       isnull(cast(stage_exerp_privilege_grant.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_exerp_privilege_grant
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_exerp_privilege_grant @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_exerp_privilege_grant (
       bk_hash,
       privilege_grant_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_exerp_privilege_grant.bk_hash,
       stage_hash_exerp_privilege_grant.id privilege_grant_id,
       isnull(cast(stage_hash_exerp_privilege_grant.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       33,
       @insert_date_time,
       @user
  from stage_hash_exerp_privilege_grant
  left join h_exerp_privilege_grant
    on stage_hash_exerp_privilege_grant.bk_hash = h_exerp_privilege_grant.bk_hash
 where h_exerp_privilege_grant_id is null
   and stage_hash_exerp_privilege_grant.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_exerp_privilege_grant
if object_id('tempdb..#l_exerp_privilege_grant_inserts') is not null drop table #l_exerp_privilege_grant_inserts
create table #l_exerp_privilege_grant_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_privilege_grant.bk_hash,
       stage_hash_exerp_privilege_grant.id privilege_grant_id,
       stage_hash_exerp_privilege_grant.source_id source_id,
       stage_hash_exerp_privilege_grant.privilege_set_id privilege_set_id,
       isnull(cast(stage_hash_exerp_privilege_grant.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_exerp_privilege_grant.id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_privilege_grant.source_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_privilege_grant.privilege_set_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_privilege_grant
 where stage_hash_exerp_privilege_grant.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_exerp_privilege_grant records
set @insert_date_time = getdate()
insert into l_exerp_privilege_grant (
       bk_hash,
       privilege_grant_id,
       source_id,
       privilege_set_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_exerp_privilege_grant_inserts.bk_hash,
       #l_exerp_privilege_grant_inserts.privilege_grant_id,
       #l_exerp_privilege_grant_inserts.source_id,
       #l_exerp_privilege_grant_inserts.privilege_set_id,
       case when l_exerp_privilege_grant.l_exerp_privilege_grant_id is null then isnull(#l_exerp_privilege_grant_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #l_exerp_privilege_grant_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_exerp_privilege_grant_inserts
  left join p_exerp_privilege_grant
    on #l_exerp_privilege_grant_inserts.bk_hash = p_exerp_privilege_grant.bk_hash
   and p_exerp_privilege_grant.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_exerp_privilege_grant
    on p_exerp_privilege_grant.bk_hash = l_exerp_privilege_grant.bk_hash
   and p_exerp_privilege_grant.l_exerp_privilege_grant_id = l_exerp_privilege_grant.l_exerp_privilege_grant_id
 where l_exerp_privilege_grant.l_exerp_privilege_grant_id is null
    or (l_exerp_privilege_grant.l_exerp_privilege_grant_id is not null
        and l_exerp_privilege_grant.dv_hash <> #l_exerp_privilege_grant_inserts.source_hash)

--calculate hash and lookup to current s_exerp_privilege_grant
if object_id('tempdb..#s_exerp_privilege_grant_inserts') is not null drop table #s_exerp_privilege_grant_inserts
create table #s_exerp_privilege_grant_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_privilege_grant.bk_hash,
       stage_hash_exerp_privilege_grant.id privilege_grant_id,
       stage_hash_exerp_privilege_grant.source_type source_type,
       stage_hash_exerp_privilege_grant.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_exerp_privilege_grant.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_exerp_privilege_grant.id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_privilege_grant.source_type,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_privilege_grant
 where stage_hash_exerp_privilege_grant.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_exerp_privilege_grant records
set @insert_date_time = getdate()
insert into s_exerp_privilege_grant (
       bk_hash,
       privilege_grant_id,
       source_type,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_exerp_privilege_grant_inserts.bk_hash,
       #s_exerp_privilege_grant_inserts.privilege_grant_id,
       #s_exerp_privilege_grant_inserts.source_type,
       #s_exerp_privilege_grant_inserts.dummy_modified_date_time,
       case when s_exerp_privilege_grant.s_exerp_privilege_grant_id is null then isnull(#s_exerp_privilege_grant_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #s_exerp_privilege_grant_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_exerp_privilege_grant_inserts
  left join p_exerp_privilege_grant
    on #s_exerp_privilege_grant_inserts.bk_hash = p_exerp_privilege_grant.bk_hash
   and p_exerp_privilege_grant.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_exerp_privilege_grant
    on p_exerp_privilege_grant.bk_hash = s_exerp_privilege_grant.bk_hash
   and p_exerp_privilege_grant.s_exerp_privilege_grant_id = s_exerp_privilege_grant.s_exerp_privilege_grant_id
 where s_exerp_privilege_grant.s_exerp_privilege_grant_id is null
    or (s_exerp_privilege_grant.s_exerp_privilege_grant_id is not null
        and s_exerp_privilege_grant.dv_hash <> #s_exerp_privilege_grant_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_exerp_privilege_grant @current_dv_batch_id

end
