CREATE PROC [dbo].[proc_etl_exerp_access_privilege] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_exerp_access_privilege

set @insert_date_time = getdate()
insert into dbo.stage_hash_exerp_access_privilege (
       bk_hash,
       id,
       privilege_set_id,
       access_group_id,
       scope_type,
       scope_id,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       id,
       privilege_set_id,
       access_group_id,
       scope_type,
       scope_id,
       dummy_modified_date_time,
       isnull(cast(stage_exerp_access_privilege.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_exerp_access_privilege
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_exerp_access_privilege @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_exerp_access_privilege (
       bk_hash,
       access_privilege_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_exerp_access_privilege.bk_hash,
       stage_hash_exerp_access_privilege.id access_privilege_id,
       isnull(cast(stage_hash_exerp_access_privilege.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       33,
       @insert_date_time,
       @user
  from stage_hash_exerp_access_privilege
  left join h_exerp_access_privilege
    on stage_hash_exerp_access_privilege.bk_hash = h_exerp_access_privilege.bk_hash
 where h_exerp_access_privilege_id is null
   and stage_hash_exerp_access_privilege.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_exerp_access_privilege
if object_id('tempdb..#l_exerp_access_privilege_inserts') is not null drop table #l_exerp_access_privilege_inserts
create table #l_exerp_access_privilege_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_access_privilege.bk_hash,
       stage_hash_exerp_access_privilege.id access_privilege_id,
       stage_hash_exerp_access_privilege.privilege_set_id privilege_set_id,
       stage_hash_exerp_access_privilege.access_group_id access_group_id,
       stage_hash_exerp_access_privilege.scope_id scope_id,
       isnull(cast(stage_hash_exerp_access_privilege.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_exerp_access_privilege.id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_access_privilege.privilege_set_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_access_privilege.access_group_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_access_privilege.scope_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_access_privilege
 where stage_hash_exerp_access_privilege.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_exerp_access_privilege records
set @insert_date_time = getdate()
insert into l_exerp_access_privilege (
       bk_hash,
       access_privilege_id,
       privilege_set_id,
       access_group_id,
       scope_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_exerp_access_privilege_inserts.bk_hash,
       #l_exerp_access_privilege_inserts.access_privilege_id,
       #l_exerp_access_privilege_inserts.privilege_set_id,
       #l_exerp_access_privilege_inserts.access_group_id,
       #l_exerp_access_privilege_inserts.scope_id,
       case when l_exerp_access_privilege.l_exerp_access_privilege_id is null then isnull(#l_exerp_access_privilege_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #l_exerp_access_privilege_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_exerp_access_privilege_inserts
  left join p_exerp_access_privilege
    on #l_exerp_access_privilege_inserts.bk_hash = p_exerp_access_privilege.bk_hash
   and p_exerp_access_privilege.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_exerp_access_privilege
    on p_exerp_access_privilege.bk_hash = l_exerp_access_privilege.bk_hash
   and p_exerp_access_privilege.l_exerp_access_privilege_id = l_exerp_access_privilege.l_exerp_access_privilege_id
 where l_exerp_access_privilege.l_exerp_access_privilege_id is null
    or (l_exerp_access_privilege.l_exerp_access_privilege_id is not null
        and l_exerp_access_privilege.dv_hash <> #l_exerp_access_privilege_inserts.source_hash)

--calculate hash and lookup to current s_exerp_access_privilege
if object_id('tempdb..#s_exerp_access_privilege_inserts') is not null drop table #s_exerp_access_privilege_inserts
create table #s_exerp_access_privilege_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_access_privilege.bk_hash,
       stage_hash_exerp_access_privilege.id access_privilege_id,
       stage_hash_exerp_access_privilege.scope_type scope_type,
       stage_hash_exerp_access_privilege.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_exerp_access_privilege.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_exerp_access_privilege.id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_access_privilege.scope_type,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_access_privilege
 where stage_hash_exerp_access_privilege.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_exerp_access_privilege records
set @insert_date_time = getdate()
insert into s_exerp_access_privilege (
       bk_hash,
       access_privilege_id,
       scope_type,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_exerp_access_privilege_inserts.bk_hash,
       #s_exerp_access_privilege_inserts.access_privilege_id,
       #s_exerp_access_privilege_inserts.scope_type,
       #s_exerp_access_privilege_inserts.dummy_modified_date_time,
       case when s_exerp_access_privilege.s_exerp_access_privilege_id is null then isnull(#s_exerp_access_privilege_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #s_exerp_access_privilege_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_exerp_access_privilege_inserts
  left join p_exerp_access_privilege
    on #s_exerp_access_privilege_inserts.bk_hash = p_exerp_access_privilege.bk_hash
   and p_exerp_access_privilege.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_exerp_access_privilege
    on p_exerp_access_privilege.bk_hash = s_exerp_access_privilege.bk_hash
   and p_exerp_access_privilege.s_exerp_access_privilege_id = s_exerp_access_privilege.s_exerp_access_privilege_id
 where s_exerp_access_privilege.s_exerp_access_privilege_id is null
    or (s_exerp_access_privilege.s_exerp_access_privilege_id is not null
        and s_exerp_access_privilege.dv_hash <> #s_exerp_access_privilege_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_exerp_access_privilege @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_exerp_access_privilege @current_dv_batch_id

end
