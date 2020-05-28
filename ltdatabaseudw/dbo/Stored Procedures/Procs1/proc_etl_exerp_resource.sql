CREATE PROC [dbo].[proc_etl_exerp_resource] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_exerp_resource

set @insert_date_time = getdate()
insert into dbo.stage_hash_exerp_resource (
       bk_hash,
       id,
       name,
       state,
       type,
       access_group_name,
       external_id,
       center_id,
       access_group_id,
       comment,
       show_calendar,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(id,'z#@$k%&P'))),2) bk_hash,
       id,
       name,
       state,
       type,
       access_group_name,
       external_id,
       center_id,
       access_group_id,
       comment,
       show_calendar,
       dummy_modified_date_time,
       isnull(cast(stage_exerp_resource.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_exerp_resource
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_exerp_resource @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_exerp_resource (
       bk_hash,
       resource_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_exerp_resource.bk_hash,
       stage_hash_exerp_resource.id resource_id,
       isnull(cast(stage_hash_exerp_resource.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       33,
       @insert_date_time,
       @user
  from stage_hash_exerp_resource
  left join h_exerp_resource
    on stage_hash_exerp_resource.bk_hash = h_exerp_resource.bk_hash
 where h_exerp_resource_id is null
   and stage_hash_exerp_resource.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_exerp_resource
if object_id('tempdb..#l_exerp_resource_inserts') is not null drop table #l_exerp_resource_inserts
create table #l_exerp_resource_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_resource.bk_hash,
       stage_hash_exerp_resource.id resource_id,
       stage_hash_exerp_resource.external_id external_id,
       stage_hash_exerp_resource.center_id center_id,
       stage_hash_exerp_resource.access_group_id access_group_id,
       isnull(cast(stage_hash_exerp_resource.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_exerp_resource.id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_resource.external_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_resource.center_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_resource.access_group_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_resource
 where stage_hash_exerp_resource.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_exerp_resource records
set @insert_date_time = getdate()
insert into l_exerp_resource (
       bk_hash,
       resource_id,
       external_id,
       center_id,
       access_group_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_exerp_resource_inserts.bk_hash,
       #l_exerp_resource_inserts.resource_id,
       #l_exerp_resource_inserts.external_id,
       #l_exerp_resource_inserts.center_id,
       #l_exerp_resource_inserts.access_group_id,
       case when l_exerp_resource.l_exerp_resource_id is null then isnull(#l_exerp_resource_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #l_exerp_resource_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_exerp_resource_inserts
  left join p_exerp_resource
    on #l_exerp_resource_inserts.bk_hash = p_exerp_resource.bk_hash
   and p_exerp_resource.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_exerp_resource
    on p_exerp_resource.bk_hash = l_exerp_resource.bk_hash
   and p_exerp_resource.l_exerp_resource_id = l_exerp_resource.l_exerp_resource_id
 where l_exerp_resource.l_exerp_resource_id is null
    or (l_exerp_resource.l_exerp_resource_id is not null
        and l_exerp_resource.dv_hash <> #l_exerp_resource_inserts.source_hash)

--calculate hash and lookup to current s_exerp_resource
if object_id('tempdb..#s_exerp_resource_inserts') is not null drop table #s_exerp_resource_inserts
create table #s_exerp_resource_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_resource.bk_hash,
       stage_hash_exerp_resource.id resource_id,
       stage_hash_exerp_resource.name name,
       stage_hash_exerp_resource.state state,
       stage_hash_exerp_resource.type type,
       stage_hash_exerp_resource.access_group_name access_group_name,
       stage_hash_exerp_resource.comment comment,
       stage_hash_exerp_resource.show_calendar show_calendar,
       stage_hash_exerp_resource.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_exerp_resource.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_exerp_resource.id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_resource.name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_resource.state,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_resource.type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_resource.access_group_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_resource.comment,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_resource.show_calendar as varchar(42)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_resource
 where stage_hash_exerp_resource.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_exerp_resource records
set @insert_date_time = getdate()
insert into s_exerp_resource (
       bk_hash,
       resource_id,
       name,
       state,
       type,
       access_group_name,
       comment,
       show_calendar,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_exerp_resource_inserts.bk_hash,
       #s_exerp_resource_inserts.resource_id,
       #s_exerp_resource_inserts.name,
       #s_exerp_resource_inserts.state,
       #s_exerp_resource_inserts.type,
       #s_exerp_resource_inserts.access_group_name,
       #s_exerp_resource_inserts.comment,
       #s_exerp_resource_inserts.show_calendar,
       #s_exerp_resource_inserts.dummy_modified_date_time,
       case when s_exerp_resource.s_exerp_resource_id is null then isnull(#s_exerp_resource_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #s_exerp_resource_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_exerp_resource_inserts
  left join p_exerp_resource
    on #s_exerp_resource_inserts.bk_hash = p_exerp_resource.bk_hash
   and p_exerp_resource.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_exerp_resource
    on p_exerp_resource.bk_hash = s_exerp_resource.bk_hash
   and p_exerp_resource.s_exerp_resource_id = s_exerp_resource.s_exerp_resource_id
 where s_exerp_resource.s_exerp_resource_id is null
    or (s_exerp_resource.s_exerp_resource_id is not null
        and s_exerp_resource.dv_hash <> #s_exerp_resource_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_exerp_resource @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_exerp_resource @current_dv_batch_id

end
