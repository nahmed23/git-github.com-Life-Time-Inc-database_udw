CREATE PROC [dbo].[proc_etl_exerp_activity_override] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_exerp_activity_override

set @insert_date_time = getdate()
insert into dbo.stage_hash_exerp_activity_override (
       bk_hash,
       id,
       activity_id,
       name,
       time_configuration_id,
       age_group_id,
       center_id,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(center_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       id,
       activity_id,
       name,
       time_configuration_id,
       age_group_id,
       center_id,
       dummy_modified_date_time,
       isnull(cast(stage_exerp_activity_override.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_exerp_activity_override
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_exerp_activity_override @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_exerp_activity_override (
       bk_hash,
       activity_override_id,
       center_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_exerp_activity_override.bk_hash,
       stage_hash_exerp_activity_override.id activity_override_id,
       stage_hash_exerp_activity_override.center_id center_id,
       isnull(cast(stage_hash_exerp_activity_override.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       33,
       @insert_date_time,
       @user
  from stage_hash_exerp_activity_override
  left join h_exerp_activity_override
    on stage_hash_exerp_activity_override.bk_hash = h_exerp_activity_override.bk_hash
 where h_exerp_activity_override_id is null
   and stage_hash_exerp_activity_override.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_exerp_activity_override
if object_id('tempdb..#l_exerp_activity_override_inserts') is not null drop table #l_exerp_activity_override_inserts
create table #l_exerp_activity_override_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_activity_override.bk_hash,
       stage_hash_exerp_activity_override.id activity_override_id,
       stage_hash_exerp_activity_override.activity_id activity_id,
       stage_hash_exerp_activity_override.time_configuration_id time_configuration_id,
       stage_hash_exerp_activity_override.age_group_id age_group_id,
       stage_hash_exerp_activity_override.center_id center_id,
       isnull(cast(stage_hash_exerp_activity_override.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_exerp_activity_override.id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_activity_override.activity_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_activity_override.time_configuration_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_activity_override.age_group_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_activity_override.center_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_activity_override
 where stage_hash_exerp_activity_override.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_exerp_activity_override records
set @insert_date_time = getdate()
insert into l_exerp_activity_override (
       bk_hash,
       activity_override_id,
       activity_id,
       time_configuration_id,
       age_group_id,
       center_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_exerp_activity_override_inserts.bk_hash,
       #l_exerp_activity_override_inserts.activity_override_id,
       #l_exerp_activity_override_inserts.activity_id,
       #l_exerp_activity_override_inserts.time_configuration_id,
       #l_exerp_activity_override_inserts.age_group_id,
       #l_exerp_activity_override_inserts.center_id,
       case when l_exerp_activity_override.l_exerp_activity_override_id is null then isnull(#l_exerp_activity_override_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #l_exerp_activity_override_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_exerp_activity_override_inserts
  left join p_exerp_activity_override
    on #l_exerp_activity_override_inserts.bk_hash = p_exerp_activity_override.bk_hash
   and p_exerp_activity_override.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_exerp_activity_override
    on p_exerp_activity_override.bk_hash = l_exerp_activity_override.bk_hash
   and p_exerp_activity_override.l_exerp_activity_override_id = l_exerp_activity_override.l_exerp_activity_override_id
 where l_exerp_activity_override.l_exerp_activity_override_id is null
    or (l_exerp_activity_override.l_exerp_activity_override_id is not null
        and l_exerp_activity_override.dv_hash <> #l_exerp_activity_override_inserts.source_hash)

--calculate hash and lookup to current s_exerp_activity_override
if object_id('tempdb..#s_exerp_activity_override_inserts') is not null drop table #s_exerp_activity_override_inserts
create table #s_exerp_activity_override_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_activity_override.bk_hash,
       stage_hash_exerp_activity_override.id activity_override_id,
       stage_hash_exerp_activity_override.name name,
       stage_hash_exerp_activity_override.center_id center_id,
       stage_hash_exerp_activity_override.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_exerp_activity_override.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_exerp_activity_override.id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_activity_override.name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_activity_override.center_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_activity_override
 where stage_hash_exerp_activity_override.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_exerp_activity_override records
set @insert_date_time = getdate()
insert into s_exerp_activity_override (
       bk_hash,
       activity_override_id,
       name,
       center_id,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_exerp_activity_override_inserts.bk_hash,
       #s_exerp_activity_override_inserts.activity_override_id,
       #s_exerp_activity_override_inserts.name,
       #s_exerp_activity_override_inserts.center_id,
       #s_exerp_activity_override_inserts.dummy_modified_date_time,
       case when s_exerp_activity_override.s_exerp_activity_override_id is null then isnull(#s_exerp_activity_override_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #s_exerp_activity_override_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_exerp_activity_override_inserts
  left join p_exerp_activity_override
    on #s_exerp_activity_override_inserts.bk_hash = p_exerp_activity_override.bk_hash
   and p_exerp_activity_override.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_exerp_activity_override
    on p_exerp_activity_override.bk_hash = s_exerp_activity_override.bk_hash
   and p_exerp_activity_override.s_exerp_activity_override_id = s_exerp_activity_override.s_exerp_activity_override_id
 where s_exerp_activity_override.s_exerp_activity_override_id is null
    or (s_exerp_activity_override.s_exerp_activity_override_id is not null
        and s_exerp_activity_override.dv_hash <> #s_exerp_activity_override_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_exerp_activity_override @current_dv_batch_id

end
