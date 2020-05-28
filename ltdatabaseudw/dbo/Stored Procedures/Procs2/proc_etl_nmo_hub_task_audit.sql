CREATE PROC [dbo].[proc_etl_nmo_hub_task_audit] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_nmo_hubtaskaudit

set @insert_date_time = getdate()
insert into dbo.stage_hash_nmo_hubtaskaudit (
       bk_hash,
       id,
       hubtaskid,
       operation,
       field,
       oldvalue,
       newvalue,
       modifiedpartyid,
       modifiedname,
       createddate,
       updateddate,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       id,
       hubtaskid,
       operation,
       field,
       oldvalue,
       newvalue,
       modifiedpartyid,
       modifiedname,
       createddate,
       updateddate,
       isnull(cast(stage_nmo_hubtaskaudit.createddate as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_nmo_hubtaskaudit
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_nmo_hub_task_audit @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_nmo_hub_task_audit (
       bk_hash,
       id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_nmo_hubtaskaudit.bk_hash,
       stage_hash_nmo_hubtaskaudit.id id,
       isnull(cast(stage_hash_nmo_hubtaskaudit.createddate as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       41,
       @insert_date_time,
       @user
  from stage_hash_nmo_hubtaskaudit
  left join h_nmo_hub_task_audit
    on stage_hash_nmo_hubtaskaudit.bk_hash = h_nmo_hub_task_audit.bk_hash
 where h_nmo_hub_task_audit_id is null
   and stage_hash_nmo_hubtaskaudit.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_nmo_hub_task_audit
if object_id('tempdb..#l_nmo_hub_task_audit_inserts') is not null drop table #l_nmo_hub_task_audit_inserts
create table #l_nmo_hub_task_audit_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_nmo_hubtaskaudit.bk_hash,
       stage_hash_nmo_hubtaskaudit.id id,
       stage_hash_nmo_hubtaskaudit.hubtaskid hub_task_id,
       stage_hash_nmo_hubtaskaudit.modifiedpartyid modified_party_id,
       isnull(cast(stage_hash_nmo_hubtaskaudit.createddate as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_nmo_hubtaskaudit.id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_nmo_hubtaskaudit.hubtaskid as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_nmo_hubtaskaudit.modifiedpartyid as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_nmo_hubtaskaudit
 where stage_hash_nmo_hubtaskaudit.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_nmo_hub_task_audit records
set @insert_date_time = getdate()
insert into l_nmo_hub_task_audit (
       bk_hash,
       id,
       hub_task_id,
       modified_party_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_nmo_hub_task_audit_inserts.bk_hash,
       #l_nmo_hub_task_audit_inserts.id,
       #l_nmo_hub_task_audit_inserts.hub_task_id,
       #l_nmo_hub_task_audit_inserts.modified_party_id,
       case when l_nmo_hub_task_audit.l_nmo_hub_task_audit_id is null then isnull(#l_nmo_hub_task_audit_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       41,
       #l_nmo_hub_task_audit_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_nmo_hub_task_audit_inserts
  left join p_nmo_hub_task_audit
    on #l_nmo_hub_task_audit_inserts.bk_hash = p_nmo_hub_task_audit.bk_hash
   and p_nmo_hub_task_audit.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_nmo_hub_task_audit
    on p_nmo_hub_task_audit.bk_hash = l_nmo_hub_task_audit.bk_hash
   and p_nmo_hub_task_audit.l_nmo_hub_task_audit_id = l_nmo_hub_task_audit.l_nmo_hub_task_audit_id
 where l_nmo_hub_task_audit.l_nmo_hub_task_audit_id is null
    or (l_nmo_hub_task_audit.l_nmo_hub_task_audit_id is not null
        and l_nmo_hub_task_audit.dv_hash <> #l_nmo_hub_task_audit_inserts.source_hash)

--calculate hash and lookup to current s_nmo_hub_task_audit
if object_id('tempdb..#s_nmo_hub_task_audit_inserts') is not null drop table #s_nmo_hub_task_audit_inserts
create table #s_nmo_hub_task_audit_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_nmo_hubtaskaudit.bk_hash,
       stage_hash_nmo_hubtaskaudit.id id,
       stage_hash_nmo_hubtaskaudit.operation operation,
       stage_hash_nmo_hubtaskaudit.field field,
       stage_hash_nmo_hubtaskaudit.oldvalue old_value,
       stage_hash_nmo_hubtaskaudit.newvalue new_value,
       stage_hash_nmo_hubtaskaudit.modifiedname modified_name,
       stage_hash_nmo_hubtaskaudit.createddate created_date,
       stage_hash_nmo_hubtaskaudit.updateddate updated_date,
       isnull(cast(stage_hash_nmo_hubtaskaudit.createddate as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_nmo_hubtaskaudit.id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_nmo_hubtaskaudit.operation,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_nmo_hubtaskaudit.field,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_nmo_hubtaskaudit.oldvalue,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_nmo_hubtaskaudit.newvalue,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_nmo_hubtaskaudit.modifiedname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_nmo_hubtaskaudit.createddate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_nmo_hubtaskaudit.updateddate,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_nmo_hubtaskaudit
 where stage_hash_nmo_hubtaskaudit.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_nmo_hub_task_audit records
set @insert_date_time = getdate()
insert into s_nmo_hub_task_audit (
       bk_hash,
       id,
       operation,
       field,
       old_value,
       new_value,
       modified_name,
       created_date,
       updated_date,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_nmo_hub_task_audit_inserts.bk_hash,
       #s_nmo_hub_task_audit_inserts.id,
       #s_nmo_hub_task_audit_inserts.operation,
       #s_nmo_hub_task_audit_inserts.field,
       #s_nmo_hub_task_audit_inserts.old_value,
       #s_nmo_hub_task_audit_inserts.new_value,
       #s_nmo_hub_task_audit_inserts.modified_name,
       #s_nmo_hub_task_audit_inserts.created_date,
       #s_nmo_hub_task_audit_inserts.updated_date,
       case when s_nmo_hub_task_audit.s_nmo_hub_task_audit_id is null then isnull(#s_nmo_hub_task_audit_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       41,
       #s_nmo_hub_task_audit_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_nmo_hub_task_audit_inserts
  left join p_nmo_hub_task_audit
    on #s_nmo_hub_task_audit_inserts.bk_hash = p_nmo_hub_task_audit.bk_hash
   and p_nmo_hub_task_audit.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_nmo_hub_task_audit
    on p_nmo_hub_task_audit.bk_hash = s_nmo_hub_task_audit.bk_hash
   and p_nmo_hub_task_audit.s_nmo_hub_task_audit_id = s_nmo_hub_task_audit.s_nmo_hub_task_audit_id
 where s_nmo_hub_task_audit.s_nmo_hub_task_audit_id is null
    or (s_nmo_hub_task_audit.s_nmo_hub_task_audit_id is not null
        and s_nmo_hub_task_audit.dv_hash <> #s_nmo_hub_task_audit_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_nmo_hub_task_audit @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_nmo_hub_task_audit @current_dv_batch_id

end
