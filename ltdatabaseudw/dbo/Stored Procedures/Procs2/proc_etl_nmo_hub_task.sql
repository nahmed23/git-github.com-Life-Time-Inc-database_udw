CREATE PROC [dbo].[proc_etl_nmo_hub_task] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_nmo_hubtask

set @insert_date_time = getdate()
insert into dbo.stage_hash_nmo_hubtask (
       bk_hash,
       id,
       title,
       description,
       hubtaskdepartmentid,
       hubtaskstatusid,
       hubtasktypeid,
       clubid,
       partyid,
       priority,
       creatorpartyid,
       creatorname,
       assigneepartyid,
       assigneename,
       duedate,
       resolutiondate,
       createddate,
       updateddate,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       id,
       title,
       description,
       hubtaskdepartmentid,
       hubtaskstatusid,
       hubtasktypeid,
       clubid,
       partyid,
       priority,
       creatorpartyid,
       creatorname,
       assigneepartyid,
       assigneename,
       duedate,
       resolutiondate,
       createddate,
       updateddate,
       isnull(cast(stage_nmo_hubtask.createddate as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_nmo_hubtask
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_nmo_hub_task @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_nmo_hub_task (
       bk_hash,
       id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_nmo_hubtask.bk_hash,
       stage_hash_nmo_hubtask.id id,
       isnull(cast(stage_hash_nmo_hubtask.createddate as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       41,
       @insert_date_time,
       @user
  from stage_hash_nmo_hubtask
  left join h_nmo_hub_task
    on stage_hash_nmo_hubtask.bk_hash = h_nmo_hub_task.bk_hash
 where h_nmo_hub_task_id is null
   and stage_hash_nmo_hubtask.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_nmo_hub_task
if object_id('tempdb..#l_nmo_hub_task_inserts') is not null drop table #l_nmo_hub_task_inserts
create table #l_nmo_hub_task_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_nmo_hubtask.bk_hash,
       stage_hash_nmo_hubtask.id id,
       stage_hash_nmo_hubtask.hubtaskdepartmentid hub_task_department_id,
       stage_hash_nmo_hubtask.hubtaskstatusid hub_task_status_id,
       stage_hash_nmo_hubtask.hubtasktypeid hub_task_type_id,
       stage_hash_nmo_hubtask.clubid club_id,
       stage_hash_nmo_hubtask.partyid party_id,
       stage_hash_nmo_hubtask.creatorpartyid creator_party_id,
       stage_hash_nmo_hubtask.assigneepartyid assignee_party_id,
       isnull(cast(stage_hash_nmo_hubtask.createddate as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_nmo_hubtask.id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_nmo_hubtask.hubtaskdepartmentid as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_nmo_hubtask.hubtaskstatusid as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_nmo_hubtask.hubtasktypeid as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_nmo_hubtask.clubid as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_nmo_hubtask.partyid as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_nmo_hubtask.creatorpartyid as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_nmo_hubtask.assigneepartyid as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_nmo_hubtask
 where stage_hash_nmo_hubtask.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_nmo_hub_task records
set @insert_date_time = getdate()
insert into l_nmo_hub_task (
       bk_hash,
       id,
       hub_task_department_id,
       hub_task_status_id,
       hub_task_type_id,
       club_id,
       party_id,
       creator_party_id,
       assignee_party_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_nmo_hub_task_inserts.bk_hash,
       #l_nmo_hub_task_inserts.id,
       #l_nmo_hub_task_inserts.hub_task_department_id,
       #l_nmo_hub_task_inserts.hub_task_status_id,
       #l_nmo_hub_task_inserts.hub_task_type_id,
       #l_nmo_hub_task_inserts.club_id,
       #l_nmo_hub_task_inserts.party_id,
       #l_nmo_hub_task_inserts.creator_party_id,
       #l_nmo_hub_task_inserts.assignee_party_id,
       case when l_nmo_hub_task.l_nmo_hub_task_id is null then isnull(#l_nmo_hub_task_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       41,
       #l_nmo_hub_task_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_nmo_hub_task_inserts
  left join p_nmo_hub_task
    on #l_nmo_hub_task_inserts.bk_hash = p_nmo_hub_task.bk_hash
   and p_nmo_hub_task.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_nmo_hub_task
    on p_nmo_hub_task.bk_hash = l_nmo_hub_task.bk_hash
   and p_nmo_hub_task.l_nmo_hub_task_id = l_nmo_hub_task.l_nmo_hub_task_id
 where l_nmo_hub_task.l_nmo_hub_task_id is null
    or (l_nmo_hub_task.l_nmo_hub_task_id is not null
        and l_nmo_hub_task.dv_hash <> #l_nmo_hub_task_inserts.source_hash)

--calculate hash and lookup to current s_nmo_hub_task
if object_id('tempdb..#s_nmo_hub_task_inserts') is not null drop table #s_nmo_hub_task_inserts
create table #s_nmo_hub_task_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_nmo_hubtask.bk_hash,
       stage_hash_nmo_hubtask.id id,
       stage_hash_nmo_hubtask.title title,
       stage_hash_nmo_hubtask.description description,
       stage_hash_nmo_hubtask.priority priority,
       stage_hash_nmo_hubtask.creatorname creator_name,
       stage_hash_nmo_hubtask.assigneename assignee_name,
       stage_hash_nmo_hubtask.duedate due_date,
       stage_hash_nmo_hubtask.resolutiondate resolution_date,
       stage_hash_nmo_hubtask.createddate created_date,
       stage_hash_nmo_hubtask.updateddate updated_date,
       isnull(cast(stage_hash_nmo_hubtask.createddate as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_nmo_hubtask.id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_nmo_hubtask.title,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_nmo_hubtask.description,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_nmo_hubtask.priority as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_nmo_hubtask.creatorname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_nmo_hubtask.assigneename,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_nmo_hubtask.duedate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_nmo_hubtask.resolutiondate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_nmo_hubtask.createddate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_nmo_hubtask.updateddate,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_nmo_hubtask
 where stage_hash_nmo_hubtask.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_nmo_hub_task records
set @insert_date_time = getdate()
insert into s_nmo_hub_task (
       bk_hash,
       id,
       title,
       description,
       priority,
       creator_name,
       assignee_name,
       due_date,
       resolution_date,
       created_date,
       updated_date,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_nmo_hub_task_inserts.bk_hash,
       #s_nmo_hub_task_inserts.id,
       #s_nmo_hub_task_inserts.title,
       #s_nmo_hub_task_inserts.description,
       #s_nmo_hub_task_inserts.priority,
       #s_nmo_hub_task_inserts.creator_name,
       #s_nmo_hub_task_inserts.assignee_name,
       #s_nmo_hub_task_inserts.due_date,
       #s_nmo_hub_task_inserts.resolution_date,
       #s_nmo_hub_task_inserts.created_date,
       #s_nmo_hub_task_inserts.updated_date,
       case when s_nmo_hub_task.s_nmo_hub_task_id is null then isnull(#s_nmo_hub_task_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       41,
       #s_nmo_hub_task_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_nmo_hub_task_inserts
  left join p_nmo_hub_task
    on #s_nmo_hub_task_inserts.bk_hash = p_nmo_hub_task.bk_hash
   and p_nmo_hub_task.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_nmo_hub_task
    on p_nmo_hub_task.bk_hash = s_nmo_hub_task.bk_hash
   and p_nmo_hub_task.s_nmo_hub_task_id = s_nmo_hub_task.s_nmo_hub_task_id
 where s_nmo_hub_task.s_nmo_hub_task_id is null
    or (s_nmo_hub_task.s_nmo_hub_task_id is not null
        and s_nmo_hub_task.dv_hash <> #s_nmo_hub_task_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_nmo_hub_task @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_nmo_hub_task @current_dv_batch_id

end
