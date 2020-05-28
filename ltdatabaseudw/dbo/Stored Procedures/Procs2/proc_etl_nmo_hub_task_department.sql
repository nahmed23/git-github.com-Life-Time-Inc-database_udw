CREATE PROC [dbo].[proc_etl_nmo_hub_task_department] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_nmo_hubtaskdepartment

set @insert_date_time = getdate()
insert into dbo.stage_hash_nmo_hubtaskdepartment (
       bk_hash,
       id,
       title,
       activationdate,
       expirationdate,
       createddate,
       updateddate,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       id,
       title,
       activationdate,
       expirationdate,
       createddate,
       updateddate,
       isnull(cast(stage_nmo_hubtaskdepartment.createddate as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_nmo_hubtaskdepartment
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_nmo_hub_task_department @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_nmo_hub_task_department (
       bk_hash,
       id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_nmo_hubtaskdepartment.bk_hash,
       stage_hash_nmo_hubtaskdepartment.id id,
       isnull(cast(stage_hash_nmo_hubtaskdepartment.createddate as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       41,
       @insert_date_time,
       @user
  from stage_hash_nmo_hubtaskdepartment
  left join h_nmo_hub_task_department
    on stage_hash_nmo_hubtaskdepartment.bk_hash = h_nmo_hub_task_department.bk_hash
 where h_nmo_hub_task_department_id is null
   and stage_hash_nmo_hubtaskdepartment.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current s_nmo_hub_task_department
if object_id('tempdb..#s_nmo_hub_task_department_inserts') is not null drop table #s_nmo_hub_task_department_inserts
create table #s_nmo_hub_task_department_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_nmo_hubtaskdepartment.bk_hash,
       stage_hash_nmo_hubtaskdepartment.id id,
       stage_hash_nmo_hubtaskdepartment.title title,
       stage_hash_nmo_hubtaskdepartment.activationdate activation_date,
       stage_hash_nmo_hubtaskdepartment.expirationdate expiration_date,
       stage_hash_nmo_hubtaskdepartment.createddate created_date,
       stage_hash_nmo_hubtaskdepartment.updateddate updated_date,
       isnull(cast(stage_hash_nmo_hubtaskdepartment.createddate as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_nmo_hubtaskdepartment.id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_nmo_hubtaskdepartment.title,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_nmo_hubtaskdepartment.activationdate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_nmo_hubtaskdepartment.expirationdate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_nmo_hubtaskdepartment.createddate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_nmo_hubtaskdepartment.updateddate,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_nmo_hubtaskdepartment
 where stage_hash_nmo_hubtaskdepartment.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_nmo_hub_task_department records
set @insert_date_time = getdate()
insert into s_nmo_hub_task_department (
       bk_hash,
       id,
       title,
       activation_date,
       expiration_date,
       created_date,
       updated_date,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_nmo_hub_task_department_inserts.bk_hash,
       #s_nmo_hub_task_department_inserts.id,
       #s_nmo_hub_task_department_inserts.title,
       #s_nmo_hub_task_department_inserts.activation_date,
       #s_nmo_hub_task_department_inserts.expiration_date,
       #s_nmo_hub_task_department_inserts.created_date,
       #s_nmo_hub_task_department_inserts.updated_date,
       case when s_nmo_hub_task_department.s_nmo_hub_task_department_id is null then isnull(#s_nmo_hub_task_department_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       41,
       #s_nmo_hub_task_department_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_nmo_hub_task_department_inserts
  left join p_nmo_hub_task_department
    on #s_nmo_hub_task_department_inserts.bk_hash = p_nmo_hub_task_department.bk_hash
   and p_nmo_hub_task_department.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_nmo_hub_task_department
    on p_nmo_hub_task_department.bk_hash = s_nmo_hub_task_department.bk_hash
   and p_nmo_hub_task_department.s_nmo_hub_task_department_id = s_nmo_hub_task_department.s_nmo_hub_task_department_id
 where s_nmo_hub_task_department.s_nmo_hub_task_department_id is null
    or (s_nmo_hub_task_department.s_nmo_hub_task_department_id is not null
        and s_nmo_hub_task_department.dv_hash <> #s_nmo_hub_task_department_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_nmo_hub_task_department @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_nmo_hub_task_department @current_dv_batch_id

end
