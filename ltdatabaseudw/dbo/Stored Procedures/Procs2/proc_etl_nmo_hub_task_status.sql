CREATE PROC [dbo].[proc_etl_nmo_hub_task_status] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_nmo_hubtaskstatus

set @insert_date_time = getdate()
insert into dbo.stage_hash_nmo_hubtaskstatus (
       bk_hash,
       id,
       title,
       description,
       activationdate,
       expirationdate,
       createddate,
       updateddate,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       id,
       title,
       description,
       activationdate,
       expirationdate,
       createddate,
       updateddate,
       isnull(cast(stage_nmo_hubtaskstatus.createddate as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_nmo_hubtaskstatus
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_nmo_hub_task_status @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_nmo_hub_task_status (
       bk_hash,
       id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_nmo_hubtaskstatus.bk_hash,
       stage_hash_nmo_hubtaskstatus.id id,
       isnull(cast(stage_hash_nmo_hubtaskstatus.createddate as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       41,
       @insert_date_time,
       @user
  from stage_hash_nmo_hubtaskstatus
  left join h_nmo_hub_task_status
    on stage_hash_nmo_hubtaskstatus.bk_hash = h_nmo_hub_task_status.bk_hash
 where h_nmo_hub_task_status_id is null
   and stage_hash_nmo_hubtaskstatus.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current s_nmo_hub_task_status
if object_id('tempdb..#s_nmo_hub_task_status_inserts') is not null drop table #s_nmo_hub_task_status_inserts
create table #s_nmo_hub_task_status_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_nmo_hubtaskstatus.bk_hash,
       stage_hash_nmo_hubtaskstatus.id id,
       stage_hash_nmo_hubtaskstatus.title title,
       stage_hash_nmo_hubtaskstatus.description description,
       stage_hash_nmo_hubtaskstatus.activationdate activation_date,
       stage_hash_nmo_hubtaskstatus.expirationdate expiration_date,
       stage_hash_nmo_hubtaskstatus.updateddate updated_date,
       stage_hash_nmo_hubtaskstatus.createddate created_date,
       isnull(cast(stage_hash_nmo_hubtaskstatus.createddate as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_nmo_hubtaskstatus.id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_nmo_hubtaskstatus.title,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_nmo_hubtaskstatus.description,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_nmo_hubtaskstatus.activationdate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_nmo_hubtaskstatus.expirationdate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_nmo_hubtaskstatus.updateddate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_nmo_hubtaskstatus.createddate,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_nmo_hubtaskstatus
 where stage_hash_nmo_hubtaskstatus.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_nmo_hub_task_status records
set @insert_date_time = getdate()
insert into s_nmo_hub_task_status (
       bk_hash,
       id,
       title,
       description,
       activation_date,
       expiration_date,
       updated_date,
       created_date,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_nmo_hub_task_status_inserts.bk_hash,
       #s_nmo_hub_task_status_inserts.id,
       #s_nmo_hub_task_status_inserts.title,
       #s_nmo_hub_task_status_inserts.description,
       #s_nmo_hub_task_status_inserts.activation_date,
       #s_nmo_hub_task_status_inserts.expiration_date,
       #s_nmo_hub_task_status_inserts.updated_date,
       #s_nmo_hub_task_status_inserts.created_date,
       case when s_nmo_hub_task_status.s_nmo_hub_task_status_id is null then isnull(#s_nmo_hub_task_status_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       41,
       #s_nmo_hub_task_status_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_nmo_hub_task_status_inserts
  left join p_nmo_hub_task_status
    on #s_nmo_hub_task_status_inserts.bk_hash = p_nmo_hub_task_status.bk_hash
   and p_nmo_hub_task_status.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_nmo_hub_task_status
    on p_nmo_hub_task_status.bk_hash = s_nmo_hub_task_status.bk_hash
   and p_nmo_hub_task_status.s_nmo_hub_task_status_id = s_nmo_hub_task_status.s_nmo_hub_task_status_id
 where s_nmo_hub_task_status.s_nmo_hub_task_status_id is null
    or (s_nmo_hub_task_status.s_nmo_hub_task_status_id is not null
        and s_nmo_hub_task_status.dv_hash <> #s_nmo_hub_task_status_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_nmo_hub_task_status @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_nmo_hub_task_status @current_dv_batch_id

end
