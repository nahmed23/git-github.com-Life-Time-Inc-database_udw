CREATE PROC [dbo].[proc_etl_mms_department] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_Department

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_Department (
       bk_hash,
       DepartmentID,
       Name,
       Description,
       SortOrder,
       InsertedDateTime,
       UpdatedDateTime,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(DepartmentID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       DepartmentID,
       Name,
       Description,
       SortOrder,
       InsertedDateTime,
       UpdatedDateTime,
       isnull(cast(stage_mms_Department.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_mms_Department
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_department @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_department (
       bk_hash,
       department_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_mms_Department.bk_hash,
       stage_hash_mms_Department.DepartmentID department_id,
       isnull(cast(stage_hash_mms_Department.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_Department
  left join h_mms_department
    on stage_hash_mms_Department.bk_hash = h_mms_department.bk_hash
 where h_mms_department_id is null
   and stage_hash_mms_Department.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current s_mms_department
if object_id('tempdb..#s_mms_department_inserts') is not null drop table #s_mms_department_inserts
create table #s_mms_department_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_Department.bk_hash,
       stage_hash_mms_Department.DepartmentID department_id,
       stage_hash_mms_Department.Name name,
       stage_hash_mms_Department.Description description,
       stage_hash_mms_Department.SortOrder sort_order,
       stage_hash_mms_Department.InsertedDateTime inserted_date_time,
       stage_hash_mms_Department.UpdatedDateTime updated_date_time,
       isnull(cast(stage_hash_mms_Department.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_Department.DepartmentID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_Department.Name,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_Department.Description,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_Department.SortOrder,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_Department.InsertedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_Department.UpdatedDateTime,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_Department
 where stage_hash_mms_Department.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_department records
set @insert_date_time = getdate()
insert into s_mms_department (
       bk_hash,
       department_id,
       name,
       description,
       sort_order,
       inserted_date_time,
       updated_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_department_inserts.bk_hash,
       #s_mms_department_inserts.department_id,
       #s_mms_department_inserts.name,
       #s_mms_department_inserts.description,
       #s_mms_department_inserts.sort_order,
       #s_mms_department_inserts.inserted_date_time,
       #s_mms_department_inserts.updated_date_time,
       case when s_mms_department.s_mms_department_id is null then isnull(#s_mms_department_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_department_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_department_inserts
  left join p_mms_department
    on #s_mms_department_inserts.bk_hash = p_mms_department.bk_hash
   and p_mms_department.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_department
    on p_mms_department.bk_hash = s_mms_department.bk_hash
   and p_mms_department.s_mms_department_id = s_mms_department.s_mms_department_id
 where s_mms_department.s_mms_department_id is null
    or (s_mms_department.s_mms_department_id is not null
        and s_mms_department.dv_hash <> #s_mms_department_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_department @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mms_department @current_dv_batch_id

end
