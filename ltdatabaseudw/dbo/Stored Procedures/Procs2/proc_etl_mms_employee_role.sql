CREATE PROC [dbo].[proc_etl_mms_employee_role] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_EmployeeRole

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_EmployeeRole (
       bk_hash,
       EmployeeRoleID,
       EmployeeID,
       ValEmployeeRoleID,
       InsertedDateTime,
       UpdatedDateTime,
       PrimaryEmployeeRoleFlag,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(EmployeeRoleID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       EmployeeRoleID,
       EmployeeID,
       ValEmployeeRoleID,
       InsertedDateTime,
       UpdatedDateTime,
       PrimaryEmployeeRoleFlag,
       isnull(cast(stage_mms_EmployeeRole.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_mms_EmployeeRole
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_employee_role @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_employee_role (
       bk_hash,
       employee_role_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_mms_EmployeeRole.bk_hash,
       stage_hash_mms_EmployeeRole.EmployeeRoleID employee_role_id,
       isnull(cast(stage_hash_mms_EmployeeRole.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_EmployeeRole
  left join h_mms_employee_role
    on stage_hash_mms_EmployeeRole.bk_hash = h_mms_employee_role.bk_hash
 where h_mms_employee_role_id is null
   and stage_hash_mms_EmployeeRole.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_employee_role
if object_id('tempdb..#l_mms_employee_role_inserts') is not null drop table #l_mms_employee_role_inserts
create table #l_mms_employee_role_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_EmployeeRole.bk_hash,
       stage_hash_mms_EmployeeRole.EmployeeRoleID employee_role_id,
       stage_hash_mms_EmployeeRole.EmployeeID employee_id,
       stage_hash_mms_EmployeeRole.ValEmployeeRoleID val_employee_role_id,
       isnull(cast(stage_hash_mms_EmployeeRole.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_EmployeeRole.EmployeeRoleID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_EmployeeRole.EmployeeID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_EmployeeRole.ValEmployeeRoleID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_EmployeeRole
 where stage_hash_mms_EmployeeRole.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_employee_role records
set @insert_date_time = getdate()
insert into l_mms_employee_role (
       bk_hash,
       employee_role_id,
       employee_id,
       val_employee_role_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_employee_role_inserts.bk_hash,
       #l_mms_employee_role_inserts.employee_role_id,
       #l_mms_employee_role_inserts.employee_id,
       #l_mms_employee_role_inserts.val_employee_role_id,
       case when l_mms_employee_role.l_mms_employee_role_id is null then isnull(#l_mms_employee_role_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_employee_role_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_employee_role_inserts
  left join p_mms_employee_role
    on #l_mms_employee_role_inserts.bk_hash = p_mms_employee_role.bk_hash
   and p_mms_employee_role.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_employee_role
    on p_mms_employee_role.bk_hash = l_mms_employee_role.bk_hash
   and p_mms_employee_role.l_mms_employee_role_id = l_mms_employee_role.l_mms_employee_role_id
 where l_mms_employee_role.l_mms_employee_role_id is null
    or (l_mms_employee_role.l_mms_employee_role_id is not null
        and l_mms_employee_role.dv_hash <> #l_mms_employee_role_inserts.source_hash)

--calculate hash and lookup to current s_mms_employee_role
if object_id('tempdb..#s_mms_employee_role_inserts') is not null drop table #s_mms_employee_role_inserts
create table #s_mms_employee_role_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_EmployeeRole.bk_hash,
       stage_hash_mms_EmployeeRole.EmployeeRoleID employee_role_id,
       stage_hash_mms_EmployeeRole.InsertedDateTime inserted_date_time,
       stage_hash_mms_EmployeeRole.UpdatedDateTime updated_date_time,
       stage_hash_mms_EmployeeRole.PrimaryEmployeeRoleFlag primary_employee_role_flag,
       isnull(cast(stage_hash_mms_EmployeeRole.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_EmployeeRole.EmployeeRoleID as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_EmployeeRole.InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_EmployeeRole.UpdatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_mms_EmployeeRole.PrimaryEmployeeRoleFlag as varchar(42)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_EmployeeRole
 where stage_hash_mms_EmployeeRole.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_employee_role records
set @insert_date_time = getdate()
insert into s_mms_employee_role (
       bk_hash,
       employee_role_id,
       inserted_date_time,
       updated_date_time,
       primary_employee_role_flag,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_employee_role_inserts.bk_hash,
       #s_mms_employee_role_inserts.employee_role_id,
       #s_mms_employee_role_inserts.inserted_date_time,
       #s_mms_employee_role_inserts.updated_date_time,
       #s_mms_employee_role_inserts.primary_employee_role_flag,
       case when s_mms_employee_role.s_mms_employee_role_id is null then isnull(#s_mms_employee_role_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_employee_role_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_employee_role_inserts
  left join p_mms_employee_role
    on #s_mms_employee_role_inserts.bk_hash = p_mms_employee_role.bk_hash
   and p_mms_employee_role.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_employee_role
    on p_mms_employee_role.bk_hash = s_mms_employee_role.bk_hash
   and p_mms_employee_role.s_mms_employee_role_id = s_mms_employee_role.s_mms_employee_role_id
 where s_mms_employee_role.s_mms_employee_role_id is null
    or (s_mms_employee_role.s_mms_employee_role_id is not null
        and s_mms_employee_role.dv_hash <> #s_mms_employee_role_inserts.source_hash)

--Run the dv_deleted proc
exec dbo.proc_dv_deleted_mms_employee_role @current_dv_batch_id, @job_start_date_time_varchar

--Run the PIT proc
exec dbo.proc_p_mms_employee_role @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mms_employee_role @current_dv_batch_id

end
