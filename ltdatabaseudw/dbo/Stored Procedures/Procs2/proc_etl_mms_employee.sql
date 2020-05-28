CREATE PROC [dbo].[proc_etl_mms_employee] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_mms_Employee

set @insert_date_time = getdate()
insert into dbo.stage_hash_mms_Employee (
       bk_hash,
       EmployeeID,
       ClubID,
       ActiveStatusFlag,
       FirstName,
       LastName,
       MiddleInt,
       InsertedDateTime,
       MemberID,
       UpdatedDateTime,
       HireDate,
       TerminationDate,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(EmployeeID as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       EmployeeID,
       ClubID,
       ActiveStatusFlag,
       FirstName,
       LastName,
       MiddleInt,
       InsertedDateTime,
       MemberID,
       UpdatedDateTime,
       HireDate,
       TerminationDate,
       isnull(cast(stage_mms_Employee.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_mms_Employee
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_mms_employee @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_mms_employee (
       bk_hash,
       employee_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_mms_Employee.bk_hash,
       stage_hash_mms_Employee.EmployeeID employee_id,
       isnull(cast(stage_hash_mms_Employee.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       2,
       @insert_date_time,
       @user
  from stage_hash_mms_Employee
  left join h_mms_employee
    on stage_hash_mms_Employee.bk_hash = h_mms_employee.bk_hash
 where h_mms_employee_id is null
   and stage_hash_mms_Employee.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_mms_employee
if object_id('tempdb..#l_mms_employee_inserts') is not null drop table #l_mms_employee_inserts
create table #l_mms_employee_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_Employee.bk_hash,
       stage_hash_mms_Employee.EmployeeID employee_id,
       stage_hash_mms_Employee.ClubID club_id,
       stage_hash_mms_Employee.MemberID member_id,
       isnull(cast(stage_hash_mms_Employee.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_Employee.EmployeeID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_Employee.ClubID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_Employee.MemberID as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_Employee
 where stage_hash_mms_Employee.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_mms_employee records
set @insert_date_time = getdate()
insert into l_mms_employee (
       bk_hash,
       employee_id,
       club_id,
       member_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_mms_employee_inserts.bk_hash,
       #l_mms_employee_inserts.employee_id,
       #l_mms_employee_inserts.club_id,
       #l_mms_employee_inserts.member_id,
       case when l_mms_employee.l_mms_employee_id is null then isnull(#l_mms_employee_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #l_mms_employee_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_mms_employee_inserts
  left join p_mms_employee
    on #l_mms_employee_inserts.bk_hash = p_mms_employee.bk_hash
   and p_mms_employee.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_mms_employee
    on p_mms_employee.bk_hash = l_mms_employee.bk_hash
   and p_mms_employee.l_mms_employee_id = l_mms_employee.l_mms_employee_id
 where l_mms_employee.l_mms_employee_id is null
    or (l_mms_employee.l_mms_employee_id is not null
        and l_mms_employee.dv_hash <> #l_mms_employee_inserts.source_hash)

--calculate hash and lookup to current s_mms_employee
if object_id('tempdb..#s_mms_employee_inserts') is not null drop table #s_mms_employee_inserts
create table #s_mms_employee_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_mms_Employee.bk_hash,
       stage_hash_mms_Employee.EmployeeID employee_id,
       stage_hash_mms_Employee.ActiveStatusFlag active_status_flag,
       stage_hash_mms_Employee.FirstName first_name,
       stage_hash_mms_Employee.LastName last_name,
       stage_hash_mms_Employee.MiddleInt middle_int,
       stage_hash_mms_Employee.InsertedDateTime inserted_date_time,
       stage_hash_mms_Employee.UpdatedDateTime updated_date_time,
       stage_hash_mms_Employee.HireDate hire_date,
       stage_hash_mms_Employee.TerminationDate termination_date,
       isnull(cast(stage_hash_mms_Employee.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_mms_Employee.EmployeeID as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_mms_Employee.ActiveStatusFlag as varchar(42)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_Employee.FirstName,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_Employee.LastName,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_mms_Employee.MiddleInt,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_Employee.InsertedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_Employee.UpdatedDateTime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_Employee.HireDate,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_mms_Employee.TerminationDate,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_mms_Employee
 where stage_hash_mms_Employee.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_mms_employee records
set @insert_date_time = getdate()
insert into s_mms_employee (
       bk_hash,
       employee_id,
       active_status_flag,
       first_name,
       last_name,
       middle_int,
       inserted_date_time,
       updated_date_time,
       hire_date,
       termination_date,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_mms_employee_inserts.bk_hash,
       #s_mms_employee_inserts.employee_id,
       #s_mms_employee_inserts.active_status_flag,
       #s_mms_employee_inserts.first_name,
       #s_mms_employee_inserts.last_name,
       #s_mms_employee_inserts.middle_int,
       #s_mms_employee_inserts.inserted_date_time,
       #s_mms_employee_inserts.updated_date_time,
       #s_mms_employee_inserts.hire_date,
       #s_mms_employee_inserts.termination_date,
       case when s_mms_employee.s_mms_employee_id is null then isnull(#s_mms_employee_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       2,
       #s_mms_employee_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_mms_employee_inserts
  left join p_mms_employee
    on #s_mms_employee_inserts.bk_hash = p_mms_employee.bk_hash
   and p_mms_employee.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_mms_employee
    on p_mms_employee.bk_hash = s_mms_employee.bk_hash
   and p_mms_employee.s_mms_employee_id = s_mms_employee.s_mms_employee_id
 where s_mms_employee.s_mms_employee_id is null
    or (s_mms_employee.s_mms_employee_id is not null
        and s_mms_employee.dv_hash <> #s_mms_employee_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_mms_employee @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_mms_employee @current_dv_batch_id
exec dbo.proc_d_mms_employee_history @current_dv_batch_id

end
