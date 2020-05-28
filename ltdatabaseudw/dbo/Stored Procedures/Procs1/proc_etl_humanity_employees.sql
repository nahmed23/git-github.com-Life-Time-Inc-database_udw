CREATE PROC [dbo].[proc_etl_humanity_employees] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_humanity_employees

set @insert_date_time = getdate()
insert into dbo.stage_hash_humanity_employees (
       bk_hash,
       employee_id,
       employee_eid,
       employee_name,
       employee_email,
       company_id,
       company_name,
       deleted_flg,
       employee_status,
       employee_role,
       position_name,
       location_name,
       employee_to_see_wages,
       last_active_date_utc,
       user_timezone,
       workday_position_id,
       ltf_file_name,
       file_arrive_date,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(employee_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(employee_eid,'z#@$k%&P')+'P%#&z$@k'+isnull(employee_name,'z#@$k%&P')+'P%#&z$@k'+isnull(employee_email,'z#@$k%&P')+'P%#&z$@k'+isnull(company_name,'z#@$k%&P')+'P%#&z$@k'+isnull(deleted_flg,'z#@$k%&P')+'P%#&z$@k'+isnull(employee_status,'z#@$k%&P')+'P%#&z$@k'+isnull(employee_role,'z#@$k%&P')+'P%#&z$@k'+isnull(position_name,'z#@$k%&P')+'P%#&z$@k'+isnull(location_name,'z#@$k%&P')+'P%#&z$@k'+isnull(employee_to_see_wages,'z#@$k%&P')+'P%#&z$@k'+isnull(last_active_date_utc,'z#@$k%&P')+'P%#&z$@k'+isnull(user_timezone,'z#@$k%&P')+'P%#&z$@k'+isnull(workday_position_id,'z#@$k%&P')+'P%#&z$@k'+isnull(company_id,'z#@$k%&P')+'P%#&z$@k'+isnull(ltf_file_name,'z#@$k%&P'))),2) bk_hash,
       employee_id,
       employee_eid,
       employee_name,
       employee_email,
       company_id,
       company_name,
       deleted_flg,
       employee_status,
       employee_role,
       position_name,
       location_name,
       employee_to_see_wages,
       last_active_date_utc,
       user_timezone,
       workday_position_id,
       ltf_file_name,
       file_arrive_date,
       dummy_modified_date_time,
       isnull(cast(stage_humanity_employees.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_humanity_employees
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_Humanity_employees @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_Humanity_employees (
       bk_hash,
       employee_id,
       employee_eid,
       employee_name,
       employee_email,
       company_name,
       deleted_flg,
       employee_status,
       employee_role,
       position_name,
       location_name,
       employee_to_see_wages,
       last_active_date_utc,
       user_timezone,
       workday_position_id,
       company_id,
       ltf_file_name,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_humanity_employees.bk_hash,
       stage_hash_humanity_employees.employee_id employee_id,
       stage_hash_humanity_employees.employee_eid employee_eid,
       stage_hash_humanity_employees.employee_name employee_name,
       stage_hash_humanity_employees.employee_email employee_email,
       stage_hash_humanity_employees.company_name company_name,
       stage_hash_humanity_employees.deleted_flg deleted_flg,
       stage_hash_humanity_employees.employee_status employee_status,
       stage_hash_humanity_employees.employee_role employee_role,
       stage_hash_humanity_employees.position_name position_name,
       stage_hash_humanity_employees.location_name location_name,
       stage_hash_humanity_employees.employee_to_see_wages employee_to_see_wages,
       stage_hash_humanity_employees.last_active_date_utc last_active_date_utc,
       stage_hash_humanity_employees.user_timezone user_timezone,
       stage_hash_humanity_employees.workday_position_id workday_position_id,
       stage_hash_humanity_employees.company_id company_id,
       stage_hash_humanity_employees.ltf_file_name ltf_file_name,
       isnull(cast(stage_hash_humanity_employees.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       47,
       @insert_date_time,
       @user
  from stage_hash_humanity_employees
  left join h_Humanity_employees
    on stage_hash_humanity_employees.bk_hash = h_Humanity_employees.bk_hash
 where h_Humanity_employees_id is null
   and stage_hash_humanity_employees.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_humanity_employees
if object_id('tempdb..#l_humanity_employees_inserts') is not null drop table #l_humanity_employees_inserts
create table #l_humanity_employees_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_humanity_employees.bk_hash,
       stage_hash_humanity_employees.employee_id employee_id,
       stage_hash_humanity_employees.employee_eid employee_eid,
       stage_hash_humanity_employees.employee_name employee_name,
       stage_hash_humanity_employees.employee_email employee_email,
       stage_hash_humanity_employees.company_name company_name,
       stage_hash_humanity_employees.deleted_flg deleted_flg,
       stage_hash_humanity_employees.employee_status employee_status,
       stage_hash_humanity_employees.employee_role employee_role,
       stage_hash_humanity_employees.position_name position_name,
       stage_hash_humanity_employees.location_name location_name,
       stage_hash_humanity_employees.employee_to_see_wages employee_to_see_wages,
       stage_hash_humanity_employees.last_active_date_utc last_active_date_utc,
       stage_hash_humanity_employees.user_timezone user_timezone,
       stage_hash_humanity_employees.workday_position_id workday_position_id,
       stage_hash_humanity_employees.ltf_file_name ltf_file_name,
       stage_hash_humanity_employees.company_id company_id,
       isnull(cast(stage_hash_humanity_employees.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_humanity_employees.employee_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_employees.employee_eid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_employees.employee_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_employees.employee_email,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_employees.company_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_employees.deleted_flg,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_employees.employee_status,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_employees.employee_role,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_employees.position_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_employees.location_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_employees.employee_to_see_wages,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_employees.last_active_date_utc,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_employees.user_timezone,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_employees.workday_position_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_employees.ltf_file_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_employees.company_id,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_humanity_employees
 where stage_hash_humanity_employees.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_humanity_employees records
set @insert_date_time = getdate()
insert into l_humanity_employees (
       bk_hash,
       employee_id,
       employee_eid,
       employee_name,
       employee_email,
       company_name,
       deleted_flg,
       employee_status,
       employee_role,
       position_name,
       location_name,
       employee_to_see_wages,
       last_active_date_utc,
       user_timezone,
       workday_position_id,
       ltf_file_name,
       company_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_humanity_employees_inserts.bk_hash,
       #l_humanity_employees_inserts.employee_id,
       #l_humanity_employees_inserts.employee_eid,
       #l_humanity_employees_inserts.employee_name,
       #l_humanity_employees_inserts.employee_email,
       #l_humanity_employees_inserts.company_name,
       #l_humanity_employees_inserts.deleted_flg,
       #l_humanity_employees_inserts.employee_status,
       #l_humanity_employees_inserts.employee_role,
       #l_humanity_employees_inserts.position_name,
       #l_humanity_employees_inserts.location_name,
       #l_humanity_employees_inserts.employee_to_see_wages,
       #l_humanity_employees_inserts.last_active_date_utc,
       #l_humanity_employees_inserts.user_timezone,
       #l_humanity_employees_inserts.workday_position_id,
       #l_humanity_employees_inserts.ltf_file_name,
       #l_humanity_employees_inserts.company_id,
       case when l_humanity_employees.l_humanity_employees_id is null then isnull(#l_humanity_employees_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       47,
       #l_humanity_employees_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_humanity_employees_inserts
  left join p_Humanity_employees
    on #l_humanity_employees_inserts.bk_hash = p_Humanity_employees.bk_hash
   and p_Humanity_employees.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_humanity_employees
    on p_Humanity_employees.bk_hash = l_humanity_employees.bk_hash
   and p_Humanity_employees.l_humanity_employees_id = l_humanity_employees.l_humanity_employees_id
 where l_humanity_employees.l_humanity_employees_id is null
    or (l_humanity_employees.l_humanity_employees_id is not null
        and l_humanity_employees.dv_hash <> #l_humanity_employees_inserts.source_hash)

--calculate hash and lookup to current s_Humanity_employees
if object_id('tempdb..#s_Humanity_employees_inserts') is not null drop table #s_Humanity_employees_inserts
create table #s_Humanity_employees_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_humanity_employees.bk_hash,
       stage_hash_humanity_employees.employee_id employee_id,
       stage_hash_humanity_employees.employee_eid employee_eid,
       stage_hash_humanity_employees.employee_name employee_name,
       stage_hash_humanity_employees.employee_email employee_email,
       stage_hash_humanity_employees.company_name company_name,
       stage_hash_humanity_employees.deleted_flg deleted_flg,
       stage_hash_humanity_employees.employee_status employee_status,
       stage_hash_humanity_employees.employee_role employee_role,
       stage_hash_humanity_employees.position_name position_name,
       stage_hash_humanity_employees.location_name location_name,
       stage_hash_humanity_employees.employee_to_see_wages employee_to_see_wages,
       stage_hash_humanity_employees.last_active_date_utc last_active_date_utc,
       stage_hash_humanity_employees.user_timezone user_timezone,
       stage_hash_humanity_employees.workday_position_id workday_position_id,
       stage_hash_humanity_employees.ltf_file_name ltf_file_name,
       stage_hash_humanity_employees.company_id company_id,
       stage_hash_humanity_employees.File_arrive_date file_arrive_date,
       stage_hash_humanity_employees.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_humanity_employees.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_humanity_employees.employee_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_employees.employee_eid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_employees.employee_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_employees.employee_email,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_employees.company_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_employees.deleted_flg,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_employees.employee_status,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_employees.employee_role,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_employees.position_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_employees.location_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_employees.employee_to_see_wages,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_employees.last_active_date_utc,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_employees.user_timezone,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_employees.workday_position_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_employees.ltf_file_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_employees.company_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_employees.File_arrive_date,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_humanity_employees
 where stage_hash_humanity_employees.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_Humanity_employees records
set @insert_date_time = getdate()
insert into s_Humanity_employees (
       bk_hash,
       employee_id,
       employee_eid,
       employee_name,
       employee_email,
       company_name,
       deleted_flg,
       employee_status,
       employee_role,
       position_name,
       location_name,
       employee_to_see_wages,
       last_active_date_utc,
       user_timezone,
       workday_position_id,
       ltf_file_name,
       company_id,
       file_arrive_date,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_Humanity_employees_inserts.bk_hash,
       #s_Humanity_employees_inserts.employee_id,
       #s_Humanity_employees_inserts.employee_eid,
       #s_Humanity_employees_inserts.employee_name,
       #s_Humanity_employees_inserts.employee_email,
       #s_Humanity_employees_inserts.company_name,
       #s_Humanity_employees_inserts.deleted_flg,
       #s_Humanity_employees_inserts.employee_status,
       #s_Humanity_employees_inserts.employee_role,
       #s_Humanity_employees_inserts.position_name,
       #s_Humanity_employees_inserts.location_name,
       #s_Humanity_employees_inserts.employee_to_see_wages,
       #s_Humanity_employees_inserts.last_active_date_utc,
       #s_Humanity_employees_inserts.user_timezone,
       #s_Humanity_employees_inserts.workday_position_id,
       #s_Humanity_employees_inserts.ltf_file_name,
       #s_Humanity_employees_inserts.company_id,
       #s_Humanity_employees_inserts.file_arrive_date,
       #s_Humanity_employees_inserts.dummy_modified_date_time,
       case when s_Humanity_employees.s_Humanity_employees_id is null then isnull(#s_Humanity_employees_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       47,
       #s_Humanity_employees_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_Humanity_employees_inserts
  left join p_Humanity_employees
    on #s_Humanity_employees_inserts.bk_hash = p_Humanity_employees.bk_hash
   and p_Humanity_employees.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_Humanity_employees
    on p_Humanity_employees.bk_hash = s_Humanity_employees.bk_hash
   and p_Humanity_employees.s_Humanity_employees_id = s_Humanity_employees.s_Humanity_employees_id
 where s_Humanity_employees.s_Humanity_employees_id is null
    or (s_Humanity_employees.s_Humanity_employees_id is not null
        and s_Humanity_employees.dv_hash <> #s_Humanity_employees_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_Humanity_employees @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_humanity_employees @current_dv_batch_id

end
