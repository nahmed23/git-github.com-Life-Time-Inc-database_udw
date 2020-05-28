CREATE PROC [dbo].[proc_etl_workday_employee] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_workday_employee

set @insert_date_time = getdate()
insert into dbo.stage_hash_workday_employee (
       bk_hash,
       employee_id,
       manager_id,
       mms_club_id,
       first_name,
       middle_name,
       last_name,
       preferred_first_name,
       preferred_middle_name,
       preferred_last_name,
       primary_work_email,
       phone_number,
       hire_date,
       termination_date,
       active_status,
       job_levels,
       job_families,
       job_sub_families,
       job_profiles,
       business_titles,
       marketing_titles,
       job_codes,
       subordinates,
       category,
       certifications,
       cf_nickname,
       is_primary,
       cf_employment_status,
       pay_rate_for_all_positions,
       three_digit_club_codes,
       jan_one,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(employee_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       employee_id,
       manager_id,
       mms_club_id,
       first_name,
       middle_name,
       last_name,
       preferred_first_name,
       preferred_middle_name,
       preferred_last_name,
       primary_work_email,
       phone_number,
       hire_date,
       termination_date,
       active_status,
       job_levels,
       job_families,
       job_sub_families,
       job_profiles,
       business_titles,
       marketing_titles,
       job_codes,
       subordinates,
       category,
       certifications,
       cf_nickname,
       is_primary,
       cf_employment_status,
       pay_rate_for_all_positions,
       three_digit_club_codes,
       jan_one,
       isnull(cast(stage_workday_employee.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_workday_employee
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_workday_employee @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_workday_employee (
       bk_hash,
       employee_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_workday_employee.bk_hash,
       stage_hash_workday_employee.employee_id employee_id,
       isnull(cast(stage_hash_workday_employee.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       32,
       @insert_date_time,
       @user
  from stage_hash_workday_employee
  left join h_workday_employee
    on stage_hash_workday_employee.bk_hash = h_workday_employee.bk_hash
 where h_workday_employee_id is null
   and stage_hash_workday_employee.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_workday_employee
if object_id('tempdb..#l_workday_employee_inserts') is not null drop table #l_workday_employee_inserts
create table #l_workday_employee_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_workday_employee.bk_hash,
       stage_hash_workday_employee.employee_id employee_id,
       stage_hash_workday_employee.manager_id manager_id,
       stage_hash_workday_employee.mms_club_id mms_club_id,
       stage_hash_workday_employee.three_digit_club_codes workday_club_id,
       isnull(cast(stage_hash_workday_employee.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_workday_employee.employee_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_workday_employee.manager_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_workday_employee.mms_club_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_workday_employee.three_digit_club_codes,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_workday_employee
 where stage_hash_workday_employee.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_workday_employee records
set @insert_date_time = getdate()
insert into l_workday_employee (
       bk_hash,
       employee_id,
       manager_id,
       mms_club_id,
       workday_club_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_workday_employee_inserts.bk_hash,
       #l_workday_employee_inserts.employee_id,
       #l_workday_employee_inserts.manager_id,
       #l_workday_employee_inserts.mms_club_id,
       #l_workday_employee_inserts.workday_club_id,
       case when l_workday_employee.l_workday_employee_id is null then isnull(#l_workday_employee_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       32,
       #l_workday_employee_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_workday_employee_inserts
  left join p_workday_employee
    on #l_workday_employee_inserts.bk_hash = p_workday_employee.bk_hash
   and p_workday_employee.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_workday_employee
    on p_workday_employee.bk_hash = l_workday_employee.bk_hash
   and p_workday_employee.l_workday_employee_id = l_workday_employee.l_workday_employee_id
 where l_workday_employee.l_workday_employee_id is null
    or (l_workday_employee.l_workday_employee_id is not null
        and l_workday_employee.dv_hash <> #l_workday_employee_inserts.source_hash)

--calculate hash and lookup to current s_workday_employee
if object_id('tempdb..#s_workday_employee_inserts') is not null drop table #s_workday_employee_inserts
create table #s_workday_employee_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_workday_employee.bk_hash,
       stage_hash_workday_employee.employee_id employee_id,
       stage_hash_workday_employee.first_name first_name,
       stage_hash_workday_employee.middle_name middle_name,
       stage_hash_workday_employee.last_name last_name,
       stage_hash_workday_employee.preferred_first_name preferred_first_name,
       stage_hash_workday_employee.preferred_middle_name preferred_middle_name,
       stage_hash_workday_employee.preferred_last_name preferred_last_name,
       stage_hash_workday_employee.primary_work_email primary_work_email,
       stage_hash_workday_employee.phone_number phone_number,
       stage_hash_workday_employee.hire_date hire_date,
       stage_hash_workday_employee.termination_date termination_date,
       stage_hash_workday_employee.active_status active_status,
       stage_hash_workday_employee.job_levels job_levels,
       stage_hash_workday_employee.job_families job_families,
       stage_hash_workday_employee.job_sub_families job_sub_families,
       stage_hash_workday_employee.job_profiles job_profiles,
       stage_hash_workday_employee.business_titles business_titles,
       stage_hash_workday_employee.marketing_titles marketing_titles,
       stage_hash_workday_employee.job_codes job_codes,
       stage_hash_workday_employee.subordinates subordinates,
       stage_hash_workday_employee.category category,
       stage_hash_workday_employee.certifications certifications,
       stage_hash_workday_employee.cf_nickname cf_nickname,
       stage_hash_workday_employee.is_primary is_primary,
       stage_hash_workday_employee.cf_employment_status cf_employment_status,
       stage_hash_workday_employee.pay_rate_for_all_positions pay_rate_for_all_positions,
       stage_hash_workday_employee.jan_one jan_one,
       isnull(cast(stage_hash_workday_employee.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_workday_employee.employee_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_workday_employee.first_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_workday_employee.middle_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_workday_employee.last_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_workday_employee.preferred_first_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_workday_employee.preferred_middle_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_workday_employee.preferred_last_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_workday_employee.primary_work_email,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_workday_employee.phone_number,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_workday_employee.hire_date,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_workday_employee.termination_date,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_workday_employee.active_status,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_workday_employee.job_levels,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_workday_employee.job_families,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_workday_employee.job_sub_families,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_workday_employee.job_profiles,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_workday_employee.business_titles,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_workday_employee.marketing_titles,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_workday_employee.job_codes,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_workday_employee.subordinates,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_workday_employee.category,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_workday_employee.certifications,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_workday_employee.cf_nickname,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_workday_employee.is_primary,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_workday_employee.cf_employment_status,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_workday_employee.pay_rate_for_all_positions,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_workday_employee
 where stage_hash_workday_employee.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_workday_employee records
set @insert_date_time = getdate()
insert into s_workday_employee (
       bk_hash,
       employee_id,
       first_name,
       middle_name,
       last_name,
       preferred_first_name,
       preferred_middle_name,
       preferred_last_name,
       primary_work_email,
       phone_number,
       hire_date,
       termination_date,
       active_status,
       job_levels,
       job_families,
       job_sub_families,
       job_profiles,
       business_titles,
       marketing_titles,
       job_codes,
       subordinates,
       category,
       certifications,
       cf_nickname,
       is_primary,
       cf_employment_status,
       pay_rate_for_all_positions,
       jan_one,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_workday_employee_inserts.bk_hash,
       #s_workday_employee_inserts.employee_id,
       #s_workday_employee_inserts.first_name,
       #s_workday_employee_inserts.middle_name,
       #s_workday_employee_inserts.last_name,
       #s_workday_employee_inserts.preferred_first_name,
       #s_workday_employee_inserts.preferred_middle_name,
       #s_workday_employee_inserts.preferred_last_name,
       #s_workday_employee_inserts.primary_work_email,
       #s_workday_employee_inserts.phone_number,
       #s_workday_employee_inserts.hire_date,
       #s_workday_employee_inserts.termination_date,
       #s_workday_employee_inserts.active_status,
       #s_workday_employee_inserts.job_levels,
       #s_workday_employee_inserts.job_families,
       #s_workday_employee_inserts.job_sub_families,
       #s_workday_employee_inserts.job_profiles,
       #s_workday_employee_inserts.business_titles,
       #s_workday_employee_inserts.marketing_titles,
       #s_workday_employee_inserts.job_codes,
       #s_workday_employee_inserts.subordinates,
       #s_workday_employee_inserts.category,
       #s_workday_employee_inserts.certifications,
       #s_workday_employee_inserts.cf_nickname,
       #s_workday_employee_inserts.is_primary,
       #s_workday_employee_inserts.cf_employment_status,
       #s_workday_employee_inserts.pay_rate_for_all_positions,
       #s_workday_employee_inserts.jan_one,
       case when s_workday_employee.s_workday_employee_id is null then isnull(#s_workday_employee_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       32,
       #s_workday_employee_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_workday_employee_inserts
  left join p_workday_employee
    on #s_workday_employee_inserts.bk_hash = p_workday_employee.bk_hash
   and p_workday_employee.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_workday_employee
    on p_workday_employee.bk_hash = s_workday_employee.bk_hash
   and p_workday_employee.s_workday_employee_id = s_workday_employee.s_workday_employee_id
 where s_workday_employee.s_workday_employee_id is null
    or (s_workday_employee.s_workday_employee_id is not null
        and s_workday_employee.dv_hash <> #s_workday_employee_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_workday_employee @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_workday_employee @current_dv_batch_id
exec dbo.proc_d_workday_employee_history @current_dv_batch_id

end
