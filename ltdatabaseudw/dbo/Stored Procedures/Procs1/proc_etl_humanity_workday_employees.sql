CREATE PROC [dbo].[proc_etl_humanity_workday_employees] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_humanity_workday_employees

set @insert_date_time = getdate()
insert into dbo.stage_hash_humanity_workday_employees (
       bk_hash,
       employee_id,
       time_in_job_profile,
       position_id,
       hourly_amount,
       job_code,
       offering,
       region,
       primary_job,
       cost_center,
       wd_file_name,
       hire_date,
       term_date,
       employee_status,
       effective_date_for_position,
       sup_org_ref_id,
       supervisory_organization,
       job_profile,
       manager,
       location_id,
       company_id,
       anticipated_weekly_work_hours,
       pay_type,
       class_rate,
       commission_plans,
       Timezone,
       worker,
       file_arrive_date,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(employee_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(time_in_job_profile,'z#@$k%&P')+'P%#&z$@k'+isnull(position_id,'z#@$k%&P')+'P%#&z$@k'+isnull(hourly_amount,'z#@$k%&P')+'P%#&z$@k'+isnull(job_code,'z#@$k%&P')+'P%#&z$@k'+isnull(offering,'z#@$k%&P')+'P%#&z$@k'+isnull(region,'z#@$k%&P')+'P%#&z$@k'+isnull(primary_job,'z#@$k%&P')+'P%#&z$@k'+isnull(cost_center,'z#@$k%&P')+'P%#&z$@k'+isnull(wd_file_name,'z#@$k%&P')+'P%#&z$@k'+isnull(hire_date,'z#@$k%&P')+'P%#&z$@k'+isnull(term_date,'z#@$k%&P')+'P%#&z$@k'+isnull(employee_status,'z#@$k%&P')+'P%#&z$@k'+isnull(effective_date_for_position,'z#@$k%&P')+'P%#&z$@k'+isnull(sup_org_ref_id,'z#@$k%&P')+'P%#&z$@k'+isnull(supervisory_organization,'z#@$k%&P')+'P%#&z$@k'+isnull(job_profile,'z#@$k%&P')+'P%#&z$@k'+isnull(manager,'z#@$k%&P')+'P%#&z$@k'+isnull(location_id,'z#@$k%&P')+'P%#&z$@k'+isnull(company_id,'z#@$k%&P')+'P%#&z$@k'+isnull(anticipated_weekly_work_hours,'z#@$k%&P')+'P%#&z$@k'+isnull(pay_type,'z#@$k%&P')+'P%#&z$@k'+isnull(class_rate,'z#@$k%&P')+'P%#&z$@k'+isnull(commission_plans,'z#@$k%&P')+'P%#&z$@k'+isnull(Timezone,'z#@$k%&P'))),2) bk_hash,
       employee_id,
       time_in_job_profile,
       position_id,
       hourly_amount,
       job_code,
       offering,
       region,
       primary_job,
       cost_center,
       wd_file_name,
       hire_date,
       term_date,
       employee_status,
       effective_date_for_position,
       sup_org_ref_id,
       supervisory_organization,
       job_profile,
       manager,
       location_id,
       company_id,
       anticipated_weekly_work_hours,
       pay_type,
       class_rate,
       commission_plans,
       Timezone,
       worker,
       file_arrive_date,
       dummy_modified_date_time,
       isnull(cast(stage_humanity_workday_employees.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_humanity_workday_employees
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_humanity_workday_employees @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_humanity_workday_employees (
       bk_hash,
       employee_id,
       time_in_job_profile,
       position_id,
       hourly_amount,
       job_code,
       offering,
       region,
       primary_job,
       cost_center,
       wd_file_name,
       hire_date,
       term_date,
       employee_status,
       effective_date_for_position,
       sup_org_ref_id,
       supervisory_organization,
       job_profile,
       manager,
       location_id,
       company_id,
       anticipated_weekly_work_hours,
       pay_type,
       class_rate,
       commission_plans,
       Timezone,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_humanity_workday_employees.bk_hash,
       stage_hash_humanity_workday_employees.employee_id employee_id,
       stage_hash_humanity_workday_employees.time_in_job_profile time_in_job_profile,
       stage_hash_humanity_workday_employees.position_id position_id,
       stage_hash_humanity_workday_employees.hourly_amount hourly_amount,
       stage_hash_humanity_workday_employees.job_code job_code,
       stage_hash_humanity_workday_employees.offering offering,
       stage_hash_humanity_workday_employees.region region,
       stage_hash_humanity_workday_employees.primary_job primary_job,
       stage_hash_humanity_workday_employees.cost_center cost_center,
       stage_hash_humanity_workday_employees.wd_file_name wd_file_name,
       stage_hash_humanity_workday_employees.hire_date hire_date,
       stage_hash_humanity_workday_employees.term_date term_date,
       stage_hash_humanity_workday_employees.employee_status employee_status,
       stage_hash_humanity_workday_employees.effective_date_for_position effective_date_for_position,
       stage_hash_humanity_workday_employees.sup_org_ref_id sup_org_ref_id,
       stage_hash_humanity_workday_employees.supervisory_organization supervisory_organization,
       stage_hash_humanity_workday_employees.job_profile job_profile,
       stage_hash_humanity_workday_employees.manager manager,
       stage_hash_humanity_workday_employees.location_id location_id,
       stage_hash_humanity_workday_employees.company_id company_id,
       stage_hash_humanity_workday_employees.anticipated_weekly_work_hours anticipated_weekly_work_hours,
       stage_hash_humanity_workday_employees.pay_type pay_type,
       stage_hash_humanity_workday_employees.class_rate class_rate,
       stage_hash_humanity_workday_employees.commission_plans commission_plans,
       stage_hash_humanity_workday_employees.Timezone Timezone,
       isnull(cast(stage_hash_humanity_workday_employees.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       47,
       @insert_date_time,
       @user
  from stage_hash_humanity_workday_employees
  left join h_humanity_workday_employees
    on stage_hash_humanity_workday_employees.bk_hash = h_humanity_workday_employees.bk_hash
 where h_humanity_workday_employees_id is null
   and stage_hash_humanity_workday_employees.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_humanity_workday_employees
if object_id('tempdb..#l_humanity_workday_employees_inserts') is not null drop table #l_humanity_workday_employees_inserts
create table #l_humanity_workday_employees_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_humanity_workday_employees.bk_hash,
       stage_hash_humanity_workday_employees.employee_id employee_id,
       stage_hash_humanity_workday_employees.time_in_job_profile time_in_job_profile,
       stage_hash_humanity_workday_employees.position_id position_id,
       stage_hash_humanity_workday_employees.hourly_amount hourly_amount,
       stage_hash_humanity_workday_employees.job_code job_code,
       stage_hash_humanity_workday_employees.offering offering,
       stage_hash_humanity_workday_employees.region region,
       stage_hash_humanity_workday_employees.primary_job primary_job,
       stage_hash_humanity_workday_employees.cost_center cost_center,
       stage_hash_humanity_workday_employees.wd_file_name wd_file_name,
       stage_hash_humanity_workday_employees.hire_date hire_date,
       stage_hash_humanity_workday_employees.term_date term_date,
       stage_hash_humanity_workday_employees.employee_status employee_status,
       stage_hash_humanity_workday_employees.effective_date_for_position effective_date_for_position,
       stage_hash_humanity_workday_employees.sup_org_ref_id sup_org_ref_id,
       stage_hash_humanity_workday_employees.supervisory_organization supervisory_organization,
       stage_hash_humanity_workday_employees.job_profile job_profile,
       stage_hash_humanity_workday_employees.manager manager,
       stage_hash_humanity_workday_employees.location_id location_id,
       stage_hash_humanity_workday_employees.company_id company_id,
       stage_hash_humanity_workday_employees.anticipated_weekly_work_hours anticipated_weekly_work_hours,
       stage_hash_humanity_workday_employees.pay_type pay_type,
       stage_hash_humanity_workday_employees.class_rate class_rate,
       stage_hash_humanity_workday_employees.commission_plans commission_plans,
       stage_hash_humanity_workday_employees.Timezone Timezone,
       isnull(cast(stage_hash_humanity_workday_employees.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_humanity_workday_employees.employee_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_workday_employees.time_in_job_profile,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_workday_employees.position_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_workday_employees.hourly_amount,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_workday_employees.job_code,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_workday_employees.offering,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_workday_employees.region,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_workday_employees.primary_job,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_workday_employees.cost_center,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_workday_employees.wd_file_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_workday_employees.hire_date,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_workday_employees.term_date,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_workday_employees.employee_status,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_workday_employees.effective_date_for_position,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_workday_employees.sup_org_ref_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_workday_employees.supervisory_organization,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_workday_employees.job_profile,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_workday_employees.manager,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_workday_employees.location_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_workday_employees.company_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_workday_employees.anticipated_weekly_work_hours,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_workday_employees.pay_type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_workday_employees.class_rate,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_workday_employees.commission_plans,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_workday_employees.Timezone,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_humanity_workday_employees
 where stage_hash_humanity_workday_employees.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_humanity_workday_employees records
set @insert_date_time = getdate()
insert into l_humanity_workday_employees (
       bk_hash,
       employee_id,
       time_in_job_profile,
       position_id,
       hourly_amount,
       job_code,
       offering,
       region,
       primary_job,
       cost_center,
       wd_file_name,
       hire_date,
       term_date,
       employee_status,
       effective_date_for_position,
       sup_org_ref_id,
       supervisory_organization,
       job_profile,
       manager,
       location_id,
       company_id,
       anticipated_weekly_work_hours,
       pay_type,
       class_rate,
       commission_plans,
       Timezone,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_humanity_workday_employees_inserts.bk_hash,
       #l_humanity_workday_employees_inserts.employee_id,
       #l_humanity_workday_employees_inserts.time_in_job_profile,
       #l_humanity_workday_employees_inserts.position_id,
       #l_humanity_workday_employees_inserts.hourly_amount,
       #l_humanity_workday_employees_inserts.job_code,
       #l_humanity_workday_employees_inserts.offering,
       #l_humanity_workday_employees_inserts.region,
       #l_humanity_workday_employees_inserts.primary_job,
       #l_humanity_workday_employees_inserts.cost_center,
       #l_humanity_workday_employees_inserts.wd_file_name,
       #l_humanity_workday_employees_inserts.hire_date,
       #l_humanity_workday_employees_inserts.term_date,
       #l_humanity_workday_employees_inserts.employee_status,
       #l_humanity_workday_employees_inserts.effective_date_for_position,
       #l_humanity_workday_employees_inserts.sup_org_ref_id,
       #l_humanity_workday_employees_inserts.supervisory_organization,
       #l_humanity_workday_employees_inserts.job_profile,
       #l_humanity_workday_employees_inserts.manager,
       #l_humanity_workday_employees_inserts.location_id,
       #l_humanity_workday_employees_inserts.company_id,
       #l_humanity_workday_employees_inserts.anticipated_weekly_work_hours,
       #l_humanity_workday_employees_inserts.pay_type,
       #l_humanity_workday_employees_inserts.class_rate,
       #l_humanity_workday_employees_inserts.commission_plans,
       #l_humanity_workday_employees_inserts.Timezone,
       case when l_humanity_workday_employees.l_humanity_workday_employees_id is null then isnull(#l_humanity_workday_employees_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       47,
       #l_humanity_workday_employees_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_humanity_workday_employees_inserts
  left join p_humanity_workday_employees
    on #l_humanity_workday_employees_inserts.bk_hash = p_humanity_workday_employees.bk_hash
   and p_humanity_workday_employees.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_humanity_workday_employees
    on p_humanity_workday_employees.bk_hash = l_humanity_workday_employees.bk_hash
   and p_humanity_workday_employees.l_humanity_workday_employees_id = l_humanity_workday_employees.l_humanity_workday_employees_id
 where l_humanity_workday_employees.l_humanity_workday_employees_id is null
    or (l_humanity_workday_employees.l_humanity_workday_employees_id is not null
        and l_humanity_workday_employees.dv_hash <> #l_humanity_workday_employees_inserts.source_hash)

--calculate hash and lookup to current s_humanity_workday_employees
if object_id('tempdb..#s_humanity_workday_employees_inserts') is not null drop table #s_humanity_workday_employees_inserts
create table #s_humanity_workday_employees_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_humanity_workday_employees.bk_hash,
       stage_hash_humanity_workday_employees.employee_id employee_id,
       stage_hash_humanity_workday_employees.time_in_job_profile time_in_job_profile,
       stage_hash_humanity_workday_employees.position_id position_id,
       stage_hash_humanity_workday_employees.hourly_amount hourly_amount,
       stage_hash_humanity_workday_employees.job_code job_code,
       stage_hash_humanity_workday_employees.offering offering,
       stage_hash_humanity_workday_employees.region region,
       stage_hash_humanity_workday_employees.primary_job primary_job,
       stage_hash_humanity_workday_employees.cost_center cost_center,
       stage_hash_humanity_workday_employees.wd_file_name wd_file_name,
       stage_hash_humanity_workday_employees.hire_date hire_date,
       stage_hash_humanity_workday_employees.term_date term_date,
       stage_hash_humanity_workday_employees.employee_status employee_status,
       stage_hash_humanity_workday_employees.effective_date_for_position effective_date_for_position,
       stage_hash_humanity_workday_employees.sup_org_ref_id sup_org_ref_id,
       stage_hash_humanity_workday_employees.supervisory_organization supervisory_organization,
       stage_hash_humanity_workday_employees.job_profile job_profile,
       stage_hash_humanity_workday_employees.manager manager,
       stage_hash_humanity_workday_employees.location_id location_id,
       stage_hash_humanity_workday_employees.company_id company_id,
       stage_hash_humanity_workday_employees.anticipated_weekly_work_hours anticipated_weekly_work_hours,
       stage_hash_humanity_workday_employees.pay_type pay_type,
       stage_hash_humanity_workday_employees.class_rate class_rate,
       stage_hash_humanity_workday_employees.commission_plans commission_plans,
       stage_hash_humanity_workday_employees.file_arrive_date file_arrive_date,
       stage_hash_humanity_workday_employees.timezone timezone,
       stage_hash_humanity_workday_employees.worker worker,
       stage_hash_humanity_workday_employees.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_humanity_workday_employees.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_humanity_workday_employees.employee_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_workday_employees.time_in_job_profile,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_workday_employees.position_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_workday_employees.hourly_amount,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_workday_employees.job_code,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_workday_employees.offering,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_workday_employees.region,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_workday_employees.primary_job,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_workday_employees.cost_center,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_workday_employees.wd_file_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_workday_employees.hire_date,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_workday_employees.term_date,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_workday_employees.employee_status,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_workday_employees.effective_date_for_position,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_workday_employees.sup_org_ref_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_workday_employees.supervisory_organization,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_workday_employees.job_profile,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_workday_employees.manager,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_workday_employees.location_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_workday_employees.company_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_workday_employees.anticipated_weekly_work_hours,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_workday_employees.pay_type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_workday_employees.class_rate,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_workday_employees.commission_plans,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_workday_employees.file_arrive_date,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_workday_employees.timezone,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_workday_employees.worker,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_humanity_workday_employees
 where stage_hash_humanity_workday_employees.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_humanity_workday_employees records
set @insert_date_time = getdate()
insert into s_humanity_workday_employees (
       bk_hash,
       employee_id,
       time_in_job_profile,
       position_id,
       hourly_amount,
       job_code,
       offering,
       region,
       primary_job,
       cost_center,
       wd_file_name,
       hire_date,
       term_date,
       employee_status,
       effective_date_for_position,
       sup_org_ref_id,
       supervisory_organization,
       job_profile,
       manager,
       location_id,
       company_id,
       anticipated_weekly_work_hours,
       pay_type,
       class_rate,
       commission_plans,
       file_arrive_date,
       timezone,
       worker,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_humanity_workday_employees_inserts.bk_hash,
       #s_humanity_workday_employees_inserts.employee_id,
       #s_humanity_workday_employees_inserts.time_in_job_profile,
       #s_humanity_workday_employees_inserts.position_id,
       #s_humanity_workday_employees_inserts.hourly_amount,
       #s_humanity_workday_employees_inserts.job_code,
       #s_humanity_workday_employees_inserts.offering,
       #s_humanity_workday_employees_inserts.region,
       #s_humanity_workday_employees_inserts.primary_job,
       #s_humanity_workday_employees_inserts.cost_center,
       #s_humanity_workday_employees_inserts.wd_file_name,
       #s_humanity_workday_employees_inserts.hire_date,
       #s_humanity_workday_employees_inserts.term_date,
       #s_humanity_workday_employees_inserts.employee_status,
       #s_humanity_workday_employees_inserts.effective_date_for_position,
       #s_humanity_workday_employees_inserts.sup_org_ref_id,
       #s_humanity_workday_employees_inserts.supervisory_organization,
       #s_humanity_workday_employees_inserts.job_profile,
       #s_humanity_workday_employees_inserts.manager,
       #s_humanity_workday_employees_inserts.location_id,
       #s_humanity_workday_employees_inserts.company_id,
       #s_humanity_workday_employees_inserts.anticipated_weekly_work_hours,
       #s_humanity_workday_employees_inserts.pay_type,
       #s_humanity_workday_employees_inserts.class_rate,
       #s_humanity_workday_employees_inserts.commission_plans,
       #s_humanity_workday_employees_inserts.file_arrive_date,
       #s_humanity_workday_employees_inserts.timezone,
       #s_humanity_workday_employees_inserts.worker,
       #s_humanity_workday_employees_inserts.dummy_modified_date_time,
       case when s_humanity_workday_employees.s_humanity_workday_employees_id is null then isnull(#s_humanity_workday_employees_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       47,
       #s_humanity_workday_employees_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_humanity_workday_employees_inserts
  left join p_humanity_workday_employees
    on #s_humanity_workday_employees_inserts.bk_hash = p_humanity_workday_employees.bk_hash
   and p_humanity_workday_employees.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_humanity_workday_employees
    on p_humanity_workday_employees.bk_hash = s_humanity_workday_employees.bk_hash
   and p_humanity_workday_employees.s_humanity_workday_employees_id = s_humanity_workday_employees.s_humanity_workday_employees_id
 where s_humanity_workday_employees.s_humanity_workday_employees_id is null
    or (s_humanity_workday_employees.s_humanity_workday_employees_id is not null
        and s_humanity_workday_employees.dv_hash <> #s_humanity_workday_employees_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_humanity_workday_employees @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_humanity_workday_employees @current_dv_batch_id

end
