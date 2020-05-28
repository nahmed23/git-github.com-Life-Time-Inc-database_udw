CREATE PROC [dbo].[proc_p_humanity_workday_employees] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

declare @wf_name varchar(500) = 'wf_dv_humanity_workday_employees'
declare @last_successful_dv_batch_id bigint = (select isnull(max(dv_batch_id) + 1,-3) from dbo.dv_job_status_history where job_name = @wf_name and job_status = 'Complete')
declare @process_dv_batch_id bigint = case when @current_dv_batch_id <= @last_successful_dv_batch_id then @current_dv_batch_id else @last_successful_dv_batch_id end

if object_id('tempdb..#process') is not null drop table #process
create table dbo.#process with(distribution=hash(bk_hash),location= user_db, clustered index (bk_hash)) as
select bk_hash
  from l_humanity_workday_employees
 where dv_batch_id >= @process_dv_batch_id
union
select bk_hash
  from s_humanity_workday_employees
 where dv_batch_id >= @process_dv_batch_id

delete from p_humanity_workday_employees where bk_hash in (select bk_hash from #process)

insert into dbo.p_humanity_workday_employees(
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
        l_humanity_workday_employees_id,
        s_humanity_workday_employees_id,
        dv_inserted_date_time,
        dv_insert_user,
        dv_load_date_time,
        dv_load_end_date_time,
        dv_batch_id)
select h.bk_hash,
       h.employee_id,
       h.time_in_job_profile,
       h.position_id,
       h.hourly_amount,
       h.job_code,
       h.offering,
       h.region,
       h.primary_job,
       h.cost_center,
       h.wd_file_name,
       h.hire_date,
       h.term_date,
       h.employee_status,
       h.effective_date_for_position,
       h.sup_org_ref_id,
       h.supervisory_organization,
       h.job_profile,
       h.manager,
       h.location_id,
       h.company_id,
       h.anticipated_weekly_work_hours,
       h.pay_type,
       h.class_rate,
       h.commission_plans,
       h.Timezone,
       l_humanity_workday_employees.l_humanity_workday_employees_id,
       s_humanity_workday_employees.s_humanity_workday_employees_id,
       getdate(),
       suser_sname(),
       case when l_humanity_workday_employees.dv_load_date_time >= s_humanity_workday_employees.dv_load_date_time then l_humanity_workday_employees.dv_load_date_time
            else s_humanity_workday_employees.dv_load_date_time end dv_load_date_time,
       'Dec 31, 9999' dv_load_end_date_time,
       case when l_humanity_workday_employees.dv_batch_id >= s_humanity_workday_employees.dv_batch_id then l_humanity_workday_employees.dv_batch_id
            else s_humanity_workday_employees.dv_batch_id end dv_batch_id
  from h_humanity_workday_employees h
  join (select bk_hash, l_humanity_workday_employees_id, dv_batch_id, dv_load_date_time
          from (select bk_hash, l_humanity_workday_employees_id, dv_batch_id, dv_load_date_time, rank() over (partition by bk_hash order by dv_load_date_time desc,l_humanity_workday_employees_id desc) r from l_humanity_workday_employees where bk_hash in (select bk_hash from #process)) x
         where r = 1) l_humanity_workday_employees
    on h.bk_hash = l_humanity_workday_employees.bk_hash
  join (select bk_hash, s_humanity_workday_employees_id, dv_batch_id, dv_load_date_time
          from (select bk_hash, s_humanity_workday_employees_id, dv_batch_id, dv_load_date_time, rank() over (partition by bk_hash order by dv_load_date_time desc,s_humanity_workday_employees_id desc) r from s_humanity_workday_employees where bk_hash in (select bk_hash from #process)) x
         where r = 1) s_humanity_workday_employees
    on h.bk_hash = s_humanity_workday_employees.bk_hash
 where h.bk_hash in (select bk_hash from #process)
end