CREATE PROC [dbo].[proc_p_humanity_employees] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

declare @wf_name varchar(500) = 'wf_dv_humanity_employees'
declare @last_successful_dv_batch_id bigint = (select isnull(max(dv_batch_id) + 1,-3) from dbo.dv_job_status_history where job_name = @wf_name and job_status = 'Complete')
declare @process_dv_batch_id bigint = case when @current_dv_batch_id <= @last_successful_dv_batch_id then @current_dv_batch_id else @last_successful_dv_batch_id end

if object_id('tempdb..#process') is not null drop table #process
create table dbo.#process with(distribution=hash(bk_hash),location= user_db, clustered index (bk_hash)) as
select bk_hash
  from l_humanity_employees
 where dv_batch_id >= @process_dv_batch_id
union
select bk_hash
  from s_Humanity_employees
 where dv_batch_id >= @process_dv_batch_id

delete from p_humanity_employees where bk_hash in (select bk_hash from #process)

insert into dbo.p_humanity_employees(
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
        l_Humanity_employees_id,
        s_Humanity_employees_id,
        dv_inserted_date_time,
        dv_insert_user,
        dv_load_date_time,
        dv_load_end_date_time,
        dv_batch_id)
select h.bk_hash,
       h.employee_id,
       h.employee_eid,
       h.employee_name,
       h.employee_email,
       h.company_name,
       h.deleted_flg,
       h.employee_status,
       h.employee_role,
       h.position_name,
       h.location_name,
       h.employee_to_see_wages,
       h.last_active_date_utc,
       h.user_timezone,
       h.workday_position_id,
       h.ltf_file_name,
       h.company_id,
       l_humanity_employees.l_Humanity_employees_id,
       s_Humanity_employees.s_Humanity_employees_id,
       getdate(),
       suser_sname(),
       case when l_humanity_employees.dv_load_date_time >= s_Humanity_employees.dv_load_date_time then l_humanity_employees.dv_load_date_time
            else s_Humanity_employees.dv_load_date_time end dv_load_date_time,
       'Dec 31, 9999' dv_load_end_date_time,
       case when l_humanity_employees.dv_batch_id >= s_Humanity_employees.dv_batch_id then l_humanity_employees.dv_batch_id
            else s_Humanity_employees.dv_batch_id end dv_batch_id
  from h_humanity_employees h
  join (select bk_hash, l_humanity_employees_id, dv_batch_id, dv_load_date_time
          from (select bk_hash, l_humanity_employees_id, dv_batch_id, dv_load_date_time, rank() over (partition by bk_hash order by dv_load_date_time desc,l_humanity_employees_id desc) r from l_humanity_employees where bk_hash in (select bk_hash from #process)) x
         where r = 1) l_humanity_employees
    on h.bk_hash = l_humanity_employees.bk_hash
  join (select bk_hash, s_Humanity_employees_id, dv_batch_id, dv_load_date_time
          from (select bk_hash, s_Humanity_employees_id, dv_batch_id, dv_load_date_time, rank() over (partition by bk_hash order by dv_load_date_time desc,s_Humanity_employees_id desc) r from s_Humanity_employees where bk_hash in (select bk_hash from #process)) x
         where r = 1) s_Humanity_employees
    on h.bk_hash = s_Humanity_employees.bk_hash
 where h.bk_hash in (select bk_hash from #process)
end