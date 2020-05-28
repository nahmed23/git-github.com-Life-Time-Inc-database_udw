CREATE PROC [dbo].[proc_d_humanity_workday_employees] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_humanity_workday_employees)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_humanity_workday_employees_insert') is not null drop table #p_humanity_workday_employees_insert
create table dbo.#p_humanity_workday_employees_insert with(distribution=hash(bk_hash), location=user_db) as
select p_humanity_workday_employees.p_humanity_workday_employees_id,
       p_humanity_workday_employees.bk_hash
  from dbo.p_humanity_workday_employees
 where p_humanity_workday_employees.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_humanity_workday_employees.dv_batch_id > @max_dv_batch_id
        or p_humanity_workday_employees.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_humanity_workday_employees.bk_hash,
       p_humanity_workday_employees.bk_hash d_humanity_workday_employees_key,
       p_humanity_workday_employees.employee_id employee_id,
       s_humanity_workday_employees.wd_file_name wd_file_name,
       s_humanity_workday_employees.hire_date hire_date,
       s_humanity_workday_employees.term_date term_date,
       s_humanity_workday_employees.employee_status employee_status,
       s_humanity_workday_employees.effective_date_for_position effective_date_for_position,
       s_humanity_workday_employees.sup_org_ref_id sup_org_ref_id,
       s_humanity_workday_employees.supervisory_organization supervisory_organization,
       s_humanity_workday_employees.job_profile job_profile,
       s_humanity_workday_employees.manager manager,
       s_humanity_workday_employees.location_id location_id,
       p_humanity_workday_employees.time_in_job_profile time_in_job_profile,
       s_humanity_workday_employees.company_id company_id,
       s_humanity_workday_employees.anticipated_weekly_work_hours anticipated_weekly_work_hours,
       s_humanity_workday_employees.pay_type pay_type,
       s_humanity_workday_employees.class_rate class_rate,
       s_humanity_workday_employees.commission_plans commission_plans,
       l_humanity_workday_employees.position_id position_id,
       l_humanity_workday_employees.hourly_amount hourly_amount,
       l_humanity_workday_employees.job_code job_code,
       l_humanity_workday_employees.offering offering,
       l_humanity_workday_employees.region region,
       l_humanity_workday_employees.primary_job primary_job,
       l_humanity_workday_employees.cost_center cost_center,
       hashbytes('md5', concat(s_humanity_workday_employees.cost_center,s_humanity_workday_employees.hourly_amount,s_humanity_workday_employees.job_code,s_humanity_workday_employees.offering,s_humanity_workday_employees.region)) cost_hour_job_offer_region_hashkey,
       cast(concat(
       substring(SUBSTRING(s_humanity_workday_employees.wd_file_name,CHARINDEX('.csv',(s_humanity_workday_employees.wd_file_name))-8,8),5,4),
       substring(SUBSTRING(s_humanity_workday_employees.wd_file_name,CHARINDEX('.csv',(s_humanity_workday_employees.wd_file_name))-8,8),1,2),
       substring(SUBSTRING(s_humanity_workday_employees.wd_file_name,CHARINDEX('.csv',(s_humanity_workday_employees.wd_file_name))-8,8),3,2)
       ) as date) effective_date_begin,
       cast(concat(
       substring(SUBSTRING(s_humanity_workday_employees.wd_file_name,CHARINDEX('.csv',(s_humanity_workday_employees.wd_file_name))-8,8),5,4),
       substring(SUBSTRING(s_humanity_workday_employees.wd_file_name,CHARINDEX('.csv',(s_humanity_workday_employees.wd_file_name))-8,8),1,2),
       substring(SUBSTRING(s_humanity_workday_employees.wd_file_name,CHARINDEX('.csv',(s_humanity_workday_employees.wd_file_name))-8,8),3,2)
       ) as date) effective_date_end,
       hashbytes('md5', concat(s_humanity_workday_employees.employee_id,s_humanity_workday_employees.position_id)) employee_position_hashkey,
       cast(concat(
       substring(SUBSTRING(s_humanity_workday_employees.wd_file_name,CHARINDEX('.csv',(s_humanity_workday_employees.wd_file_name))-8,8),5,4),
       substring(SUBSTRING(s_humanity_workday_employees.wd_file_name,CHARINDEX('.csv',(s_humanity_workday_employees.wd_file_name))-8,8),1,2),
       substring(SUBSTRING(s_humanity_workday_employees.wd_file_name,CHARINDEX('.csv',(s_humanity_workday_employees.wd_file_name))-8,8),3,2)
       ) as date) file_arrive_date,
       s_humanity_workday_employees.worker worker,
       isnull(h_humanity_workday_employees.dv_deleted,0) dv_deleted,
       p_humanity_workday_employees.p_humanity_workday_employees_id,
       p_humanity_workday_employees.dv_batch_id,
       p_humanity_workday_employees.dv_load_date_time,
       p_humanity_workday_employees.dv_load_end_date_time
  from dbo.h_humanity_workday_employees
  join dbo.p_humanity_workday_employees
    on h_humanity_workday_employees.bk_hash = p_humanity_workday_employees.bk_hash
  join #p_humanity_workday_employees_insert
    on p_humanity_workday_employees.bk_hash = #p_humanity_workday_employees_insert.bk_hash
   and p_humanity_workday_employees.p_humanity_workday_employees_id = #p_humanity_workday_employees_insert.p_humanity_workday_employees_id
  join dbo.l_humanity_workday_employees
    on p_humanity_workday_employees.bk_hash = l_humanity_workday_employees.bk_hash
   and p_humanity_workday_employees.l_humanity_workday_employees_id = l_humanity_workday_employees.l_humanity_workday_employees_id
  join dbo.s_humanity_workday_employees
    on p_humanity_workday_employees.bk_hash = s_humanity_workday_employees.bk_hash
   and p_humanity_workday_employees.s_humanity_workday_employees_id = s_humanity_workday_employees.s_humanity_workday_employees_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_humanity_workday_employees
   where d_humanity_workday_employees.bk_hash in (select bk_hash from #p_humanity_workday_employees_insert)

  insert dbo.d_humanity_workday_employees(
             bk_hash,
             d_humanity_workday_employees_key,
             employee_id,
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
             time_in_job_profile,
             company_id,
             anticipated_weekly_work_hours,
             pay_type,
             class_rate,
             commission_plans,
             position_id,
             hourly_amount,
             job_code,
             offering,
             region,
             primary_job,
             cost_center,
             cost_hour_job_offer_region_hashkey,
             effective_date_begin,
             effective_date_end,
             employee_position_hashkey,
             file_arrive_date,
             worker,
             deleted_flag,
             p_humanity_workday_employees_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         d_humanity_workday_employees_key,
         employee_id,
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
         time_in_job_profile,
         company_id,
         anticipated_weekly_work_hours,
         pay_type,
         class_rate,
         commission_plans,
         position_id,
         hourly_amount,
         job_code,
         offering,
         region,
         primary_job,
         cost_center,
         cost_hour_job_offer_region_hashkey,
         effective_date_begin,
         effective_date_end,
         employee_position_hashkey,
         file_arrive_date,
         worker,
         dv_deleted,
         p_humanity_workday_employees_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_humanity_workday_employees)
--Done!
end
