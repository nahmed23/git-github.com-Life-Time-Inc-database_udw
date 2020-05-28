CREATE PROC [dbo].[proc_d_workday_employee] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_workday_employee)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_workday_employee_insert') is not null drop table #p_workday_employee_insert
create table dbo.#p_workday_employee_insert with(distribution=hash(bk_hash), location=user_db) as
select p_workday_employee.p_workday_employee_id,
       p_workday_employee.bk_hash
  from dbo.p_workday_employee
 where p_workday_employee.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_workday_employee.dv_batch_id > @max_dv_batch_id
        or p_workday_employee.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_workday_employee.bk_hash,
       p_workday_employee.bk_hash dim_employee_key,
       p_workday_employee.employee_id employee_id,
       isnull(s_workday_employee.active_status,'') active_status,
       isnull(s_workday_employee.business_titles,'') business_titles,
       isnull(s_workday_employee.category, '') category,
       isnull(s_workday_employee.certifications, '') certifications,
       s_workday_employee.cf_employment_status cf_employment_status,
       s_workday_employee.cf_nickname cf_nickname,
       isnull(s_workday_employee.first_name,'') first_name,
       s_workday_employee.hire_date hire_date,
       s_workday_employee.is_primary is_primary,
       isnull(s_workday_employee.job_codes,'') job_codes,
       isnull(s_workday_employee.job_families,'') job_families,
       isnull(s_workday_employee.job_levels,'') job_levels,
       isnull(s_workday_employee.job_profiles,'') job_profiles,
       isnull(s_workday_employee.job_sub_families,'') job_sub_families,
       isnull(s_workday_employee.last_name,'') last_name,
       case when p_workday_employee.bk_hash in ('-997','-998','-999') then p_workday_employee.bk_hash     when l_workday_employee.manager_id is null then '-998' 	when l_workday_employee.manager_id in (0) then '-998' 	else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_workday_employee.manager_id as int) as varchar(500)),'z#@$k%&P'))),2)   end manager_dim_employee_key,
       l_workday_employee.manager_id manager_id,
       isnull(s_workday_employee.marketing_titles,'') marketing_titles,
       isnull(s_workday_employee.middle_name,'') middle_name,
       l_workday_employee.mms_club_id mms_club_id,
       s_workday_employee.pay_rate_for_all_positions pay_rate_for_all_positions,
       isnull(s_workday_employee.phone_number, '') phone_number,
       isnull(s_workday_employee.preferred_first_name, '') preferred_first_name,
       isnull(s_workday_employee.preferred_last_name, '') preferred_last_name,
       isnull(s_workday_employee.preferred_middle_name, '') preferred_middle_name,
       isnull(s_workday_employee.primary_work_email, '') primary_work_email,
       isnull(s_workday_employee.subordinates,'') subordinates,
       s_workday_employee.termination_date termination_date,
       l_workday_employee.workday_club_id workday_club_id,
       isnull(h_workday_employee.dv_deleted,0) dv_deleted,
       p_workday_employee.p_workday_employee_id,
       p_workday_employee.dv_batch_id,
       p_workday_employee.dv_load_date_time,
       p_workday_employee.dv_load_end_date_time
  from dbo.h_workday_employee
  join dbo.p_workday_employee
    on h_workday_employee.bk_hash = p_workday_employee.bk_hash
  join #p_workday_employee_insert
    on p_workday_employee.bk_hash = #p_workday_employee_insert.bk_hash
   and p_workday_employee.p_workday_employee_id = #p_workday_employee_insert.p_workday_employee_id
  join dbo.l_workday_employee
    on p_workday_employee.bk_hash = l_workday_employee.bk_hash
   and p_workday_employee.l_workday_employee_id = l_workday_employee.l_workday_employee_id
  join dbo.s_workday_employee
    on p_workday_employee.bk_hash = s_workday_employee.bk_hash
   and p_workday_employee.s_workday_employee_id = s_workday_employee.s_workday_employee_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_workday_employee
   where d_workday_employee.bk_hash in (select bk_hash from #p_workday_employee_insert)

  insert dbo.d_workday_employee(
             bk_hash,
             dim_employee_key,
             employee_id,
             active_status,
             business_titles,
             category,
             certifications,
             cf_employment_status,
             cf_nickname,
             first_name,
             hire_date,
             is_primary,
             job_codes,
             job_families,
             job_levels,
             job_profiles,
             job_sub_families,
             last_name,
             manager_dim_employee_key,
             manager_id,
             marketing_titles,
             middle_name,
             mms_club_id,
             pay_rate_for_all_positions,
             phone_number,
             preferred_first_name,
             preferred_last_name,
             preferred_middle_name,
             primary_work_email,
             subordinates,
             termination_date,
             workday_club_id,
             deleted_flag,
             p_workday_employee_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_employee_key,
         employee_id,
         active_status,
         business_titles,
         category,
         certifications,
         cf_employment_status,
         cf_nickname,
         first_name,
         hire_date,
         is_primary,
         job_codes,
         job_families,
         job_levels,
         job_profiles,
         job_sub_families,
         last_name,
         manager_dim_employee_key,
         manager_id,
         marketing_titles,
         middle_name,
         mms_club_id,
         pay_rate_for_all_positions,
         phone_number,
         preferred_first_name,
         preferred_last_name,
         preferred_middle_name,
         primary_work_email,
         subordinates,
         termination_date,
         workday_club_id,
         dv_deleted,
         p_workday_employee_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_workday_employee)
--Done!
end
