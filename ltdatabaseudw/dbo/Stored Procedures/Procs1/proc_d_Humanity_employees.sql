CREATE PROC [dbo].[proc_d_Humanity_employees] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_Humanity_employees)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_Humanity_employees_insert') is not null drop table #p_Humanity_employees_insert
create table dbo.#p_Humanity_employees_insert with(distribution=hash(bk_hash), location=user_db) as
select p_Humanity_employees.p_Humanity_employees_id,
       p_Humanity_employees.bk_hash
  from dbo.p_Humanity_employees
 where p_Humanity_employees.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_Humanity_employees.dv_batch_id > @max_dv_batch_id
        or p_Humanity_employees.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_Humanity_employees.bk_hash,
       p_humanity_employees.bk_hash d_humanity_employees_key,
       p_humanity_employees.employee_id employee_id,
       s_humanity_employees.location_name location_name,
       s_humanity_employees.employee_to_see_wages employee_to_see_wages,
       s_humanity_employees.last_active_date_utc last_active_date_utc,
       s_humanity_employees.user_timezone user_timezone,
       s_humanity_employees.workday_position_id workday_position_id,
       s_humanity_employees.ltf_file_name ltf_file_name,
       l_humanity_employees.company_id company_id,
       s_humanity_employees.employee_eid employee_eid,
       s_humanity_employees.employee_name employee_name,
       s_humanity_employees.employee_email employee_email,
       s_humanity_employees.company_name company_name,
       s_humanity_employees.deleted_flg deleted_flg,
       s_humanity_employees.employee_status employee_status,
       s_humanity_employees.employee_role employee_role,
       s_humanity_employees.position_name position_name,
       s_humanity_employees.file_arrive_date file_arrive_date,
       isnull(h_Humanity_employees.dv_deleted,0) dv_deleted,
       p_Humanity_employees.p_Humanity_employees_id,
       p_Humanity_employees.dv_batch_id,
       p_Humanity_employees.dv_load_date_time,
       p_Humanity_employees.dv_load_end_date_time
  from dbo.h_Humanity_employees
  join dbo.p_Humanity_employees
    on h_Humanity_employees.bk_hash = p_Humanity_employees.bk_hash
  join #p_Humanity_employees_insert
    on p_Humanity_employees.bk_hash = #p_Humanity_employees_insert.bk_hash
   and p_Humanity_employees.p_Humanity_employees_id = #p_Humanity_employees_insert.p_Humanity_employees_id
  join dbo.l_humanity_employees
    on p_humanity_employees.bk_hash = l_humanity_employees.bk_hash
   and p_humanity_employees.l_Humanity_employees_id = l_humanity_employees.l_Humanity_employees_id
  join dbo.s_Humanity_employees
    on p_humanity_employees.bk_hash = s_Humanity_employees.bk_hash
   and p_humanity_employees.s_Humanity_employees_id = s_Humanity_employees.s_Humanity_employees_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_Humanity_employees
   where d_Humanity_employees.bk_hash in (select bk_hash from #p_Humanity_employees_insert)

  insert dbo.d_Humanity_employees(
             bk_hash,
             d_humanity_employees_key,
             employee_id,
             location_name,
             employee_to_see_wages,
             last_active_date_utc,
             user_timezone,
             workday_position_id,
             ltf_file_name,
             company_id,
             employee_eid,
             employee_name,
             employee_email,
             company_name,
             deleted_flg,
             employee_status,
             employee_role,
             position_name,
             file_arrive_date,
             deleted_flag,
             p_Humanity_employees_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         d_humanity_employees_key,
         employee_id,
         location_name,
         employee_to_see_wages,
         last_active_date_utc,
         user_timezone,
         workday_position_id,
         ltf_file_name,
         company_id,
         employee_eid,
         employee_name,
         employee_email,
         company_name,
         deleted_flg,
         employee_status,
         employee_role,
         position_name,
         file_arrive_date,
         dv_deleted,
         p_Humanity_employees_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_Humanity_employees)
--Done!
end
