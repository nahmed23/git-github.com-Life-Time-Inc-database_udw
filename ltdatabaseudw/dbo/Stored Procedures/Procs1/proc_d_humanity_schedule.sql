CREATE PROC [dbo].[proc_d_humanity_schedule] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_humanity_schedule)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_humanity_schedule_insert') is not null drop table #p_humanity_schedule_insert
create table dbo.#p_humanity_schedule_insert with(distribution=hash(bk_hash), location=user_db) as
select p_humanity_schedule.p_humanity_schedule_id,
       p_humanity_schedule.bk_hash
  from dbo.p_humanity_schedule
 where p_humanity_schedule.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_humanity_schedule.dv_batch_id > @max_dv_batch_id
        or p_humanity_schedule.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_humanity_schedule.bk_hash,
       p_humanity_schedule.bk_hash d_humanity_schedule_key,
       p_humanity_schedule.shift_id shift_id,
       s_humanity_schedule.employee_id employee_id,
       l_humanity_schedule.position_id position_id,
       s_humanity_schedule.ltf_file_name ltf_file_name,
       l_humanity_schedule.company_id company_id,
       s_humanity_schedule.company_name company_name,
       s_humanity_schedule.created_by_eid created_by_eid,
       s_humanity_schedule.created_by_id created_by_id,
       s_humanity_schedule.created_by_name created_by_name,
       s_humanity_schedule.created_datetime_utc created_datetime_utc,
       s_humanity_schedule.employee_eid employee_eid,
       s_humanity_schedule.employee_name employee_name,
       s_humanity_schedule.employees_needed employees_needed,
       s_humanity_schedule.employees_working employees_working,
       cast(substring(s_humanity_schedule.ltf_file_name,charindex('.csv',(s_humanity_schedule.ltf_file_name))-8,8) as date) file_arrive_date,
       s_humanity_schedule.hours hours,
       s_humanity_schedule.is_deleted is_deleted,
       s_humanity_schedule.location_id location_id,
       s_humanity_schedule.location_name location_name,
       s_humanity_schedule.notes notes,
       s_humanity_schedule.position_name position_name,
       s_humanity_schedule.published published,
       s_humanity_schedule.published_datetime_utc published_datetime_utc,
       s_humanity_schedule.recurring_shift recurring_shift,
       s_humanity_schedule.shift_end_date_utc shift_end_date_utc,
       s_humanity_schedule.shift_end_time shift_end_time,
       s_humanity_schedule.shift_start_date_utc shift_start_date_utc,
       s_humanity_schedule.shift_start_time shift_start_time,
       s_humanity_schedule.shift_type shift_type,
       s_humanity_schedule.updated_at_utc updated_at_utc,
       s_humanity_schedule.wage wage,
       s_humanity_schedule.workday_position_id workday_position_id,
       isnull(h_humanity_schedule.dv_deleted,0) dv_deleted,
       p_humanity_schedule.p_humanity_schedule_id,
       p_humanity_schedule.dv_batch_id,
       p_humanity_schedule.dv_load_date_time,
       p_humanity_schedule.dv_load_end_date_time
  from dbo.h_humanity_schedule
  join dbo.p_humanity_schedule
    on h_humanity_schedule.bk_hash = p_humanity_schedule.bk_hash
  join #p_humanity_schedule_insert
    on p_humanity_schedule.bk_hash = #p_humanity_schedule_insert.bk_hash
   and p_humanity_schedule.p_humanity_schedule_id = #p_humanity_schedule_insert.p_humanity_schedule_id
  join dbo.l_humanity_schedule
    on p_humanity_schedule.bk_hash = l_humanity_schedule.bk_hash
   and p_humanity_schedule.l_humanity_schedule_id = l_humanity_schedule.l_humanity_schedule_id
  join dbo.s_humanity_schedule
    on p_humanity_schedule.bk_hash = s_humanity_schedule.bk_hash
   and p_humanity_schedule.s_humanity_schedule_id = s_humanity_schedule.s_humanity_schedule_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_humanity_schedule
   where d_humanity_schedule.bk_hash in (select bk_hash from #p_humanity_schedule_insert)

  insert dbo.d_humanity_schedule(
             bk_hash,
             d_humanity_schedule_key,
             shift_id,
             employee_id,
             position_id,
             ltf_file_name,
             company_id,
             company_name,
             created_by_eid,
             created_by_id,
             created_by_name,
             created_datetime_utc,
             employee_eid,
             employee_name,
             employees_needed,
             employees_working,
             file_arrive_date,
             hours,
             is_deleted,
             location_id,
             location_name,
             notes,
             position_name,
             published,
             published_datetime_utc,
             recurring_shift,
             shift_end_date_utc,
             shift_end_time,
             shift_start_date_utc,
             shift_start_time,
             shift_type,
             updated_at_utc,
             wage,
             workday_position_id,
             deleted_flag,
             p_humanity_schedule_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         d_humanity_schedule_key,
         shift_id,
         employee_id,
         position_id,
         ltf_file_name,
         company_id,
         company_name,
         created_by_eid,
         created_by_id,
         created_by_name,
         created_datetime_utc,
         employee_eid,
         employee_name,
         employees_needed,
         employees_working,
         file_arrive_date,
         hours,
         is_deleted,
         location_id,
         location_name,
         notes,
         position_name,
         published,
         published_datetime_utc,
         recurring_shift,
         shift_end_date_utc,
         shift_end_time,
         shift_start_date_utc,
         shift_start_time,
         shift_type,
         updated_at_utc,
         wage,
         workday_position_id,
         dv_deleted,
         p_humanity_schedule_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_humanity_schedule)
--Done!
end
