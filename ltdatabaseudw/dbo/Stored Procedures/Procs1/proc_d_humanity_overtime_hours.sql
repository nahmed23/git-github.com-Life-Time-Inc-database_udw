CREATE PROC [dbo].[proc_d_humanity_overtime_hours] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_humanity_overtime_hours)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_humanity_overtime_hours_insert') is not null drop table #p_humanity_overtime_hours_insert
create table dbo.#p_humanity_overtime_hours_insert with(distribution=hash(bk_hash), location=user_db) as
select p_humanity_overtime_hours.p_humanity_overtime_hours_id,
       p_humanity_overtime_hours.bk_hash
  from dbo.p_humanity_overtime_hours
 where p_humanity_overtime_hours.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_humanity_overtime_hours.dv_batch_id > @max_dv_batch_id
        or p_humanity_overtime_hours.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_humanity_overtime_hours.bk_hash,
       p_humanity_overtime_hours.bk_hash d_humanity_overtime_hours_key,
       l_humanity_overtime_hours.userid userid,
       l_humanity_overtime_hours.start_time start_time,
       l_humanity_overtime_hours.end_time end_time,
       s_humanity_overtime_hours.employee_name employee_name,
       l_humanity_overtime_hours.employee_id employee_id,
       l_humanity_overtime_hours.date_formatted date_formatted,
       s_humanity_overtime_hours.hours_regular hours_regular,
       s_humanity_overtime_hours.hours_overtime hours_overtime,
       s_humanity_overtime_hours.hours_d_overtime hours_d_overtime,
       s_humanity_overtime_hours.hours_position_id hours_position_id,
       s_humanity_overtime_hours.hours_location_id hours_location_id,
       l_humanity_overtime_hours.company_id company_id,
       substring(s_humanity_overtime_hours.ltf_file_name,charindex('.csv',(s_humanity_overtime_hours.ltf_file_name))-10,10) file_arrive_date,
       case when p_humanity_overtime_hours.bk_hash in ('-997', '-998', '-999') then p_humanity_overtime_hours.bk_hash        
       when s_humanity_overtime_hours.date_formatted is null then '-998'        else convert(varchar, cast(s_humanity_overtime_hours.date_formatted as date), 112) end ot_date_formatted_dim_date_key,
       case when p_humanity_overtime_hours.bk_hash in ('-997', '-998', '-999') then p_humanity_overtime_hours.bk_hash
       when s_humanity_overtime_hours.end_time is null then '-998'
       else '1' + substring(replace(CONVERT(VARCHAR, cast(s_humanity_overtime_hours.end_time as time), 108),':',''),1,4) end ot_end_time_dim_time_key,
       case when p_humanity_overtime_hours.bk_hash in ('-997','-998','-999') then p_humanity_overtime_hours.bk_hash
       when s_humanity_overtime_hours.start_time is null then '-998'
       else '1' +  substring(replace(CONVERT(VARCHAR, cast(s_humanity_overtime_hours.start_time as time), 108),':',''),1,4) end ot_start_time_dim_time_key,
       isnull(h_humanity_overtime_hours.dv_deleted,0) dv_deleted,
       p_humanity_overtime_hours.p_humanity_overtime_hours_id,
       p_humanity_overtime_hours.dv_batch_id,
       p_humanity_overtime_hours.dv_load_date_time,
       p_humanity_overtime_hours.dv_load_end_date_time
  from dbo.h_humanity_overtime_hours
  join dbo.p_humanity_overtime_hours
    on h_humanity_overtime_hours.bk_hash = p_humanity_overtime_hours.bk_hash
  join #p_humanity_overtime_hours_insert
    on p_humanity_overtime_hours.bk_hash = #p_humanity_overtime_hours_insert.bk_hash
   and p_humanity_overtime_hours.p_humanity_overtime_hours_id = #p_humanity_overtime_hours_insert.p_humanity_overtime_hours_id
  join dbo.l_humanity_overtime_hours
    on p_humanity_overtime_hours.bk_hash = l_humanity_overtime_hours.bk_hash
   and p_humanity_overtime_hours.l_humanity_overtime_hours_id = l_humanity_overtime_hours.l_humanity_overtime_hours_id
  join dbo.s_humanity_overtime_hours
    on p_humanity_overtime_hours.bk_hash = s_humanity_overtime_hours.bk_hash
   and p_humanity_overtime_hours.s_humanity_overtime_hours_id = s_humanity_overtime_hours.s_humanity_overtime_hours_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_humanity_overtime_hours
   where d_humanity_overtime_hours.bk_hash in (select bk_hash from #p_humanity_overtime_hours_insert)

  insert dbo.d_humanity_overtime_hours(
             bk_hash,
             d_humanity_overtime_hours_key,
             userid,
             start_time,
             end_time,
             employee_name,
             employee_id,
             date_formatted,
             hours_regular,
             hours_overtime,
             hours_d_overtime,
             hours_position_id,
             hours_location_id,
             company_id,
             file_arrive_date,
             ot_date_formatted_dim_date_key,
             ot_end_time_dim_time_key,
             ot_start_time_dim_time_key,
             deleted_flag,
             p_humanity_overtime_hours_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         d_humanity_overtime_hours_key,
         userid,
         start_time,
         end_time,
         employee_name,
         employee_id,
         date_formatted,
         hours_regular,
         hours_overtime,
         hours_d_overtime,
         hours_position_id,
         hours_location_id,
         company_id,
         file_arrive_date,
         ot_date_formatted_dim_date_key,
         ot_end_time_dim_time_key,
         ot_start_time_dim_time_key,
         dv_deleted,
         p_humanity_overtime_hours_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_humanity_overtime_hours)
--Done!
end
