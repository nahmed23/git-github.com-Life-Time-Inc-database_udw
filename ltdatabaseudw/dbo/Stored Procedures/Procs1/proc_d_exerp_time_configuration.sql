CREATE PROC [dbo].[proc_d_exerp_time_configuration] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_exerp_time_configuration)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_exerp_time_configuration_insert') is not null drop table #p_exerp_time_configuration_insert
create table dbo.#p_exerp_time_configuration_insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_time_configuration.p_exerp_time_configuration_id,
       p_exerp_time_configuration.bk_hash
  from dbo.p_exerp_time_configuration
 where p_exerp_time_configuration.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_exerp_time_configuration.dv_batch_id > @max_dv_batch_id
        or p_exerp_time_configuration.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_time_configuration.bk_hash,
       p_exerp_time_configuration.time_configuration_id time_configuration_id,
       s_exerp_time_configuration.cancel_sanc_start cancel_sanc_start,
       s_exerp_time_configuration.cancel_sanc_start_unit cancel_sanc_start_unit,
       s_exerp_time_configuration.cancel_stop_cust cancel_stop_cust,
       s_exerp_time_configuration.cancel_stop_cust_unit cancel_stop_cust_unit,
       s_exerp_time_configuration.cancel_stop_staff cancel_stop_staff,
       s_exerp_time_configuration.cancel_stop_staff_unit cancel_stop_staff_unit,
       s_exerp_time_configuration.course_sign_start course_sign_start,
       s_exerp_time_configuration.course_sign_start_unit course_sign_start_unit,
       s_exerp_time_configuration.course_stop course_stop,
       s_exerp_time_configuration.course_stop_unit course_stop_unit,
       s_exerp_time_configuration.part_cust_stop part_cust_stop,
       s_exerp_time_configuration.part_cust_stop_unit part_cust_stop_unit,
       s_exerp_time_configuration.part_from part_from,
       s_exerp_time_configuration.part_from_unit part_from_unit,
       s_exerp_time_configuration.part_staff_stop part_staff_stop,
       s_exerp_time_configuration.part_staff_stop_unit part_staff_stop_unit,
       s_exerp_time_configuration.recurrence_in_past recurrence_in_past,
       s_exerp_time_configuration.recurrence_in_past_unit recurrence_in_past_unit,
       s_exerp_time_configuration.name time_configuration_name,
       isnull(h_exerp_time_configuration.dv_deleted,0) dv_deleted,
       p_exerp_time_configuration.p_exerp_time_configuration_id,
       p_exerp_time_configuration.dv_batch_id,
       p_exerp_time_configuration.dv_load_date_time,
       p_exerp_time_configuration.dv_load_end_date_time
  from dbo.h_exerp_time_configuration
  join dbo.p_exerp_time_configuration
    on h_exerp_time_configuration.bk_hash = p_exerp_time_configuration.bk_hash
  join #p_exerp_time_configuration_insert
    on p_exerp_time_configuration.bk_hash = #p_exerp_time_configuration_insert.bk_hash
   and p_exerp_time_configuration.p_exerp_time_configuration_id = #p_exerp_time_configuration_insert.p_exerp_time_configuration_id
  join dbo.s_exerp_time_configuration
    on p_exerp_time_configuration.bk_hash = s_exerp_time_configuration.bk_hash
   and p_exerp_time_configuration.s_exerp_time_configuration_id = s_exerp_time_configuration.s_exerp_time_configuration_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_exerp_time_configuration
   where d_exerp_time_configuration.bk_hash in (select bk_hash from #p_exerp_time_configuration_insert)

  insert dbo.d_exerp_time_configuration(
             bk_hash,
             time_configuration_id,
             cancel_sanc_start,
             cancel_sanc_start_unit,
             cancel_stop_cust,
             cancel_stop_cust_unit,
             cancel_stop_staff,
             cancel_stop_staff_unit,
             course_sign_start,
             course_sign_start_unit,
             course_stop,
             course_stop_unit,
             part_cust_stop,
             part_cust_stop_unit,
             part_from,
             part_from_unit,
             part_staff_stop,
             part_staff_stop_unit,
             recurrence_in_past,
             recurrence_in_past_unit,
             time_configuration_name,
             deleted_flag,
             p_exerp_time_configuration_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         time_configuration_id,
         cancel_sanc_start,
         cancel_sanc_start_unit,
         cancel_stop_cust,
         cancel_stop_cust_unit,
         cancel_stop_staff,
         cancel_stop_staff_unit,
         course_sign_start,
         course_sign_start_unit,
         course_stop,
         course_stop_unit,
         part_cust_stop,
         part_cust_stop_unit,
         part_from,
         part_from_unit,
         part_staff_stop,
         part_staff_stop_unit,
         recurrence_in_past,
         recurrence_in_past_unit,
         time_configuration_name,
         dv_deleted,
         p_exerp_time_configuration_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_exerp_time_configuration)
--Done!
end
