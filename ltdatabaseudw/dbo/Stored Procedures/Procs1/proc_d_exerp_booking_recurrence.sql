﻿CREATE PROC [dbo].[proc_d_exerp_booking_recurrence] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_exerp_booking_recurrence)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_exerp_booking_recurrence_insert') is not null drop table #p_exerp_booking_recurrence_insert
create table dbo.#p_exerp_booking_recurrence_insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_booking_recurrence.p_exerp_booking_recurrence_id,
       p_exerp_booking_recurrence.bk_hash
  from dbo.p_exerp_booking_recurrence
 where p_exerp_booking_recurrence.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_exerp_booking_recurrence.dv_batch_id > @max_dv_batch_id
        or p_exerp_booking_recurrence.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_booking_recurrence.bk_hash,
       p_exerp_booking_recurrence.main_booking_id main_booking_id,
       l_exerp_booking_recurrence.center_id center_id,
       case  when p_exerp_booking_recurrence.bk_hash in('-997', '-998', '-999') then p_exerp_booking_recurrence.bk_hash
            when l_exerp_booking_recurrence.center_id is null then '-998'
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_exerp_booking_recurrence.center_id as int) as varchar(500)),'z#@$k%&P'))),2)  end d_exerp_center_bk_hash,
       s_exerp_booking_recurrence.recurrence recurrence,
       case when p_exerp_booking_recurrence.bk_hash in('-997', '-998', '-999') then p_exerp_booking_recurrence.bk_hash
            when s_exerp_booking_recurrence.recurrence_end is null then '-998'
         else convert(varchar, s_exerp_booking_recurrence.recurrence_end, 112) end recurrence_end_dim_date_key,
       case when p_exerp_booking_recurrence.bk_hash in('-997', '-998', '-999') then p_exerp_booking_recurrence.bk_hash
            when s_exerp_booking_recurrence.recurrence_start_datetime is null then '-998'
         else convert(varchar, s_exerp_booking_recurrence.recurrence_start_datetime, 112) end recurrence_start_dim_date_key,
       case when p_exerp_booking_recurrence.bk_hash in ('-997','-998','-999') then p_exerp_booking_recurrence.bk_hash
        when s_exerp_booking_recurrence.recurrence_start_datetime is null then '-998'
        else '1' + replace(substring(convert(varchar,s_exerp_booking_recurrence.recurrence_start_datetime,114), 1, 5),':','')  end recurrence_start_dim_time_key,
       s_exerp_booking_recurrence.recurrence_type recurrence_type,
       isnull(h_exerp_booking_recurrence.dv_deleted,0) dv_deleted,
       p_exerp_booking_recurrence.p_exerp_booking_recurrence_id,
       p_exerp_booking_recurrence.dv_batch_id,
       p_exerp_booking_recurrence.dv_load_date_time,
       p_exerp_booking_recurrence.dv_load_end_date_time
  from dbo.h_exerp_booking_recurrence
  join dbo.p_exerp_booking_recurrence
    on h_exerp_booking_recurrence.bk_hash = p_exerp_booking_recurrence.bk_hash
  join #p_exerp_booking_recurrence_insert
    on p_exerp_booking_recurrence.bk_hash = #p_exerp_booking_recurrence_insert.bk_hash
   and p_exerp_booking_recurrence.p_exerp_booking_recurrence_id = #p_exerp_booking_recurrence_insert.p_exerp_booking_recurrence_id
  join dbo.l_exerp_booking_recurrence
    on p_exerp_booking_recurrence.bk_hash = l_exerp_booking_recurrence.bk_hash
   and p_exerp_booking_recurrence.l_exerp_booking_recurrence_id = l_exerp_booking_recurrence.l_exerp_booking_recurrence_id
  join dbo.s_exerp_booking_recurrence
    on p_exerp_booking_recurrence.bk_hash = s_exerp_booking_recurrence.bk_hash
   and p_exerp_booking_recurrence.s_exerp_booking_recurrence_id = s_exerp_booking_recurrence.s_exerp_booking_recurrence_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_exerp_booking_recurrence
   where d_exerp_booking_recurrence.bk_hash in (select bk_hash from #p_exerp_booking_recurrence_insert)

  insert dbo.d_exerp_booking_recurrence(
             bk_hash,
             main_booking_id,
             center_id,
             d_exerp_center_bk_hash,
             recurrence,
             recurrence_end_dim_date_key,
             recurrence_start_dim_date_key,
             recurrence_start_dim_time_key,
             recurrence_type,
             deleted_flag,
             p_exerp_booking_recurrence_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         main_booking_id,
         center_id,
         d_exerp_center_bk_hash,
         recurrence,
         recurrence_end_dim_date_key,
         recurrence_start_dim_date_key,
         recurrence_start_dim_time_key,
         recurrence_type,
         dv_deleted,
         p_exerp_booking_recurrence_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_exerp_booking_recurrence)
--Done!
end
