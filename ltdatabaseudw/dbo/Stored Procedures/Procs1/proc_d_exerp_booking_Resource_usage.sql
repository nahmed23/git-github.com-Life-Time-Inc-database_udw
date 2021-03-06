﻿CREATE PROC [dbo].[proc_d_exerp_booking_Resource_usage] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_exerp_booking_Resource_usage)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_exerp_booking_Resource_usage_insert') is not null drop table #p_exerp_booking_Resource_usage_insert
create table dbo.#p_exerp_booking_Resource_usage_insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_booking_Resource_usage.p_exerp_booking_Resource_usage_id,
       p_exerp_booking_Resource_usage.bk_hash
  from dbo.p_exerp_booking_Resource_usage
 where p_exerp_booking_Resource_usage.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_exerp_booking_Resource_usage.dv_batch_id > @max_dv_batch_id
        or p_exerp_booking_Resource_usage.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_booking_Resource_usage.bk_hash,
       p_exerp_booking_resource_usage.resource_id resource_id,
       p_exerp_booking_resource_usage.booking_id booking_id,
       s_exerp_booking_resource_usage.state booking_resource_usage_state,
       case when p_exerp_booking_resource_usage.bk_hash in('-997', '-998', '-999') then p_exerp_booking_resource_usage.bk_hash
           when s_exerp_booking_resource_usage.booking_start_datetime is null then '-998'
        else convert(varchar, s_exerp_booking_resource_usage.booking_start_datetime, 112)    end booking_start_dim_date_key,
       case when p_exerp_booking_resource_usage.bk_hash in ('-997','-998','-999') then p_exerp_booking_resource_usage.bk_hash
       when s_exerp_booking_resource_usage.booking_start_datetime is null then '-998'
       else '1' + replace(substring(convert(varchar,s_exerp_booking_resource_usage.booking_start_datetime,114), 1, 5),':','') end booking_start_dim_time_key,
       case when p_exerp_booking_resource_usage.bk_hash in('-997', '-998', '-999') then p_exerp_booking_resource_usage.bk_hash
           when s_exerp_booking_resource_usage.booking_stop_datetime is null then '-998'
        else convert(varchar, s_exerp_booking_resource_usage.booking_stop_datetime, 112)    end booking_stop_dim_date_key,
       case when p_exerp_booking_resource_usage.bk_hash in ('-997','-998','-999') then p_exerp_booking_resource_usage.bk_hash
       when s_exerp_booking_resource_usage.booking_stop_datetime is null then '-998'
       else '1' + replace(substring(convert(varchar,s_exerp_booking_resource_usage.booking_stop_datetime,114), 1, 5),':','') end booking_stop_dim_time_key,
       case when p_exerp_booking_resource_usage.bk_hash in('-997', '-998', '-999') then p_exerp_booking_resource_usage.bk_hash
           when p_exerp_booking_resource_usage.booking_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(p_exerp_booking_resource_usage.booking_id as varchar(4000)),'z#@$k%&P'))),2) end d_exerp_booking_bk_hash,
       case when p_exerp_booking_resource_usage.bk_hash in('-997', '-998', '-999') then p_exerp_booking_resource_usage.bk_hash
           when p_exerp_booking_resource_usage.resource_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(p_exerp_booking_resource_usage.resource_id as varchar(4000)),'z#@$k%&P'))),2) end d_exerp_resource_bk_hash,
       case when p_exerp_booking_resource_usage.bk_hash in ('-997','-998','-999') then p_exerp_booking_resource_usage.bk_hash     
         when l_exerp_booking_resource_usage.center_id is null then '-998'   
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_exerp_booking_resource_usage.center_id as int) as varchar(500)),'z#@$k%&P'))),2)   end dim_club_key,
       s_exerp_booking_resource_usage.ets ets,
       isnull(h_exerp_booking_Resource_usage.dv_deleted,0) dv_deleted,
       p_exerp_booking_Resource_usage.p_exerp_booking_Resource_usage_id,
       p_exerp_booking_Resource_usage.dv_batch_id,
       p_exerp_booking_Resource_usage.dv_load_date_time,
       p_exerp_booking_Resource_usage.dv_load_end_date_time
  from dbo.h_exerp_booking_Resource_usage
  join dbo.p_exerp_booking_Resource_usage
    on h_exerp_booking_Resource_usage.bk_hash = p_exerp_booking_Resource_usage.bk_hash
  join #p_exerp_booking_Resource_usage_insert
    on p_exerp_booking_Resource_usage.bk_hash = #p_exerp_booking_Resource_usage_insert.bk_hash
   and p_exerp_booking_Resource_usage.p_exerp_booking_Resource_usage_id = #p_exerp_booking_Resource_usage_insert.p_exerp_booking_Resource_usage_id
  join dbo.l_exerp_booking_resource_usage
    on p_exerp_booking_resource_usage.bk_hash = l_exerp_booking_resource_usage.bk_hash
   and p_exerp_booking_resource_usage.l_exerp_booking_resource_usage_id = l_exerp_booking_resource_usage.l_exerp_booking_resource_usage_id
  join dbo.s_exerp_booking_resource_usage
    on p_exerp_booking_resource_usage.bk_hash = s_exerp_booking_resource_usage.bk_hash
   and p_exerp_booking_resource_usage.s_exerp_booking_resource_usage_id = s_exerp_booking_resource_usage.s_exerp_booking_resource_usage_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_exerp_booking_Resource_usage
   where d_exerp_booking_Resource_usage.bk_hash in (select bk_hash from #p_exerp_booking_Resource_usage_insert)

  insert dbo.d_exerp_booking_Resource_usage(
             bk_hash,
             resource_id,
             booking_id,
             booking_resource_usage_state,
             booking_start_dim_date_key,
             booking_start_dim_time_key,
             booking_stop_dim_date_key,
             booking_stop_dim_time_key,
             d_exerp_booking_bk_hash,
             d_exerp_resource_bk_hash,
             dim_club_key,
             ets,
             deleted_flag,
             p_exerp_booking_Resource_usage_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         resource_id,
         booking_id,
         booking_resource_usage_state,
         booking_start_dim_date_key,
         booking_start_dim_time_key,
         booking_stop_dim_date_key,
         booking_stop_dim_time_key,
         d_exerp_booking_bk_hash,
         d_exerp_resource_bk_hash,
         dim_club_key,
         ets,
         dv_deleted,
         p_exerp_booking_Resource_usage_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_exerp_booking_Resource_usage)
--Done!
end
