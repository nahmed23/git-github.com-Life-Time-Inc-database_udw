CREATE PROC [dbo].[proc_d_chronotrack_location] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_chronotrack_location)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_chronotrack_location_insert') is not null drop table #p_chronotrack_location_insert
create table dbo.#p_chronotrack_location_insert with(distribution=hash(bk_hash), location=user_db) as
select p_chronotrack_location.p_chronotrack_location_id,
       p_chronotrack_location.bk_hash
  from dbo.p_chronotrack_location
 where p_chronotrack_location.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_chronotrack_location.dv_batch_id > @max_dv_batch_id
        or p_chronotrack_location.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_chronotrack_location.bk_hash,
       p_chronotrack_location.location_id location_id,
       s_chronotrack_location.city city,
       s_chronotrack_location.county county,
       s_chronotrack_location.create_time create_time,
       s_chronotrack_location.latitude latitude,
       s_chronotrack_location.longitude longitude,
       s_chronotrack_location.modified_time modified_time,
       s_chronotrack_location.name name,
       s_chronotrack_location.postal_code postal_code,
       l_chronotrack_location.region_id region_id,
       s_chronotrack_location.street street,
       s_chronotrack_location.street_2 street_2,
       s_chronotrack_location.time_zone time_zone,
       isnull(h_chronotrack_location.dv_deleted,0) dv_deleted,
       p_chronotrack_location.p_chronotrack_location_id,
       p_chronotrack_location.dv_batch_id,
       p_chronotrack_location.dv_load_date_time,
       p_chronotrack_location.dv_load_end_date_time
  from dbo.h_chronotrack_location
  join dbo.p_chronotrack_location
    on h_chronotrack_location.bk_hash = p_chronotrack_location.bk_hash
  join #p_chronotrack_location_insert
    on p_chronotrack_location.bk_hash = #p_chronotrack_location_insert.bk_hash
   and p_chronotrack_location.p_chronotrack_location_id = #p_chronotrack_location_insert.p_chronotrack_location_id
  join dbo.l_chronotrack_location
    on p_chronotrack_location.bk_hash = l_chronotrack_location.bk_hash
   and p_chronotrack_location.l_chronotrack_location_id = l_chronotrack_location.l_chronotrack_location_id
  join dbo.s_chronotrack_location
    on p_chronotrack_location.bk_hash = s_chronotrack_location.bk_hash
   and p_chronotrack_location.s_chronotrack_location_id = s_chronotrack_location.s_chronotrack_location_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_chronotrack_location
   where d_chronotrack_location.bk_hash in (select bk_hash from #p_chronotrack_location_insert)

  insert dbo.d_chronotrack_location(
             bk_hash,
             location_id,
             city,
             county,
             create_time,
             latitude,
             longitude,
             modified_time,
             name,
             postal_code,
             region_id,
             street,
             street_2,
             time_zone,
             deleted_flag,
             p_chronotrack_location_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         location_id,
         city,
         county,
         create_time,
         latitude,
         longitude,
         modified_time,
         name,
         postal_code,
         region_id,
         street,
         street_2,
         time_zone,
         dv_deleted,
         p_chronotrack_location_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_chronotrack_location)
--Done!
end
