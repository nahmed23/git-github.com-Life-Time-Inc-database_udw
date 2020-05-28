CREATE PROC [dbo].[proc_d_boss_asi_resource] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_boss_asi_resource)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_boss_asi_resource_insert') is not null drop table #p_boss_asi_resource_insert
create table dbo.#p_boss_asi_resource_insert with(distribution=hash(bk_hash), location=user_db) as
select p_boss_asi_resource.p_boss_asi_resource_id,
       p_boss_asi_resource.bk_hash
  from dbo.p_boss_asi_resource
 where p_boss_asi_resource.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_boss_asi_resource.dv_batch_id > @max_dv_batch_id
        or p_boss_asi_resource.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_boss_asi_resource.bk_hash,
       p_boss_asi_resource.resource_type_id resource_type_id,
       isnull(s_boss_asi_resource.resource_type, '') resource_type,
       h_boss_asi_resource.dv_deleted,
       p_boss_asi_resource.p_boss_asi_resource_id,
       p_boss_asi_resource.dv_batch_id,
       p_boss_asi_resource.dv_load_date_time,
       p_boss_asi_resource.dv_load_end_date_time
  from dbo.h_boss_asi_resource
  join dbo.p_boss_asi_resource
    on h_boss_asi_resource.bk_hash = p_boss_asi_resource.bk_hash  join #p_boss_asi_resource_insert
    on p_boss_asi_resource.bk_hash = #p_boss_asi_resource_insert.bk_hash
   and p_boss_asi_resource.p_boss_asi_resource_id = #p_boss_asi_resource_insert.p_boss_asi_resource_id
  join dbo.l_boss_asi_resource
    on p_boss_asi_resource.bk_hash = l_boss_asi_resource.bk_hash
   and p_boss_asi_resource.l_boss_asi_resource_id = l_boss_asi_resource.l_boss_asi_resource_id
  join dbo.s_boss_asi_resource
    on p_boss_asi_resource.bk_hash = s_boss_asi_resource.bk_hash
   and p_boss_asi_resource.s_boss_asi_resource_id = s_boss_asi_resource.s_boss_asi_resource_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_boss_asi_resource
   where d_boss_asi_resource.bk_hash in (select bk_hash from #p_boss_asi_resource_insert)

  insert dbo.d_boss_asi_resource(
             bk_hash,
             resource_type_id,
             resource_type,
             deleted_flag,
             p_boss_asi_resource_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         resource_type_id,
         resource_type,
         dv_deleted,
         p_boss_asi_resource_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_boss_asi_resource)
--Done!
end
