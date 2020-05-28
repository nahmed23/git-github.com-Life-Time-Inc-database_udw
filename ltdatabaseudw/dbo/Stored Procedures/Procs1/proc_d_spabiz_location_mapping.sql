CREATE PROC [dbo].[proc_d_spabiz_location_mapping] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_spabiz_location_mapping)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_spabiz_location_mapping_insert') is not null drop table #p_spabiz_location_mapping_insert
create table dbo.#p_spabiz_location_mapping_insert with(distribution=hash(bk_hash), location=user_db) as
select p_spabiz_location_mapping.p_spabiz_location_mapping_id,
       p_spabiz_location_mapping.bk_hash
  from dbo.p_spabiz_location_mapping
 where p_spabiz_location_mapping.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_spabiz_location_mapping.dv_batch_id > @max_dv_batch_id
        or p_spabiz_location_mapping.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_spabiz_location_mapping.bk_hash,
       p_spabiz_location_mapping.bk_hash dim_spabiz_location_mapping_key,
       p_spabiz_location_mapping.spabiz_store_number spabiz_store_number,
       s_spabiz_location_mapping.name store_name,
       l_spabiz_location_mapping.workday_id workday_id,
       p_spabiz_location_mapping.p_spabiz_location_mapping_id,
       p_spabiz_location_mapping.dv_batch_id,
       p_spabiz_location_mapping.dv_load_date_time,
       p_spabiz_location_mapping.dv_load_end_date_time
  from dbo.p_spabiz_location_mapping
  join #p_spabiz_location_mapping_insert
    on p_spabiz_location_mapping.bk_hash = #p_spabiz_location_mapping_insert.bk_hash
   and p_spabiz_location_mapping.p_spabiz_location_mapping_id = #p_spabiz_location_mapping_insert.p_spabiz_location_mapping_id
  join dbo.l_spabiz_location_mapping
    on p_spabiz_location_mapping.bk_hash = l_spabiz_location_mapping.bk_hash
   and p_spabiz_location_mapping.l_spabiz_location_mapping_id = l_spabiz_location_mapping.l_spabiz_location_mapping_id
  join dbo.s_spabiz_location_mapping
    on p_spabiz_location_mapping.bk_hash = s_spabiz_location_mapping.bk_hash
   and p_spabiz_location_mapping.s_spabiz_location_mapping_id = s_spabiz_location_mapping.s_spabiz_location_mapping_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_spabiz_location_mapping
   where d_spabiz_location_mapping.bk_hash in (select bk_hash from #p_spabiz_location_mapping_insert)

  insert dbo.d_spabiz_location_mapping(
             bk_hash,
             dim_spabiz_location_mapping_key,
             spabiz_store_number,
             store_name,
             workday_id,
             p_spabiz_location_mapping_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_spabiz_location_mapping_key,
         spabiz_store_number,
         store_name,
         workday_id,
         p_spabiz_location_mapping_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_spabiz_location_mapping)
--Done!
end
