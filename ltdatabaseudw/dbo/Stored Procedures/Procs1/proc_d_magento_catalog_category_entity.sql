CREATE PROC [dbo].[proc_d_magento_catalog_category_entity] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_magento_catalog_category_entity)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_magento_catalog_category_entity_insert') is not null drop table #p_magento_catalog_category_entity_insert
create table dbo.#p_magento_catalog_category_entity_insert with(distribution=hash(bk_hash), location=user_db) as
select p_magento_catalog_category_entity.p_magento_catalog_category_entity_id,
       p_magento_catalog_category_entity.bk_hash
  from dbo.p_magento_catalog_category_entity
 where p_magento_catalog_category_entity.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_magento_catalog_category_entity.dv_batch_id > @max_dv_batch_id
        or p_magento_catalog_category_entity.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_magento_catalog_category_entity.bk_hash,
       p_magento_catalog_category_entity.row_id row_id,
       l_magento_catalog_category_entity.attribute_set_id attribute_set_id,
       s_magento_catalog_category_entity.children_count children_count,
       s_magento_catalog_category_entity.created_at created_at,
       case when p_magento_catalog_category_entity.bk_hash in('-997', '-998', '-999') then p_magento_catalog_category_entity.bk_hash
           when s_magento_catalog_category_entity.created_at is null then '-998'
        else convert(varchar, s_magento_catalog_category_entity.created_at, 112)    end created_dim_date_key,
       case when p_magento_catalog_category_entity.bk_hash in ('-997','-998','-999') then p_magento_catalog_category_entity.bk_hash
       when s_magento_catalog_category_entity.created_at is null then '-998'
       else '1' + replace(substring(convert(varchar,s_magento_catalog_category_entity.created_at,114), 1, 5),':','') end created_dim_time_key,
       s_magento_catalog_category_entity.created_in created_in,
       l_magento_catalog_category_entity.entity_id entity_id,
       s_magento_catalog_category_entity.level level,
       l_magento_catalog_category_entity.parent_id parent_id,
       s_magento_catalog_category_entity.path path,
       s_magento_catalog_category_entity.position position,
       s_magento_catalog_category_entity.updated_at updated_at,
       case when p_magento_catalog_category_entity.bk_hash in('-997', '-998', '-999') then p_magento_catalog_category_entity.bk_hash
           when s_magento_catalog_category_entity.updated_at is null then '-998'
        else convert(varchar, s_magento_catalog_category_entity.updated_at, 112)    end updated_dim_date_key,
       case when p_magento_catalog_category_entity.bk_hash in ('-997','-998','-999') then p_magento_catalog_category_entity.bk_hash
       when s_magento_catalog_category_entity.updated_at is null then '-998'
       else '1' + replace(substring(convert(varchar,s_magento_catalog_category_entity.updated_at,114), 1, 5),':','') end updated_dim_time_key,
       s_magento_catalog_category_entity.updated_in updated_in,
       isnull(h_magento_catalog_category_entity.dv_deleted,0) dv_deleted,
       p_magento_catalog_category_entity.p_magento_catalog_category_entity_id,
       p_magento_catalog_category_entity.dv_batch_id,
       p_magento_catalog_category_entity.dv_load_date_time,
       p_magento_catalog_category_entity.dv_load_end_date_time
  from dbo.h_magento_catalog_category_entity
  join dbo.p_magento_catalog_category_entity
    on h_magento_catalog_category_entity.bk_hash = p_magento_catalog_category_entity.bk_hash
  join #p_magento_catalog_category_entity_insert
    on p_magento_catalog_category_entity.bk_hash = #p_magento_catalog_category_entity_insert.bk_hash
   and p_magento_catalog_category_entity.p_magento_catalog_category_entity_id = #p_magento_catalog_category_entity_insert.p_magento_catalog_category_entity_id
  join dbo.l_magento_catalog_category_entity
    on p_magento_catalog_category_entity.bk_hash = l_magento_catalog_category_entity.bk_hash
   and p_magento_catalog_category_entity.l_magento_catalog_category_entity_id = l_magento_catalog_category_entity.l_magento_catalog_category_entity_id
  join dbo.s_magento_catalog_category_entity
    on p_magento_catalog_category_entity.bk_hash = s_magento_catalog_category_entity.bk_hash
   and p_magento_catalog_category_entity.s_magento_catalog_category_entity_id = s_magento_catalog_category_entity.s_magento_catalog_category_entity_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_magento_catalog_category_entity
   where d_magento_catalog_category_entity.bk_hash in (select bk_hash from #p_magento_catalog_category_entity_insert)

  insert dbo.d_magento_catalog_category_entity(
             bk_hash,
             row_id,
             attribute_set_id,
             children_count,
             created_at,
             created_dim_date_key,
             created_dim_time_key,
             created_in,
             entity_id,
             level,
             parent_id,
             path,
             position,
             updated_at,
             updated_dim_date_key,
             updated_dim_time_key,
             updated_in,
             deleted_flag,
             p_magento_catalog_category_entity_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         row_id,
         attribute_set_id,
         children_count,
         created_at,
         created_dim_date_key,
         created_dim_time_key,
         created_in,
         entity_id,
         level,
         parent_id,
         path,
         position,
         updated_at,
         updated_dim_date_key,
         updated_dim_time_key,
         updated_in,
         dv_deleted,
         p_magento_catalog_category_entity_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_magento_catalog_category_entity)
--Done!
end
