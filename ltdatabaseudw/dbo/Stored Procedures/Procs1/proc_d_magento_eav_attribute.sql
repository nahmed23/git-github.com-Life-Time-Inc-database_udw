CREATE PROC [dbo].[proc_d_magento_eav_attribute] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_magento_eav_attribute)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_magento_eav_attribute_insert') is not null drop table #p_magento_eav_attribute_insert
create table dbo.#p_magento_eav_attribute_insert with(distribution=hash(bk_hash), location=user_db) as
select p_magento_eav_attribute.p_magento_eav_attribute_id,
       p_magento_eav_attribute.bk_hash
  from dbo.p_magento_eav_attribute
 where p_magento_eav_attribute.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_magento_eav_attribute.dv_batch_id > @max_dv_batch_id
        or p_magento_eav_attribute.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_magento_eav_attribute.bk_hash,
       p_magento_eav_attribute.bk_hash attribute_bk_hash,
       p_magento_eav_attribute.attribute_id attribute_id,
       s_magento_eav_attribute.attribute_code attribute_code,
       case when s_magento_eav_attribute.source_model = 'Magento\Eav\Model\Entity\Attribute\Source\Table' and s_magento_eav_attribute.frontend_input = 'select' and s_magento_eav_attribute.backend_type = 'int' then 'Y' else 'N' end is_option_value_lookup,
       isnull(h_magento_eav_attribute.dv_deleted,0) dv_deleted,
       p_magento_eav_attribute.p_magento_eav_attribute_id,
       p_magento_eav_attribute.dv_batch_id,
       p_magento_eav_attribute.dv_load_date_time,
       p_magento_eav_attribute.dv_load_end_date_time
  from dbo.h_magento_eav_attribute
  join dbo.p_magento_eav_attribute
    on h_magento_eav_attribute.bk_hash = p_magento_eav_attribute.bk_hash
  join #p_magento_eav_attribute_insert
    on p_magento_eav_attribute.bk_hash = #p_magento_eav_attribute_insert.bk_hash
   and p_magento_eav_attribute.p_magento_eav_attribute_id = #p_magento_eav_attribute_insert.p_magento_eav_attribute_id
  join dbo.l_magento_eav_attribute
    on p_magento_eav_attribute.bk_hash = l_magento_eav_attribute.bk_hash
   and p_magento_eav_attribute.l_magento_eav_attribute_id = l_magento_eav_attribute.l_magento_eav_attribute_id
  join dbo.s_magento_eav_attribute
    on p_magento_eav_attribute.bk_hash = s_magento_eav_attribute.bk_hash
   and p_magento_eav_attribute.s_magento_eav_attribute_id = s_magento_eav_attribute.s_magento_eav_attribute_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_magento_eav_attribute
   where d_magento_eav_attribute.bk_hash in (select bk_hash from #p_magento_eav_attribute_insert)

  insert dbo.d_magento_eav_attribute(
             bk_hash,
             attribute_bk_hash,
             attribute_id,
             attribute_code,
             is_option_value_lookup,
             deleted_flag,
             p_magento_eav_attribute_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         attribute_bk_hash,
         attribute_id,
         attribute_code,
         is_option_value_lookup,
         dv_deleted,
         p_magento_eav_attribute_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_magento_eav_attribute)
--Done!
end
