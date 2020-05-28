CREATE PROC [dbo].[proc_d_magento_customer_eav_attribute] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_magento_customer_eav_attribute)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_magento_customer_eav_attribute_insert') is not null drop table #p_magento_customer_eav_attribute_insert
create table dbo.#p_magento_customer_eav_attribute_insert with(distribution=hash(bk_hash), location=user_db) as
select p_magento_customer_eav_attribute.p_magento_customer_eav_attribute_id,
       p_magento_customer_eav_attribute.bk_hash
  from dbo.p_magento_customer_eav_attribute
 where p_magento_customer_eav_attribute.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_magento_customer_eav_attribute.dv_batch_id > @max_dv_batch_id
        or p_magento_customer_eav_attribute.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_magento_customer_eav_attribute.bk_hash,
       p_magento_customer_eav_attribute.attribute_id attribute_id,
       s_magento_customer_eav_attribute.data_model data_model,
       s_magento_customer_eav_attribute.input_filter input_filter,
       case when s_magento_customer_eav_attribute.is_filterable_in_grid = 1 then 'Y' else 'N' end is_filterable_in_grid,
       case when s_magento_customer_eav_attribute.is_searchable_in_grid = 1 then 'Y' else 'N' end is_searchable_in_grid,
       case when s_magento_customer_eav_attribute.is_system  = 1 then 'Y' else 'N' end is_system,
       case when s_magento_customer_eav_attribute.is_used_for_customer_segment  = 1 then 'Y' else 'N' end is_used_for_customer_segment,
       case when s_magento_customer_eav_attribute.is_used_in_grid = 1 then 'Y' else 'N' end is_used_in_grid,
       case when s_magento_customer_eav_attribute.is_visible = 1 then 'Y' else 'N' end is_visible,
       case when s_magento_customer_eav_attribute.is_visible_in_grid = 1 then 'Y' else 'N' end is_visible_in_grid,
       s_magento_customer_eav_attribute.multi_line_count multi_line_count,
       s_magento_customer_eav_attribute.sort_order sort_order,
       s_magento_customer_eav_attribute.validate_rules validate_rules,
       isnull(h_magento_customer_eav_attribute.dv_deleted,0) dv_deleted,
       p_magento_customer_eav_attribute.p_magento_customer_eav_attribute_id,
       p_magento_customer_eav_attribute.dv_batch_id,
       p_magento_customer_eav_attribute.dv_load_date_time,
       p_magento_customer_eav_attribute.dv_load_end_date_time
  from dbo.h_magento_customer_eav_attribute
  join dbo.p_magento_customer_eav_attribute
    on h_magento_customer_eav_attribute.bk_hash = p_magento_customer_eav_attribute.bk_hash
  join #p_magento_customer_eav_attribute_insert
    on p_magento_customer_eav_attribute.bk_hash = #p_magento_customer_eav_attribute_insert.bk_hash
   and p_magento_customer_eav_attribute.p_magento_customer_eav_attribute_id = #p_magento_customer_eav_attribute_insert.p_magento_customer_eav_attribute_id
  join dbo.s_magento_customer_eav_attribute
    on p_magento_customer_eav_attribute.bk_hash = s_magento_customer_eav_attribute.bk_hash
   and p_magento_customer_eav_attribute.s_magento_customer_eav_attribute_id = s_magento_customer_eav_attribute.s_magento_customer_eav_attribute_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_magento_customer_eav_attribute
   where d_magento_customer_eav_attribute.bk_hash in (select bk_hash from #p_magento_customer_eav_attribute_insert)

  insert dbo.d_magento_customer_eav_attribute(
             bk_hash,
             attribute_id,
             data_model,
             input_filter,
             is_filterable_in_grid,
             is_searchable_in_grid,
             is_system,
             is_used_for_customer_segment,
             is_used_in_grid,
             is_visible,
             is_visible_in_grid,
             multi_line_count,
             sort_order,
             validate_rules,
             deleted_flag,
             p_magento_customer_eav_attribute_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         attribute_id,
         data_model,
         input_filter,
         is_filterable_in_grid,
         is_searchable_in_grid,
         is_system,
         is_used_for_customer_segment,
         is_used_in_grid,
         is_visible,
         is_visible_in_grid,
         multi_line_count,
         sort_order,
         validate_rules,
         dv_deleted,
         p_magento_customer_eav_attribute_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_magento_customer_eav_attribute)
--Done!
end
