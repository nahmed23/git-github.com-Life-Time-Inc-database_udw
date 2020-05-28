CREATE PROC [dbo].[proc_d_hybris_stock_levels] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_hybris_stock_levels)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_hybris_stock_levels_insert') is not null drop table #p_hybris_stock_levels_insert
create table dbo.#p_hybris_stock_levels_insert with(distribution=hash(bk_hash), location=user_db) as
select p_hybris_stock_levels.p_hybris_stock_levels_id,
       p_hybris_stock_levels.bk_hash
  from dbo.p_hybris_stock_levels
 where p_hybris_stock_levels.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_hybris_stock_levels.dv_batch_id > @max_dv_batch_id
        or p_hybris_stock_levels.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_hybris_stock_levels.bk_hash,
       p_hybris_stock_levels.bk_hash d_hybris_stock_levels_key,
       p_hybris_stock_levels.stock_levels_pk stock_levels_pk,
       s_hybris_stock_levels.acl_ts acl_ts,
       s_hybris_stock_levels.created_ts created_ts,
       s_hybris_stock_levels.hjmpts hjmpts,
       s_hybris_stock_levels.modified_ts modified_ts,
       s_hybris_stock_levels.Owner_Pk_String owner_pk_string,
       s_hybris_stock_levels.p_available p_available,
       l_hybris_stock_levels.p_in_stock_status p_in_stock_status,
       s_hybris_stock_levels.p_max_pre_order p_max_pre_order,
       s_hybris_stock_levels.p_max_stock_level_history_count p_max_stock_level_history_count,
       s_hybris_stock_levels.p_next_delivery_time p_next_delivery_time,
       s_hybris_stock_levels.p_over_selling p_over_selling,
       s_hybris_stock_levels.p_preorder p_pre_order,
       l_hybris_stock_levels.p_product_code p_product_code,
       s_hybris_stock_levels.p_release_date p_release_date,
       s_hybris_stock_levels.p_reserved p_reserved,
       s_hybris_stock_levels.p_treat_negative_as_zero p_treat_negative_as_zero,
       s_hybris_stock_levels.p_warehouse p_warehouse,
       s_hybris_stock_levels.prop_ts prop_ts,
       l_hybris_stock_levels.type_pk_string type_pk_string,
       p_hybris_stock_levels.p_hybris_stock_levels_id,
       p_hybris_stock_levels.dv_batch_id,
       p_hybris_stock_levels.dv_load_date_time,
       p_hybris_stock_levels.dv_load_end_date_time
  from dbo.h_hybris_stock_levels
  join dbo.p_hybris_stock_levels
    on h_hybris_stock_levels.bk_hash = p_hybris_stock_levels.bk_hash  join #p_hybris_stock_levels_insert
    on p_hybris_stock_levels.bk_hash = #p_hybris_stock_levels_insert.bk_hash
   and p_hybris_stock_levels.p_hybris_stock_levels_id = #p_hybris_stock_levels_insert.p_hybris_stock_levels_id
  join dbo.l_hybris_stock_levels
    on p_hybris_stock_levels.bk_hash = l_hybris_stock_levels.bk_hash
   and p_hybris_stock_levels.l_hybris_stock_levels_id = l_hybris_stock_levels.l_hybris_stock_levels_id
  join dbo.s_hybris_stock_levels
    on p_hybris_stock_levels.bk_hash = s_hybris_stock_levels.bk_hash
   and p_hybris_stock_levels.s_hybris_stock_levels_id = s_hybris_stock_levels.s_hybris_stock_levels_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_hybris_stock_levels
   where d_hybris_stock_levels.bk_hash in (select bk_hash from #p_hybris_stock_levels_insert)

  insert dbo.d_hybris_stock_levels(
             bk_hash,
             d_hybris_stock_levels_key,
             stock_levels_pk,
             acl_ts,
             created_ts,
             hjmpts,
             modified_ts,
             owner_pk_string,
             p_available,
             p_in_stock_status,
             p_max_pre_order,
             p_max_stock_level_history_count,
             p_next_delivery_time,
             p_over_selling,
             p_pre_order,
             p_product_code,
             p_release_date,
             p_reserved,
             p_treat_negative_as_zero,
             p_warehouse,
             prop_ts,
             type_pk_string,
             p_hybris_stock_levels_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         d_hybris_stock_levels_key,
         stock_levels_pk,
         acl_ts,
         created_ts,
         hjmpts,
         modified_ts,
         owner_pk_string,
         p_available,
         p_in_stock_status,
         p_max_pre_order,
         p_max_stock_level_history_count,
         p_next_delivery_time,
         p_over_selling,
         p_pre_order,
         p_product_code,
         p_release_date,
         p_reserved,
         p_treat_negative_as_zero,
         p_warehouse,
         prop_ts,
         type_pk_string,
         p_hybris_stock_levels_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_hybris_stock_levels)
--Done!
end
