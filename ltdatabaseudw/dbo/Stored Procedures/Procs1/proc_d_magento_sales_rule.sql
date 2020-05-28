CREATE PROC [dbo].[proc_d_magento_sales_rule] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_magento_sales_rule)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_magento_sales_rule_insert') is not null drop table #p_magento_sales_rule_insert
create table dbo.#p_magento_sales_rule_insert with(distribution=hash(bk_hash), location=user_db) as
select p_magento_sales_rule.p_magento_sales_rule_id,
       p_magento_sales_rule.bk_hash
  from dbo.p_magento_sales_rule
 where p_magento_sales_rule.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_magento_sales_rule.dv_batch_id > @max_dv_batch_id
        or p_magento_sales_rule.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_magento_sales_rule.bk_hash,
       p_magento_sales_rule.row_id row_id,
       s_magento_sales_rule.actions_serialized actions_serialized,
       s_magento_sales_rule.apply_to_shipping apply_to_shipping,
       s_magento_sales_rule.conditions_serialized conditions_serialized,
       s_magento_sales_rule.coupon_type coupon_type,
       s_magento_sales_rule.created_in created_in,
       s_magento_sales_rule.description description,
       s_magento_sales_rule.discount_amount discount_amount,
       s_magento_sales_rule.discount_qty discount_qty,
       s_magento_sales_rule.discount_step discount_step,
       s_magento_sales_rule.from_date from_date,
       case when p_magento_sales_rule.bk_hash in('-997', '-998', '-999') then p_magento_sales_rule.bk_hash
           when s_magento_sales_rule.from_date is null then '-998'
        else convert(varchar, s_magento_sales_rule.from_date, 112) end from_dim_date_key,
       case when s_magento_sales_rule.is_active= 1 then 'Y' else 'N' end is_active_flag,
       case when s_magento_sales_rule.is_advanced = 1 then 'Y' else 'N' end is_advanced_flag,
       case when s_magento_sales_rule.is_rss = 1 then 'Y' else 'N' end is_rss_flag,
       s_magento_sales_rule.name name,
       s_magento_sales_rule.product_ids product_ids,
       l_magento_sales_rule.rule_id rule_id,
       s_magento_sales_rule.simple_action simple_action,
       s_magento_sales_rule.simple_free_shipping simple_free_shipping,
       s_magento_sales_rule.sort_order sort_order,
       s_magento_sales_rule.stop_rules_processing stop_rules_processing,
       s_magento_sales_rule.times_used times_used,
       s_magento_sales_rule.to_date to_date,
       case when p_magento_sales_rule.bk_hash in('-997', '-998', '-999') then p_magento_sales_rule.bk_hash
           when s_magento_sales_rule.to_date is null then '-998'
        else convert(varchar, s_magento_sales_rule.to_date, 112) end to_dim_date_key,
       s_magento_sales_rule.updated_in updated_in,
       s_magento_sales_rule.use_auto_generation use_auto_generation,
       s_magento_sales_rule.uses_per_coupon uses_per_coupon,
       s_magento_sales_rule.uses_per_customer uses_per_customer,
       isnull(h_magento_sales_rule.dv_deleted,0) dv_deleted,
       p_magento_sales_rule.p_magento_sales_rule_id,
       p_magento_sales_rule.dv_batch_id,
       p_magento_sales_rule.dv_load_date_time,
       p_magento_sales_rule.dv_load_end_date_time
  from dbo.h_magento_sales_rule
  join dbo.p_magento_sales_rule
    on h_magento_sales_rule.bk_hash = p_magento_sales_rule.bk_hash
  join #p_magento_sales_rule_insert
    on p_magento_sales_rule.bk_hash = #p_magento_sales_rule_insert.bk_hash
   and p_magento_sales_rule.p_magento_sales_rule_id = #p_magento_sales_rule_insert.p_magento_sales_rule_id
  join dbo.l_magento_sales_rule
    on p_magento_sales_rule.bk_hash = l_magento_sales_rule.bk_hash
   and p_magento_sales_rule.l_magento_sales_rule_id = l_magento_sales_rule.l_magento_sales_rule_id
  join dbo.s_magento_sales_rule
    on p_magento_sales_rule.bk_hash = s_magento_sales_rule.bk_hash
   and p_magento_sales_rule.s_magento_sales_rule_id = s_magento_sales_rule.s_magento_sales_rule_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_magento_sales_rule
   where d_magento_sales_rule.bk_hash in (select bk_hash from #p_magento_sales_rule_insert)

  insert dbo.d_magento_sales_rule(
             bk_hash,
             row_id,
             actions_serialized,
             apply_to_shipping,
             conditions_serialized,
             coupon_type,
             created_in,
             description,
             discount_amount,
             discount_qty,
             discount_step,
             from_date,
             from_dim_date_key,
             is_active_flag,
             is_advanced_flag,
             is_rss_flag,
             name,
             product_ids,
             rule_id,
             simple_action,
             simple_free_shipping,
             sort_order,
             stop_rules_processing,
             times_used,
             to_date,
             to_dim_date_key,
             updated_in,
             use_auto_generation,
             uses_per_coupon,
             uses_per_customer,
             deleted_flag,
             p_magento_sales_rule_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         row_id,
         actions_serialized,
         apply_to_shipping,
         conditions_serialized,
         coupon_type,
         created_in,
         description,
         discount_amount,
         discount_qty,
         discount_step,
         from_date,
         from_dim_date_key,
         is_active_flag,
         is_advanced_flag,
         is_rss_flag,
         name,
         product_ids,
         rule_id,
         simple_action,
         simple_free_shipping,
         sort_order,
         stop_rules_processing,
         times_used,
         to_date,
         to_dim_date_key,
         updated_in,
         use_auto_generation,
         uses_per_coupon,
         uses_per_customer,
         dv_deleted,
         p_magento_sales_rule_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_magento_sales_rule)
--Done!
end
