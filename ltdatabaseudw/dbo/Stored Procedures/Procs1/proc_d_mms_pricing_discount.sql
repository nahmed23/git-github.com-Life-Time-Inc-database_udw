CREATE PROC [dbo].[proc_d_mms_pricing_discount] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_pricing_discount)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mms_pricing_discount_insert') is not null drop table #p_mms_pricing_discount_insert
create table dbo.#p_mms_pricing_discount_insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_pricing_discount.p_mms_pricing_discount_id,
       p_mms_pricing_discount.bk_hash
  from dbo.p_mms_pricing_discount
 where p_mms_pricing_discount.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_mms_pricing_discount.dv_batch_id > @max_dv_batch_id
        or p_mms_pricing_discount.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_pricing_discount.bk_hash,
       p_mms_pricing_discount.bk_hash dim_mms_pricing_discount_key,
       p_mms_pricing_discount.pricing_discount_id pricing_discount_id,
       s_mms_pricing_discount.all_products_discount_use_limit all_products_discount_use_limit,
       case when isnull(s_mms_pricing_discount.available_for_all_products_flag,0) = 1 then 'Y'
       	        else 'N'
       	   end  available_for_all_products_flag,
       s_mms_pricing_discount.discount_value discount_value,
       s_mms_pricing_discount.effective_from_date_time effective_from_date_time,
       case when p_mms_pricing_discount.bk_hash in ('-997', '-998', '-999') then p_mms_pricing_discount.bk_hash
             when s_mms_pricing_discount.effective_from_date_time is null then '-998'
              else convert(varchar, s_mms_pricing_discount.effective_from_date_time, 112)
       	   end effective_from_dim_date_key,
       s_mms_pricing_discount.effective_thru_date_time effective_thru_date_time,
       case when p_mms_pricing_discount.bk_hash in ('-997', '-998', '-999') then p_mms_pricing_discount.bk_hash
             when s_mms_pricing_discount.effective_thru_date_time is null then '-998'
              else convert(varchar, s_mms_pricing_discount.effective_thru_date_time, 112)
       	   end effective_thru_dim_date_key,
       isnull(s_mms_pricing_discount.sales_commission_percent,0) sales_commission_percent,
       l_mms_pricing_discount.sales_promotion_id sales_promotion_id,
       isnull(s_mms_pricing_discount.service_commission_percent,0) service_commission_percent,
       l_mms_pricing_discount.val_discount_application_type_id val_discount_application_type_id,
       l_mms_pricing_discount.val_discount_combine_rule_id val_discount_combine_rule_id,
       l_mms_pricing_discount.val_discount_type_id val_discount_type_id,
       p_mms_pricing_discount.p_mms_pricing_discount_id,
       p_mms_pricing_discount.dv_batch_id,
       p_mms_pricing_discount.dv_load_date_time,
       p_mms_pricing_discount.dv_load_end_date_time
  from dbo.h_mms_pricing_discount
  join dbo.p_mms_pricing_discount
    on h_mms_pricing_discount.bk_hash = p_mms_pricing_discount.bk_hash  join #p_mms_pricing_discount_insert
    on p_mms_pricing_discount.bk_hash = #p_mms_pricing_discount_insert.bk_hash
   and p_mms_pricing_discount.p_mms_pricing_discount_id = #p_mms_pricing_discount_insert.p_mms_pricing_discount_id
  join dbo.l_mms_pricing_discount
    on p_mms_pricing_discount.bk_hash = l_mms_pricing_discount.bk_hash
   and p_mms_pricing_discount.l_mms_pricing_discount_id = l_mms_pricing_discount.l_mms_pricing_discount_id
  join dbo.s_mms_pricing_discount
    on p_mms_pricing_discount.bk_hash = s_mms_pricing_discount.bk_hash
   and p_mms_pricing_discount.s_mms_pricing_discount_id = s_mms_pricing_discount.s_mms_pricing_discount_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_mms_pricing_discount
   where d_mms_pricing_discount.bk_hash in (select bk_hash from #p_mms_pricing_discount_insert)

  insert dbo.d_mms_pricing_discount(
             bk_hash,
             dim_mms_pricing_discount_key,
             pricing_discount_id,
             all_products_discount_use_limit,
             available_for_all_products_flag,
             discount_value,
             effective_from_date_time,
             effective_from_dim_date_key,
             effective_thru_date_time,
             effective_thru_dim_date_key,
             sales_commission_percent,
             sales_promotion_id,
             service_commission_percent,
             val_discount_application_type_id,
             val_discount_combine_rule_id,
             val_discount_type_id,
             p_mms_pricing_discount_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_mms_pricing_discount_key,
         pricing_discount_id,
         all_products_discount_use_limit,
         available_for_all_products_flag,
         discount_value,
         effective_from_date_time,
         effective_from_dim_date_key,
         effective_thru_date_time,
         effective_thru_dim_date_key,
         sales_commission_percent,
         sales_promotion_id,
         service_commission_percent,
         val_discount_application_type_id,
         val_discount_combine_rule_id,
         val_discount_type_id,
         p_mms_pricing_discount_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_pricing_discount)
--Done!
end
