CREATE PROC [dbo].[proc_d_mms_sales_promotion] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_sales_promotion)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mms_sales_promotion_insert') is not null drop table #p_mms_sales_promotion_insert
create table dbo.#p_mms_sales_promotion_insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_sales_promotion.p_mms_sales_promotion_id,
       p_mms_sales_promotion.bk_hash
  from dbo.p_mms_sales_promotion
 where p_mms_sales_promotion.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_mms_sales_promotion.dv_batch_id > @max_dv_batch_id
        or p_mms_sales_promotion.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_sales_promotion.bk_hash,
       p_mms_sales_promotion.bk_hash dim_mms_sales_promotion_key,
       p_mms_sales_promotion.sales_promotion_id sales_promotion_id,
       s_mms_sales_promotion.effective_from_date_time effective_from_date_time,
       s_mms_sales_promotion.effective_thru_date_time effective_thru_date_time,
       case when s_mms_sales_promotion.exclude_from_attrition_reporting_flag = 1 then 'Y'
                   else 'N'
               end exclude_from_attrition_reporting_flag,
       case when s_mms_sales_promotion.exclude_my_health_check_flag = 1 then 'Y'
                   else 'N'
               end exclude_my_health_check_flag,
       isnull(s_mms_sales_promotion.display_text,'') sales_promotion_display_text,
       isnull(s_mms_sales_promotion.receipt_text,'') sales_promotion_receipt_text,
       isnull(l_mms_sales_promotion.val_revenue_reporting_category_id,'-998') val_revenue_reporting_category_id,
       isnull(l_mms_sales_promotion.val_sales_promotion_type_id,'-998') val_sales_promotion_type_id,
       isnull(l_mms_sales_promotion.val_sales_reporting_category_id,'-998') val_sales_reporting_category_id,
       isnull(h_mms_sales_promotion.dv_deleted,0) dv_deleted,
       p_mms_sales_promotion.p_mms_sales_promotion_id,
       p_mms_sales_promotion.dv_batch_id,
       p_mms_sales_promotion.dv_load_date_time,
       p_mms_sales_promotion.dv_load_end_date_time
  from dbo.h_mms_sales_promotion
  join dbo.p_mms_sales_promotion
    on h_mms_sales_promotion.bk_hash = p_mms_sales_promotion.bk_hash
  join #p_mms_sales_promotion_insert
    on p_mms_sales_promotion.bk_hash = #p_mms_sales_promotion_insert.bk_hash
   and p_mms_sales_promotion.p_mms_sales_promotion_id = #p_mms_sales_promotion_insert.p_mms_sales_promotion_id
  join dbo.l_mms_sales_promotion
    on p_mms_sales_promotion.bk_hash = l_mms_sales_promotion.bk_hash
   and p_mms_sales_promotion.l_mms_sales_promotion_id = l_mms_sales_promotion.l_mms_sales_promotion_id
  join dbo.s_mms_sales_promotion
    on p_mms_sales_promotion.bk_hash = s_mms_sales_promotion.bk_hash
   and p_mms_sales_promotion.s_mms_sales_promotion_id = s_mms_sales_promotion.s_mms_sales_promotion_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_mms_sales_promotion
   where d_mms_sales_promotion.bk_hash in (select bk_hash from #p_mms_sales_promotion_insert)

  insert dbo.d_mms_sales_promotion(
             bk_hash,
             dim_mms_sales_promotion_key,
             sales_promotion_id,
             effective_from_date_time,
             effective_thru_date_time,
             exclude_from_attrition_reporting_flag,
             exclude_my_health_check_flag,
             sales_promotion_display_text,
             sales_promotion_receipt_text,
             val_revenue_reporting_category_id,
             val_sales_promotion_type_id,
             val_sales_reporting_category_id,
             deleted_flag,
             p_mms_sales_promotion_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_mms_sales_promotion_key,
         sales_promotion_id,
         effective_from_date_time,
         effective_thru_date_time,
         exclude_from_attrition_reporting_flag,
         exclude_my_health_check_flag,
         sales_promotion_display_text,
         sales_promotion_receipt_text,
         val_revenue_reporting_category_id,
         val_sales_promotion_type_id,
         val_sales_reporting_category_id,
         dv_deleted,
         p_mms_sales_promotion_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_sales_promotion)
--Done!
end
