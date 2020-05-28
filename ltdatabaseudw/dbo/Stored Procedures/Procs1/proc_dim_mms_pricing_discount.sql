CREATE PROC [dbo].[proc_dim_mms_pricing_discount] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on

declare @max_dv_batch_id bigint = (select max(isnull(dv_batch_id,-1)) from dim_mms_pricing_discount)
declare @current_dv_batch_id bigint = @dv_batch_id
declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end


if object_id('tempdb..#dim_mms_pricing_discount') is not null drop table #dim_mms_pricing_discount
create table dbo.#dim_mms_pricing_discount with(distribution=hash(dim_mms_pricing_discount_key), location=user_db, heap) as

select d_mms_pricing_discount.dim_mms_pricing_discount_key dim_mms_pricing_discount_key,  --generate the key in d_mms_pricing_discount
       d_mms_pricing_discount.pricing_discount_id pricing_discount_id,
	   d_mms_pricing_discount.p_mms_pricing_discount_id p_mms_pricing_discount_id,
       isnull(d_mms_sales_promotion.sales_promotion_display_text,'') sales_promotion_display_text,
	   isnull(d_mms_sales_promotion.sales_promotion_receipt_text,'') sales_promotion_receipt_text,
	   isnull(r_mms_val_discount_type.description,'') discount_type,
	   isnull(r_mms_val_discount_application_type.description,'') discount_application_type,
	   isnull(r_mms_val_discount_combine_rule.description,'') discount_combine_rule,
	   d_mms_pricing_discount.discount_value discount_value,
	   d_mms_pricing_discount.sales_commission_percent sales_commission_percent,
	   d_mms_pricing_discount.service_commission_percent service_commission_percent,
	   d_mms_pricing_discount.available_for_all_products_flag available_for_all_products_flag,
	   d_mms_pricing_discount.all_products_discount_use_limit all_products_discount_use_limit,
       d_mms_pricing_discount.effective_from_dim_date_key,
	   d_mms_pricing_discount.effective_thru_dim_date_key,
	   d_mms_pricing_discount.effective_from_date_time effective_from_date_time,
	   d_mms_pricing_discount.effective_thru_date_time effective_thru_date_time,
case when d_mms_pricing_discount.dv_load_date_time >= isnull(d_mms_sales_promotion.dv_load_date_time,'Jan 1, 1753')
      and d_mms_pricing_discount.dv_load_date_time >= isnull(r_mms_val_discount_application_type.dv_load_date_time,'Jan 1, 1753')
      and d_mms_pricing_discount.dv_load_date_time >= isnull(r_mms_val_discount_combine_rule.dv_load_date_time,'Jan 1, 1753')
      and d_mms_pricing_discount.dv_load_date_time >= isnull(r_mms_val_discount_type.dv_load_date_time,'Jan 1, 1753')
     then d_mms_pricing_discount.dv_load_date_time
     when d_mms_sales_promotion.dv_load_date_time >= isnull(r_mms_val_discount_application_type.dv_load_date_time,'Jan 1, 1753')
      and d_mms_sales_promotion.dv_load_date_time >= isnull(r_mms_val_discount_combine_rule.dv_load_date_time,'Jan 1, 1753')
      and d_mms_sales_promotion.dv_load_date_time >= isnull(r_mms_val_discount_type.dv_load_date_time,'Jan 1, 1753')
     then d_mms_sales_promotion.dv_load_date_time
     when r_mms_val_discount_application_type.dv_load_date_time >= isnull(r_mms_val_discount_combine_rule.dv_load_date_time,'Jan 1, 1753')
      and r_mms_val_discount_application_type.dv_load_date_time >= isnull(r_mms_val_discount_type.dv_load_date_time,'Jan 1, 1753')
     then r_mms_val_discount_application_type.dv_load_date_time
     when r_mms_val_discount_combine_rule.dv_load_date_time >= isnull(r_mms_val_discount_type.dv_load_date_time,'Jan 1, 1753')
     then r_mms_val_discount_combine_rule.dv_load_date_time
     else isnull(r_mms_val_discount_type.dv_load_date_time,'Jan 1, 1753') end dv_load_date_time,
       'Dec 31, 9999' dv_load_end_date_time,
    case when d_mms_pricing_discount.dv_batch_id >= isnull(d_mms_sales_promotion.dv_batch_id,-1)
          and d_mms_pricing_discount.dv_batch_id >= isnull(r_mms_val_discount_application_type.dv_batch_id,-1)
          and d_mms_pricing_discount.dv_batch_id >= isnull(r_mms_val_discount_combine_rule.dv_batch_id,-1)
          and d_mms_pricing_discount.dv_batch_id >= isnull(r_mms_val_discount_type.dv_batch_id,-1)
         then d_mms_pricing_discount.dv_batch_id
         when d_mms_sales_promotion.dv_batch_id >= isnull(r_mms_val_discount_application_type.dv_batch_id,-1)
          and d_mms_sales_promotion.dv_batch_id >= isnull(r_mms_val_discount_combine_rule.dv_batch_id,-1)
          and d_mms_sales_promotion.dv_batch_id >= isnull(r_mms_val_discount_type.dv_batch_id,-1)
         then d_mms_sales_promotion.dv_batch_id
         when r_mms_val_discount_application_type.dv_batch_id >= isnull(r_mms_val_discount_combine_rule.dv_batch_id,-1)
          and r_mms_val_discount_application_type.dv_batch_id >= isnull(r_mms_val_discount_type.dv_batch_id,-1)
         then r_mms_val_discount_application_type.dv_batch_id
         when r_mms_val_discount_combine_rule.dv_batch_id >= isnull(r_mms_val_discount_type.dv_batch_id,-1)
         then r_mms_val_discount_combine_rule.dv_batch_id
         else isnull(r_mms_val_discount_type.dv_batch_id,-1) end dv_batch_id,
       getdate() dv_inserted_date_time,
       suser_sname() dv_insert_user
   from d_mms_pricing_discount d_mms_pricing_discount
   left join d_mms_sales_promotion d_mms_sales_promotion
     on d_mms_pricing_discount.sales_promotion_id = d_mms_sales_promotion.sales_promotion_id
   left join r_mms_val_discount_application_type r_mms_val_discount_application_type
     on d_mms_pricing_discount.val_discount_application_type_id = r_mms_val_discount_application_type.val_discount_application_type_id
	and r_mms_val_discount_application_type.dv_load_end_date_time = 'dec 31, 9999'
   left join r_mms_val_discount_combine_rule r_mms_val_discount_combine_rule
     on d_mms_pricing_discount.val_discount_combine_rule_id = r_mms_val_discount_combine_rule.val_discount_combine_rule_id
	and r_mms_val_discount_combine_rule.dv_load_end_date_time = 'dec 31, 9999'
   left join r_mms_val_discount_type r_mms_val_discount_type
     on d_mms_pricing_discount.val_discount_type_id = r_mms_val_discount_type.val_discount_type_id
	and r_mms_val_discount_type.dv_load_end_date_time = 'dec 31, 9999'
  where r_mms_val_discount_type.dv_batch_id >= @load_dv_batch_id
	 or d_mms_pricing_discount.dv_batch_id >= @load_dv_batch_id
	 or r_mms_val_discount_application_type.dv_batch_id >= @load_dv_batch_id
	 or r_mms_val_discount_combine_rule.dv_batch_id >= @load_dv_batch_id
	 or d_mms_sales_promotion.dv_batch_id >= @load_dv_batch_id

	   
-- Delete and re-insert as a single transaction
--   Delete records from the table that exist
--   Insert records from records from current and missing batches

begin tran

  delete dbo.dim_mms_pricing_discount
   where dim_mms_pricing_discount_key in (select dim_mms_pricing_discount_key from dbo.#dim_mms_pricing_discount) 

  insert into dim_mms_pricing_discount
   (dim_mms_pricing_discount_key,
	pricing_discount_id,
    discount_application_type,
    sales_promotion_pos_display_text,
    sales_promotion_receipt_text,
    discount_type,
    discount_combine_rule,
    discount_value,
	sales_commission_percent,
	service_commission_percent,
	available_for_all_products_flag,
	all_products_discount_use_limit,
	effective_from_dim_date_key,
	effective_thru_dim_date_key,
	effective_from_date_time,
	effective_thru_date_time,
    dv_load_date_time,
    dv_load_end_date_time,
    dv_batch_id,
    dv_inserted_date_time,
    dv_insert_user
     )
 select dim_mms_pricing_discount_key,
    pricing_discount_id,
    discount_application_type,
    sales_promotion_display_text,
    sales_promotion_receipt_text,
    discount_type,
    discount_combine_rule,
    discount_value,
	sales_commission_percent,
	service_commission_percent,
	available_for_all_products_flag,
	all_products_discount_use_limit,
	effective_from_dim_date_key,
	effective_thru_dim_date_key,
	effective_from_date_time,
	effective_thru_date_time,
    dv_load_date_time,
    dv_load_end_date_time,
    dv_batch_id,
    dv_inserted_date_time,
    dv_insert_user
    from #dim_mms_pricing_discount

commit tran

end
