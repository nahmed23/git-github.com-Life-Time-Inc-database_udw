CREATE PROC [dbo].[proc_fact_cafe_payment] @dv_batch_id [varchar](500) AS
begin

set xact_abort on
set nocount on

declare @max_dv_batch_id bigint = (select max(isnull(dv_batch_id,-1)) from fact_cafe_payment)
declare @current_dv_batch_id bigint = @dv_batch_id
declare @load_dv_batch_id bigint = case when @max_dv_batch_id < @current_dv_batch_id then @max_dv_batch_id else @current_dv_batch_id end


if object_id('tempdb..#etl_step_1') is not null drop table #etl_step_1
create table dbo.#etl_step_1 with(distribution=hash(fact_cafe_payment_key), location=user_db) as 
  select 
     d_ig_it_trn_order_tender.fact_cafe_payment_key fact_cafe_payment_key,
     d_ig_it_trn_order_tender.order_hdr_id order_hdr_id,
     d_ig_it_trn_order_tender.tender_seq tender_seq,
     d_ig_it_trn_order_tender.change_amount change_amount,
     d_ig_it_trn_order_tender.charges_to_date_amount charges_to_date_amount,
     d_ig_it_trn_order_tender.dim_cafe_payment_type_key dim_cafe_payment_type_key,
     isnull(d_mms_pt_credit_card_transaction.dim_mms_member_key, '-998') dim_mms_member_key,
     d_ig_it_trn_order_tender.pro_rata_discount_amount pro_rata_discount_amount,
     d_ig_it_trn_order_tender.pro_rata_gratuity_amount pro_rata_gratuity_amount,
     d_ig_it_trn_order_tender.pro_rata_sales_amount_gross pro_rata_sales_amount_gross,
	 d_ig_it_trn_order_tender.pro_rata_service_charge_amount pro_rata_service_charge_amount,
	 d_ig_it_trn_order_tender.pro_rata_tax_amount pro_rata_tax_amount,
     d_ig_it_trn_order_tender.remaining_balance_amount remaining_balance_amount,
     d_ig_it_trn_order_tender.tender_amount tender_amount,
     d_ig_it_trn_order_tender.tender_type_id tender_type_id,
     d_ig_it_trn_order_tender.tip_amount tip_amount,
	 d_ig_it_trn_order_tender.dv_load_date_time dv_load_date_time,
	 'dec 31, 9999'  dv_load_end_date_time ,
	 d_ig_it_trn_order_tender.dv_batch_id dv_batch_id,
	 getdate()  dv_inserted_date_time,
	 suser_sname()  dv_insert_user
	   from d_ig_it_trn_order_tender
  left join d_mms_pt_credit_card_transaction
    on d_ig_it_trn_order_tender.d_mms_pt_credit_card_transaction_bk_hash = d_mms_pt_credit_card_transaction.bk_hash
 where d_ig_it_trn_order_tender.dv_batch_id >= @load_dv_batch_id
 
 begin tran

  delete dbo.fact_cafe_payment
   where fact_cafe_payment_key in (select fact_cafe_payment_key from dbo.#etl_step_1) 
   insert into fact_cafe_payment
   (fact_cafe_payment_key,
   order_hdr_id,
   tender_seq,
   change_amount,
   charges_to_date_amount,
   dim_cafe_payment_type_key,
   dim_mms_member_key,
   pro_rata_discount_amount,
   pro_rata_gratuity_amount,
   pro_rata_sales_amount_gross,
   pro_rata_service_charge_amount,
   pro_rata_tax_amount,
   remaining_balance_amount,
   tender_amount,
   tender_type_id,
   tip_amount,
   dv_load_date_time,
   dv_load_end_date_time,
   dv_batch_id,
   dv_inserted_date_time,
   dv_insert_user)
   
   select fact_cafe_payment_key,
   order_hdr_id,
   tender_seq,
   change_amount,
   charges_to_date_amount,
   dim_cafe_payment_type_key,
   dim_mms_member_key,
   pro_rata_discount_amount,
   pro_rata_gratuity_amount,
   pro_rata_sales_amount_gross,
   pro_rata_service_charge_amount,
   pro_rata_tax_amount,
   remaining_balance_amount,
   tender_amount,
   tender_type_id,
   tip_amount,
   dv_load_date_time,
   dv_load_end_date_time,
   dv_batch_id,
   dv_inserted_date_time,
   dv_insert_user
   from #etl_step_1
   
  commit tran
  
 end
