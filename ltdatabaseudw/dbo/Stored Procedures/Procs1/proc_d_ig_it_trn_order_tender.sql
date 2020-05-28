CREATE PROC [dbo].[proc_d_ig_it_trn_order_tender] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_ig_it_trn_order_tender)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_ig_it_trn_order_tender_insert') is not null drop table #p_ig_it_trn_order_tender_insert
create table dbo.#p_ig_it_trn_order_tender_insert with(distribution=hash(bk_hash), location=user_db) as
select p_ig_it_trn_order_tender.p_ig_it_trn_order_tender_id,
       p_ig_it_trn_order_tender.bk_hash
  from dbo.p_ig_it_trn_order_tender
 where p_ig_it_trn_order_tender.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_ig_it_trn_order_tender.dv_batch_id > @max_dv_batch_id
        or p_ig_it_trn_order_tender.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_ig_it_trn_order_tender.bk_hash,
       p_ig_it_trn_order_tender.bk_hash fact_cafe_payment_key,
       p_ig_it_trn_order_tender.order_hdr_id order_hdr_id,
       p_ig_it_trn_order_tender.tender_seq tender_seq,
       s_ig_it_trn_order_tender.change_amt change_amount,
       s_ig_it_trn_order_tender.charges_to_date_amt charges_to_date_amount,
       case when p_ig_it_trn_order_tender.bk_hash in ('-997', '-998', '-999') then p_ig_it_trn_order_tender.bk_hash
    when l_ig_it_trn_order_tender.order_hdr_id is null then '-998'
	else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_ig_it_trn_order_tender.order_hdr_id as varchar(500)),'z#@$k%&P'))),2) 
 end d_ig_it_trn_order_header_bk_hash,
       case when p_ig_it_trn_order_tender.bk_hash in ('-997', '-998', '-999') then p_ig_it_trn_order_tender.bk_hash
    when l_ig_it_trn_order_tender.post_acct_no is null then '-998'
	else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_ig_it_trn_order_tender.post_acct_no as varchar(500)),'z#@$k%&P'))),2) 
 end d_mms_pt_credit_card_transaction_bk_hash,
       case when p_ig_it_trn_order_tender.bk_hash in ('-997','-998','-999')
       then p_ig_it_trn_order_tender.bk_hash
       when l_ig_it_trn_order_tender.tender_id is null then '-998'
       else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_ig_it_trn_order_tender.tender_id as varchar(500)),'z#@$k%&P'))),2) end dim_cafe_payment_type_key,
       s_ig_it_trn_order_tender.pro_rata_discount_amt pro_rata_discount_amount,
       s_ig_it_trn_order_tender.pro_rata_grat_amt pro_rata_gratuity_amount,
       s_ig_it_trn_order_tender.pro_rata_sales_amt_gross pro_rata_sales_amount_gross,
       s_ig_it_trn_order_tender.pro_rata_svc_chg_amt pro_rata_service_charge_amount,
       s_ig_it_trn_order_tender.pro_rata_tax_amt pro_rata_tax_amount,
       s_ig_it_trn_order_tender.remaining_balance_amt remaining_balance_amount,
       s_ig_it_trn_order_tender.tender_amt tender_amount,
       l_ig_it_trn_order_tender.tender_id tender_id,
       l_ig_it_trn_order_tender.tender_type_id tender_type_id,
       s_ig_it_trn_order_tender.tip_amt tip_amount,
       h_ig_it_trn_order_tender.dv_deleted,
       p_ig_it_trn_order_tender.p_ig_it_trn_order_tender_id,
       p_ig_it_trn_order_tender.dv_batch_id,
       p_ig_it_trn_order_tender.dv_load_date_time,
       p_ig_it_trn_order_tender.dv_load_end_date_time
  from dbo.h_ig_it_trn_order_tender
  join dbo.p_ig_it_trn_order_tender
    on h_ig_it_trn_order_tender.bk_hash = p_ig_it_trn_order_tender.bk_hash  join #p_ig_it_trn_order_tender_insert
    on p_ig_it_trn_order_tender.bk_hash = #p_ig_it_trn_order_tender_insert.bk_hash
   and p_ig_it_trn_order_tender.p_ig_it_trn_order_tender_id = #p_ig_it_trn_order_tender_insert.p_ig_it_trn_order_tender_id
  join dbo.l_ig_it_trn_order_tender
    on p_ig_it_trn_order_tender.bk_hash = l_ig_it_trn_order_tender.bk_hash
   and p_ig_it_trn_order_tender.l_ig_it_trn_order_tender_id = l_ig_it_trn_order_tender.l_ig_it_trn_order_tender_id
  join dbo.s_ig_it_trn_order_tender
    on p_ig_it_trn_order_tender.bk_hash = s_ig_it_trn_order_tender.bk_hash
   and p_ig_it_trn_order_tender.s_ig_it_trn_order_tender_id = s_ig_it_trn_order_tender.s_ig_it_trn_order_tender_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_ig_it_trn_order_tender
   where d_ig_it_trn_order_tender.bk_hash in (select bk_hash from #p_ig_it_trn_order_tender_insert)

  insert dbo.d_ig_it_trn_order_tender(
             bk_hash,
             fact_cafe_payment_key,
             order_hdr_id,
             tender_seq,
             change_amount,
             charges_to_date_amount,
             d_ig_it_trn_order_header_bk_hash,
             d_mms_pt_credit_card_transaction_bk_hash,
             dim_cafe_payment_type_key,
             pro_rata_discount_amount,
             pro_rata_gratuity_amount,
             pro_rata_sales_amount_gross,
             pro_rata_service_charge_amount,
             pro_rata_tax_amount,
             remaining_balance_amount,
             tender_amount,
             tender_id,
             tender_type_id,
             tip_amount,
             deleted_flag,
             p_ig_it_trn_order_tender_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         fact_cafe_payment_key,
         order_hdr_id,
         tender_seq,
         change_amount,
         charges_to_date_amount,
         d_ig_it_trn_order_header_bk_hash,
         d_mms_pt_credit_card_transaction_bk_hash,
         dim_cafe_payment_type_key,
         pro_rata_discount_amount,
         pro_rata_gratuity_amount,
         pro_rata_sales_amount_gross,
         pro_rata_service_charge_amount,
         pro_rata_tax_amount,
         remaining_balance_amount,
         tender_amount,
         tender_id,
         tender_type_id,
         tip_amount,
         dv_deleted,
         p_ig_it_trn_order_tender_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_ig_it_trn_order_tender)
--Done!
end
