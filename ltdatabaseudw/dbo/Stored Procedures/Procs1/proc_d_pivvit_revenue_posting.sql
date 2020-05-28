CREATE PROC [dbo].[proc_d_pivvit_revenue_posting] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_pivvit_revenue_posting)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_pivvit_revenue_posting_insert') is not null drop table #p_pivvit_revenue_posting_insert
create table dbo.#p_pivvit_revenue_posting_insert with(distribution=hash(bk_hash), location=user_db) as
select p_pivvit_revenue_posting.p_pivvit_revenue_posting_id,
       p_pivvit_revenue_posting.bk_hash
  from dbo.p_pivvit_revenue_posting
 where p_pivvit_revenue_posting.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_pivvit_revenue_posting.dv_batch_id > @max_dv_batch_id
        or p_pivvit_revenue_posting.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_pivvit_revenue_posting.bk_hash,
       p_pivvit_revenue_posting.bk_hash fact_pivvit_revenue_posting_key,
       s_pivvit_revenue_posting.transaction_id transaction_id,
       s_pivvit_revenue_posting.tender_type_id tender_type_id,
       s_pivvit_revenue_posting.batch_id batch_id,
       l_pivvit_revenue_posting.club_id club_id,
       l_pivvit_revenue_posting.company_id company_id,
       l_pivvit_revenue_posting.cost_center_id cost_center_id,
       l_pivvit_revenue_posting.currency_id currency_id,
       s_pivvit_revenue_posting.discount_amount discount_amount,
       s_pivvit_revenue_posting.mms_member_id mms_member_id,
       l_pivvit_revenue_posting.mms_product_code mms_product_code,
       cast(s_pivvit_revenue_posting.posted_date as date) posted_date,
       case when s_pivvit_revenue_posting.transaction_amount<0 then 'NEG' else 'POS' end sign_flag,
       s_pivvit_revenue_posting.tax_amount tax_amount,
       s_pivvit_revenue_posting.transaction_amount transaction_amount,
       cast(s_pivvit_revenue_posting.transaction_date as date) transaction_date,
       s_pivvit_revenue_posting.transaction_line_category_id transaction_line_category_id,
       s_pivvit_revenue_posting.transaction_lineamount transaction_lineamount,
       s_pivvit_revenue_posting.transaction_memo transaction_memo,
       isnull(h_pivvit_revenue_posting.dv_deleted,0) dv_deleted,
       p_pivvit_revenue_posting.p_pivvit_revenue_posting_id,
       p_pivvit_revenue_posting.dv_batch_id,
       p_pivvit_revenue_posting.dv_load_date_time,
       p_pivvit_revenue_posting.dv_load_end_date_time
  from dbo.h_pivvit_revenue_posting
  join dbo.p_pivvit_revenue_posting
    on h_pivvit_revenue_posting.bk_hash = p_pivvit_revenue_posting.bk_hash
  join #p_pivvit_revenue_posting_insert
    on p_pivvit_revenue_posting.bk_hash = #p_pivvit_revenue_posting_insert.bk_hash
   and p_pivvit_revenue_posting.p_pivvit_revenue_posting_id = #p_pivvit_revenue_posting_insert.p_pivvit_revenue_posting_id
  join dbo.l_pivvit_revenue_posting
    on p_pivvit_revenue_posting.bk_hash = l_pivvit_revenue_posting.bk_hash
   and p_pivvit_revenue_posting.l_pivvit_revenue_posting_id = l_pivvit_revenue_posting.l_pivvit_revenue_posting_id
  join dbo.s_pivvit_revenue_posting
    on p_pivvit_revenue_posting.bk_hash = s_pivvit_revenue_posting.bk_hash
   and p_pivvit_revenue_posting.s_pivvit_revenue_posting_id = s_pivvit_revenue_posting.s_pivvit_revenue_posting_id

   
   truncate table dbo.d_pivvit_revenue_posting;
-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  --delete dbo.d_pivvit_revenue_posting
   --where d_pivvit_revenue_posting.bk_hash in (select bk_hash from #p_pivvit_revenue_posting_insert)

  insert dbo.d_pivvit_revenue_posting(
             bk_hash,
             fact_pivvit_revenue_posting_key,
             transaction_id,
             tender_type_id,
             batch_id,
             club_id,
             company_id,
             cost_center_id,
             currency_id,
             discount_amount,
             mms_member_id,
             mms_product_code,
             posted_date,
             sign_flag,
             tax_amount,
             transaction_amount,
             transaction_date,
             transaction_line_category_id,
             transaction_lineamount,
             transaction_memo,
             deleted_flag,
             p_pivvit_revenue_posting_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         fact_pivvit_revenue_posting_key,
         transaction_id,
         tender_type_id,
         batch_id,
         club_id,
         company_id,
         cost_center_id,
         currency_id,
         discount_amount,
         mms_member_id,
         mms_product_code,
         posted_date,
         sign_flag,
         tax_amount,
         transaction_amount,
         transaction_date,
         transaction_line_category_id,
         transaction_lineamount,
         transaction_memo,
         dv_deleted,
         p_pivvit_revenue_posting_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_pivvit_revenue_posting)
--Done!
end
