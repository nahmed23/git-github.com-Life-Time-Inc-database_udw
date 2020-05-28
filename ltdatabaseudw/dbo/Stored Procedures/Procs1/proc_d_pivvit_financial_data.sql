CREATE PROC [dbo].[proc_d_pivvit_financial_data] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_pivvit_financial_data)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_pivvit_financial_data_insert') is not null drop table #p_pivvit_financial_data_insert
create table dbo.#p_pivvit_financial_data_insert with(distribution=hash(bk_hash), location=user_db) as
select p_pivvit_financial_data.p_pivvit_financial_data_id,
       p_pivvit_financial_data.bk_hash
  from dbo.p_pivvit_financial_data
 where p_pivvit_financial_data.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (--p_pivvit_financial_data.dv_batch_id > @max_dv_batch_id
        --or 
		p_pivvit_financial_data.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_pivvit_financial_data.bk_hash,
       p_pivvit_financial_data.bk_hash fact_pivvit_financial_data_key,
       s_pivvit_financial_data.batch_id batch_id,
       s_pivvit_financial_data.transaction_id transaction_id,
       s_pivvit_financial_data.tender_type_id tender_type_id,
       l_pivvit_financial_data.club_id club_id,
       l_pivvit_financial_data.cost_center_id cost_center_id,
       s_pivvit_financial_data.discount_amount discount_amount,
       s_pivvit_financial_data.offering_id offering_id,
       cast(s_pivvit_financial_data.posted_date as date) posted_date,
       case when s_pivvit_financial_data.transaction_amount<0 then 'NEG' else 'POS' end sign_flag,
       s_pivvit_financial_data.tax_amount tax_amount,
       s_pivvit_financial_data.transaction_amount transaction_amount,
       cast(s_pivvit_financial_data.transaction_date as date) transaction_date,
       s_pivvit_financial_data.transaction_line_category_id transaction_line_category_id,
       s_pivvit_financial_data.transaction_lineamount transaction_lineamount,
       s_pivvit_financial_data.transaction_memo transaction_memo,
       isnull(h_pivvit_financial_data.dv_deleted,0) dv_deleted,
       p_pivvit_financial_data.p_pivvit_financial_data_id,
       p_pivvit_financial_data.dv_batch_id,
       p_pivvit_financial_data.dv_load_date_time,
       p_pivvit_financial_data.dv_load_end_date_time
  from dbo.h_pivvit_financial_data
  join dbo.p_pivvit_financial_data
    on h_pivvit_financial_data.bk_hash = p_pivvit_financial_data.bk_hash
  join #p_pivvit_financial_data_insert
    on p_pivvit_financial_data.bk_hash = #p_pivvit_financial_data_insert.bk_hash
   and p_pivvit_financial_data.p_pivvit_financial_data_id = #p_pivvit_financial_data_insert.p_pivvit_financial_data_id
  join dbo.l_pivvit_financial_data
    on p_pivvit_financial_data.bk_hash = l_pivvit_financial_data.bk_hash
   and p_pivvit_financial_data.l_pivvit_financial_data_id = l_pivvit_financial_data.l_pivvit_financial_data_id
  join dbo.s_pivvit_financial_data
    on p_pivvit_financial_data.bk_hash = s_pivvit_financial_data.bk_hash
   and p_pivvit_financial_data.s_pivvit_financial_data_id = s_pivvit_financial_data.s_pivvit_financial_data_id

truncate table dbo.d_pivvit_financial_data;
-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  --delete dbo.d_pivvit_financial_data
   --where d_pivvit_financial_data.bk_hash in (select bk_hash from #p_pivvit_financial_data_insert)

  insert dbo.d_pivvit_financial_data(
             bk_hash,
             fact_pivvit_financial_data_key,
             batch_id,
             transaction_id,
             tender_type_id,
             club_id,
             cost_center_id,
             discount_amount,
             offering_id,
             posted_date,
             sign_flag,
             tax_amount,
             transaction_amount,
             transaction_date,
             transaction_line_category_id,
             transaction_lineamount,
             transaction_memo,
             deleted_flag,
             p_pivvit_financial_data_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         fact_pivvit_financial_data_key,
         batch_id,
         transaction_id,
         tender_type_id,
         club_id,
         cost_center_id,
         discount_amount,
         offering_id,
         posted_date,
         sign_flag,
         tax_amount,
         transaction_amount,
         transaction_date,
         transaction_line_category_id,
         transaction_lineamount,
         transaction_memo,
         dv_deleted,
         p_pivvit_financial_data_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_pivvit_financial_data)
--Done!
end
