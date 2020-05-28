CREATE PROC [dbo].[proc_d_lt_bucks_transactions] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_lt_bucks_transactions)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_lt_bucks_transactions_insert') is not null drop table #p_lt_bucks_transactions_insert
create table dbo.#p_lt_bucks_transactions_insert with(distribution=hash(bk_hash), location=user_db) as
select p_lt_bucks_transactions.p_lt_bucks_transactions_id,
       p_lt_bucks_transactions.bk_hash
  from dbo.p_lt_bucks_transactions
 where p_lt_bucks_transactions.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_lt_bucks_transactions.dv_batch_id > @max_dv_batch_id
        or p_lt_bucks_transactions.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_lt_bucks_transactions.bk_hash,
       p_lt_bucks_transactions.bk_hash fact_lt_bucks_transactions_key,
       s_lt_bucks_transactions.transaction_id transaction_id,
       case when s_lt_bucks_transactions.transaction_amount is null then ''
                   when s_lt_bucks_transactions.transaction_type <= 1 then ''
                   when s_lt_bucks_transactions.transaction_type = 2279 then ''
                   else s_lt_bucks_transactions.transaction_ext_ref
               end award_reason,
       isnull(s_lt_bucks_transactions.transaction_amount, 0) bucks_amount,
       isnull(s_lt_bucks_transactions.transaction_date_1, convert(datetime, '9999.12.31',102)) bucks_expiration_date_time,
       s_lt_bucks_transactions.transaction_int_1 cancelled_order_original_fact_mylt_bucks_transaction_id,
       s_lt_bucks_transactions.transaction_int_2 cancelled_order_original_fact_mylt_bucks_transaction_item_id,
       case when p_lt_bucks_transactions.bk_hash in ('-997','-998','-999') then p_lt_bucks_transactions.bk_hash
            when l_lt_bucks_transactions.transaction_user is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_lt_bucks_transactions.transaction_user as varchar(500)),'z#@$k%&P'))),2)
        end dim_lt_bucks_user_key,
       case when s_lt_bucks_transactions.transaction_type = 1 then s_lt_bucks_transactions.transaction_timestamp
                   else null
               end pended_date_time,
       s_lt_bucks_transactions.transaction_timestamp transaction_date_time,
       s_lt_bucks_transactions.transaction_type transaction_type_id,
       p_lt_bucks_transactions.p_lt_bucks_transactions_id,
       p_lt_bucks_transactions.dv_batch_id,
       p_lt_bucks_transactions.dv_load_date_time,
       p_lt_bucks_transactions.dv_load_end_date_time
  from dbo.p_lt_bucks_transactions
  join #p_lt_bucks_transactions_insert
    on p_lt_bucks_transactions.bk_hash = #p_lt_bucks_transactions_insert.bk_hash
   and p_lt_bucks_transactions.p_lt_bucks_transactions_id = #p_lt_bucks_transactions_insert.p_lt_bucks_transactions_id
  join dbo.l_lt_bucks_transactions
    on p_lt_bucks_transactions.bk_hash = l_lt_bucks_transactions.bk_hash
   and p_lt_bucks_transactions.l_lt_bucks_transactions_id = l_lt_bucks_transactions.l_lt_bucks_transactions_id
  join dbo.s_lt_bucks_transactions
    on p_lt_bucks_transactions.bk_hash = s_lt_bucks_transactions.bk_hash
   and p_lt_bucks_transactions.s_lt_bucks_transactions_id = s_lt_bucks_transactions.s_lt_bucks_transactions_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_lt_bucks_transactions
   where d_lt_bucks_transactions.bk_hash in (select bk_hash from #p_lt_bucks_transactions_insert)

  insert dbo.d_lt_bucks_transactions(
             bk_hash,
             fact_lt_bucks_transactions_key,
             transaction_id,
             award_reason,
             bucks_amount,
             bucks_expiration_date_time,
             cancelled_order_original_fact_mylt_bucks_transaction_id,
             cancelled_order_original_fact_mylt_bucks_transaction_item_id,
             dim_lt_bucks_user_key,
             pended_date_time,
             transaction_date_time,
             transaction_type_id,
             p_lt_bucks_transactions_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         fact_lt_bucks_transactions_key,
         transaction_id,
         award_reason,
         bucks_amount,
         bucks_expiration_date_time,
         cancelled_order_original_fact_mylt_bucks_transaction_id,
         cancelled_order_original_fact_mylt_bucks_transaction_item_id,
         dim_lt_bucks_user_key,
         pended_date_time,
         transaction_date_time,
         transaction_type_id,
         p_lt_bucks_transactions_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_lt_bucks_transactions)
--Done!
end
