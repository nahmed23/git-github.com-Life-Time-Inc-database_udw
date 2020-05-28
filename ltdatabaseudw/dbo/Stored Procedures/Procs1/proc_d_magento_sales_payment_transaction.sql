CREATE PROC [dbo].[proc_d_magento_sales_payment_transaction] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_magento_sales_payment_transaction)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_magento_sales_payment_transaction_insert') is not null drop table #p_magento_sales_payment_transaction_insert
create table dbo.#p_magento_sales_payment_transaction_insert with(distribution=hash(bk_hash), location=user_db) as
select p_magento_sales_payment_transaction.p_magento_sales_payment_transaction_id,
       p_magento_sales_payment_transaction.bk_hash
  from dbo.p_magento_sales_payment_transaction
 where p_magento_sales_payment_transaction.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_magento_sales_payment_transaction.dv_batch_id > @max_dv_batch_id
        or p_magento_sales_payment_transaction.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_magento_sales_payment_transaction.bk_hash,
       p_magento_sales_payment_transaction.transaction_id transaction_id,
       s_magento_sales_payment_transaction.created_at created_at,
       case when p_magento_sales_payment_transaction.bk_hash in('-997', '-998', '-999') then p_magento_sales_payment_transaction.bk_hash
           when s_magento_sales_payment_transaction.created_at is null then '-998'
        else convert(varchar, s_magento_sales_payment_transaction.created_at, 112)    end created_dim_date_key,
       case when p_magento_sales_payment_transaction.bk_hash in ('-997','-998','-999') then p_magento_sales_payment_transaction.bk_hash
       when s_magento_sales_payment_transaction.created_at is null then '-998'
       else '1' + replace(substring(convert(varchar,s_magento_sales_payment_transaction.created_at,114), 1, 5),':','') end created_dim_time_key,
       case when p_magento_sales_payment_transaction.bk_hash in('-997', '-998', '-999') then p_magento_sales_payment_transaction.bk_hash
           when l_magento_sales_payment_transaction.txn_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_magento_sales_payment_transaction.txn_id as varchar(100)),'z#@$k%&P'))),2)   end fact_magento_payment_key,
       case when p_magento_sales_payment_transaction.bk_hash in('-997', '-998', '-999') then p_magento_sales_payment_transaction.bk_hash
           when l_magento_sales_payment_transaction.order_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_magento_sales_payment_transaction.order_id as int) as varchar(500)),'z#@$k%&P'))),2)   end fact_magento_sales_order_key,
       case when p_magento_sales_payment_transaction.bk_hash in('-997', '-998', '-999') then p_magento_sales_payment_transaction.bk_hash
           when l_magento_sales_payment_transaction.payment_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_magento_sales_payment_transaction.payment_id as int) as varchar(500)),'z#@$k%&P'))),2)   end fact_magento_sales_order_payment_key,
       case when s_magento_sales_payment_transaction.is_closed= 1 then 'Y' else 'N' end is_closed_flag,
       l_magento_sales_payment_transaction.parent_id parent_id,
       l_magento_sales_payment_transaction.parent_txn_id parent_txn_id,
       l_magento_sales_payment_transaction.txn_id txn_id,
       s_magento_sales_payment_transaction.txn_type txn_type,
       isnull(h_magento_sales_payment_transaction.dv_deleted,0) dv_deleted,
       p_magento_sales_payment_transaction.p_magento_sales_payment_transaction_id,
       p_magento_sales_payment_transaction.dv_batch_id,
       p_magento_sales_payment_transaction.dv_load_date_time,
       p_magento_sales_payment_transaction.dv_load_end_date_time
  from dbo.h_magento_sales_payment_transaction
  join dbo.p_magento_sales_payment_transaction
    on h_magento_sales_payment_transaction.bk_hash = p_magento_sales_payment_transaction.bk_hash
  join #p_magento_sales_payment_transaction_insert
    on p_magento_sales_payment_transaction.bk_hash = #p_magento_sales_payment_transaction_insert.bk_hash
   and p_magento_sales_payment_transaction.p_magento_sales_payment_transaction_id = #p_magento_sales_payment_transaction_insert.p_magento_sales_payment_transaction_id
  join dbo.l_magento_sales_payment_transaction
    on p_magento_sales_payment_transaction.bk_hash = l_magento_sales_payment_transaction.bk_hash
   and p_magento_sales_payment_transaction.l_magento_sales_payment_transaction_id = l_magento_sales_payment_transaction.l_magento_sales_payment_transaction_id
  join dbo.s_magento_sales_payment_transaction
    on p_magento_sales_payment_transaction.bk_hash = s_magento_sales_payment_transaction.bk_hash
   and p_magento_sales_payment_transaction.s_magento_sales_payment_transaction_id = s_magento_sales_payment_transaction.s_magento_sales_payment_transaction_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_magento_sales_payment_transaction
   where d_magento_sales_payment_transaction.bk_hash in (select bk_hash from #p_magento_sales_payment_transaction_insert)

  insert dbo.d_magento_sales_payment_transaction(
             bk_hash,
             transaction_id,
             created_at,
             created_dim_date_key,
             created_dim_time_key,
             fact_magento_payment_key,
             fact_magento_sales_order_key,
             fact_magento_sales_order_payment_key,
             is_closed_flag,
             parent_id,
             parent_txn_id,
             txn_id,
             txn_type,
             deleted_flag,
             p_magento_sales_payment_transaction_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         transaction_id,
         created_at,
         created_dim_date_key,
         created_dim_time_key,
         fact_magento_payment_key,
         fact_magento_sales_order_key,
         fact_magento_sales_order_payment_key,
         is_closed_flag,
         parent_id,
         parent_txn_id,
         txn_id,
         txn_type,
         dv_deleted,
         p_magento_sales_payment_transaction_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_magento_sales_payment_transaction)
--Done!
end
