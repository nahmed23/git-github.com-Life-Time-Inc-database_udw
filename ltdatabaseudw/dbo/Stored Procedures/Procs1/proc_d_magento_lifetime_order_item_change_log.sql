CREATE PROC [dbo].[proc_d_magento_lifetime_order_item_change_log] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_magento_lifetime_order_item_change_log)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_magento_lifetime_order_item_change_log_insert') is not null drop table #p_magento_lifetime_order_item_change_log_insert
create table dbo.#p_magento_lifetime_order_item_change_log_insert with(distribution=hash(bk_hash), location=user_db) as
select p_magento_lifetime_order_item_change_log.p_magento_lifetime_order_item_change_log_id,
       p_magento_lifetime_order_item_change_log.bk_hash
  from dbo.p_magento_lifetime_order_item_change_log
 where p_magento_lifetime_order_item_change_log.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_magento_lifetime_order_item_change_log.dv_batch_id > @max_dv_batch_id
        or p_magento_lifetime_order_item_change_log.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_magento_lifetime_order_item_change_log.bk_hash,
       p_magento_lifetime_order_item_change_log.entity_id entity_id,
       case when p_magento_lifetime_order_item_change_log.bk_hash in ('-997', '-998', '-999') then p_magento_lifetime_order_item_change_log.bk_hash
            when l_magento_lifetime_order_item_change_log.mms_id is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(cast(ltrim(rtrim(l_magento_lifetime_order_item_change_log.mms_id)) as int) as int) as varchar(500)),'z#@$k%&P'))),2)  
        end dim_mms_product_key,
       case when p_magento_lifetime_order_item_change_log.bk_hash in ('-997', '-998', '-999') then p_magento_lifetime_order_item_change_log.bk_hash
            when l_magento_lifetime_order_item_change_log.item_id is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_magento_lifetime_order_item_change_log.item_id as int) as varchar(500)),'z#@$k%&P'))),2) 
        end fact_magento_order_item_key ,
       case when p_magento_lifetime_order_item_change_log.bk_hash in ('-997', '-998', '-999') then p_magento_lifetime_order_item_change_log.bk_hash
            when l_magento_lifetime_order_item_change_log.transaction_id is null then '-998'
       	 when isnumeric(l_magento_lifetime_order_item_change_log.transaction_id) = 0 then '-998' --there are some transaction id like MMS Transaction
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(cast(ltrim(rtrim(l_magento_lifetime_order_item_change_log.transaction_id)) as int) as int) as varchar(500)),'z#@$k%&P'))),2) 
        end fact_mms_transaction_key,
       l_magento_lifetime_order_item_change_log.transaction_id mms_tran_id,
       s_magento_lifetime_order_item_change_log.status [status],
       s_magento_lifetime_order_item_change_log.transaction_type transaction_type,
       isnull(h_magento_lifetime_order_item_change_log.dv_deleted,0) dv_deleted,
       p_magento_lifetime_order_item_change_log.p_magento_lifetime_order_item_change_log_id,
       p_magento_lifetime_order_item_change_log.dv_batch_id,
       p_magento_lifetime_order_item_change_log.dv_load_date_time,
       p_magento_lifetime_order_item_change_log.dv_load_end_date_time
  from dbo.h_magento_lifetime_order_item_change_log
  join dbo.p_magento_lifetime_order_item_change_log
    on h_magento_lifetime_order_item_change_log.bk_hash = p_magento_lifetime_order_item_change_log.bk_hash
  join #p_magento_lifetime_order_item_change_log_insert
    on p_magento_lifetime_order_item_change_log.bk_hash = #p_magento_lifetime_order_item_change_log_insert.bk_hash
   and p_magento_lifetime_order_item_change_log.p_magento_lifetime_order_item_change_log_id = #p_magento_lifetime_order_item_change_log_insert.p_magento_lifetime_order_item_change_log_id
  join dbo.l_magento_lifetime_order_item_change_log
    on p_magento_lifetime_order_item_change_log.bk_hash = l_magento_lifetime_order_item_change_log.bk_hash
   and p_magento_lifetime_order_item_change_log.l_magento_lifetime_order_item_change_log_id = l_magento_lifetime_order_item_change_log.l_magento_lifetime_order_item_change_log_id
  join dbo.s_magento_lifetime_order_item_change_log
    on p_magento_lifetime_order_item_change_log.bk_hash = s_magento_lifetime_order_item_change_log.bk_hash
   and p_magento_lifetime_order_item_change_log.s_magento_lifetime_order_item_change_log_id = s_magento_lifetime_order_item_change_log.s_magento_lifetime_order_item_change_log_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_magento_lifetime_order_item_change_log
   where d_magento_lifetime_order_item_change_log.bk_hash in (select bk_hash from #p_magento_lifetime_order_item_change_log_insert)

  insert dbo.d_magento_lifetime_order_item_change_log(
             bk_hash,
             entity_id,
             dim_mms_product_key,
             fact_magento_order_item_key ,
             fact_mms_transaction_key,
             mms_tran_id,
             [status],
             transaction_type,
             deleted_flag,
             p_magento_lifetime_order_item_change_log_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         entity_id,
         dim_mms_product_key,
         fact_magento_order_item_key ,
         fact_mms_transaction_key,
         mms_tran_id,
         [status],
         transaction_type,
         dv_deleted,
         p_magento_lifetime_order_item_change_log_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_magento_lifetime_order_item_change_log)
--Done!
end
