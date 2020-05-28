CREATE PROC [dbo].[proc_d_mms_tran_item] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_tran_item)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mms_tran_item_insert') is not null drop table #p_mms_tran_item_insert
create table dbo.#p_mms_tran_item_insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_tran_item.p_mms_tran_item_id,
       p_mms_tran_item.bk_hash
  from dbo.p_mms_tran_item
 where p_mms_tran_item.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_mms_tran_item.dv_batch_id > @max_dv_batch_id
        or p_mms_tran_item.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_tran_item.bk_hash,
       p_mms_tran_item.bk_hash fact_mms_sales_transaction_item_key,
       p_mms_tran_item.tran_item_id tran_item_id,
       case when p_mms_tran_item.bk_hash in ('-997', '-998', '-999') then p_mms_tran_item.bk_hash
        when l_mms_tran_item.bundle_product_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_tran_item.bundle_product_id as int) as varchar(500)),'z#@$k%&P'))),2) end bundle_dim_mms_product_key,
       l_mms_tran_item.bundle_product_id bundle_product_id,
       s_mms_tran_item.club_id club_id,
       case when p_mms_tran_item.bk_hash in ('-997', '-998', '-999') then p_mms_tran_item.bk_hash
            when s_mms_tran_item.club_id is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(s_mms_tran_item.club_id as int) as varchar(500)),'z#@$k%&P'))),2)
        end dim_club_key,
       case when p_mms_tran_item.bk_hash in ('-997', '-998', '-999') then p_mms_tran_item.bk_hash
            when l_mms_tran_item.product_id is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_tran_item.product_id as int) as varchar(500)),'z#@$k%&P'))),2)
        end dim_mms_product_key,
       l_mms_tran_item.external_item_id external_item_id,
       case when p_mms_tran_item.bk_hash in ('-997', '-998', '-999') then p_mms_tran_item.bk_hash
            when l_mms_tran_item.mms_tran_id is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_tran_item.mms_tran_id as int) as varchar(500)),'z#@$k%&P'))),2)
        end fact_mms_sales_transaction_key,
       s_mms_tran_item.inserted_date_time inserted_date_time,
       case when p_mms_tran_item.bk_hash in ('-997', '-998', '-999') then p_mms_tran_item.bk_hash 
              when s_mms_tran_item.inserted_date_time is null then '-998'    
              else convert(varchar, s_mms_tran_item.inserted_date_time, 112)   end inserted_dim_date_key,
       case when p_mms_tran_item.bk_hash in ('-997', '-998', '-999') then p_mms_tran_item.bk_hash 
              when s_mms_tran_item.inserted_date_time is null then '-998'    
              else '1' + replace(substring(convert(varchar,s_mms_tran_item.inserted_date_time,114), 1, 5),':','')   end inserted_dim_time_key,
       isnull(s_mms_tran_item.item_lt_bucks_amount,0) item_lt_bucks_amount,
       s_mms_tran_item_1.item_lt_bucks_sales_tax item_lt_bucks_sales_tax,
       l_mms_tran_item.mms_tran_id mms_tran_id,
       l_mms_tran_item.product_id product_id,
       isnull(s_mms_tran_item.item_amount, 0) + isnull(s_mms_tran_item.item_discount_amount, 0) + isnull(s_mms_tran_item.item_lt_bucks_amount, 0) sales_amount_gross,
       isnull(s_mms_tran_item.item_discount_amount, 0) sales_discount_dollar_amount,
       s_mms_tran_item.item_amount sales_dollar_amount,
       s_mms_tran_item.quantity sales_quantity,
       case when s_mms_tran_item.item_sales_tax is null then 0
            else s_mms_tran_item.item_sales_tax
        end sales_tax_amount,
       case when s_mms_tran_item.sold_not_serviced_flag = 1 then 'Y'
            else 'N'
        end sold_not_serviced_flag,
       s_mms_tran_item.transaction_source transaction_source,
       s_mms_tran_item.updated_date_time updated_date_time,
       case when p_mms_tran_item.bk_hash in ('-997', '-998', '-999') then p_mms_tran_item.bk_hash 
              when s_mms_tran_item.updated_date_time is null then '-998'    
              else convert(varchar, s_mms_tran_item.updated_date_time, 112)   end updated_dim_date_key,
       case when p_mms_tran_item.bk_hash in ('-997', '-998', '-999') then p_mms_tran_item.bk_hash 
              when s_mms_tran_item.updated_date_time is null then '-998'    
              else '1' + replace(substring(convert(varchar,s_mms_tran_item.updated_date_time,114), 1, 5),':','')   end updated_dim_time_key,
       isnull(h_mms_tran_item.dv_deleted,0) dv_deleted,
       p_mms_tran_item.p_mms_tran_item_id,
       p_mms_tran_item.dv_batch_id,
       p_mms_tran_item.dv_load_date_time,
       p_mms_tran_item.dv_load_end_date_time
  from dbo.h_mms_tran_item
  join dbo.p_mms_tran_item
    on h_mms_tran_item.bk_hash = p_mms_tran_item.bk_hash
  join #p_mms_tran_item_insert
    on p_mms_tran_item.bk_hash = #p_mms_tran_item_insert.bk_hash
   and p_mms_tran_item.p_mms_tran_item_id = #p_mms_tran_item_insert.p_mms_tran_item_id
  join dbo.l_mms_tran_item
    on p_mms_tran_item.bk_hash = l_mms_tran_item.bk_hash
   and p_mms_tran_item.l_mms_tran_item_id = l_mms_tran_item.l_mms_tran_item_id
  join dbo.s_mms_tran_item
    on p_mms_tran_item.bk_hash = s_mms_tran_item.bk_hash
   and p_mms_tran_item.s_mms_tran_item_id = s_mms_tran_item.s_mms_tran_item_id
  join dbo.s_mms_tran_item_1
    on p_mms_tran_item.bk_hash = s_mms_tran_item_1.bk_hash
   and p_mms_tran_item.s_mms_tran_item_1_id = s_mms_tran_item_1.s_mms_tran_item_1_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_mms_tran_item
   where d_mms_tran_item.bk_hash in (select bk_hash from #p_mms_tran_item_insert)

  insert dbo.d_mms_tran_item(
             bk_hash,
             fact_mms_sales_transaction_item_key,
             tran_item_id,
             bundle_dim_mms_product_key,
             bundle_product_id,
             club_id,
             dim_club_key,
             dim_mms_product_key,
             external_item_id,
             fact_mms_sales_transaction_key,
             inserted_date_time,
             inserted_dim_date_key,
             inserted_dim_time_key,
             item_lt_bucks_amount,
             item_lt_bucks_sales_tax,
             mms_tran_id,
             product_id,
             sales_amount_gross,
             sales_discount_dollar_amount,
             sales_dollar_amount,
             sales_quantity,
             sales_tax_amount,
             sold_not_serviced_flag,
             transaction_source,
             updated_date_time,
             updated_dim_date_key,
             updated_dim_time_key,
             deleted_flag,
             p_mms_tran_item_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         fact_mms_sales_transaction_item_key,
         tran_item_id,
         bundle_dim_mms_product_key,
         bundle_product_id,
         club_id,
         dim_club_key,
         dim_mms_product_key,
         external_item_id,
         fact_mms_sales_transaction_key,
         inserted_date_time,
         inserted_dim_date_key,
         inserted_dim_time_key,
         item_lt_bucks_amount,
         item_lt_bucks_sales_tax,
         mms_tran_id,
         product_id,
         sales_amount_gross,
         sales_discount_dollar_amount,
         sales_dollar_amount,
         sales_quantity,
         sales_tax_amount,
         sold_not_serviced_flag,
         transaction_source,
         updated_date_time,
         updated_dim_date_key,
         updated_dim_time_key,
         dv_deleted,
         p_mms_tran_item_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_tran_item)
--Done!
end
