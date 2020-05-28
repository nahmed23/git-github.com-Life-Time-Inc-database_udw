CREATE PROC [dbo].[proc_d_magento_sales_shipment] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_magento_sales_shipment)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_magento_sales_shipment_insert') is not null drop table #p_magento_sales_shipment_insert
create table dbo.#p_magento_sales_shipment_insert with(distribution=hash(bk_hash), location=user_db) as
select p_magento_sales_shipment.p_magento_sales_shipment_id,
       p_magento_sales_shipment.bk_hash
  from dbo.p_magento_sales_shipment
 where p_magento_sales_shipment.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_magento_sales_shipment.dv_batch_id > @max_dv_batch_id
        or p_magento_sales_shipment.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_magento_sales_shipment.bk_hash,
       p_magento_sales_shipment.entity_id sales_shipment_id,
       l_magento_sales_shipment.billing_address_id billing_address_id,
       s_magento_sales_shipment.created_at created_at,
       case when p_magento_sales_shipment.bk_hash in('-997', '-998', '-999') then p_magento_sales_shipment.bk_hash
           when s_magento_sales_shipment.created_at is null then '-998'
       	 else convert(varchar, s_magento_sales_shipment.created_at, 112) end  created_at_dim_date_key,
       case when p_magento_sales_shipment.bk_hash in ('-997','-998','-999') then p_magento_sales_shipment.bk_hash
       when s_magento_sales_shipment.created_at is null then '-998'
       else '1' + replace(substring(convert(varchar,s_magento_sales_shipment.created_at,114), 1, 5),':','') end  created_at_dim_time_key,
       s_magento_sales_shipment.customer_note customer_note,
       s_magento_sales_shipment.customer_note_notify customer_note_notify,
       case when p_magento_sales_shipment.bk_hash in('-997', '-998', '-999') then p_magento_sales_shipment.bk_hash
           when l_magento_sales_shipment.customer_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_magento_sales_shipment.customer_id as int) as varchar(500)),'z#@$k%&P'))),2) end dim_magento_customer_key,
       s_magento_sales_shipment.email_sent email_sent,
       case when p_magento_sales_shipment.bk_hash in ('-997', '-998', '-999') then p_magento_sales_shipment.bk_hash
            when l_magento_sales_shipment.order_id is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_magento_sales_shipment.order_id as int) as varchar(500)),'z#@$k%&P'))),2) 
        end fact_magento_order_key,
       l_magento_sales_shipment.m1_shipment_id m1_shipment_id,
       s_magento_sales_shipment.increment_id sales_shipment_increment_id,
       s_magento_sales_shipment.total_qty sales_shipment_total_qty,
       s_magento_sales_shipment.total_weight sales_shipment_total_weight,
       s_magento_sales_shipment.send_email send_email,
       s_magento_sales_shipment.shipment_status shipment_status,
       l_magento_sales_shipment.shipping_address_id shipping_address_id,
       l_magento_sales_shipment.store_id store_id,
       s_magento_sales_shipment.updated_at updated_at,
       case when p_magento_sales_shipment.bk_hash in('-997', '-998', '-999') then p_magento_sales_shipment.bk_hash
           when s_magento_sales_shipment.updated_at is null then '-998'
       	 else convert(varchar, s_magento_sales_shipment.updated_at, 112) end  updated_at_dim_date_key,
       case when p_magento_sales_shipment.bk_hash in ('-997','-998','-999') then p_magento_sales_shipment.bk_hash
       when s_magento_sales_shipment.updated_at is null then '-998'
       else '1' + replace(substring(convert(varchar,s_magento_sales_shipment.updated_at,114), 1, 5),':','') end  updated_at_dim_time_key,
       isnull(h_magento_sales_shipment.dv_deleted,0) dv_deleted,
       p_magento_sales_shipment.p_magento_sales_shipment_id,
       p_magento_sales_shipment.dv_batch_id,
       p_magento_sales_shipment.dv_load_date_time,
       p_magento_sales_shipment.dv_load_end_date_time
  from dbo.h_magento_sales_shipment
  join dbo.p_magento_sales_shipment
    on h_magento_sales_shipment.bk_hash = p_magento_sales_shipment.bk_hash
  join #p_magento_sales_shipment_insert
    on p_magento_sales_shipment.bk_hash = #p_magento_sales_shipment_insert.bk_hash
   and p_magento_sales_shipment.p_magento_sales_shipment_id = #p_magento_sales_shipment_insert.p_magento_sales_shipment_id
  join dbo.l_magento_sales_shipment
    on p_magento_sales_shipment.bk_hash = l_magento_sales_shipment.bk_hash
   and p_magento_sales_shipment.l_magento_sales_shipment_id = l_magento_sales_shipment.l_magento_sales_shipment_id
  join dbo.s_magento_sales_shipment
    on p_magento_sales_shipment.bk_hash = s_magento_sales_shipment.bk_hash
   and p_magento_sales_shipment.s_magento_sales_shipment_id = s_magento_sales_shipment.s_magento_sales_shipment_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_magento_sales_shipment
   where d_magento_sales_shipment.bk_hash in (select bk_hash from #p_magento_sales_shipment_insert)

  insert dbo.d_magento_sales_shipment(
             bk_hash,
             sales_shipment_id,
             billing_address_id,
             created_at,
             created_at_dim_date_key,
             created_at_dim_time_key,
             customer_note,
             customer_note_notify,
             dim_magento_customer_key,
             email_sent,
             fact_magento_order_key,
             m1_shipment_id,
             sales_shipment_increment_id,
             sales_shipment_total_qty,
             sales_shipment_total_weight,
             send_email,
             shipment_status,
             shipping_address_id,
             store_id,
             updated_at,
             updated_at_dim_date_key,
             updated_at_dim_time_key,
             deleted_flag,
             p_magento_sales_shipment_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         sales_shipment_id,
         billing_address_id,
         created_at,
         created_at_dim_date_key,
         created_at_dim_time_key,
         customer_note,
         customer_note_notify,
         dim_magento_customer_key,
         email_sent,
         fact_magento_order_key,
         m1_shipment_id,
         sales_shipment_increment_id,
         sales_shipment_total_qty,
         sales_shipment_total_weight,
         send_email,
         shipment_status,
         shipping_address_id,
         store_id,
         updated_at,
         updated_at_dim_date_key,
         updated_at_dim_time_key,
         dv_deleted,
         p_magento_sales_shipment_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_magento_sales_shipment)
--Done!
end
