CREATE PROC [dbo].[proc_d_magento_sales_order_address] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_magento_sales_order_address)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_magento_sales_order_address_insert') is not null drop table #p_magento_sales_order_address_insert
create table dbo.#p_magento_sales_order_address_insert with(distribution=hash(bk_hash), location=user_db) as
select p_magento_sales_order_address.p_magento_sales_order_address_id,
       p_magento_sales_order_address.bk_hash
  from dbo.p_magento_sales_order_address
 where p_magento_sales_order_address.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_magento_sales_order_address.dv_batch_id > @max_dv_batch_id
        or p_magento_sales_order_address.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_magento_sales_order_address.bk_hash,
       p_magento_sales_order_address.entity_id entity_id,
       s_magento_sales_order_address.address_type address_type,
       s_magento_sales_order_address.city city,
       s_magento_sales_order_address.company company,
       s_magento_sales_order_address.country_id country_id,
       l_magento_sales_order_address.customer_address_id customer_address_id,
       l_magento_sales_order_address.customer_id customer_id,
       case when p_magento_sales_order_address.bk_hash in('-997', '-998', '-999') then p_magento_sales_order_address.bk_hash
           when l_magento_sales_order_address.parent_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_magento_sales_order_address.parent_id as int) as varchar(500)),'z#@$k%&P'))),2)   end d_magento_sales_order_parent_bk_hash,
       s_magento_sales_order_address.email email,
       s_magento_sales_order_address.fax fax,
       s_magento_sales_order_address.first_name first_name,
       s_magento_sales_order_address.last_name last_name,
       s_magento_sales_order_address.middle_name middle_name,
       s_magento_sales_order_address.post_code post_code,
       s_magento_sales_order_address.prefix prefix,
       l_magento_sales_order_address.quote_address_id quote_address_id,
       s_magento_sales_order_address.region region,
       l_magento_sales_order_address.region_id region_id,
       s_magento_sales_order_address.street street,
       s_magento_sales_order_address.suffix suffix,
       s_magento_sales_order_address.telephone telephone,
       isnull(h_magento_sales_order_address.dv_deleted,0) dv_deleted,
       p_magento_sales_order_address.p_magento_sales_order_address_id,
       p_magento_sales_order_address.dv_batch_id,
       p_magento_sales_order_address.dv_load_date_time,
       p_magento_sales_order_address.dv_load_end_date_time
  from dbo.h_magento_sales_order_address
  join dbo.p_magento_sales_order_address
    on h_magento_sales_order_address.bk_hash = p_magento_sales_order_address.bk_hash
  join #p_magento_sales_order_address_insert
    on p_magento_sales_order_address.bk_hash = #p_magento_sales_order_address_insert.bk_hash
   and p_magento_sales_order_address.p_magento_sales_order_address_id = #p_magento_sales_order_address_insert.p_magento_sales_order_address_id
  join dbo.l_magento_sales_order_address
    on p_magento_sales_order_address.bk_hash = l_magento_sales_order_address.bk_hash
   and p_magento_sales_order_address.l_magento_sales_order_address_id = l_magento_sales_order_address.l_magento_sales_order_address_id
  join dbo.s_magento_sales_order_address
    on p_magento_sales_order_address.bk_hash = s_magento_sales_order_address.bk_hash
   and p_magento_sales_order_address.s_magento_sales_order_address_id = s_magento_sales_order_address.s_magento_sales_order_address_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_magento_sales_order_address
   where d_magento_sales_order_address.bk_hash in (select bk_hash from #p_magento_sales_order_address_insert)

  insert dbo.d_magento_sales_order_address(
             bk_hash,
             entity_id,
             address_type,
             city,
             company,
             country_id,
             customer_address_id,
             customer_id,
             d_magento_sales_order_parent_bk_hash,
             email,
             fax,
             first_name,
             last_name,
             middle_name,
             post_code,
             prefix,
             quote_address_id,
             region,
             region_id,
             street,
             suffix,
             telephone,
             deleted_flag,
             p_magento_sales_order_address_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         entity_id,
         address_type,
         city,
         company,
         country_id,
         customer_address_id,
         customer_id,
         d_magento_sales_order_parent_bk_hash,
         email,
         fax,
         first_name,
         last_name,
         middle_name,
         post_code,
         prefix,
         quote_address_id,
         region,
         region_id,
         street,
         suffix,
         telephone,
         dv_deleted,
         p_magento_sales_order_address_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_magento_sales_order_address)
--Done!
end
