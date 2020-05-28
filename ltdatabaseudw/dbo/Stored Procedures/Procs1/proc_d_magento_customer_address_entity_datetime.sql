CREATE PROC [dbo].[proc_d_magento_customer_address_entity_datetime] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_magento_customer_address_entity_datetime)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_magento_customer_address_entity_datetime_insert') is not null drop table #p_magento_customer_address_entity_datetime_insert
create table dbo.#p_magento_customer_address_entity_datetime_insert with(distribution=hash(bk_hash), location=user_db) as
select p_magento_customer_address_entity_datetime.p_magento_customer_address_entity_datetime_id,
       p_magento_customer_address_entity_datetime.bk_hash
  from dbo.p_magento_customer_address_entity_datetime
 where p_magento_customer_address_entity_datetime.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_magento_customer_address_entity_datetime.dv_batch_id > @max_dv_batch_id
        or p_magento_customer_address_entity_datetime.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_magento_customer_address_entity_datetime.bk_hash,
       p_magento_customer_address_entity_datetime.value_id value_id,
       case when p_magento_customer_address_entity_datetime.bk_hash in('-997', '-998', '-999') then p_magento_customer_address_entity_datetime.bk_hash
           when l_magento_customer_address_entity_datetime.entity_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_magento_customer_address_entity_datetime.entity_id as int) as varchar(500)),'z#@$k%&P'))),2)   end d_magento_customer_address_entity_bk_hash,
       case when p_magento_customer_address_entity_datetime.bk_hash in('-997', '-998', '-999') then p_magento_customer_address_entity_datetime.bk_hash
           when l_magento_customer_address_entity_datetime.attribute_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_magento_customer_address_entity_datetime.attribute_id as int) as varchar(500)),'z#@$k%&P'))),2)   end d_magento_eav_attribute_bk_hash,
       case when p_magento_customer_address_entity_datetime.bk_hash in('-997', '-998', '-999') then p_magento_customer_address_entity_datetime.bk_hash
           when s_magento_customer_address_entity_datetime.value is null then '-998'
        else convert(varchar, s_magento_customer_address_entity_datetime.value, 112)    end value_dim_date_key,
       case when p_magento_customer_address_entity_datetime.bk_hash in ('-997','-998','-999') then p_magento_customer_address_entity_datetime.bk_hash
       when s_magento_customer_address_entity_datetime.value is null then '-998'
       else '1' + replace(substring(convert(varchar,s_magento_customer_address_entity_datetime.value,114), 1, 5),':','') end value_dim_time_key,
       isnull(h_magento_customer_address_entity_datetime.dv_deleted,0) dv_deleted,
       p_magento_customer_address_entity_datetime.p_magento_customer_address_entity_datetime_id,
       p_magento_customer_address_entity_datetime.dv_batch_id,
       p_magento_customer_address_entity_datetime.dv_load_date_time,
       p_magento_customer_address_entity_datetime.dv_load_end_date_time
  from dbo.h_magento_customer_address_entity_datetime
  join dbo.p_magento_customer_address_entity_datetime
    on h_magento_customer_address_entity_datetime.bk_hash = p_magento_customer_address_entity_datetime.bk_hash
  join #p_magento_customer_address_entity_datetime_insert
    on p_magento_customer_address_entity_datetime.bk_hash = #p_magento_customer_address_entity_datetime_insert.bk_hash
   and p_magento_customer_address_entity_datetime.p_magento_customer_address_entity_datetime_id = #p_magento_customer_address_entity_datetime_insert.p_magento_customer_address_entity_datetime_id
  join dbo.l_magento_customer_address_entity_datetime
    on p_magento_customer_address_entity_datetime.bk_hash = l_magento_customer_address_entity_datetime.bk_hash
   and p_magento_customer_address_entity_datetime.l_magento_customer_address_entity_datetime_id = l_magento_customer_address_entity_datetime.l_magento_customer_address_entity_datetime_id
  join dbo.s_magento_customer_address_entity_datetime
    on p_magento_customer_address_entity_datetime.bk_hash = s_magento_customer_address_entity_datetime.bk_hash
   and p_magento_customer_address_entity_datetime.s_magento_customer_address_entity_datetime_id = s_magento_customer_address_entity_datetime.s_magento_customer_address_entity_datetime_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_magento_customer_address_entity_datetime
   where d_magento_customer_address_entity_datetime.bk_hash in (select bk_hash from #p_magento_customer_address_entity_datetime_insert)

  insert dbo.d_magento_customer_address_entity_datetime(
             bk_hash,
             value_id,
             d_magento_customer_address_entity_bk_hash,
             d_magento_eav_attribute_bk_hash,
             value_dim_date_key,
             value_dim_time_key,
             deleted_flag,
             p_magento_customer_address_entity_datetime_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         value_id,
         d_magento_customer_address_entity_bk_hash,
         d_magento_eav_attribute_bk_hash,
         value_dim_date_key,
         value_dim_time_key,
         dv_deleted,
         p_magento_customer_address_entity_datetime_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_magento_customer_address_entity_datetime)
--Done!
end
