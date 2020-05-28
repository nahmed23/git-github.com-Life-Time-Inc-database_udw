CREATE PROC [dbo].[proc_d_magento_customer_entity] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_magento_customer_entity)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_magento_customer_entity_insert') is not null drop table #p_magento_customer_entity_insert
create table dbo.#p_magento_customer_entity_insert with(distribution=hash(bk_hash), location=user_db) as
select p_magento_customer_entity.p_magento_customer_entity_id,
       p_magento_customer_entity.bk_hash
  from dbo.p_magento_customer_entity
 where p_magento_customer_entity.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_magento_customer_entity.dv_batch_id > @max_dv_batch_id
        or p_magento_customer_entity.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_magento_customer_entity.bk_hash,
       p_magento_customer_entity.bk_hash dim_magento_customer_key,
       p_magento_customer_entity.entity_id customer_id,
       s_magento_customer_entity.confirmation confirmation,
       s_magento_customer_entity.created_at created_at,
       case when p_magento_customer_entity.bk_hash in('-997', '-998', '-999') then p_magento_customer_entity.bk_hash
           when s_magento_customer_entity.created_at is null then '-998'
        else convert(varchar, s_magento_customer_entity.created_at, 112)    end created_dim_date_key,
       case when p_magento_customer_entity.bk_hash in ('-997','-998','-999') then p_magento_customer_entity.bk_hash
       when s_magento_customer_entity.created_at is null then '-998'
       else '1' + replace(substring(convert(varchar,s_magento_customer_entity.created_at,114), 1, 5),':','') end created_dim_time_key,
       s_magento_customer_entity.created_in created_in,
       s_magento_customer_entity.default_billing default_billing,
       s_magento_customer_entity.default_shipping default_shipping,
       s_magento_customer_entity.dob dob,
       case when p_magento_customer_entity.bk_hash in('-997', '-998', '-999') then p_magento_customer_entity.bk_hash
           when s_magento_customer_entity.dob is null then '-998'
        else convert(varchar, s_magento_customer_entity.dob, 112)    end dob_dim_date_key,
       s_magento_customer_entity.email email,
       s_magento_customer_entity.failures_num failures_num,
       case when p_magento_customer_entity.bk_hash in('-997', '-998', '-999') then p_magento_customer_entity.bk_hash
           when s_magento_customer_entity.first_failure is null then '-998'
        else convert(varchar, s_magento_customer_entity.first_failure, 112)    end first_failure_dim_date_key,
       case when p_magento_customer_entity.bk_hash in ('-997','-998','-999') then p_magento_customer_entity.bk_hash
       when s_magento_customer_entity.first_failure is null then '-998'
       else '1' + replace(substring(convert(varchar,s_magento_customer_entity.first_failure,114), 1, 5),':','') end first_failure_dim_time_key,
       s_magento_customer_entity.first_name first_name,
       s_magento_customer_entity.gender gender,
       l_magento_customer_entity.group_id group_id,
       l_magento_customer_entity.increment_id increment_id,
       case when s_magento_customer_entity.is_active= 1 then 'Y' else 'N' end is_active_flag,
       s_magento_customer_entity.last_name last_name,
       case when p_magento_customer_entity.bk_hash in('-997', '-998', '-999') then p_magento_customer_entity.bk_hash
           when s_magento_customer_entity.lock_expires is null then '-998'
        else convert(varchar, s_magento_customer_entity.lock_expires, 112)    end lock_expires_dim_date_key,
       case when p_magento_customer_entity.bk_hash in ('-997','-998','-999') then p_magento_customer_entity.bk_hash
       when s_magento_customer_entity.lock_expires is null then '-998'
       else '1' + replace(substring(convert(varchar,s_magento_customer_entity.lock_expires,114), 1, 5),':','') end lock_expires_dim_time_key,
       l_magento_customer_entity.m1_customer_id m1_customer_id,
       s_magento_customer_entity.middle_name middle_name,
       s_magento_customer_entity.prefix prefix,
       l_magento_customer_entity.store_id store_id,
       s_magento_customer_entity.suffix suffix,
       s_magento_customer_entity.taxvat tax_vat,
       s_magento_customer_entity.updated_at updated_at,
       case when p_magento_customer_entity.bk_hash in('-997', '-998', '-999') then p_magento_customer_entity.bk_hash
           when s_magento_customer_entity.updated_at is null then '-998'
        else convert(varchar, s_magento_customer_entity.updated_at, 112)    end updated_dim_date_key,
       case when p_magento_customer_entity.bk_hash in ('-997','-998','-999') then p_magento_customer_entity.bk_hash
       when s_magento_customer_entity.updated_at is null then '-998'
       else '1' + replace(substring(convert(varchar,s_magento_customer_entity.updated_at,114), 1, 5),':','') end updated_dim_time_key,
       l_magento_customer_entity.website_id website_id,
       isnull(h_magento_customer_entity.dv_deleted,0) dv_deleted,
       p_magento_customer_entity.p_magento_customer_entity_id,
       p_magento_customer_entity.dv_batch_id,
       p_magento_customer_entity.dv_load_date_time,
       p_magento_customer_entity.dv_load_end_date_time
  from dbo.h_magento_customer_entity
  join dbo.p_magento_customer_entity
    on h_magento_customer_entity.bk_hash = p_magento_customer_entity.bk_hash
  join #p_magento_customer_entity_insert
    on p_magento_customer_entity.bk_hash = #p_magento_customer_entity_insert.bk_hash
   and p_magento_customer_entity.p_magento_customer_entity_id = #p_magento_customer_entity_insert.p_magento_customer_entity_id
  join dbo.l_magento_customer_entity
    on p_magento_customer_entity.bk_hash = l_magento_customer_entity.bk_hash
   and p_magento_customer_entity.l_magento_customer_entity_id = l_magento_customer_entity.l_magento_customer_entity_id
  join dbo.s_magento_customer_entity
    on p_magento_customer_entity.bk_hash = s_magento_customer_entity.bk_hash
   and p_magento_customer_entity.s_magento_customer_entity_id = s_magento_customer_entity.s_magento_customer_entity_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_magento_customer_entity
   where d_magento_customer_entity.bk_hash in (select bk_hash from #p_magento_customer_entity_insert)

  insert dbo.d_magento_customer_entity(
             bk_hash,
             dim_magento_customer_key,
             customer_id,
             confirmation,
             created_at,
             created_dim_date_key,
             created_dim_time_key,
             created_in,
             default_billing,
             default_shipping,
             dob,
             dob_dim_date_key,
             email,
             failures_num,
             first_failure_dim_date_key,
             first_failure_dim_time_key,
             first_name,
             gender,
             group_id,
             increment_id,
             is_active_flag,
             last_name,
             lock_expires_dim_date_key,
             lock_expires_dim_time_key,
             m1_customer_id,
             middle_name,
             prefix,
             store_id,
             suffix,
             tax_vat,
             updated_at,
             updated_dim_date_key,
             updated_dim_time_key,
             website_id,
             deleted_flag,
             p_magento_customer_entity_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_magento_customer_key,
         customer_id,
         confirmation,
         created_at,
         created_dim_date_key,
         created_dim_time_key,
         created_in,
         default_billing,
         default_shipping,
         dob,
         dob_dim_date_key,
         email,
         failures_num,
         first_failure_dim_date_key,
         first_failure_dim_time_key,
         first_name,
         gender,
         group_id,
         increment_id,
         is_active_flag,
         last_name,
         lock_expires_dim_date_key,
         lock_expires_dim_time_key,
         m1_customer_id,
         middle_name,
         prefix,
         store_id,
         suffix,
         tax_vat,
         updated_at,
         updated_dim_date_key,
         updated_dim_time_key,
         website_id,
         dv_deleted,
         p_magento_customer_entity_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_magento_customer_entity)
--Done!
end
