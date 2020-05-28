CREATE PROC [dbo].[proc_d_magento_customer_address_entity] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_magento_customer_address_entity)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_magento_customer_address_entity_insert') is not null drop table #p_magento_customer_address_entity_insert
create table dbo.#p_magento_customer_address_entity_insert with(distribution=hash(bk_hash), location=user_db) as
select p_magento_customer_address_entity.p_magento_customer_address_entity_id,
       p_magento_customer_address_entity.bk_hash
  from dbo.p_magento_customer_address_entity
 where p_magento_customer_address_entity.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_magento_customer_address_entity.dv_batch_id > @max_dv_batch_id
        or p_magento_customer_address_entity.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_magento_customer_address_entity.bk_hash,
       p_magento_customer_address_entity.entity_id customer_address_id,
       s_magento_customer_address_entity.city city,
       s_magento_customer_address_entity.company company,
       s_magento_customer_address_entity.country_id country_id,
       case when p_magento_customer_address_entity.bk_hash in('-997', '-998', '-999') then p_magento_customer_address_entity.bk_hash
           when s_magento_customer_address_entity.created_at is null then '-998'
        else convert(varchar, s_magento_customer_address_entity.created_at, 112)    end created_dim_date_key,
       case when p_magento_customer_address_entity.bk_hash in ('-997','-998','-999') then p_magento_customer_address_entity.bk_hash
       when s_magento_customer_address_entity.created_at is null then '-998'
       else '1' + replace(substring(convert(varchar,s_magento_customer_address_entity.created_at,114), 1, 5),':','') end created_dim_time_key,
       s_magento_customer_address_entity.fax fax,
       s_magento_customer_address_entity.first_name first_name,
       s_magento_customer_address_entity.increment_id increment_id,
       case when s_magento_customer_address_entity.is_active= 1 then 'Y' else 'N' end is_active_flag,
       s_magento_customer_address_entity.last_name last_name,
       l_magento_customer_address_entity.m1_customer_address_id m1_customer_address_id,
       s_magento_customer_address_entity.middle_name middle_name,
       case when p_magento_customer_address_entity.bk_hash in('-997', '-998', '-999') then p_magento_customer_address_entity.bk_hash
           when l_magento_customer_address_entity.parent_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_magento_customer_address_entity.parent_id as int) as varchar(500)),'z#@$k%&P'))),2)   end parent_d_magento_customer_entity_bk_hash,
       s_magento_customer_address_entity.post_code post_code,
       s_magento_customer_address_entity.prefix prefix,
       s_magento_customer_address_entity.region region,
       l_magento_customer_address_entity.region_id region_id,
       s_magento_customer_address_entity.street street,
       s_magento_customer_address_entity.suffix suffix,
       s_magento_customer_address_entity.telephone telephone,
       case when p_magento_customer_address_entity.bk_hash in('-997', '-998', '-999') then p_magento_customer_address_entity.bk_hash
           when s_magento_customer_address_entity.updated_at is null then '-998'
        else convert(varchar, s_magento_customer_address_entity.updated_at, 112)    end updated_dim_date_key,
       case when p_magento_customer_address_entity.bk_hash in ('-997','-998','-999') then p_magento_customer_address_entity.bk_hash
       when s_magento_customer_address_entity.updated_at is null then '-998'
       else '1' + replace(substring(convert(varchar,s_magento_customer_address_entity.updated_at,114), 1, 5),':','') end updated_dim_time_key,
       isnull(h_magento_customer_address_entity.dv_deleted,0) dv_deleted,
       p_magento_customer_address_entity.p_magento_customer_address_entity_id,
       p_magento_customer_address_entity.dv_batch_id,
       p_magento_customer_address_entity.dv_load_date_time,
       p_magento_customer_address_entity.dv_load_end_date_time
  from dbo.h_magento_customer_address_entity
  join dbo.p_magento_customer_address_entity
    on h_magento_customer_address_entity.bk_hash = p_magento_customer_address_entity.bk_hash
  join #p_magento_customer_address_entity_insert
    on p_magento_customer_address_entity.bk_hash = #p_magento_customer_address_entity_insert.bk_hash
   and p_magento_customer_address_entity.p_magento_customer_address_entity_id = #p_magento_customer_address_entity_insert.p_magento_customer_address_entity_id
  join dbo.l_magento_customer_address_entity
    on p_magento_customer_address_entity.bk_hash = l_magento_customer_address_entity.bk_hash
   and p_magento_customer_address_entity.l_magento_customer_address_entity_id = l_magento_customer_address_entity.l_magento_customer_address_entity_id
  join dbo.s_magento_customer_address_entity
    on p_magento_customer_address_entity.bk_hash = s_magento_customer_address_entity.bk_hash
   and p_magento_customer_address_entity.s_magento_customer_address_entity_id = s_magento_customer_address_entity.s_magento_customer_address_entity_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_magento_customer_address_entity
   where d_magento_customer_address_entity.bk_hash in (select bk_hash from #p_magento_customer_address_entity_insert)

  insert dbo.d_magento_customer_address_entity(
             bk_hash,
             customer_address_id,
             city,
             company,
             country_id,
             created_dim_date_key,
             created_dim_time_key,
             fax,
             first_name,
             increment_id,
             is_active_flag,
             last_name,
             m1_customer_address_id,
             middle_name,
             parent_d_magento_customer_entity_bk_hash,
             post_code,
             prefix,
             region,
             region_id,
             street,
             suffix,
             telephone,
             updated_dim_date_key,
             updated_dim_time_key,
             deleted_flag,
             p_magento_customer_address_entity_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         customer_address_id,
         city,
         company,
         country_id,
         created_dim_date_key,
         created_dim_time_key,
         fax,
         first_name,
         increment_id,
         is_active_flag,
         last_name,
         m1_customer_address_id,
         middle_name,
         parent_d_magento_customer_entity_bk_hash,
         post_code,
         prefix,
         region,
         region_id,
         street,
         suffix,
         telephone,
         updated_dim_date_key,
         updated_dim_time_key,
         dv_deleted,
         p_magento_customer_address_entity_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_magento_customer_address_entity)
--Done!
end
