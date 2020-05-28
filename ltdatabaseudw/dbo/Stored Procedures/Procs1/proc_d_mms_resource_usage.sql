CREATE PROC [dbo].[proc_d_mms_resource_usage] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_resource_usage)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mms_resource_usage_insert') is not null drop table #p_mms_resource_usage_insert
create table dbo.#p_mms_resource_usage_insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_resource_usage.p_mms_resource_usage_id,
       p_mms_resource_usage.bk_hash
  from dbo.p_mms_resource_usage
 where p_mms_resource_usage.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_mms_resource_usage.dv_batch_id > @max_dv_batch_id
        or p_mms_resource_usage.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_resource_usage.bk_hash,
       p_mms_resource_usage.resource_usage_id resource_usage_id,
       case when p_mms_resource_usage.bk_hash in('-997', '-998', '-999') then p_mms_resource_usage.bk_hash
           when l_mms_resource_usage.ltf_key_owner_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_resource_usage.ltf_key_owner_id as int) as varchar(500)),'z#@$k%&P'))),2)   end d_mms_ltf_key_owner_bk_hash,
       case when p_mms_resource_usage.bk_hash in('-997', '-998', '-999') then p_mms_resource_usage.bk_hash
           when l_mms_resource_usage.ltf_resource_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_resource_usage.ltf_resource_id as int) as varchar(500)),'z#@$k%&P'))),2)   end d_mms_ltf_resource_bk_hash,
       s_mms_resource_usage.inserted_date_time inserted_date_time,
       case when p_mms_resource_usage.bk_hash in('-997', '-998', '-999') then p_mms_resource_usage.bk_hash
           when s_mms_resource_usage.inserted_date_time is null then '-998'
        else convert(varchar, s_mms_resource_usage.inserted_date_time, 112)    end inserted_dim_date_key,
       case when p_mms_resource_usage.bk_hash in ('-997','-998','-999') then p_mms_resource_usage.bk_hash
       when s_mms_resource_usage.inserted_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_mms_resource_usage.inserted_date_time,114), 1, 5),':','') end inserted_dim_time_key,
       l_mms_resource_usage.ltf_key_owner_id ltf_key_owner_id,
       l_mms_resource_usage.ltf_resource_id ltf_resource_id,
       l_mms_resource_usage.party_id party_id,
       l_mms_resource_usage.val_resource_usage_source_type_id ref_val_resource_usage_source_type_id,
       s_mms_resource_usage.updated_date_time updated_date_time,
       case when p_mms_resource_usage.bk_hash in('-997', '-998', '-999') then p_mms_resource_usage.bk_hash
           when s_mms_resource_usage.updated_date_time is null then '-998'
        else convert(varchar, s_mms_resource_usage.updated_date_time, 112)    end updated_dim_date_key,
       case when p_mms_resource_usage.bk_hash in ('-997','-998','-999') then p_mms_resource_usage.bk_hash
       when s_mms_resource_usage.updated_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_mms_resource_usage.updated_date_time,114), 1, 5),':','') end updated_dim_time_key,
       s_mms_resource_usage.usage_date_time usage_date_time,
       s_mms_resource_usage.usage_date_time_zone usage_date_time_zone,
       case when p_mms_resource_usage.bk_hash in('-997', '-998', '-999') then p_mms_resource_usage.bk_hash
           when s_mms_resource_usage.usage_date_time is null then '-998'
        else convert(varchar, s_mms_resource_usage.usage_date_time, 112)    end usage_dim_date_key,
       case when p_mms_resource_usage.bk_hash in ('-997','-998','-999') then p_mms_resource_usage.bk_hash
       when s_mms_resource_usage.usage_date_time is null then '-998'
       else '1' + substring(replace(s_mms_resource_usage.usage_date_time,':',''),1,4) end usage_dim_time_key,
       isnull(h_mms_resource_usage.dv_deleted,0) dv_deleted,
       p_mms_resource_usage.p_mms_resource_usage_id,
       p_mms_resource_usage.dv_batch_id,
       p_mms_resource_usage.dv_load_date_time,
       p_mms_resource_usage.dv_load_end_date_time
  from dbo.h_mms_resource_usage
  join dbo.p_mms_resource_usage
    on h_mms_resource_usage.bk_hash = p_mms_resource_usage.bk_hash
  join #p_mms_resource_usage_insert
    on p_mms_resource_usage.bk_hash = #p_mms_resource_usage_insert.bk_hash
   and p_mms_resource_usage.p_mms_resource_usage_id = #p_mms_resource_usage_insert.p_mms_resource_usage_id
  join dbo.l_mms_resource_usage
    on p_mms_resource_usage.bk_hash = l_mms_resource_usage.bk_hash
   and p_mms_resource_usage.l_mms_resource_usage_id = l_mms_resource_usage.l_mms_resource_usage_id
  join dbo.s_mms_resource_usage
    on p_mms_resource_usage.bk_hash = s_mms_resource_usage.bk_hash
   and p_mms_resource_usage.s_mms_resource_usage_id = s_mms_resource_usage.s_mms_resource_usage_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_mms_resource_usage
   where d_mms_resource_usage.bk_hash in (select bk_hash from #p_mms_resource_usage_insert)

  insert dbo.d_mms_resource_usage(
             bk_hash,
             resource_usage_id,
             d_mms_ltf_key_owner_bk_hash,
             d_mms_ltf_resource_bk_hash,
             inserted_date_time,
             inserted_dim_date_key,
             inserted_dim_time_key,
             ltf_key_owner_id,
             ltf_resource_id,
             party_id,
             ref_val_resource_usage_source_type_id,
             updated_date_time,
             updated_dim_date_key,
             updated_dim_time_key,
             usage_date_time,
             usage_date_time_zone,
             usage_dim_date_key,
             usage_dim_time_key,
             deleted_flag,
             p_mms_resource_usage_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         resource_usage_id,
         d_mms_ltf_key_owner_bk_hash,
         d_mms_ltf_resource_bk_hash,
         inserted_date_time,
         inserted_dim_date_key,
         inserted_dim_time_key,
         ltf_key_owner_id,
         ltf_resource_id,
         party_id,
         ref_val_resource_usage_source_type_id,
         updated_date_time,
         updated_dim_date_key,
         updated_dim_time_key,
         usage_date_time,
         usage_date_time_zone,
         usage_dim_date_key,
         usage_dim_time_key,
         dv_deleted,
         p_mms_resource_usage_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_resource_usage)
--Done!
end
