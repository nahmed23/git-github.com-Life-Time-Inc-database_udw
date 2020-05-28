CREATE PROC [dbo].[proc_d_mms_ltf_key_owner] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_ltf_key_owner)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mms_ltf_key_owner_insert') is not null drop table #p_mms_ltf_key_owner_insert
create table dbo.#p_mms_ltf_key_owner_insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_ltf_key_owner.p_mms_ltf_key_owner_id,
       p_mms_ltf_key_owner.bk_hash
  from dbo.p_mms_ltf_key_owner
 where p_mms_ltf_key_owner.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_mms_ltf_key_owner.dv_batch_id > @max_dv_batch_id
        or p_mms_ltf_key_owner.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_ltf_key_owner.bk_hash,
       p_mms_ltf_key_owner.ltf_key_owner_id ltf_key_owner_id,
       l_mms_ltf_key_owner.acquisition_id acquisition_id,
       case when p_mms_ltf_key_owner.bk_hash in('-997', '-998', '-999') then p_mms_ltf_key_owner.bk_hash
           when l_mms_ltf_key_owner.ltf_key_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_ltf_key_owner.ltf_key_id as int) as varchar(500)),'z#@$k%&P'))),2)   end d_ltf_key_bk_hash,
       s_mms_ltf_key_owner.display_name display_name,
       s_mms_ltf_key_owner.inserted_date_time inserted_date_time,
       case when p_mms_ltf_key_owner.bk_hash in('-997', '-998', '-999') then p_mms_ltf_key_owner.bk_hash
           when s_mms_ltf_key_owner.inserted_date_time is null then '-998'
        else convert(varchar, s_mms_ltf_key_owner.inserted_date_time, 112)    end inserted_dim_date_key,
       case when p_mms_ltf_key_owner.bk_hash in ('-997','-998','-999') then p_mms_ltf_key_owner.bk_hash
       when s_mms_ltf_key_owner.inserted_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_mms_ltf_key_owner.inserted_date_time,114), 1, 5),':','') end inserted_dim_time_key,
       s_mms_ltf_key_owner.key_priority key_priority,
       l_mms_ltf_key_owner.ltf_key_acquisition_id ltf_key_acquisition_id,
       l_mms_ltf_key_owner.ltf_key_id ltf_key_id,
       case when p_mms_ltf_key_owner.bk_hash in('-997', '-998', '-999') then p_mms_ltf_key_owner.bk_hash
           when s_mms_ltf_key_owner.from_date is null then '-998'
        else convert(varchar, s_mms_ltf_key_owner.from_date, 112)    end ltf_key_owner_from_dim_date_key,
       case when p_mms_ltf_key_owner.bk_hash in ('-997','-998','-999') then p_mms_ltf_key_owner.bk_hash
       when s_mms_ltf_key_owner.from_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_mms_ltf_key_owner.from_date,114), 1, 5),':','') end ltf_key_owner_from_dim_time_key,
       case when p_mms_ltf_key_owner.bk_hash in('-997', '-998', '-999') then p_mms_ltf_key_owner.bk_hash
           when s_mms_ltf_key_owner.thru_date is null then '-998'
        else convert(varchar, s_mms_ltf_key_owner.thru_date, 112)    end ltf_key_owner_thru_dim_date_key,
       case when p_mms_ltf_key_owner.bk_hash in ('-997','-998','-999') then p_mms_ltf_key_owner.bk_hash
       when s_mms_ltf_key_owner.thru_time is null then '-998'
       else '1' + substring(replace(s_mms_ltf_key_owner.thru_time,':',''),1,4) end ltf_key_owner_thru_dim_time_key,
       l_mms_ltf_key_owner.party_id party_id,
       s_mms_ltf_key_owner.updated_date_time updated_date_time,
       case when p_mms_ltf_key_owner.bk_hash in('-997', '-998', '-999') then p_mms_ltf_key_owner.bk_hash
           when s_mms_ltf_key_owner.updated_date_time is null then '-998'
        else convert(varchar, s_mms_ltf_key_owner.updated_date_time, 112)    end updated_dim_date_key,
       case when p_mms_ltf_key_owner.bk_hash in ('-997','-998','-999') then p_mms_ltf_key_owner.bk_hash
       when s_mms_ltf_key_owner.updated_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_mms_ltf_key_owner.updated_date_time,114), 1, 5),':','') end updated_dim_time_key,
       s_mms_ltf_key_owner.usage_count usage_count,
       s_mms_ltf_key_owner.usage_limit usage_limit,
       l_mms_ltf_key_owner.val_acquisition_type_id val_acquisition_type_id,
       l_mms_ltf_key_owner.val_ownership_type_id val_ownership_type_id,
       isnull(h_mms_ltf_key_owner.dv_deleted,0) dv_deleted,
       p_mms_ltf_key_owner.p_mms_ltf_key_owner_id,
       p_mms_ltf_key_owner.dv_batch_id,
       p_mms_ltf_key_owner.dv_load_date_time,
       p_mms_ltf_key_owner.dv_load_end_date_time
  from dbo.h_mms_ltf_key_owner
  join dbo.p_mms_ltf_key_owner
    on h_mms_ltf_key_owner.bk_hash = p_mms_ltf_key_owner.bk_hash
  join #p_mms_ltf_key_owner_insert
    on p_mms_ltf_key_owner.bk_hash = #p_mms_ltf_key_owner_insert.bk_hash
   and p_mms_ltf_key_owner.p_mms_ltf_key_owner_id = #p_mms_ltf_key_owner_insert.p_mms_ltf_key_owner_id
  join dbo.l_mms_ltf_key_owner
    on p_mms_ltf_key_owner.bk_hash = l_mms_ltf_key_owner.bk_hash
   and p_mms_ltf_key_owner.l_mms_ltf_key_owner_id = l_mms_ltf_key_owner.l_mms_ltf_key_owner_id
  join dbo.s_mms_ltf_key_owner
    on p_mms_ltf_key_owner.bk_hash = s_mms_ltf_key_owner.bk_hash
   and p_mms_ltf_key_owner.s_mms_ltf_key_owner_id = s_mms_ltf_key_owner.s_mms_ltf_key_owner_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_mms_ltf_key_owner
   where d_mms_ltf_key_owner.bk_hash in (select bk_hash from #p_mms_ltf_key_owner_insert)

  insert dbo.d_mms_ltf_key_owner(
             bk_hash,
             ltf_key_owner_id,
             acquisition_id,
             d_ltf_key_bk_hash,
             display_name,
             inserted_date_time,
             inserted_dim_date_key,
             inserted_dim_time_key,
             key_priority,
             ltf_key_acquisition_id,
             ltf_key_id,
             ltf_key_owner_from_dim_date_key,
             ltf_key_owner_from_dim_time_key,
             ltf_key_owner_thru_dim_date_key,
             ltf_key_owner_thru_dim_time_key,
             party_id,
             updated_date_time,
             updated_dim_date_key,
             updated_dim_time_key,
             usage_count,
             usage_limit,
             val_acquisition_type_id,
             val_ownership_type_id,
             deleted_flag,
             p_mms_ltf_key_owner_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         ltf_key_owner_id,
         acquisition_id,
         d_ltf_key_bk_hash,
         display_name,
         inserted_date_time,
         inserted_dim_date_key,
         inserted_dim_time_key,
         key_priority,
         ltf_key_acquisition_id,
         ltf_key_id,
         ltf_key_owner_from_dim_date_key,
         ltf_key_owner_from_dim_time_key,
         ltf_key_owner_thru_dim_date_key,
         ltf_key_owner_thru_dim_time_key,
         party_id,
         updated_date_time,
         updated_dim_date_key,
         updated_dim_time_key,
         usage_count,
         usage_limit,
         val_acquisition_type_id,
         val_ownership_type_id,
         dv_deleted,
         p_mms_ltf_key_owner_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_ltf_key_owner)
--Done!
end
