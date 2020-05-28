﻿CREATE PROC [dbo].[proc_d_mms_member_attribute] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_member_attribute)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mms_member_attribute_insert') is not null drop table #p_mms_member_attribute_insert
create table dbo.#p_mms_member_attribute_insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_member_attribute.p_mms_member_attribute_id,
       p_mms_member_attribute.bk_hash
  from dbo.p_mms_member_attribute
 where p_mms_member_attribute.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_mms_member_attribute.dv_batch_id > @max_dv_batch_id
        or p_mms_member_attribute.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_member_attribute.bk_hash,
       p_mms_member_attribute.member_attribute_id member_attribute_id,
       s_mms_member_attribute.attribute_value attribute_value,
       case when p_mms_member_attribute.bk_hash in('-997', '-998', '-999') then p_mms_member_attribute.bk_hash
           when l_mms_member_attribute.member_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_member_attribute.member_id as int) as varchar(500)),'z#@$k%&P'))),2)   end dim_mms_member_key,
       s_mms_member_attribute_1.effective_from_date_time effective_from_date_time,
       case when p_mms_member_attribute.bk_hash in('-997', '-998', '-999') then p_mms_member_attribute.bk_hash
           when s_mms_member_attribute_1.effective_from_date_time is null then '-998'
        else convert(varchar, s_mms_member_attribute_1.effective_from_date_time, 112)    end effective_from_dim_date_key,
       case when p_mms_member_attribute.bk_hash in ('-997','-998','-999') then p_mms_member_attribute.bk_hash
       when s_mms_member_attribute_1.effective_from_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_mms_member_attribute_1.effective_from_date_time,114), 1, 5),':','') end effective_from_dim_time_key,
       s_mms_member_attribute_1.effective_thru_date_time effective_thru_date_time,
       case when p_mms_member_attribute.bk_hash in('-997', '-998', '-999') then p_mms_member_attribute.bk_hash
           when s_mms_member_attribute_1.effective_thru_date_time is null then '-998'
        else convert(varchar, s_mms_member_attribute_1.effective_thru_date_time, 112)    end effective_thru_dim_date_key,
       case when p_mms_member_attribute.bk_hash in ('-997','-998','-999') then p_mms_member_attribute.bk_hash
       when s_mms_member_attribute_1.effective_thru_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_mms_member_attribute_1.effective_thru_date_time,114), 1, 5),':','') end effective_thru_dim_time_key,
       s_mms_member_attribute.expiration_date expiration_date,
       case when p_mms_member_attribute.bk_hash in('-997', '-998', '-999') then p_mms_member_attribute.bk_hash
           when s_mms_member_attribute.expiration_date is null then '-998'
        else convert(varchar, s_mms_member_attribute.expiration_date, 112)    end expiration_dim_date_key,
       case when p_mms_member_attribute.bk_hash in ('-997','-998','-999') then p_mms_member_attribute.bk_hash
       when s_mms_member_attribute.expiration_date is null then '-998'
       else '1' + replace(substring(convert(varchar,s_mms_member_attribute.expiration_date,114), 1, 5),':','') end expiration_dim_time_key,
       s_mms_member_attribute.inserted_date_time inserted_date_time,
       case when p_mms_member_attribute.bk_hash in('-997', '-998', '-999') then p_mms_member_attribute.bk_hash
           when s_mms_member_attribute.inserted_date_time is null then '-998'
        else convert(varchar, s_mms_member_attribute.inserted_date_time, 112)    end inserted_dim_date_key,
       case when p_mms_member_attribute.bk_hash in ('-997','-998','-999') then p_mms_member_attribute.bk_hash
       when s_mms_member_attribute.inserted_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_mms_member_attribute.inserted_date_time,114), 1, 5),':','') end inserted_dim_time_key,
       case when p_mms_member_attribute.bk_hash in('-997', '-998', '-999') then p_mms_member_attribute.bk_hash
           when l_mms_member_attribute.val_member_attribute_type_id is null then '-998'
        else 'r_mms_val_member_attribute_type_'+convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_mms_member_attribute.val_member_attribute_type_id as int) as varchar(500)),'z#@$k%&P'))),2)   end member_attribute_type_dim_description_key,
       l_mms_member_attribute.member_id member_id,
       s_mms_member_attribute.updated_date_time updated_date_time,
       case when p_mms_member_attribute.bk_hash in('-997', '-998', '-999') then p_mms_member_attribute.bk_hash
           when s_mms_member_attribute.updated_date_time is null then '-998'
        else convert(varchar, s_mms_member_attribute.updated_date_time, 112)    end updated_dim_date_key,
       case when p_mms_member_attribute.bk_hash in ('-997','-998','-999') then p_mms_member_attribute.bk_hash
       when s_mms_member_attribute.updated_date_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_mms_member_attribute.updated_date_time,114), 1, 5),':','') end updated_dim_time_key,
       l_mms_member_attribute.val_member_attribute_type_id  val_member_attribute_type_id,
       isnull(h_mms_member_attribute.dv_deleted,0) dv_deleted,
       p_mms_member_attribute.p_mms_member_attribute_id,
       p_mms_member_attribute.dv_batch_id,
       p_mms_member_attribute.dv_load_date_time,
       p_mms_member_attribute.dv_load_end_date_time
  from dbo.h_mms_member_attribute
  join dbo.p_mms_member_attribute
    on h_mms_member_attribute.bk_hash = p_mms_member_attribute.bk_hash
  join #p_mms_member_attribute_insert
    on p_mms_member_attribute.bk_hash = #p_mms_member_attribute_insert.bk_hash
   and p_mms_member_attribute.p_mms_member_attribute_id = #p_mms_member_attribute_insert.p_mms_member_attribute_id
  join dbo.l_mms_member_attribute
    on p_mms_member_attribute.bk_hash = l_mms_member_attribute.bk_hash
   and p_mms_member_attribute.l_mms_member_attribute_id = l_mms_member_attribute.l_mms_member_attribute_id
  join dbo.s_mms_member_attribute
    on p_mms_member_attribute.bk_hash = s_mms_member_attribute.bk_hash
   and p_mms_member_attribute.s_mms_member_attribute_id = s_mms_member_attribute.s_mms_member_attribute_id
  join dbo.s_mms_member_attribute_1
    on p_mms_member_attribute.bk_hash = s_mms_member_attribute_1.bk_hash
   and p_mms_member_attribute.s_mms_member_attribute_1_id = s_mms_member_attribute_1.s_mms_member_attribute_1_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_mms_member_attribute
   where d_mms_member_attribute.bk_hash in (select bk_hash from #p_mms_member_attribute_insert)

  insert dbo.d_mms_member_attribute(
             bk_hash,
             member_attribute_id,
             attribute_value,
             dim_mms_member_key,
             effective_from_date_time,
             effective_from_dim_date_key,
             effective_from_dim_time_key,
             effective_thru_date_time,
             effective_thru_dim_date_key,
             effective_thru_dim_time_key,
             expiration_date,
             expiration_dim_date_key,
             expiration_dim_time_key,
             inserted_date_time,
             inserted_dim_date_key,
             inserted_dim_time_key,
             member_attribute_type_dim_description_key,
             member_id,
             updated_date_time,
             updated_dim_date_key,
             updated_dim_time_key,
             val_member_attribute_type_id,
             deleted_flag,
             p_mms_member_attribute_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         member_attribute_id,
         attribute_value,
         dim_mms_member_key,
         effective_from_date_time,
         effective_from_dim_date_key,
         effective_from_dim_time_key,
         effective_thru_date_time,
         effective_thru_dim_date_key,
         effective_thru_dim_time_key,
         expiration_date,
         expiration_dim_date_key,
         expiration_dim_time_key,
         inserted_date_time,
         inserted_dim_date_key,
         inserted_dim_time_key,
         member_attribute_type_dim_description_key,
         member_id,
         updated_date_time,
         updated_dim_date_key,
         updated_dim_time_key,
         val_member_attribute_type_id,
         dv_deleted,
         p_mms_member_attribute_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_member_attribute)
--Done!
end
