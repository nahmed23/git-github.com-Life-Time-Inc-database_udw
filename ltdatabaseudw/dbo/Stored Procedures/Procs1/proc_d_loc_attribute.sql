CREATE PROC [dbo].[proc_d_loc_attribute] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_loc_attribute)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_loc_attribute_insert') is not null drop table #p_loc_attribute_insert
create table dbo.#p_loc_attribute_insert with(distribution=hash(bk_hash), location=user_db) as
select p_loc_attribute.p_loc_attribute_id,
       p_loc_attribute.bk_hash
  from dbo.p_loc_attribute
 where p_loc_attribute.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_loc_attribute.dv_batch_id > @max_dv_batch_id
        or p_loc_attribute.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_loc_attribute.bk_hash,
       p_loc_attribute.attribute_id attribute_id,
       s_loc_attribute.attribute_value attribute_value,
       case when p_loc_attribute.bk_hash in ('-997','-998','-999') then p_loc_attribute.bk_hash
         when l_loc_attribute.udw_business_key is null then '-998'
         else udw_business_key  end business_key,
       s_loc_attribute.udw_source_name business_source_name,
       s_loc_attribute.created_by created_by,
       case when p_loc_attribute.bk_hash in('-997', '-998', '-999') then p_loc_attribute.bk_hash
           when s_loc_attribute.created_date_time is null then '-998'
        else convert(varchar, s_loc_attribute.created_date_time, 112)    end created_dim_date_key,
       case when l_loc_attribute.bk_hash in ('-997','-998','-999') then p_loc_attribute.bk_hash
         when l_loc_attribute.val_attribute_type_id is null then '-998'
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_loc_attribute.val_attribute_type_id as bigint) as varchar(500)),'z#@$k%&P'))),2)   end d_loc_val_attribute_type_bk_hash,
       s_loc_attribute.deleted_by deleted_by,
       case when p_loc_attribute.bk_hash in ('-997','-998','-999') then p_loc_attribute.bk_hash
         when l_loc_attribute.location_id is null then '-998'
         else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_loc_attribute.location_id as bigint) as varchar(500)),'z#@$k%&P'))),2)   end dim_location_key,
       case when s_loc_attribute.managed_by_udw='1' then 'Y' else 'N' end managed_by_udw_flag,
       s_loc_attribute.last_updated_by updated_by,
       case when p_loc_attribute.bk_hash in('-997', '-998', '-999') then p_loc_attribute.bk_hash
           when s_loc_attribute.last_updated_date_time is null then '-998'
        else convert(varchar, s_loc_attribute.last_updated_date_time, 112)    end updated_dim_date_key,
       isnull(h_loc_attribute.dv_deleted,0) dv_deleted,
       p_loc_attribute.p_loc_attribute_id,
       p_loc_attribute.dv_batch_id,
       p_loc_attribute.dv_load_date_time,
       p_loc_attribute.dv_load_end_date_time
  from dbo.h_loc_attribute
  join dbo.p_loc_attribute
    on h_loc_attribute.bk_hash = p_loc_attribute.bk_hash
  join #p_loc_attribute_insert
    on p_loc_attribute.bk_hash = #p_loc_attribute_insert.bk_hash
   and p_loc_attribute.p_loc_attribute_id = #p_loc_attribute_insert.p_loc_attribute_id
  join dbo.l_loc_attribute
    on p_loc_attribute.bk_hash = l_loc_attribute.bk_hash
   and p_loc_attribute.l_loc_attribute_id = l_loc_attribute.l_loc_attribute_id
  join dbo.s_loc_attribute
    on p_loc_attribute.bk_hash = s_loc_attribute.bk_hash
   and p_loc_attribute.s_loc_attribute_id = s_loc_attribute.s_loc_attribute_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_loc_attribute
   where d_loc_attribute.bk_hash in (select bk_hash from #p_loc_attribute_insert)

  insert dbo.d_loc_attribute(
             bk_hash,
             attribute_id,
             attribute_value,
             business_key,
             business_source_name,
             created_by,
             created_dim_date_key,
             d_loc_val_attribute_type_bk_hash,
             deleted_by,
             dim_location_key,
             managed_by_udw_flag,
             updated_by,
             updated_dim_date_key,
             deleted_flag,
             p_loc_attribute_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         attribute_id,
         attribute_value,
         business_key,
         business_source_name,
         created_by,
         created_dim_date_key,
         d_loc_val_attribute_type_bk_hash,
         deleted_by,
         dim_location_key,
         managed_by_udw_flag,
         updated_by,
         updated_dim_date_key,
         dv_deleted,
         p_loc_attribute_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_loc_attribute)
--Done!
end
