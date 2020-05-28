CREATE PROC [dbo].[proc_d_loc_val_attribute_type_group] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_loc_val_attribute_type_group)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_loc_val_attribute_type_group_insert') is not null drop table #p_loc_val_attribute_type_group_insert
create table dbo.#p_loc_val_attribute_type_group_insert with(distribution=hash(bk_hash), location=user_db) as
select p_loc_val_attribute_type_group.p_loc_val_attribute_type_group_id,
       p_loc_val_attribute_type_group.bk_hash
  from dbo.p_loc_val_attribute_type_group
 where p_loc_val_attribute_type_group.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_loc_val_attribute_type_group.dv_batch_id > @max_dv_batch_id
        or p_loc_val_attribute_type_group.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_loc_val_attribute_type_group.bk_hash,
       p_loc_val_attribute_type_group.val_attribute_type_group_id val_attribute_type_group_id,
       s_loc_val_attribute_type_group.display_name display_name,
       s_loc_val_attribute_type_group.val_attribute_type_group_name val_attribute_type_group_name,
       isnull(h_loc_val_attribute_type_group.dv_deleted,0) dv_deleted,
       p_loc_val_attribute_type_group.p_loc_val_attribute_type_group_id,
       p_loc_val_attribute_type_group.dv_batch_id,
       p_loc_val_attribute_type_group.dv_load_date_time,
       p_loc_val_attribute_type_group.dv_load_end_date_time
  from dbo.h_loc_val_attribute_type_group
  join dbo.p_loc_val_attribute_type_group
    on h_loc_val_attribute_type_group.bk_hash = p_loc_val_attribute_type_group.bk_hash
  join #p_loc_val_attribute_type_group_insert
    on p_loc_val_attribute_type_group.bk_hash = #p_loc_val_attribute_type_group_insert.bk_hash
   and p_loc_val_attribute_type_group.p_loc_val_attribute_type_group_id = #p_loc_val_attribute_type_group_insert.p_loc_val_attribute_type_group_id
  join dbo.s_loc_val_attribute_type_group
    on p_loc_val_attribute_type_group.bk_hash = s_loc_val_attribute_type_group.bk_hash
   and p_loc_val_attribute_type_group.s_loc_val_attribute_type_group_id = s_loc_val_attribute_type_group.s_loc_val_attribute_type_group_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_loc_val_attribute_type_group
   where d_loc_val_attribute_type_group.bk_hash in (select bk_hash from #p_loc_val_attribute_type_group_insert)

  insert dbo.d_loc_val_attribute_type_group(
             bk_hash,
             val_attribute_type_group_id,
             display_name,
             val_attribute_type_group_name,
             deleted_flag,
             p_loc_val_attribute_type_group_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         val_attribute_type_group_id,
         display_name,
         val_attribute_type_group_name,
         dv_deleted,
         p_loc_val_attribute_type_group_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_loc_val_attribute_type_group)
--Done!
end
