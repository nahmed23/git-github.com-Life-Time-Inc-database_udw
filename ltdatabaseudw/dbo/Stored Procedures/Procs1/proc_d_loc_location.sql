CREATE PROC [dbo].[proc_d_loc_location] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_loc_location)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_loc_location_insert') is not null drop table #p_loc_location_insert
create table dbo.#p_loc_location_insert with(distribution=hash(bk_hash), location=user_db) as
select p_loc_location.p_loc_location_id,
       p_loc_location.bk_hash
  from dbo.p_loc_location
 where p_loc_location.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_loc_location.dv_batch_id > @max_dv_batch_id
        or p_loc_location.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_loc_location.bk_hash,
       p_loc_location.location_id location_id,
       s_loc_location.created_by created_by,
       case when p_loc_location.bk_hash in ('-997', '-998', '-999') then p_loc_location.bk_hash 
              when s_loc_location.created_date_time is null then '-998'    
              else convert(varchar, s_loc_location.created_date_time, 112)   end created_date_key,
       s_loc_location.created_date_time created_date_time,
       s_loc_location.deleted_by deleted_by,
       case when p_loc_location.bk_hash in ('-997', '-998', '-999') then p_loc_location.bk_hash 
              when s_loc_location.deleted_date_time is null then '-998'    
              else convert(varchar, s_loc_location.deleted_date_time, 112)   end deleted_date_key,
       s_loc_location.deleted_date_time deleted_date_time,
       s_loc_location.description description,
       s_loc_location.display_name display_name,
       l_loc_location.external_id external_id,
       s_loc_location.hierarchy_level hierarchy_level,
       s_loc_location.last_updated_by last_updated_by,
       case when p_loc_location.bk_hash in ('-997', '-998', '-999') then p_loc_location.bk_hash 
              when s_loc_location.last_updated_date_time is null then '-998'    
              else convert(varchar, s_loc_location.last_updated_date_time, 112)   end last_updated_date_key,
       s_loc_location.last_updated_date_time last_updated_date_time,
       l_loc_location.parent_location_id parent_location_id,
       s_loc_location.slug slug,
       l_loc_location.top_level_location_id top_level_location_id,
       case when p_loc_location.bk_hash in('-997', '-998', '-999') then p_loc_location.bk_hash
           when l_loc_location.val_location_type_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_loc_location.val_location_type_id as bigint) as varchar(500)),'z#@$k%&P'))),2)   end val_location_type_id_bk_hash,
       isnull(h_loc_location.dv_deleted,0) dv_deleted,
       p_loc_location.p_loc_location_id,
       p_loc_location.dv_batch_id,
       p_loc_location.dv_load_date_time,
       p_loc_location.dv_load_end_date_time
  from dbo.h_loc_location
  join dbo.p_loc_location
    on h_loc_location.bk_hash = p_loc_location.bk_hash
  join #p_loc_location_insert
    on p_loc_location.bk_hash = #p_loc_location_insert.bk_hash
   and p_loc_location.p_loc_location_id = #p_loc_location_insert.p_loc_location_id
  join dbo.l_loc_location
    on p_loc_location.bk_hash = l_loc_location.bk_hash
   and p_loc_location.l_loc_location_id = l_loc_location.l_loc_location_id
  join dbo.s_loc_location
    on p_loc_location.bk_hash = s_loc_location.bk_hash
   and p_loc_location.s_loc_location_id = s_loc_location.s_loc_location_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_loc_location
   where d_loc_location.bk_hash in (select bk_hash from #p_loc_location_insert)

  insert dbo.d_loc_location(
             bk_hash,
             location_id,
             created_by,
             created_date_key,
             created_date_time,
             deleted_by,
             deleted_date_key,
             deleted_date_time,
             description,
             display_name,
             external_id,
             hierarchy_level,
             last_updated_by,
             last_updated_date_key,
             last_updated_date_time,
             parent_location_id,
             slug,
             top_level_location_id,
             val_location_type_id_bk_hash,
             deleted_flag,
             p_loc_location_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         location_id,
         created_by,
         created_date_key,
         created_date_time,
         deleted_by,
         deleted_date_key,
         deleted_date_time,
         description,
         display_name,
         external_id,
         hierarchy_level,
         last_updated_by,
         last_updated_date_key,
         last_updated_date_time,
         parent_location_id,
         slug,
         top_level_location_id,
         val_location_type_id_bk_hash,
         dv_deleted,
         p_loc_location_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_loc_location)
--Done!
end
