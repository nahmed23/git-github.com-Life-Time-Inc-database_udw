CREATE PROC [dbo].[proc_d_boss_tags] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_boss_tags)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_boss_tags_insert') is not null drop table #p_boss_tags_insert
create table dbo.#p_boss_tags_insert with(distribution=hash(bk_hash), location=user_db) as
select p_boss_tags.p_boss_tags_id,
       p_boss_tags.bk_hash
  from dbo.p_boss_tags
 where p_boss_tags.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_boss_tags.dv_batch_id > @max_dv_batch_id
        or p_boss_tags.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_boss_tags.bk_hash,
       p_boss_tags.tags_id tags_id,
       s_boss_tags.name tag_name,
       s_boss_tags.kind tag_type,
       isnull(h_boss_tags.dv_deleted,0) dv_deleted,
       p_boss_tags.p_boss_tags_id,
       p_boss_tags.dv_batch_id,
       p_boss_tags.dv_load_date_time,
       p_boss_tags.dv_load_end_date_time
  from dbo.h_boss_tags
  join dbo.p_boss_tags
    on h_boss_tags.bk_hash = p_boss_tags.bk_hash
  join #p_boss_tags_insert
    on p_boss_tags.bk_hash = #p_boss_tags_insert.bk_hash
   and p_boss_tags.p_boss_tags_id = #p_boss_tags_insert.p_boss_tags_id
  join dbo.l_boss_tags
    on p_boss_tags.bk_hash = l_boss_tags.bk_hash
   and p_boss_tags.l_boss_tags_id = l_boss_tags.l_boss_tags_id
  join dbo.s_boss_tags
    on p_boss_tags.bk_hash = s_boss_tags.bk_hash
   and p_boss_tags.s_boss_tags_id = s_boss_tags.s_boss_tags_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_boss_tags
   where d_boss_tags.bk_hash in (select bk_hash from #p_boss_tags_insert)

  insert dbo.d_boss_tags(
             bk_hash,
             tags_id,
             tag_name,
             tag_type,
             deleted_flag,
             p_boss_tags_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         tags_id,
         tag_name,
         tag_type,
         dv_deleted,
         p_boss_tags_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_boss_tags)
--Done!
end
