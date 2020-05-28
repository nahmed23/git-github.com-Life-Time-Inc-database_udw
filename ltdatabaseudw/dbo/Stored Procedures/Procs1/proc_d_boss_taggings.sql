CREATE PROC [dbo].[proc_d_boss_taggings] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_boss_taggings)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_boss_taggings_insert') is not null drop table #p_boss_taggings_insert
create table dbo.#p_boss_taggings_insert with(distribution=hash(bk_hash), location=user_db) as
select p_boss_taggings.p_boss_taggings_id,
       p_boss_taggings.bk_hash
  from dbo.p_boss_taggings
 where p_boss_taggings.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_boss_taggings.dv_batch_id > @max_dv_batch_id
        or p_boss_taggings.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_boss_taggings.bk_hash,
       p_boss_taggings.taggings_id taggings_id,
       case when p_boss_taggings.bk_hash in('-997', '-998', '-999') then p_boss_taggings.bk_hash
           when l_boss_taggings.tag_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_boss_taggings.tag_id as int) as varchar(500)),'z#@$k%&P'))),2)   end d_boss_tags_bk_hash,
       l_boss_taggings.taggable_id taggable_id,
       s_boss_taggings.taggable_type taggable_type,
       isnull(h_boss_taggings.dv_deleted,0) dv_deleted,
       p_boss_taggings.p_boss_taggings_id,
       p_boss_taggings.dv_batch_id,
       p_boss_taggings.dv_load_date_time,
       p_boss_taggings.dv_load_end_date_time
  from dbo.h_boss_taggings
  join dbo.p_boss_taggings
    on h_boss_taggings.bk_hash = p_boss_taggings.bk_hash
  join #p_boss_taggings_insert
    on p_boss_taggings.bk_hash = #p_boss_taggings_insert.bk_hash
   and p_boss_taggings.p_boss_taggings_id = #p_boss_taggings_insert.p_boss_taggings_id
  join dbo.l_boss_taggings
    on p_boss_taggings.bk_hash = l_boss_taggings.bk_hash
   and p_boss_taggings.l_boss_taggings_id = l_boss_taggings.l_boss_taggings_id
  join dbo.s_boss_taggings
    on p_boss_taggings.bk_hash = s_boss_taggings.bk_hash
   and p_boss_taggings.s_boss_taggings_id = s_boss_taggings.s_boss_taggings_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_boss_taggings
   where d_boss_taggings.bk_hash in (select bk_hash from #p_boss_taggings_insert)

  insert dbo.d_boss_taggings(
             bk_hash,
             taggings_id,
             d_boss_tags_bk_hash,
             taggable_id,
             taggable_type,
             deleted_flag,
             p_boss_taggings_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         taggings_id,
         d_boss_tags_bk_hash,
         taggable_id,
         taggable_type,
         dv_deleted,
         p_boss_taggings_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_boss_taggings)
--Done!
end
