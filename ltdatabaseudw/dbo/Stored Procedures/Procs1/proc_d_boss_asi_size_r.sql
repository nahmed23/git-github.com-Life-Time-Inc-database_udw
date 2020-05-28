CREATE PROC [dbo].[proc_d_boss_asi_size_r] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_boss_asi_size_r)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_boss_asi_size_r_insert') is not null drop table #p_boss_asi_size_r_insert
create table dbo.#p_boss_asi_size_r_insert with(distribution=hash(bk_hash), location=user_db) as
select p_boss_asi_size_r.p_boss_asi_size_r_id,
       p_boss_asi_size_r.bk_hash
  from dbo.p_boss_asi_size_r
 where p_boss_asi_size_r.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_boss_asi_size_r.dv_batch_id > @max_dv_batch_id
        or p_boss_asi_size_r.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_boss_asi_size_r.bk_hash,
       p_boss_asi_size_r.size_r_dept size_r_dept,
       p_boss_asi_size_r.size_r_class size_r_class,
       p_boss_asi_size_r.size_r_code size_r_code,
       isnull(s_boss_asi_size_r.size_r_desc, '') product_hierarchy_level_2,
       h_boss_asi_size_r.dv_deleted,
       p_boss_asi_size_r.p_boss_asi_size_r_id,
       p_boss_asi_size_r.dv_batch_id,
       p_boss_asi_size_r.dv_load_date_time,
       p_boss_asi_size_r.dv_load_end_date_time
  from dbo.h_boss_asi_size_r
  join dbo.p_boss_asi_size_r
    on h_boss_asi_size_r.bk_hash = p_boss_asi_size_r.bk_hash  join #p_boss_asi_size_r_insert
    on p_boss_asi_size_r.bk_hash = #p_boss_asi_size_r_insert.bk_hash
   and p_boss_asi_size_r.p_boss_asi_size_r_id = #p_boss_asi_size_r_insert.p_boss_asi_size_r_id
  join dbo.l_boss_asi_size_r
    on p_boss_asi_size_r.bk_hash = l_boss_asi_size_r.bk_hash
   and p_boss_asi_size_r.l_boss_asi_size_r_id = l_boss_asi_size_r.l_boss_asi_size_r_id
  join dbo.s_boss_asi_size_r
    on p_boss_asi_size_r.bk_hash = s_boss_asi_size_r.bk_hash
   and p_boss_asi_size_r.s_boss_asi_size_r_id = s_boss_asi_size_r.s_boss_asi_size_r_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_boss_asi_size_r
   where d_boss_asi_size_r.bk_hash in (select bk_hash from #p_boss_asi_size_r_insert)

  insert dbo.d_boss_asi_size_r(
             bk_hash,
             size_r_dept,
             size_r_class,
             size_r_code,
             product_hierarchy_level_2,
             deleted_flag,
             p_boss_asi_size_r_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         size_r_dept,
         size_r_class,
         size_r_code,
         product_hierarchy_level_2,
         dv_deleted,
         p_boss_asi_size_r_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_boss_asi_size_r)
--Done!
end
