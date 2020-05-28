CREATE PROC [dbo].[proc_d_boss_interest] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_boss_interest)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_boss_interest_insert') is not null drop table #p_boss_interest_insert
create table dbo.#p_boss_interest_insert with(distribution=hash(bk_hash), location=user_db) as
select p_boss_interest.p_boss_interest_id,
       p_boss_interest.bk_hash
  from dbo.p_boss_interest
 where p_boss_interest.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_boss_interest.dv_batch_id > @max_dv_batch_id
        or p_boss_interest.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_boss_interest.bk_hash,
       p_boss_interest.interest_id interest_id,
       s_boss_interest.long_desc interest_long_desc,
       s_boss_interest.short_desc interest_short_desc,
       isnull(h_boss_interest.dv_deleted,0) dv_deleted,
       p_boss_interest.p_boss_interest_id,
       p_boss_interest.dv_batch_id,
       p_boss_interest.dv_load_date_time,
       p_boss_interest.dv_load_end_date_time
  from dbo.h_boss_interest
  join dbo.p_boss_interest
    on h_boss_interest.bk_hash = p_boss_interest.bk_hash
  join #p_boss_interest_insert
    on p_boss_interest.bk_hash = #p_boss_interest_insert.bk_hash
   and p_boss_interest.p_boss_interest_id = #p_boss_interest_insert.p_boss_interest_id
  join dbo.s_boss_interest
    on p_boss_interest.bk_hash = s_boss_interest.bk_hash
   and p_boss_interest.s_boss_interest_id = s_boss_interest.s_boss_interest_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_boss_interest
   where d_boss_interest.bk_hash in (select bk_hash from #p_boss_interest_insert)

  insert dbo.d_boss_interest(
             bk_hash,
             interest_id,
             interest_long_desc,
             interest_short_desc,
             deleted_flag,
             p_boss_interest_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         interest_id,
         interest_long_desc,
         interest_short_desc,
         dv_deleted,
         p_boss_interest_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_boss_interest)
--Done!
end
