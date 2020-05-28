CREATE PROC [dbo].[proc_d_commprefs_parties] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_commprefs_parties)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_commprefs_parties_insert') is not null drop table #p_commprefs_parties_insert
create table dbo.#p_commprefs_parties_insert with(distribution=hash(bk_hash), location=user_db) as
select p_commprefs_parties.p_commprefs_parties_id,
       p_commprefs_parties.bk_hash
  from dbo.p_commprefs_parties
 where p_commprefs_parties.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_commprefs_parties.dv_batch_id > @max_dv_batch_id
        or p_commprefs_parties.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_commprefs_parties.bk_hash,
       p_commprefs_parties.bk_hash d_commprefs_parties_key,
       p_commprefs_parties.parties_id parties_id,
       h_commprefs_parties.dv_deleted,
       p_commprefs_parties.p_commprefs_parties_id,
       p_commprefs_parties.dv_batch_id,
       p_commprefs_parties.dv_load_date_time,
       p_commprefs_parties.dv_load_end_date_time
  from dbo.h_commprefs_parties
  join dbo.p_commprefs_parties
    on h_commprefs_parties.bk_hash = p_commprefs_parties.bk_hash  join #p_commprefs_parties_insert
    on p_commprefs_parties.bk_hash = #p_commprefs_parties_insert.bk_hash
   and p_commprefs_parties.p_commprefs_parties_id = #p_commprefs_parties_insert.p_commprefs_parties_id
  join dbo.s_commprefs_parties
    on p_commprefs_parties.bk_hash = s_commprefs_parties.bk_hash
   and p_commprefs_parties.s_commprefs_parties_id = s_commprefs_parties.s_commprefs_parties_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_commprefs_parties
   where d_commprefs_parties.bk_hash in (select bk_hash from #p_commprefs_parties_insert)

  insert dbo.d_commprefs_parties(
             bk_hash,
             d_commprefs_parties_key,
             parties_id,
             deleted_flag,
             p_commprefs_parties_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         d_commprefs_parties_key,
         parties_id,
         dv_deleted,
         p_commprefs_parties_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_commprefs_parties)
--Done!
end
