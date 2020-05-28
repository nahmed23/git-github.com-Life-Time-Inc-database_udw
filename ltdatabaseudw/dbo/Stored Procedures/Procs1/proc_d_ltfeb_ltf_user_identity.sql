CREATE PROC [dbo].[proc_d_ltfeb_ltf_user_identity] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_ltfeb_ltf_user_identity)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_ltfeb_ltf_user_identity_insert') is not null drop table #p_ltfeb_ltf_user_identity_insert
create table dbo.#p_ltfeb_ltf_user_identity_insert with(distribution=hash(bk_hash), location=user_db) as
select p_ltfeb_ltf_user_identity.p_ltfeb_ltf_user_identity_id,
       p_ltfeb_ltf_user_identity.bk_hash
  from dbo.p_ltfeb_ltf_user_identity
 where p_ltfeb_ltf_user_identity.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_ltfeb_ltf_user_identity.dv_batch_id > @max_dv_batch_id
        or p_ltfeb_ltf_user_identity.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_ltfeb_ltf_user_identity.bk_hash,
       s_ltfeb_ltf_user_identity.party_id party_id,
       s_ltfeb_ltf_user_identity.ltf_user_name ltf_user_name,
       isnull(h_ltfeb_ltf_user_identity.dv_deleted,0) dv_deleted,
       p_ltfeb_ltf_user_identity.p_ltfeb_ltf_user_identity_id,
       p_ltfeb_ltf_user_identity.dv_batch_id,
       p_ltfeb_ltf_user_identity.dv_load_date_time,
       p_ltfeb_ltf_user_identity.dv_load_end_date_time
  from dbo.h_ltfeb_ltf_user_identity
  join dbo.p_ltfeb_ltf_user_identity
    on h_ltfeb_ltf_user_identity.bk_hash = p_ltfeb_ltf_user_identity.bk_hash
  join #p_ltfeb_ltf_user_identity_insert
    on p_ltfeb_ltf_user_identity.bk_hash = #p_ltfeb_ltf_user_identity_insert.bk_hash
   and p_ltfeb_ltf_user_identity.p_ltfeb_ltf_user_identity_id = #p_ltfeb_ltf_user_identity_insert.p_ltfeb_ltf_user_identity_id
  join dbo.s_ltfeb_ltf_user_identity
    on p_ltfeb_ltf_user_identity.bk_hash = s_ltfeb_ltf_user_identity.bk_hash
   and p_ltfeb_ltf_user_identity.s_ltfeb_ltf_user_identity_id = s_ltfeb_ltf_user_identity.s_ltfeb_ltf_user_identity_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_ltfeb_ltf_user_identity
   where d_ltfeb_ltf_user_identity.bk_hash in (select bk_hash from #p_ltfeb_ltf_user_identity_insert)

  insert dbo.d_ltfeb_ltf_user_identity(
             bk_hash,
             party_id,
             ltf_user_name,
             deleted_flag,
             p_ltfeb_ltf_user_identity_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         party_id,
         ltf_user_name,
         dv_deleted,
         p_ltfeb_ltf_user_identity_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_ltfeb_ltf_user_identity)
--Done!
end
