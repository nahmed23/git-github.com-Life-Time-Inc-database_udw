CREATE PROC [dbo].[proc_d_ltfeb_party_role] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_ltfeb_party_role)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_ltfeb_party_role_insert') is not null drop table #p_ltfeb_party_role_insert
create table dbo.#p_ltfeb_party_role_insert with(distribution=hash(bk_hash), location=user_db) as
select p_ltfeb_party_role.p_ltfeb_party_role_id,
       p_ltfeb_party_role.bk_hash
  from dbo.p_ltfeb_party_role
 where p_ltfeb_party_role.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_ltfeb_party_role.dv_batch_id > @max_dv_batch_id
        or p_ltfeb_party_role.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_ltfeb_party_role.bk_hash,
       p_ltfeb_party_role.party_role_id party_role_id,
       l_ltfeb_party_role.headquarters_facility_id headquarters_facility_id,
       s_ltfeb_party_role.party_role_type party_role_type,
       l_ltfeb_party_role.pr_party_id pr_party_id,
       h_ltfeb_party_role.dv_deleted,
       p_ltfeb_party_role.p_ltfeb_party_role_id,
       p_ltfeb_party_role.dv_batch_id,
       p_ltfeb_party_role.dv_load_date_time,
       p_ltfeb_party_role.dv_load_end_date_time
  from dbo.h_ltfeb_party_role
  join dbo.p_ltfeb_party_role
    on h_ltfeb_party_role.bk_hash = p_ltfeb_party_role.bk_hash
  join #p_ltfeb_party_role_insert
    on p_ltfeb_party_role.bk_hash = #p_ltfeb_party_role_insert.bk_hash
   and p_ltfeb_party_role.p_ltfeb_party_role_id = #p_ltfeb_party_role_insert.p_ltfeb_party_role_id
  join dbo.l_ltfeb_party_role
    on p_ltfeb_party_role.bk_hash = l_ltfeb_party_role.bk_hash
   and p_ltfeb_party_role.l_ltfeb_party_role_id = l_ltfeb_party_role.l_ltfeb_party_role_id
  join dbo.s_ltfeb_party_role
    on p_ltfeb_party_role.bk_hash = s_ltfeb_party_role.bk_hash
   and p_ltfeb_party_role.s_ltfeb_party_role_id = s_ltfeb_party_role.s_ltfeb_party_role_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_ltfeb_party_role
   where d_ltfeb_party_role.bk_hash in (select bk_hash from #p_ltfeb_party_role_insert)

  insert dbo.d_ltfeb_party_role(
             bk_hash,
             party_role_id,
             headquarters_facility_id,
             party_role_type,
             pr_party_id,
             deleted_flag,
             p_ltfeb_party_role_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         party_role_id,
         headquarters_facility_id,
         party_role_type,
         pr_party_id,
         dv_deleted,
         p_ltfeb_party_role_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_ltfeb_party_role)
--Done!
end
