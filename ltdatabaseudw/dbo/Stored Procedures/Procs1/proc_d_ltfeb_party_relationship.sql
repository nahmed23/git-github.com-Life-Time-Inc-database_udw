CREATE PROC [dbo].[proc_d_ltfeb_party_relationship] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_ltfeb_party_relationship)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_ltfeb_party_relationship_insert') is not null drop table #p_ltfeb_party_relationship_insert
create table dbo.#p_ltfeb_party_relationship_insert with(distribution=hash(bk_hash), location=user_db) as
select p_ltfeb_party_relationship.p_ltfeb_party_relationship_id,
       p_ltfeb_party_relationship.bk_hash
  from dbo.p_ltfeb_party_relationship
 where p_ltfeb_party_relationship.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_ltfeb_party_relationship.dv_batch_id > @max_dv_batch_id
        or p_ltfeb_party_relationship.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_ltfeb_party_relationship.bk_hash,
       p_ltfeb_party_relationship.party_relationship_id party_relationship_id,
       l_ltfeb_party_relationship.from_party_role_id from_party_role_id,
       l_ltfeb_party_relationship.to_party_role_id to_party_role_id,
       case when p_ltfeb_party_relationship.bk_hash in('-997', '-998', '-999') then p_ltfeb_party_relationship.bk_hash     when s_ltfeb_party_relationship.from_date_in_effect is null then '-998' 	else convert(varchar, s_ltfeb_party_relationship.from_date_in_effect, 112)  end effective_from_dim_date_key,
       case when p_ltfeb_party_relationship.bk_hash in('-997', '-998', '-999') then p_ltfeb_party_relationship.bk_hash     when s_ltfeb_party_relationship.party_relationship_thru_date is null then '-998' 	else convert(varchar, s_ltfeb_party_relationship.party_relationship_thru_date, 112)  end effective_to_dim_date_key,
       h_ltfeb_party_relationship.dv_deleted,
       p_ltfeb_party_relationship.p_ltfeb_party_relationship_id,
       p_ltfeb_party_relationship.dv_batch_id,
       p_ltfeb_party_relationship.dv_load_date_time,
       p_ltfeb_party_relationship.dv_load_end_date_time
  from dbo.h_ltfeb_party_relationship
  join dbo.p_ltfeb_party_relationship
    on h_ltfeb_party_relationship.bk_hash = p_ltfeb_party_relationship.bk_hash
  join #p_ltfeb_party_relationship_insert
    on p_ltfeb_party_relationship.bk_hash = #p_ltfeb_party_relationship_insert.bk_hash
   and p_ltfeb_party_relationship.p_ltfeb_party_relationship_id = #p_ltfeb_party_relationship_insert.p_ltfeb_party_relationship_id
  join dbo.l_ltfeb_party_relationship
    on p_ltfeb_party_relationship.bk_hash = l_ltfeb_party_relationship.bk_hash
   and p_ltfeb_party_relationship.l_ltfeb_party_relationship_id = l_ltfeb_party_relationship.l_ltfeb_party_relationship_id
  join dbo.s_ltfeb_party_relationship
    on p_ltfeb_party_relationship.bk_hash = s_ltfeb_party_relationship.bk_hash
   and p_ltfeb_party_relationship.s_ltfeb_party_relationship_id = s_ltfeb_party_relationship.s_ltfeb_party_relationship_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_ltfeb_party_relationship
   where d_ltfeb_party_relationship.bk_hash in (select bk_hash from #p_ltfeb_party_relationship_insert)

  insert dbo.d_ltfeb_party_relationship(
             bk_hash,
             party_relationship_id,
             from_party_role_id,
             to_party_role_id,
             effective_from_dim_date_key,
             effective_to_dim_date_key,
             p_ltfeb_party_relationship_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         party_relationship_id,
         from_party_role_id,
         to_party_role_id,
         effective_from_dim_date_key,
         effective_to_dim_date_key,
         p_ltfeb_party_relationship_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_ltfeb_party_relationship)
--Done!
end
