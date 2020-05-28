CREATE PROC [dbo].[proc_d_boss_participation] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_boss_participation)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_boss_participation_insert') is not null drop table #p_boss_participation_insert
create table dbo.#p_boss_participation_insert with(distribution=hash(bk_hash), location=user_db) as
select p_boss_participation.p_boss_participation_id,
       p_boss_participation.bk_hash
  from dbo.p_boss_participation
 where p_boss_participation.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_boss_participation.dv_batch_id > @max_dv_batch_id
        or p_boss_participation.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_boss_participation.bk_hash,
       p_boss_participation.participation_id participation_id,
       case when p_boss_participation.bk_hash in('-997', '-998', '-999') then p_boss_participation.bk_hash
           when s_boss_participation.created_at is null then '-998'
       	else convert(varchar, s_boss_participation.created_at, 112) 
       end created_dim_date_key,
       case when p_boss_participation.bk_hash in ('-997', '-998', '-999') then p_boss_participation.bk_hash
           when l_boss_participation.reservation is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_boss_participation.reservation as int) as varchar(500)),'z#@$k%&P'))),2) end dim_boss_reservation_key,
       h_boss_participation.dv_deleted dv_deleted_flag,
       s_boss_participation.mod_count mod_count,
       s_boss_participation.no_non_mbr number_of_non_mbr,
       s_boss_participation.no_participants number_of_participants,
       case when p_boss_participation.bk_hash in('-997', '-998', '-999') then p_boss_participation.bk_hash
           when s_boss_participation.participation_date is null then '-998'
       	else convert(varchar, s_boss_participation.participation_date, 112) 
       end participation_dim_date_key,
       isnull(h_boss_participation.dv_deleted,0) dv_deleted,
       p_boss_participation.p_boss_participation_id,
       p_boss_participation.dv_batch_id,
       p_boss_participation.dv_load_date_time,
       p_boss_participation.dv_load_end_date_time
  from dbo.h_boss_participation
  join dbo.p_boss_participation
    on h_boss_participation.bk_hash = p_boss_participation.bk_hash
  join #p_boss_participation_insert
    on p_boss_participation.bk_hash = #p_boss_participation_insert.bk_hash
   and p_boss_participation.p_boss_participation_id = #p_boss_participation_insert.p_boss_participation_id
  join dbo.l_boss_participation
    on p_boss_participation.bk_hash = l_boss_participation.bk_hash
   and p_boss_participation.l_boss_participation_id = l_boss_participation.l_boss_participation_id
  join dbo.s_boss_participation
    on p_boss_participation.bk_hash = s_boss_participation.bk_hash
   and p_boss_participation.s_boss_participation_id = s_boss_participation.s_boss_participation_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_boss_participation
   where d_boss_participation.bk_hash in (select bk_hash from #p_boss_participation_insert)

  insert dbo.d_boss_participation(
             bk_hash,
             participation_id,
             created_dim_date_key,
             dim_boss_reservation_key,
             dv_deleted_flag,
             mod_count,
             number_of_non_mbr,
             number_of_participants,
             participation_dim_date_key,
             deleted_flag,
             p_boss_participation_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         participation_id,
         created_dim_date_key,
         dim_boss_reservation_key,
         dv_deleted_flag,
         mod_count,
         number_of_non_mbr,
         number_of_participants,
         participation_dim_date_key,
         dv_deleted,
         p_boss_participation_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_boss_participation)
--Done!
end
