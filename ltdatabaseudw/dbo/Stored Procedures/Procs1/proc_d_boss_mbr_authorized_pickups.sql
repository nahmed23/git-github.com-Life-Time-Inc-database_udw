CREATE PROC [dbo].[proc_d_boss_mbr_authorized_pickups] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_boss_mbr_authorized_pickups)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_boss_mbr_authorized_pickups_insert') is not null drop table #p_boss_mbr_authorized_pickups_insert
create table dbo.#p_boss_mbr_authorized_pickups_insert with(distribution=hash(bk_hash), location=user_db) as
select p_boss_mbr_authorized_pickups.p_boss_mbr_authorized_pickups_id,
       p_boss_mbr_authorized_pickups.bk_hash
  from dbo.p_boss_mbr_authorized_pickups
 where p_boss_mbr_authorized_pickups.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_boss_mbr_authorized_pickups.dv_batch_id > @max_dv_batch_id
        or p_boss_mbr_authorized_pickups.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_boss_mbr_authorized_pickups.bk_hash,
       p_boss_mbr_authorized_pickups.mbr_authorized_pickups_id mbr_authorized_pickups_id,
       case when p_boss_mbr_authorized_pickups.bk_hash in('-997', '-998', '-999') then p_boss_mbr_authorized_pickups.bk_hash
            when s_boss_mbr_authorized_pickups.created_at is null then '-998'
         else convert(varchar, s_boss_mbr_authorized_pickups.created_at, 112)    end created_dim_date_key,
       case when p_boss_mbr_authorized_pickups.bk_hash in ('-997','-998','-999') then p_boss_mbr_authorized_pickups.bk_hash
        when s_boss_mbr_authorized_pickups.created_at is null then '-998'
        else '1' + replace(substring(convert(varchar,s_boss_mbr_authorized_pickups.created_at,114), 1, 5),':','') end created_dim_time_key,
       s_boss_mbr_authorized_pickups.cust_code mbr_authorized_pickups_cust_code,
       s_boss_mbr_authorized_pickups.mbr_code mbr_authorized_pickups_mbr_code,
       s_boss_mbr_authorized_pickups.notes mbr_authorized_pickups_notes,
       case when p_boss_mbr_authorized_pickups.bk_hash in('-997', '-998', '-999') then p_boss_mbr_authorized_pickups.bk_hash
            when s_boss_mbr_authorized_pickups.updated_at is null then '-998'
         else convert(varchar, s_boss_mbr_authorized_pickups.updated_at, 112)    end updated_dim_date_key,
       case when p_boss_mbr_authorized_pickups.bk_hash in ('-997','-998','-999') then p_boss_mbr_authorized_pickups.bk_hash
        when s_boss_mbr_authorized_pickups.updated_at is null then '-998'
        else '1' + replace(substring(convert(varchar,s_boss_mbr_authorized_pickups.updated_at,114), 1, 5),':','') end updated_dim_time_key,
       isnull(h_boss_mbr_authorized_pickups.dv_deleted,0) dv_deleted,
       p_boss_mbr_authorized_pickups.p_boss_mbr_authorized_pickups_id,
       p_boss_mbr_authorized_pickups.dv_batch_id,
       p_boss_mbr_authorized_pickups.dv_load_date_time,
       p_boss_mbr_authorized_pickups.dv_load_end_date_time
  from dbo.h_boss_mbr_authorized_pickups
  join dbo.p_boss_mbr_authorized_pickups
    on h_boss_mbr_authorized_pickups.bk_hash = p_boss_mbr_authorized_pickups.bk_hash
  join #p_boss_mbr_authorized_pickups_insert
    on p_boss_mbr_authorized_pickups.bk_hash = #p_boss_mbr_authorized_pickups_insert.bk_hash
   and p_boss_mbr_authorized_pickups.p_boss_mbr_authorized_pickups_id = #p_boss_mbr_authorized_pickups_insert.p_boss_mbr_authorized_pickups_id
  join dbo.s_boss_mbr_authorized_pickups
    on p_boss_mbr_authorized_pickups.bk_hash = s_boss_mbr_authorized_pickups.bk_hash
   and p_boss_mbr_authorized_pickups.s_boss_mbr_authorized_pickups_id = s_boss_mbr_authorized_pickups.s_boss_mbr_authorized_pickups_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_boss_mbr_authorized_pickups
   where d_boss_mbr_authorized_pickups.bk_hash in (select bk_hash from #p_boss_mbr_authorized_pickups_insert)

  insert dbo.d_boss_mbr_authorized_pickups(
             bk_hash,
             mbr_authorized_pickups_id,
             created_dim_date_key,
             created_dim_time_key,
             mbr_authorized_pickups_cust_code,
             mbr_authorized_pickups_mbr_code,
             mbr_authorized_pickups_notes,
             updated_dim_date_key,
             updated_dim_time_key,
             deleted_flag,
             p_boss_mbr_authorized_pickups_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         mbr_authorized_pickups_id,
         created_dim_date_key,
         created_dim_time_key,
         mbr_authorized_pickups_cust_code,
         mbr_authorized_pickups_mbr_code,
         mbr_authorized_pickups_notes,
         updated_dim_date_key,
         updated_dim_time_key,
         dv_deleted,
         p_boss_mbr_authorized_pickups_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_boss_mbr_authorized_pickups)
--Done!
end
