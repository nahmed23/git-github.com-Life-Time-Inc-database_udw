CREATE PROC [dbo].[proc_d_boss_asi_available] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_boss_asi_available)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_boss_asi_available_insert') is not null drop table #p_boss_asi_available_insert
create table dbo.#p_boss_asi_available_insert with(distribution=hash(bk_hash), location=user_db) as
select p_boss_asi_available.p_boss_asi_available_id,
       p_boss_asi_available.bk_hash
  from dbo.p_boss_asi_available
 where p_boss_asi_available.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_boss_asi_available.dv_batch_id > @max_dv_batch_id
        or p_boss_asi_available.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_boss_asi_available.bk_hash,
       p_boss_asi_available.club club,
       p_boss_asi_available.resource_id resource_id,
       p_boss_asi_available.start_time start_time,
       case when p_boss_asi_available.bk_hash in('-997', '-998', '-999') then p_boss_asi_available.bk_hash
           when s_boss_asi_available.club is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(s_boss_asi_available.club as int) as varchar(500)),'z#@$k%&P'))),2)   end club_d_boss_asi_club_res_bk_hash,
       case when p_boss_asi_available.bk_hash in('-997', '-998', '-999') then p_boss_asi_available.bk_hash
           when s_boss_asi_available.end_time is null then '-998'
       	when  convert(varchar, s_boss_asi_available.end_time, 112) > '20991231' then '99991231'
       	when convert(varchar, s_boss_asi_available.end_time, 112)< '19000101' then '19000101'
        else convert(varchar, s_boss_asi_available.end_time, 112)    end end_dim_date_key,
       case when p_boss_asi_available.bk_hash in ('-997','-998','-999') then p_boss_asi_available.bk_hash
       when s_boss_asi_available.end_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_boss_asi_available.end_time,114), 1, 5),':','') end end_dim_time_key,
       s_boss_asi_available.end_time end_time,
       case when p_boss_asi_available.bk_hash in('-997', '-998', '-999') then p_boss_asi_available.bk_hash
           when s_boss_asi_available.resource_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(s_boss_asi_available.resource_id as int) as varchar(500)),'z#@$k%&P'))),2)   end resource_d_boss_asi_club_res_bk_hash,
       s_boss_asi_available.schedule_type schedule_type,
       case when p_boss_asi_available.bk_hash in('-997', '-998', '-999') then p_boss_asi_available.bk_hash
           when s_boss_asi_available.start_time is null then '-998'
       	when  convert(varchar, s_boss_asi_available.start_time, 112) > '20991231' then '99991231'
       	when convert(varchar, s_boss_asi_available.start_time, 112)< '19000101' then '19000101'
        else convert(varchar, s_boss_asi_available.start_time, 112)    end start_dim_date_key,
       case when p_boss_asi_available.bk_hash in ('-997','-998','-999') then p_boss_asi_available.bk_hash
       when s_boss_asi_available.start_time is null then '-998'
       else '1' + replace(substring(convert(varchar,s_boss_asi_available.start_time,114), 1, 5),':','') end start_dim_time_key,
       isnull(h_boss_asi_available.dv_deleted,0) dv_deleted,
       p_boss_asi_available.p_boss_asi_available_id,
       p_boss_asi_available.dv_batch_id,
       p_boss_asi_available.dv_load_date_time,
       p_boss_asi_available.dv_load_end_date_time
  from dbo.h_boss_asi_available
  join dbo.p_boss_asi_available
    on h_boss_asi_available.bk_hash = p_boss_asi_available.bk_hash
  join #p_boss_asi_available_insert
    on p_boss_asi_available.bk_hash = #p_boss_asi_available_insert.bk_hash
   and p_boss_asi_available.p_boss_asi_available_id = #p_boss_asi_available_insert.p_boss_asi_available_id
  join dbo.l_boss_asi_available
    on p_boss_asi_available.bk_hash = l_boss_asi_available.bk_hash
   and p_boss_asi_available.l_boss_asi_available_id = l_boss_asi_available.l_boss_asi_available_id
  join dbo.s_boss_asi_available
    on p_boss_asi_available.bk_hash = s_boss_asi_available.bk_hash
   and p_boss_asi_available.s_boss_asi_available_id = s_boss_asi_available.s_boss_asi_available_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_boss_asi_available
   where d_boss_asi_available.bk_hash in (select bk_hash from #p_boss_asi_available_insert)

  insert dbo.d_boss_asi_available(
             bk_hash,
             club,
             resource_id,
             start_time,
             club_d_boss_asi_club_res_bk_hash,
             end_dim_date_key,
             end_dim_time_key,
             end_time,
             resource_d_boss_asi_club_res_bk_hash,
             schedule_type,
             start_dim_date_key,
             start_dim_time_key,
             deleted_flag,
             p_boss_asi_available_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         club,
         resource_id,
         start_time,
         club_d_boss_asi_club_res_bk_hash,
         end_dim_date_key,
         end_dim_time_key,
         end_time,
         resource_d_boss_asi_club_res_bk_hash,
         schedule_type,
         start_dim_date_key,
         start_dim_time_key,
         dv_deleted,
         p_boss_asi_available_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_boss_asi_available)
--Done!
end
