CREATE PROC [dbo].[proc_d_boss_evt_registration_processes] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_boss_evt_registration_processes)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_boss_evt_registration_processes_insert') is not null drop table #p_boss_evt_registration_processes_insert
create table dbo.#p_boss_evt_registration_processes_insert with(distribution=hash(bk_hash), location=user_db) as
select p_boss_evt_registration_processes.p_boss_evt_registration_processes_id,
       p_boss_evt_registration_processes.bk_hash
  from dbo.p_boss_evt_registration_processes
 where p_boss_evt_registration_processes.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_boss_evt_registration_processes.dv_batch_id > @max_dv_batch_id
        or p_boss_evt_registration_processes.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_boss_evt_registration_processes.bk_hash,
       p_boss_evt_registration_processes.evt_registration_processes_id evt_registration_processes_id,
       case when p_boss_evt_registration_processes.bk_hash in('-997', '-998', '-999') then p_boss_evt_registration_processes.bk_hash
           when s_boss_evt_registration_processes.created_at is null then '-998'
        else convert(varchar, s_boss_evt_registration_processes.created_at, 112)    end created_dim_date_key,
       case when p_boss_evt_registration_processes.bk_hash in ('-997','-998','-999') then p_boss_evt_registration_processes.bk_hash
       when s_boss_evt_registration_processes.created_at is null then '-998'
       else '1' + replace(substring(convert(varchar,s_boss_evt_registration_processes.created_at,114), 1, 5),':','') end created_dim_time_key,
       case when p_boss_evt_registration_processes.bk_hash in('-997', '-998', '-999') then p_boss_evt_registration_processes.bk_hash
           when l_boss_evt_registration_processes.reservation_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_boss_evt_registration_processes.reservation_id as int) as varchar(500)),'z#@$k%&P'))),2)   end d_boss_asi_reserv_bk_hash,
       case when p_boss_evt_registration_processes.bk_hash in('-997', '-998', '-999') then p_boss_evt_registration_processes.bk_hash
           when l_boss_evt_registration_processes.user_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_boss_evt_registration_processes.user_id as int) as varchar(500)),'z#@$k%&P'))),2)   end d_lt_bucks_users_bk_hash,
       case when p_boss_evt_registration_processes.bk_hash in('-997', '-998', '-999') then p_boss_evt_registration_processes.bk_hash
           when l_boss_evt_registration_processes.member_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_boss_evt_registration_processes.member_id as int) as varchar(500)),'z#@$k%&P'))),2)   end d_mms_member_bk_hash,
       s_boss_evt_registration_processes.state evt_registration_processes_state,
       case when p_boss_evt_registration_processes.bk_hash in('-997', '-998', '-999') then p_boss_evt_registration_processes.bk_hash
           when s_boss_evt_registration_processes.expires_at is null then '-998'
        else convert(varchar, s_boss_evt_registration_processes.expires_at, 112)    end expires_dim_date_key,
       case when p_boss_evt_registration_processes.bk_hash in ('-997','-998','-999') then p_boss_evt_registration_processes.bk_hash
       when s_boss_evt_registration_processes.expires_at is null then '-998'
       else '1' + replace(substring(convert(varchar,s_boss_evt_registration_processes.expires_at,114), 1, 5),':','') end expires_dim_time_key,
       l_boss_evt_registration_processes.member_id member_id,
       l_boss_evt_registration_processes.reservation_id reservation_id,
       case when s_boss_evt_registration_processes.roster_only = 1 then 'Y'        else 'N'  end roster_flag,
       l_boss_evt_registration_processes.roster_id roster_id,
       case when p_boss_evt_registration_processes.bk_hash in('-997', '-998', '-999') then p_boss_evt_registration_processes.bk_hash
           when s_boss_evt_registration_processes.updated_at is null then '-998'
        else convert(varchar, s_boss_evt_registration_processes.updated_at, 112)    end updated_dim_date_key,
       case when p_boss_evt_registration_processes.bk_hash in ('-997','-998','-999') then p_boss_evt_registration_processes.bk_hash
       when s_boss_evt_registration_processes.updated_at is null then '-998'
       else '1' + replace(substring(convert(varchar,s_boss_evt_registration_processes.updated_at,114), 1, 5),':','') end updated_dim_time_key,
       l_boss_evt_registration_processes.user_id user_id,
       isnull(h_boss_evt_registration_processes.dv_deleted,0) dv_deleted,
       p_boss_evt_registration_processes.p_boss_evt_registration_processes_id,
       p_boss_evt_registration_processes.dv_batch_id,
       p_boss_evt_registration_processes.dv_load_date_time,
       p_boss_evt_registration_processes.dv_load_end_date_time
  from dbo.h_boss_evt_registration_processes
  join dbo.p_boss_evt_registration_processes
    on h_boss_evt_registration_processes.bk_hash = p_boss_evt_registration_processes.bk_hash
  join #p_boss_evt_registration_processes_insert
    on p_boss_evt_registration_processes.bk_hash = #p_boss_evt_registration_processes_insert.bk_hash
   and p_boss_evt_registration_processes.p_boss_evt_registration_processes_id = #p_boss_evt_registration_processes_insert.p_boss_evt_registration_processes_id
  join dbo.l_boss_evt_registration_processes
    on p_boss_evt_registration_processes.bk_hash = l_boss_evt_registration_processes.bk_hash
   and p_boss_evt_registration_processes.l_boss_evt_registration_processes_id = l_boss_evt_registration_processes.l_boss_evt_registration_processes_id
  join dbo.s_boss_evt_registration_processes
    on p_boss_evt_registration_processes.bk_hash = s_boss_evt_registration_processes.bk_hash
   and p_boss_evt_registration_processes.s_boss_evt_registration_processes_id = s_boss_evt_registration_processes.s_boss_evt_registration_processes_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_boss_evt_registration_processes
   where d_boss_evt_registration_processes.bk_hash in (select bk_hash from #p_boss_evt_registration_processes_insert)

  insert dbo.d_boss_evt_registration_processes(
             bk_hash,
             evt_registration_processes_id,
             created_dim_date_key,
             created_dim_time_key,
             d_boss_asi_reserv_bk_hash,
             d_lt_bucks_users_bk_hash,
             d_mms_member_bk_hash,
             evt_registration_processes_state,
             expires_dim_date_key,
             expires_dim_time_key,
             member_id,
             reservation_id,
             roster_flag,
             roster_id,
             updated_dim_date_key,
             updated_dim_time_key,
             user_id,
             deleted_flag,
             p_boss_evt_registration_processes_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         evt_registration_processes_id,
         created_dim_date_key,
         created_dim_time_key,
         d_boss_asi_reserv_bk_hash,
         d_lt_bucks_users_bk_hash,
         d_mms_member_bk_hash,
         evt_registration_processes_state,
         expires_dim_date_key,
         expires_dim_time_key,
         member_id,
         reservation_id,
         roster_flag,
         roster_id,
         updated_dim_date_key,
         updated_dim_time_key,
         user_id,
         dv_deleted,
         p_boss_evt_registration_processes_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_boss_evt_registration_processes)
--Done!
end
