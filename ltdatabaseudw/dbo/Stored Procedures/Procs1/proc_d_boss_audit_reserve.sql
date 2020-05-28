CREATE PROC [dbo].[proc_d_boss_audit_reserve] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_boss_audit_reserve)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_boss_audit_reserve_insert') is not null drop table #p_boss_audit_reserve_insert
create table dbo.#p_boss_audit_reserve_insert with(distribution=hash(bk_hash), location=user_db) as
select p_boss_audit_reserve.p_boss_audit_reserve_id,
       p_boss_audit_reserve.bk_hash
  from dbo.p_boss_audit_reserve
 where p_boss_audit_reserve.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_boss_audit_reserve.dv_batch_id > @max_dv_batch_id
        or p_boss_audit_reserve.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_boss_audit_reserve.bk_hash,
       p_boss_audit_reserve.bk_hash dim_boss_audit_reserve_key,
       p_boss_audit_reserve.audit_reserve_id audit_reserve_id,
       s_boss_audit_reserve.audit_performed audit_performed,
       isnull(s_boss_audit_reserve.audit_type,'') audit_type,
       case when p_boss_audit_reserve.bk_hash in('-997', '-998', '-999') then p_boss_audit_reserve.bk_hash
           when s_boss_audit_reserve.create_date is null then '-998'
       	else convert(varchar, s_boss_audit_reserve.create_date, 112) 
       end created_date_key,
       case when p_boss_audit_reserve.bk_hash in('-997', '-998', '-999') then p_boss_audit_reserve.bk_hash
           when s_boss_audit_reserve.create_date is null then '-998'
       	else '1' + replace(substring(convert(varchar,s_boss_audit_reserve.create_date,114), 1, 5),':','')
       end created_time_key,
       case when p_boss_audit_reserve.bk_hash in ('-997', '-998', '-999') then p_boss_audit_reserve.bk_hash
           when l_boss_audit_reserve.reservation is null then '-998'
           else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_boss_audit_reserve.reservation as int) as varchar(500)),'z#@$k%&P'))),2) 
       end dim_boss_reservation_key,
       case when p_boss_audit_reserve.bk_hash in ('-997', '-998', '-999') then p_boss_audit_reserve.bk_hash
           when s_boss_audit_reserve.club is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(s_boss_audit_reserve.club as int) as varchar(500)),'z#@$k%&P'))),2) 
       end dim_club_key,
       isnull(s_boss_audit_reserve.reservation_type,'') reservation_type,
       s_boss_audit_reserve.start_date start_date_time,
       isnull(s_boss_audit_reserve.upc_desc,'') upc_desc,
       isnull(h_boss_audit_reserve.dv_deleted,0) dv_deleted,
       p_boss_audit_reserve.p_boss_audit_reserve_id,
       p_boss_audit_reserve.dv_batch_id,
       p_boss_audit_reserve.dv_load_date_time,
       p_boss_audit_reserve.dv_load_end_date_time
  from dbo.h_boss_audit_reserve
  join dbo.p_boss_audit_reserve
    on h_boss_audit_reserve.bk_hash = p_boss_audit_reserve.bk_hash
  join #p_boss_audit_reserve_insert
    on p_boss_audit_reserve.bk_hash = #p_boss_audit_reserve_insert.bk_hash
   and p_boss_audit_reserve.p_boss_audit_reserve_id = #p_boss_audit_reserve_insert.p_boss_audit_reserve_id
  join dbo.l_boss_audit_reserve
    on p_boss_audit_reserve.bk_hash = l_boss_audit_reserve.bk_hash
   and p_boss_audit_reserve.l_boss_audit_reserve_id = l_boss_audit_reserve.l_boss_audit_reserve_id
  join dbo.s_boss_audit_reserve
    on p_boss_audit_reserve.bk_hash = s_boss_audit_reserve.bk_hash
   and p_boss_audit_reserve.s_boss_audit_reserve_id = s_boss_audit_reserve.s_boss_audit_reserve_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_boss_audit_reserve
   where d_boss_audit_reserve.bk_hash in (select bk_hash from #p_boss_audit_reserve_insert)

  insert dbo.d_boss_audit_reserve(
             bk_hash,
             dim_boss_audit_reserve_key,
             audit_reserve_id,
             audit_performed,
             audit_type,
             created_date_key,
             created_time_key,
             dim_boss_reservation_key,
             dim_club_key,
             reservation_type,
             start_date_time,
             upc_desc,
             deleted_flag,
             p_boss_audit_reserve_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_boss_audit_reserve_key,
         audit_reserve_id,
         audit_performed,
         audit_type,
         created_date_key,
         created_time_key,
         dim_boss_reservation_key,
         dim_club_key,
         reservation_type,
         start_date_time,
         upc_desc,
         dv_deleted,
         p_boss_audit_reserve_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_boss_audit_reserve)
--Done!
end
