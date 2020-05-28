CREATE PROC [dbo].[proc_d_boss_attendance] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_boss_attendance)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_boss_attendance_insert') is not null drop table #p_boss_attendance_insert
create table dbo.#p_boss_attendance_insert with(distribution=hash(bk_hash), location=user_db) as
select p_boss_attendance.p_boss_attendance_id,
       p_boss_attendance.bk_hash
  from dbo.p_boss_attendance
 where p_boss_attendance.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_boss_attendance.dv_batch_id > @max_dv_batch_id
        or p_boss_attendance.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_boss_attendance.bk_hash,
       p_boss_attendance.reservation reservation_id,
       p_boss_attendance.cust_code cust_code,
       p_boss_attendance.mbr_code mbr_code,
       p_boss_attendance.attendance_date attendance_date,
       case when p_boss_attendance.bk_hash in ('-997', '-998', '-999') then p_boss_attendance.bk_hash
            when p_boss_attendance.attendance_date is null then '-998'
            else convert(char(8), p_boss_attendance.attendance_date, 112)
        end attendance_dim_date_key,
       case when s_boss_attendance.checked_in = 'Y' then 'Y'
            else 'N'
        end checked_in_flag,
       case when p_boss_attendance.bk_hash in ('-997', '-998', '-999') then p_boss_attendance.bk_hash
            when p_boss_attendance.reservation is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(p_boss_attendance.reservation as int) as varchar(500)),'z#@$k%&P'))),2)
        end dim_boss_reservation_key,
       h_boss_attendance.dv_deleted dv_deleted_flag,
       isnull(h_boss_attendance.dv_deleted,0) dv_deleted,
       p_boss_attendance.p_boss_attendance_id,
       p_boss_attendance.dv_batch_id,
       p_boss_attendance.dv_load_date_time,
       p_boss_attendance.dv_load_end_date_time
  from dbo.h_boss_attendance
  join dbo.p_boss_attendance
    on h_boss_attendance.bk_hash = p_boss_attendance.bk_hash
  join #p_boss_attendance_insert
    on p_boss_attendance.bk_hash = #p_boss_attendance_insert.bk_hash
   and p_boss_attendance.p_boss_attendance_id = #p_boss_attendance_insert.p_boss_attendance_id
  join dbo.l_boss_attendance
    on p_boss_attendance.bk_hash = l_boss_attendance.bk_hash
   and p_boss_attendance.l_boss_attendance_id = l_boss_attendance.l_boss_attendance_id
  join dbo.s_boss_attendance
    on p_boss_attendance.bk_hash = s_boss_attendance.bk_hash
   and p_boss_attendance.s_boss_attendance_id = s_boss_attendance.s_boss_attendance_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_boss_attendance
   where d_boss_attendance.bk_hash in (select bk_hash from #p_boss_attendance_insert)

  insert dbo.d_boss_attendance(
             bk_hash,
             reservation_id,
             cust_code,
             mbr_code,
             attendance_date,
             attendance_dim_date_key,
             checked_in_flag,
             dim_boss_reservation_key,
             dv_deleted_flag,
             deleted_flag,
             p_boss_attendance_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         reservation_id,
         cust_code,
         mbr_code,
         attendance_date,
         attendance_dim_date_key,
         checked_in_flag,
         dim_boss_reservation_key,
         dv_deleted_flag,
         dv_deleted,
         p_boss_attendance_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_boss_attendance)
--Done!
end
