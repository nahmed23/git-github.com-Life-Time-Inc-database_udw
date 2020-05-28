CREATE PROC [dbo].[proc_dv_deleted_boss_attendance] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)
declare @insert_date_time datetime
declare @user varchar(500) = suser_sname()

--THIS LOGIC (#bk_hash) IS ONLY A PLACEHOLDER AS AN EXAMPLE
--It needs to be manually updated for each individual object
--More logic than a simple query may be required, but the end result should be a #bk_hash table populated with bk_hashes, deleted times, and deleted batchids
if object_id('tempdb..#bk_hash') is not null drop table #bk_hash
create table #bk_hash with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select d_boss_attendance.bk_hash, 
       deletes.dv_load_date_time, 
	   deletes.dv_batch_id
from (
select dim_boss_reservation_key,
       dv_load_date_time,
       dv_batch_id
  from d_boss_audit_reserve
 where dv_batch_id >= @current_dv_batch_id
   and audit_type = 'DELETE'
   ) deletes
join d_boss_attendance on deletes.dim_boss_reservation_key = d_boss_attendance.dim_boss_reservation_key 

set @insert_date_time = getdate()
update h_boss_attendance
   set dv_deleted = 1,
       dv_updated_date_time = @insert_date_time,
       dv_update_user = @user
  from #bk_hash
 where h_boss_attendance.bk_hash = #bk_hash.bk_hash

--Insert all updated and new l_boss_attendance records
set @insert_date_time = getdate()
insert into l_boss_attendance (
       bk_hash,
       reservation,
       attendance_date,
       cust_code,
       mbr_code,
       employee_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user,
       dv_deleted)
select l_boss_attendance.bk_hash,
       l_boss_attendance.reservation,
       l_boss_attendance.attendance_date,
       l_boss_attendance.cust_code,
       l_boss_attendance.mbr_code,
       l_boss_attendance.employee_id,
       #bk_hash.dv_load_date_time,
       #bk_hash.dv_batch_id,
       l_boss_attendance.dv_r_load_source_id,
       l_boss_attendance.dv_hash,
       @insert_date_time,
       @user,
       1 --deleted
  from p_boss_attendance
  join l_boss_attendance
    on p_boss_attendance.bk_hash = l_boss_attendance.bk_hash
   and p_boss_attendance.l_boss_attendance_id = l_boss_attendance.l_boss_attendance_id
  join #bk_hash
    on p_boss_attendance.bk_hash = #bk_hash.bk_hash
 where p_boss_attendance.bk_hash in (select bk_hash from #bk_hash)
   and p_boss_attendance.dv_load_end_date_time = 'dec 31, 9999'
   and isnull(l_boss_attendance.dv_deleted,0) != 1

--Insert all updated and new s_boss_attendance records
set @insert_date_time = getdate()
insert into s_boss_attendance (
       bk_hash,
       reservation,
       attendance_date,
       cust_code,
       mbr_code,
       checked_in,
       comment,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user,
       dv_deleted)
select s_boss_attendance.bk_hash,
       s_boss_attendance.reservation,
       s_boss_attendance.attendance_date,
       s_boss_attendance.cust_code,
       s_boss_attendance.mbr_code,
       s_boss_attendance.checked_in,
       s_boss_attendance.comment,
       #bk_hash.dv_load_date_time,
       #bk_hash.dv_batch_id,
       s_boss_attendance.dv_r_load_source_id,
       s_boss_attendance.dv_hash,
       @insert_date_time,
       @user,
       1 --deleted
  from p_boss_attendance
  join s_boss_attendance
    on p_boss_attendance.bk_hash = s_boss_attendance.bk_hash
   and p_boss_attendance.s_boss_attendance_id = s_boss_attendance.s_boss_attendance_id
  join #bk_hash
    on p_boss_attendance.bk_hash = #bk_hash.bk_hash
 where p_boss_attendance.bk_hash in (select bk_hash from #bk_hash)
   and p_boss_attendance.dv_load_end_date_time = 'dec 31, 9999'
   and isnull(s_boss_attendance.dv_deleted,0) != 1

end