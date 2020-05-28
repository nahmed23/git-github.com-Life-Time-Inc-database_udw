CREATE PROC [dbo].[proc_dv_deleted_boss_asi_player] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
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
select d_boss_asi_player.bk_hash, 
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
join d_boss_asi_player on deletes.dim_boss_reservation_key = d_boss_asi_player.dim_boss_reservation_key 

set @insert_date_time = getdate()
update h_boss_asi_player
   set dv_deleted = 1,
       dv_updated_date_time = @insert_date_time,
       dv_update_user = @user
  from #bk_hash
 where h_boss_asi_player.bk_hash = #bk_hash.bk_hash

--Insert all updated and new l_boss_asi_player records
set @insert_date_time = getdate()
insert into l_boss_asi_player (
       bk_hash,
       reservation,
       mbr_code,
       employee_id,
       asi_player_id,
       contact_id,
       mbrship_type_id,
       mms_trans_id,
       recurrence_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user,
       dv_deleted)
select l_boss_asi_player.bk_hash,
       l_boss_asi_player.reservation,
       l_boss_asi_player.mbr_code,
       l_boss_asi_player.employee_id,
       l_boss_asi_player.asi_player_id,
       l_boss_asi_player.contact_id,
       l_boss_asi_player.mbrship_type_id,
       l_boss_asi_player.mms_trans_id,
       l_boss_asi_player.recurrence_id,
       #bk_hash.dv_load_date_time,
       #bk_hash.dv_batch_id,
       l_boss_asi_player.dv_r_load_source_id,
       l_boss_asi_player.dv_hash,
       @insert_date_time,
       @user,
       1 --deleted
  from p_boss_asi_player
  join l_boss_asi_player
    on p_boss_asi_player.bk_hash = l_boss_asi_player.bk_hash
   and p_boss_asi_player.l_boss_asi_player_id = l_boss_asi_player.l_boss_asi_player_id
  join #bk_hash
    on p_boss_asi_player.bk_hash = #bk_hash.bk_hash
 where p_boss_asi_player.bk_hash in (select bk_hash from #bk_hash)
   and p_boss_asi_player.dv_load_end_date_time = 'dec 31, 9999'
   and isnull(l_boss_asi_player.dv_deleted,0) != 1

--Insert all updated and new s_boss_asi_player records
set @insert_date_time = getdate()
insert into s_boss_asi_player (
       bk_hash,
       date_used,
       cust_code,
       sequence,
       price,
       tax_amt,
       paid,
       trans,
       instructor,
       comm_paid,
       phone,
       player_name,
       can_charge,
       checked_in,
       email,
       cancel_date,
       notes,
       status,
       start_date,
       origin,
       dob,
       mbr_type,
       house_acct,
       created_at,
       asi_player_id,
       balance_due,
       rostered_by,
       cust_type,
       updated_at,
       pmt_start,
       pmt_end,
       check_in_date,
       last_paid_date,
       mms_swipe,
       package_balance,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user,
       dv_deleted)
select s_boss_asi_player.bk_hash,
       s_boss_asi_player.date_used,
       s_boss_asi_player.cust_code,
       s_boss_asi_player.sequence,
       s_boss_asi_player.price,
       s_boss_asi_player.tax_amt,
       s_boss_asi_player.paid,
       s_boss_asi_player.trans,
       s_boss_asi_player.instructor,
       s_boss_asi_player.comm_paid,
       s_boss_asi_player.phone,
       s_boss_asi_player.player_name,
       s_boss_asi_player.can_charge,
       s_boss_asi_player.checked_in,
       s_boss_asi_player.email,
       s_boss_asi_player.cancel_date,
       s_boss_asi_player.notes,
       s_boss_asi_player.status,
       s_boss_asi_player.start_date,
       s_boss_asi_player.origin,
       s_boss_asi_player.dob,
       s_boss_asi_player.mbr_type,
       s_boss_asi_player.house_acct,
       s_boss_asi_player.created_at,
       s_boss_asi_player.asi_player_id,
       s_boss_asi_player.balance_due,
       s_boss_asi_player.rostered_by,
       s_boss_asi_player.cust_type,
       s_boss_asi_player.updated_at,
       s_boss_asi_player.pmt_start,
       s_boss_asi_player.pmt_end,
       s_boss_asi_player.check_in_date,
       s_boss_asi_player.last_paid_date,
       s_boss_asi_player.mms_swipe,
       s_boss_asi_player.package_balance,
       #bk_hash.dv_load_date_time,
       #bk_hash.dv_batch_id,
       s_boss_asi_player.dv_r_load_source_id,
       s_boss_asi_player.dv_hash,
       @insert_date_time,
       @user,
       1 --deleted
  from p_boss_asi_player
  join s_boss_asi_player
    on p_boss_asi_player.bk_hash = s_boss_asi_player.bk_hash
   and p_boss_asi_player.s_boss_asi_player_id = s_boss_asi_player.s_boss_asi_player_id
  join #bk_hash
    on p_boss_asi_player.bk_hash = #bk_hash.bk_hash
 where p_boss_asi_player.bk_hash in (select bk_hash from #bk_hash)
   and p_boss_asi_player.dv_load_end_date_time = 'dec 31, 9999'
   and isnull(s_boss_asi_player.dv_deleted,0) != 1

end