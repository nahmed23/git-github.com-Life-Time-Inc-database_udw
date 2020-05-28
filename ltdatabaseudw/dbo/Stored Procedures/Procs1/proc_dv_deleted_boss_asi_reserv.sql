CREATE PROC [dbo].[proc_dv_deleted_boss_asi_reserv] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
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
if object_id('tempdb..#bk_hash') is not null drop table #bk_hash
create table #bk_hash with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select d_boss_asi_reserv.bk_hash, 
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
join d_boss_asi_reserv on deletes.dim_boss_reservation_key = d_boss_asi_reserv.dim_boss_reservation_key 

set @insert_date_time = getdate()
update h_boss_asi_reserv
   set dv_deleted = 1,
       dv_updated_date_time = @insert_date_time,
       dv_update_user = @user
  from #bk_hash
 where h_boss_asi_reserv.bk_hash = #bk_hash.bk_hash

--Insert all updated and new l_boss_asi_reserv records
set @insert_date_time = getdate()
insert into l_boss_asi_reserv (
       bk_hash,
       reservation,
       trainer_cust_code,
       upc_code,
       club,
       resource_id,
       link_to,
       interest_id,
       format_id,
       mms_product_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user,
       dv_deleted)
select l_boss_asi_reserv.bk_hash,
       l_boss_asi_reserv.reservation,
       l_boss_asi_reserv.trainer_cust_code,
       l_boss_asi_reserv.upc_code,
       l_boss_asi_reserv.club,
       l_boss_asi_reserv.resource_id,
       l_boss_asi_reserv.link_to,
       l_boss_asi_reserv.interest_id,
       l_boss_asi_reserv.format_id,
       l_boss_asi_reserv.mms_product_id,
       #bk_hash.dv_load_date_time,
       #bk_hash.dv_batch_id,
       l_boss_asi_reserv.dv_r_load_source_id,
       l_boss_asi_reserv.dv_hash,
       @insert_date_time,
       @user,
       1 --deleted
  from p_boss_asi_reserv
  join l_boss_asi_reserv
    on p_boss_asi_reserv.bk_hash = l_boss_asi_reserv.bk_hash
   and p_boss_asi_reserv.l_boss_asi_reserv_id = l_boss_asi_reserv.l_boss_asi_reserv_id
  join #bk_hash
    on p_boss_asi_reserv.bk_hash = #bk_hash.bk_hash
 where p_boss_asi_reserv.bk_hash in (select bk_hash from #bk_hash)
   and p_boss_asi_reserv.dv_load_end_date_time = 'dec 31, 9999'
   and isnull(l_boss_asi_reserv.dv_deleted,0) != 1

--Insert all updated and new s_boss_asi_reserv records
set @insert_date_time = getdate()
insert into s_boss_asi_reserv (
       bk_hash,
       reservation,
       trainer_mbr_code,
       reserve_type,
       start_date,
       end_date,
       session_id,
       program_id,
       def_price,
       instructor,
       billing_count,
       status,
       free_date,
       qoh,
       limit,
       recurring,
       color,
       shape,
       comment,
       resource,
       create_date,
       min_limit,
       non_mbr_price,
       ical_recur_rule,
       upc_desc,
       respect_holidays,
       origin,
       print_desc,
       day_plan_string,
       day_plan_ints,
       publish,
       web_register,
       target,
       class_expense,
       instructor_expense,
       age_low,
       age_high,
       web_purchase,
       payment_freq,
       last_modified,
       waiver_reqd,
       deposit_amt,
       payment_reqd_days,
       pre_assgn_instr_cnt,
       grace_days,
       continuous,
       allow_wait_list,
       use_for_LT_Bucks,
       cancel_dates,
       published_duration,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user,
       dv_deleted)
select s_boss_asi_reserv.bk_hash,
       s_boss_asi_reserv.reservation,
       s_boss_asi_reserv.trainer_mbr_code,
       s_boss_asi_reserv.reserve_type,
       s_boss_asi_reserv.start_date,
       s_boss_asi_reserv.end_date,
       s_boss_asi_reserv.session_id,
       s_boss_asi_reserv.program_id,
       s_boss_asi_reserv.def_price,
       s_boss_asi_reserv.instructor,
       s_boss_asi_reserv.billing_count,
       s_boss_asi_reserv.status,
       s_boss_asi_reserv.free_date,
       s_boss_asi_reserv.qoh,
       s_boss_asi_reserv.limit,
       s_boss_asi_reserv.recurring,
       s_boss_asi_reserv.color,
       s_boss_asi_reserv.shape,
       s_boss_asi_reserv.comment,
       s_boss_asi_reserv.resource,
       s_boss_asi_reserv.create_date,
       s_boss_asi_reserv.min_limit,
       s_boss_asi_reserv.non_mbr_price,
       s_boss_asi_reserv.ical_recur_rule,
       s_boss_asi_reserv.upc_desc,
       s_boss_asi_reserv.respect_holidays,
       s_boss_asi_reserv.origin,
       s_boss_asi_reserv.print_desc,
       s_boss_asi_reserv.day_plan_string,
       s_boss_asi_reserv.day_plan_ints,
       s_boss_asi_reserv.publish,
       s_boss_asi_reserv.web_register,
       s_boss_asi_reserv.target,
       s_boss_asi_reserv.class_expense,
       s_boss_asi_reserv.instructor_expense,
       s_boss_asi_reserv.age_low,
       s_boss_asi_reserv.age_high,
       s_boss_asi_reserv.web_purchase,
       s_boss_asi_reserv.payment_freq,
       s_boss_asi_reserv.last_modified,
       s_boss_asi_reserv.waiver_reqd,
       s_boss_asi_reserv.deposit_amt,
       s_boss_asi_reserv.payment_reqd_days,
       s_boss_asi_reserv.pre_assgn_instr_cnt,
       s_boss_asi_reserv.grace_days,
       s_boss_asi_reserv.continuous,
       s_boss_asi_reserv.allow_wait_list,
       s_boss_asi_reserv.use_for_LT_Bucks,
       s_boss_asi_reserv.cancel_dates,
       s_boss_asi_reserv.published_duration,
       #bk_hash.dv_load_date_time,
       #bk_hash.dv_batch_id,
       s_boss_asi_reserv.dv_r_load_source_id,
       s_boss_asi_reserv.dv_hash,
       @insert_date_time,
       @user,
       1 --deleted
  from p_boss_asi_reserv
  join s_boss_asi_reserv
    on p_boss_asi_reserv.bk_hash = s_boss_asi_reserv.bk_hash
   and p_boss_asi_reserv.s_boss_asi_reserv_id = s_boss_asi_reserv.s_boss_asi_reserv_id
  join #bk_hash
    on p_boss_asi_reserv.bk_hash = #bk_hash.bk_hash
 where p_boss_asi_reserv.bk_hash in (select bk_hash from #bk_hash)
   and p_boss_asi_reserv.dv_load_end_date_time = 'dec 31, 9999'
   and isnull(s_boss_asi_reserv.dv_deleted,0) != 1

end