CREATE PROC [dbo].[proc_dv_deleted_boss_asi_res_inst] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
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
create table #bk_hash with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select d_boss_asi_res_inst.bk_hash, 
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
join d_boss_asi_res_inst on deletes.dim_boss_reservation_key = d_boss_asi_res_inst.dim_boss_reservation_key 

set @insert_date_time = getdate()
update h_boss_asi_res_inst
   set dv_deleted = 1,
       dv_updated_date_time = @insert_date_time,
       dv_update_user = @user
  from #bk_hash
 where h_boss_asi_res_inst.bk_hash = #bk_hash.bk_hash

--Insert all updated and new l_boss_asi_res_inst records
set @insert_date_time = getdate()
insert into l_boss_asi_res_inst (
       bk_hash,
       reservation,
       instructor_id,
       asi_res_inst_id,
       employee_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user,
       dv_deleted)
select l_boss_asi_res_inst.bk_hash,
       l_boss_asi_res_inst.reservation,
       l_boss_asi_res_inst.instructor_id,
       l_boss_asi_res_inst.asi_res_inst_id,
       l_boss_asi_res_inst.employee_id,
       #bk_hash.dv_load_date_time,
       #bk_hash.dv_batch_id,
       l_boss_asi_res_inst.dv_r_load_source_id,
       l_boss_asi_res_inst.dv_hash,
       @insert_date_time,
       @user,
       1 --deleted
  from p_boss_asi_res_inst
  join l_boss_asi_res_inst
    on p_boss_asi_res_inst.bk_hash = l_boss_asi_res_inst.bk_hash
   and p_boss_asi_res_inst.l_boss_asi_res_inst_id = l_boss_asi_res_inst.l_boss_asi_res_inst_id
  join #bk_hash
    on p_boss_asi_res_inst.bk_hash = #bk_hash.bk_hash
 where p_boss_asi_res_inst.bk_hash in (select bk_hash from #bk_hash)
   and p_boss_asi_res_inst.dv_load_end_date_time = 'dec 31, 9999'
   and isnull(l_boss_asi_res_inst.dv_deleted,0) != 1

--Insert all updated and new s_boss_asi_res_inst records
set @insert_date_time = getdate()
insert into s_boss_asi_res_inst (
       bk_hash,
       start_date,
       end_date,
       name,
       comment,
       cost,
       substitute,
       sub_for,
       asi_res_inst_id,
       updated_at,
       created_at,
       res_color,
       use_for_lt_bucks,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user,
       dv_deleted)
select s_boss_asi_res_inst.bk_hash,
       s_boss_asi_res_inst.start_date,
       s_boss_asi_res_inst.end_date,
       s_boss_asi_res_inst.name,
       s_boss_asi_res_inst.comment,
       s_boss_asi_res_inst.cost,
       s_boss_asi_res_inst.substitute,
       s_boss_asi_res_inst.sub_for,
       s_boss_asi_res_inst.asi_res_inst_id,
       s_boss_asi_res_inst.updated_at,
       s_boss_asi_res_inst.created_at,
       s_boss_asi_res_inst.res_color,
       s_boss_asi_res_inst.use_for_lt_bucks,
       #bk_hash.dv_load_date_time,
       #bk_hash.dv_batch_id,
       s_boss_asi_res_inst.dv_r_load_source_id,
       s_boss_asi_res_inst.dv_hash,
       @insert_date_time,
       @user,
       1 --deleted
  from p_boss_asi_res_inst
  join s_boss_asi_res_inst
    on p_boss_asi_res_inst.bk_hash = s_boss_asi_res_inst.bk_hash
   and p_boss_asi_res_inst.s_boss_asi_res_inst_id = s_boss_asi_res_inst.s_boss_asi_res_inst_id
  join #bk_hash
    on p_boss_asi_res_inst.bk_hash = #bk_hash.bk_hash
 where p_boss_asi_res_inst.bk_hash in (select bk_hash from #bk_hash)
   and p_boss_asi_res_inst.dv_load_end_date_time = 'dec 31, 9999'
   and isnull(s_boss_asi_res_inst.dv_deleted,0) != 1

end