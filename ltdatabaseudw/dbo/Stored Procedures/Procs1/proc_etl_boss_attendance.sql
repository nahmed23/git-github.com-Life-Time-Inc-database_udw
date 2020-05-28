CREATE PROC [dbo].[proc_etl_boss_attendance] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_boss_attendance

set @insert_date_time = getdate()
insert into dbo.stage_hash_boss_attendance (
       bk_hash,
       reservation,
       attendance_date,
       cust_code,
       mbr_code,
       checked_in,
       employee_id,
       comment,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(reservation as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(convert(varchar,attendance_date,120),'z#@$k%&P')+'P%#&z$@k'+isnull(cust_code,'z#@$k%&P')+'P%#&z$@k'+isnull(mbr_code,'z#@$k%&P'))),2) bk_hash,
       reservation,
       attendance_date,
       cust_code,
       mbr_code,
       checked_in,
       employee_id,
       comment,
       isnull(cast(stage_boss_attendance.attendance_date as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_boss_attendance
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_boss_attendance @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_boss_attendance (
       bk_hash,
       reservation,
       attendance_date,
       cust_code,
       mbr_code,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_boss_attendance.bk_hash,
       stage_hash_boss_attendance.reservation reservation,
       stage_hash_boss_attendance.attendance_date attendance_date,
       stage_hash_boss_attendance.cust_code cust_code,
       stage_hash_boss_attendance.mbr_code mbr_code,
       isnull(cast(stage_hash_boss_attendance.attendance_date as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       26,
       @insert_date_time,
       @user
  from stage_hash_boss_attendance
  left join h_boss_attendance
    on stage_hash_boss_attendance.bk_hash = h_boss_attendance.bk_hash
 where h_boss_attendance_id is null
   and stage_hash_boss_attendance.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_boss_attendance
if object_id('tempdb..#l_boss_attendance_inserts') is not null drop table #l_boss_attendance_inserts
create table #l_boss_attendance_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_boss_attendance.bk_hash,
       stage_hash_boss_attendance.reservation reservation,
       stage_hash_boss_attendance.attendance_date attendance_date,
       stage_hash_boss_attendance.cust_code cust_code,
       stage_hash_boss_attendance.mbr_code mbr_code,
       stage_hash_boss_attendance.employee_id employee_id,
       isnull(cast(stage_hash_boss_attendance.attendance_date as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_boss_attendance.reservation as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_attendance.attendance_date,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_attendance.cust_code,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_attendance.mbr_code,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_attendance.employee_id,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_boss_attendance
 where stage_hash_boss_attendance.dv_batch_id = @current_dv_batch_id

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
       dv_insert_user)
select #l_boss_attendance_inserts.bk_hash,
       #l_boss_attendance_inserts.reservation,
       #l_boss_attendance_inserts.attendance_date,
       #l_boss_attendance_inserts.cust_code,
       #l_boss_attendance_inserts.mbr_code,
       #l_boss_attendance_inserts.employee_id,
       case when l_boss_attendance.l_boss_attendance_id is null then isnull(#l_boss_attendance_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       26,
       #l_boss_attendance_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_boss_attendance_inserts
  left join p_boss_attendance
    on #l_boss_attendance_inserts.bk_hash = p_boss_attendance.bk_hash
   and p_boss_attendance.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_boss_attendance
    on p_boss_attendance.bk_hash = l_boss_attendance.bk_hash
   and p_boss_attendance.l_boss_attendance_id = l_boss_attendance.l_boss_attendance_id
 where l_boss_attendance.l_boss_attendance_id is null
    or (l_boss_attendance.l_boss_attendance_id is not null
        and l_boss_attendance.dv_hash <> #l_boss_attendance_inserts.source_hash)

--calculate hash and lookup to current s_boss_attendance
if object_id('tempdb..#s_boss_attendance_inserts') is not null drop table #s_boss_attendance_inserts
create table #s_boss_attendance_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_boss_attendance.bk_hash,
       stage_hash_boss_attendance.reservation reservation,
       stage_hash_boss_attendance.attendance_date attendance_date,
       stage_hash_boss_attendance.cust_code cust_code,
       stage_hash_boss_attendance.mbr_code mbr_code,
       stage_hash_boss_attendance.checked_in checked_in,
       stage_hash_boss_attendance.comment comment,
       isnull(cast(stage_hash_boss_attendance.attendance_date as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_boss_attendance.reservation as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_boss_attendance.attendance_date,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_attendance.cust_code,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_attendance.mbr_code,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_attendance.checked_in,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_boss_attendance.comment,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_boss_attendance
 where stage_hash_boss_attendance.dv_batch_id = @current_dv_batch_id

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
       dv_insert_user)
select #s_boss_attendance_inserts.bk_hash,
       #s_boss_attendance_inserts.reservation,
       #s_boss_attendance_inserts.attendance_date,
       #s_boss_attendance_inserts.cust_code,
       #s_boss_attendance_inserts.mbr_code,
       #s_boss_attendance_inserts.checked_in,
       #s_boss_attendance_inserts.comment,
       case when s_boss_attendance.s_boss_attendance_id is null then isnull(#s_boss_attendance_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       26,
       #s_boss_attendance_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_boss_attendance_inserts
  left join p_boss_attendance
    on #s_boss_attendance_inserts.bk_hash = p_boss_attendance.bk_hash
   and p_boss_attendance.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_boss_attendance
    on p_boss_attendance.bk_hash = s_boss_attendance.bk_hash
   and p_boss_attendance.s_boss_attendance_id = s_boss_attendance.s_boss_attendance_id
 where s_boss_attendance.s_boss_attendance_id is null
    or (s_boss_attendance.s_boss_attendance_id is not null
        and s_boss_attendance.dv_hash <> #s_boss_attendance_inserts.source_hash)

--Run the dv_deleted proc
exec dbo.proc_dv_deleted_boss_attendance @current_dv_batch_id, @job_start_date_time_varchar

--Run the PIT proc
exec dbo.proc_p_boss_attendance @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_boss_attendance @current_dv_batch_id

end
