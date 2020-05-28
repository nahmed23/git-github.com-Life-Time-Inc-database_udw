CREATE PROC [dbo].[proc_etl_humanity_schedule] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_humanity_schedule

set @insert_date_time = getdate()
insert into dbo.stage_hash_humanity_schedule (
       bk_hash,
       shift_id,
       company_id,
       company_name,
       employee_id,
       employee_eid,
       employee_name,
       location_id,
       location_name,
       position_id,
       workday_position_id,
       position_name,
       shift_start_date_utc,
       shift_start_time,
       shift_end_date_utc,
       shift_end_time,
       hours,
       wage,
       published,
       published_datetime_utc,
       shift_type,
       employees_needed,
       employees_working,
       recurring_shift,
       created_by_eid,
       created_by_name,
       created_datetime_utc,
       updated_at_utc,
       notes,
       created_by_id,
       is_deleted,
       ltf_file_name,
       file_arrive_date,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(shift_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(employee_id,'z#@$k%&P')+'P%#&z$@k'+isnull(position_id,'z#@$k%&P')+'P%#&z$@k'+isnull(ltf_file_name,'z#@$k%&P'))),2) bk_hash,
       shift_id,
       company_id,
       company_name,
       employee_id,
       employee_eid,
       employee_name,
       location_id,
       location_name,
       position_id,
       workday_position_id,
       position_name,
       shift_start_date_utc,
       shift_start_time,
       shift_end_date_utc,
       shift_end_time,
       hours,
       wage,
       published,
       published_datetime_utc,
       shift_type,
       employees_needed,
       employees_working,
       recurring_shift,
       created_by_eid,
       created_by_name,
       created_datetime_utc,
       updated_at_utc,
       notes,
       created_by_id,
       is_deleted,
       ltf_file_name,
       file_arrive_date,
       dummy_modified_date_time,
       isnull(cast(stage_humanity_schedule.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_humanity_schedule
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_humanity_schedule @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_humanity_schedule (
       bk_hash,
       shift_id,
       employee_id,
       position_id,
       ltf_file_name,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_humanity_schedule.bk_hash,
       stage_hash_humanity_schedule.shift_id shift_id,
       stage_hash_humanity_schedule.employee_id employee_id,
       stage_hash_humanity_schedule.position_id position_id,
       stage_hash_humanity_schedule.ltf_file_name ltf_file_name,
       isnull(cast(stage_hash_humanity_schedule.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       47,
       @insert_date_time,
       @user
  from stage_hash_humanity_schedule
  left join h_humanity_schedule
    on stage_hash_humanity_schedule.bk_hash = h_humanity_schedule.bk_hash
 where h_humanity_schedule_id is null
   and stage_hash_humanity_schedule.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_humanity_schedule
if object_id('tempdb..#l_humanity_schedule_inserts') is not null drop table #l_humanity_schedule_inserts
create table #l_humanity_schedule_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_humanity_schedule.bk_hash,
       stage_hash_humanity_schedule.shift_id shift_id,
       stage_hash_humanity_schedule.employee_id employee_id,
       stage_hash_humanity_schedule.position_id position_id,
       stage_hash_humanity_schedule.ltf_file_name ltf_file_name,
       stage_hash_humanity_schedule.company_id company_id,
       isnull(cast(stage_hash_humanity_schedule.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_humanity_schedule.shift_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_schedule.employee_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_schedule.position_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_schedule.ltf_file_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_schedule.company_id,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_humanity_schedule
 where stage_hash_humanity_schedule.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_humanity_schedule records
set @insert_date_time = getdate()
insert into l_humanity_schedule (
       bk_hash,
       shift_id,
       employee_id,
       position_id,
       ltf_file_name,
       company_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_humanity_schedule_inserts.bk_hash,
       #l_humanity_schedule_inserts.shift_id,
       #l_humanity_schedule_inserts.employee_id,
       #l_humanity_schedule_inserts.position_id,
       #l_humanity_schedule_inserts.ltf_file_name,
       #l_humanity_schedule_inserts.company_id,
       case when l_humanity_schedule.l_humanity_schedule_id is null then isnull(#l_humanity_schedule_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       47,
       #l_humanity_schedule_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_humanity_schedule_inserts
  left join p_humanity_schedule
    on #l_humanity_schedule_inserts.bk_hash = p_humanity_schedule.bk_hash
   and p_humanity_schedule.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_humanity_schedule
    on p_humanity_schedule.bk_hash = l_humanity_schedule.bk_hash
   and p_humanity_schedule.l_humanity_schedule_id = l_humanity_schedule.l_humanity_schedule_id
 where l_humanity_schedule.l_humanity_schedule_id is null
    or (l_humanity_schedule.l_humanity_schedule_id is not null
        and l_humanity_schedule.dv_hash <> #l_humanity_schedule_inserts.source_hash)

--calculate hash and lookup to current s_humanity_schedule
if object_id('tempdb..#s_humanity_schedule_inserts') is not null drop table #s_humanity_schedule_inserts
create table #s_humanity_schedule_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_humanity_schedule.bk_hash,
       stage_hash_humanity_schedule.shift_id shift_id,
       stage_hash_humanity_schedule.employee_id employee_id,
       stage_hash_humanity_schedule.position_id position_id,
       stage_hash_humanity_schedule.ltf_file_name ltf_file_name,
       stage_hash_humanity_schedule.employee_eid employee_eid,
       stage_hash_humanity_schedule.employee_name employee_name,
       stage_hash_humanity_schedule.location_id location_id,
       stage_hash_humanity_schedule.location_name location_name,
       stage_hash_humanity_schedule.workday_position_id workday_position_id,
       stage_hash_humanity_schedule.position_name position_name,
       stage_hash_humanity_schedule.shift_start_date_utc shift_start_date_utc,
       stage_hash_humanity_schedule.shift_start_time shift_start_time,
       stage_hash_humanity_schedule.shift_end_date_utc shift_end_date_utc,
       stage_hash_humanity_schedule.shift_end_time shift_end_time,
       stage_hash_humanity_schedule.hours hours,
       stage_hash_humanity_schedule.wage wage,
       stage_hash_humanity_schedule.published published,
       stage_hash_humanity_schedule.published_datetime_utc published_datetime_utc,
       stage_hash_humanity_schedule.shift_type shift_type,
       stage_hash_humanity_schedule.employees_needed employees_needed,
       stage_hash_humanity_schedule.employees_working employees_working,
       stage_hash_humanity_schedule.recurring_shift recurring_shift,
       stage_hash_humanity_schedule.created_by_id created_by_id,
       stage_hash_humanity_schedule.created_by_eid created_by_eid,
       stage_hash_humanity_schedule.created_by_name created_by_name,
       stage_hash_humanity_schedule.created_datetime_utc created_datetime_utc,
       stage_hash_humanity_schedule.notes notes,
       stage_hash_humanity_schedule.is_deleted is_deleted,
       stage_hash_humanity_schedule.File_arrive_date File_arrive_date,
       stage_hash_humanity_schedule.updated_at_utc updated_at_utc,
       stage_hash_humanity_schedule.company_name company_name,
       stage_hash_humanity_schedule.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_humanity_schedule.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_humanity_schedule.shift_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_schedule.employee_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_schedule.position_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_schedule.ltf_file_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_schedule.employee_eid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_schedule.employee_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_schedule.location_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_schedule.location_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_schedule.workday_position_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_schedule.position_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_schedule.shift_start_date_utc,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_schedule.shift_start_time,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_schedule.shift_end_date_utc,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_schedule.shift_end_time,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_schedule.hours,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_schedule.wage,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_schedule.published,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_schedule.published_datetime_utc,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_schedule.shift_type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_schedule.employees_needed,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_schedule.employees_working,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_schedule.recurring_shift,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_schedule.created_by_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_schedule.created_by_eid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_schedule.created_by_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_schedule.created_datetime_utc,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_schedule.notes,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_schedule.is_deleted,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_schedule.File_arrive_date,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_schedule.updated_at_utc,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_schedule.company_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_humanity_schedule.dummy_modified_date_time,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_humanity_schedule
 where stage_hash_humanity_schedule.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_humanity_schedule records
set @insert_date_time = getdate()
insert into s_humanity_schedule (
       bk_hash,
       shift_id,
       employee_id,
       position_id,
       ltf_file_name,
       employee_eid,
       employee_name,
       location_id,
       location_name,
       workday_position_id,
       position_name,
       shift_start_date_utc,
       shift_start_time,
       shift_end_date_utc,
       shift_end_time,
       hours,
       wage,
       published,
       published_datetime_utc,
       shift_type,
       employees_needed,
       employees_working,
       recurring_shift,
       created_by_id,
       created_by_eid,
       created_by_name,
       created_datetime_utc,
       notes,
       is_deleted,
       File_arrive_date,
       updated_at_utc,
       company_name,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_humanity_schedule_inserts.bk_hash,
       #s_humanity_schedule_inserts.shift_id,
       #s_humanity_schedule_inserts.employee_id,
       #s_humanity_schedule_inserts.position_id,
       #s_humanity_schedule_inserts.ltf_file_name,
       #s_humanity_schedule_inserts.employee_eid,
       #s_humanity_schedule_inserts.employee_name,
       #s_humanity_schedule_inserts.location_id,
       #s_humanity_schedule_inserts.location_name,
       #s_humanity_schedule_inserts.workday_position_id,
       #s_humanity_schedule_inserts.position_name,
       #s_humanity_schedule_inserts.shift_start_date_utc,
       #s_humanity_schedule_inserts.shift_start_time,
       #s_humanity_schedule_inserts.shift_end_date_utc,
       #s_humanity_schedule_inserts.shift_end_time,
       #s_humanity_schedule_inserts.hours,
       #s_humanity_schedule_inserts.wage,
       #s_humanity_schedule_inserts.published,
       #s_humanity_schedule_inserts.published_datetime_utc,
       #s_humanity_schedule_inserts.shift_type,
       #s_humanity_schedule_inserts.employees_needed,
       #s_humanity_schedule_inserts.employees_working,
       #s_humanity_schedule_inserts.recurring_shift,
       #s_humanity_schedule_inserts.created_by_id,
       #s_humanity_schedule_inserts.created_by_eid,
       #s_humanity_schedule_inserts.created_by_name,
       #s_humanity_schedule_inserts.created_datetime_utc,
       #s_humanity_schedule_inserts.notes,
       #s_humanity_schedule_inserts.is_deleted,
       #s_humanity_schedule_inserts.File_arrive_date,
       #s_humanity_schedule_inserts.updated_at_utc,
       #s_humanity_schedule_inserts.company_name,
       #s_humanity_schedule_inserts.dummy_modified_date_time,
       case when s_humanity_schedule.s_humanity_schedule_id is null then isnull(#s_humanity_schedule_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       47,
       #s_humanity_schedule_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_humanity_schedule_inserts
  left join p_humanity_schedule
    on #s_humanity_schedule_inserts.bk_hash = p_humanity_schedule.bk_hash
   and p_humanity_schedule.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_humanity_schedule
    on p_humanity_schedule.bk_hash = s_humanity_schedule.bk_hash
   and p_humanity_schedule.s_humanity_schedule_id = s_humanity_schedule.s_humanity_schedule_id
 where s_humanity_schedule.s_humanity_schedule_id is null
    or (s_humanity_schedule.s_humanity_schedule_id is not null
        and s_humanity_schedule.dv_hash <> #s_humanity_schedule_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_humanity_schedule @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_humanity_schedule @current_dv_batch_id

end
