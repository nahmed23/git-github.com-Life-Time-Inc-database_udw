CREATE PROC [dbo].[proc_etl_humanity_overtime_hours] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_humanity_overtime_hours

set @insert_date_time = getdate()
insert into dbo.stage_hash_humanity_overtime_hours (
       bk_hash,
       userid,
       employee_name,
       employee_id,
       date_formatted,
       hours_regular,
       hours_overtime,
       hours_d_overtime,
       hours_position_id,
       hours_location_id,
       company_id,
       start_time,
       end_time,
       ltf_file_name,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(userid as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(employee_id,'z#@$k%&P')+'P%#&z$@k'+isnull(date_formatted,'z#@$k%&P')+'P%#&z$@k'+isnull(hours_regular,'z#@$k%&P')+'P%#&z$@k'+isnull(hours_overtime,'z#@$k%&P')+'P%#&z$@k'+isnull(hours_d_overtime,'z#@$k%&P')+'P%#&z$@k'+isnull(hours_position_id,'z#@$k%&P')+'P%#&z$@k'+isnull(hours_location_id,'z#@$k%&P')+'P%#&z$@k'+isnull(company_id,'z#@$k%&P')+'P%#&z$@k'+isnull(start_time,'z#@$k%&P')+'P%#&z$@k'+isnull(end_time,'z#@$k%&P')+'P%#&z$@k'+isnull(ltf_file_name,'z#@$k%&P'))),2) bk_hash,
       userid,
       employee_name,
       employee_id,
       date_formatted,
       hours_regular,
       hours_overtime,
       hours_d_overtime,
       hours_position_id,
       hours_location_id,
       company_id,
       start_time,
       end_time,
       ltf_file_name,
       dummy_modified_date_time,
       isnull(cast(stage_humanity_overtime_hours.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_humanity_overtime_hours
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_humanity_overtime_hours @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_humanity_overtime_hours (
       bk_hash,
       userid,
       employee_id,
       date_formatted,
       hours_regular,
       hours_overtime,
       hours_d_overtime,
       hours_position_id,
       hours_location_id,
       company_id,
       start_time,
       end_time,
       ltf_file_name,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_humanity_overtime_hours.bk_hash,
       stage_hash_humanity_overtime_hours.userid userid,
       stage_hash_humanity_overtime_hours.employee_id employee_id,
       stage_hash_humanity_overtime_hours.date_formatted date_formatted,
       stage_hash_humanity_overtime_hours.hours_regular hours_regular,
       stage_hash_humanity_overtime_hours.hours_overtime hours_overtime,
       stage_hash_humanity_overtime_hours.hours_d_overtime hours_d_overtime,
       stage_hash_humanity_overtime_hours.hours_position_id hours_position_id,
       stage_hash_humanity_overtime_hours.hours_location_id hours_location_id,
       stage_hash_humanity_overtime_hours.company_id company_id,
       stage_hash_humanity_overtime_hours.start_time start_time,
       stage_hash_humanity_overtime_hours.end_time end_time,
       stage_hash_humanity_overtime_hours.ltf_file_name ltf_file_name,
       isnull(cast(stage_hash_humanity_overtime_hours.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       47,
       @insert_date_time,
       @user
  from stage_hash_humanity_overtime_hours
  left join h_humanity_overtime_hours
    on stage_hash_humanity_overtime_hours.bk_hash = h_humanity_overtime_hours.bk_hash
 where h_humanity_overtime_hours_id is null
   and stage_hash_humanity_overtime_hours.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_humanity_overtime_hours
if object_id('tempdb..#l_humanity_overtime_hours_inserts') is not null drop table #l_humanity_overtime_hours_inserts
create table #l_humanity_overtime_hours_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_humanity_overtime_hours.bk_hash,
       stage_hash_humanity_overtime_hours.userid userid,
       stage_hash_humanity_overtime_hours.employee_id employee_id,
       stage_hash_humanity_overtime_hours.date_formatted date_formatted,
       stage_hash_humanity_overtime_hours.hours_regular hours_regular,
       stage_hash_humanity_overtime_hours.hours_overtime hours_overtime,
       stage_hash_humanity_overtime_hours.hours_d_overtime hours_d_overtime,
       stage_hash_humanity_overtime_hours.hours_position_id hours_position_id,
       stage_hash_humanity_overtime_hours.hours_location_id hours_location_id,
       stage_hash_humanity_overtime_hours.company_id company_id,
       stage_hash_humanity_overtime_hours.start_time start_time,
       stage_hash_humanity_overtime_hours.end_time end_time,
       stage_hash_humanity_overtime_hours.ltf_file_name ltf_file_name,
       isnull(cast(stage_hash_humanity_overtime_hours.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_humanity_overtime_hours.userid as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_overtime_hours.employee_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_overtime_hours.date_formatted,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_overtime_hours.hours_regular,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_overtime_hours.hours_overtime,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_overtime_hours.hours_d_overtime,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_overtime_hours.hours_position_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_overtime_hours.hours_location_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_overtime_hours.company_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_overtime_hours.start_time,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_overtime_hours.end_time,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_overtime_hours.ltf_file_name,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_humanity_overtime_hours
 where stage_hash_humanity_overtime_hours.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_humanity_overtime_hours records
set @insert_date_time = getdate()
insert into l_humanity_overtime_hours (
       bk_hash,
       userid,
       employee_id,
       date_formatted,
       hours_regular,
       hours_overtime,
       hours_d_overtime,
       hours_position_id,
       hours_location_id,
       company_id,
       start_time,
       end_time,
       ltf_file_name,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_humanity_overtime_hours_inserts.bk_hash,
       #l_humanity_overtime_hours_inserts.userid,
       #l_humanity_overtime_hours_inserts.employee_id,
       #l_humanity_overtime_hours_inserts.date_formatted,
       #l_humanity_overtime_hours_inserts.hours_regular,
       #l_humanity_overtime_hours_inserts.hours_overtime,
       #l_humanity_overtime_hours_inserts.hours_d_overtime,
       #l_humanity_overtime_hours_inserts.hours_position_id,
       #l_humanity_overtime_hours_inserts.hours_location_id,
       #l_humanity_overtime_hours_inserts.company_id,
       #l_humanity_overtime_hours_inserts.start_time,
       #l_humanity_overtime_hours_inserts.end_time,
       #l_humanity_overtime_hours_inserts.ltf_file_name,
       case when l_humanity_overtime_hours.l_humanity_overtime_hours_id is null then isnull(#l_humanity_overtime_hours_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       47,
       #l_humanity_overtime_hours_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_humanity_overtime_hours_inserts
  left join p_humanity_overtime_hours
    on #l_humanity_overtime_hours_inserts.bk_hash = p_humanity_overtime_hours.bk_hash
   and p_humanity_overtime_hours.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_humanity_overtime_hours
    on p_humanity_overtime_hours.bk_hash = l_humanity_overtime_hours.bk_hash
   and p_humanity_overtime_hours.l_humanity_overtime_hours_id = l_humanity_overtime_hours.l_humanity_overtime_hours_id
 where l_humanity_overtime_hours.l_humanity_overtime_hours_id is null
    or (l_humanity_overtime_hours.l_humanity_overtime_hours_id is not null
        and l_humanity_overtime_hours.dv_hash <> #l_humanity_overtime_hours_inserts.source_hash)

--calculate hash and lookup to current s_humanity_overtime_hours
if object_id('tempdb..#s_humanity_overtime_hours_inserts') is not null drop table #s_humanity_overtime_hours_inserts
create table #s_humanity_overtime_hours_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_humanity_overtime_hours.bk_hash,
       stage_hash_humanity_overtime_hours.userid userid,
       stage_hash_humanity_overtime_hours.employee_id employee_id,
       stage_hash_humanity_overtime_hours.date_formatted date_formatted,
       stage_hash_humanity_overtime_hours.hours_regular hours_regular,
       stage_hash_humanity_overtime_hours.hours_overtime hours_overtime,
       stage_hash_humanity_overtime_hours.hours_d_overtime hours_d_overtime,
       stage_hash_humanity_overtime_hours.hours_position_id hours_position_id,
       stage_hash_humanity_overtime_hours.hours_location_id hours_location_id,
       stage_hash_humanity_overtime_hours.company_id company_id,
       stage_hash_humanity_overtime_hours.start_time start_time,
       stage_hash_humanity_overtime_hours.end_time end_time,
       stage_hash_humanity_overtime_hours.ltf_file_name ltf_file_name,
       stage_hash_humanity_overtime_hours.employee_name employee_name,
       stage_hash_humanity_overtime_hours.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_humanity_overtime_hours.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_humanity_overtime_hours.userid as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_overtime_hours.employee_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_overtime_hours.date_formatted,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_overtime_hours.hours_regular,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_overtime_hours.hours_overtime,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_overtime_hours.hours_d_overtime,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_overtime_hours.hours_position_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_overtime_hours.hours_location_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_overtime_hours.company_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_overtime_hours.start_time,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_overtime_hours.end_time,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_overtime_hours.ltf_file_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_humanity_overtime_hours.employee_name,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_humanity_overtime_hours
 where stage_hash_humanity_overtime_hours.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_humanity_overtime_hours records
set @insert_date_time = getdate()
insert into s_humanity_overtime_hours (
       bk_hash,
       userid,
       employee_id,
       date_formatted,
       hours_regular,
       hours_overtime,
       hours_d_overtime,
       hours_position_id,
       hours_location_id,
       company_id,
       start_time,
       end_time,
       ltf_file_name,
       employee_name,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_humanity_overtime_hours_inserts.bk_hash,
       #s_humanity_overtime_hours_inserts.userid,
       #s_humanity_overtime_hours_inserts.employee_id,
       #s_humanity_overtime_hours_inserts.date_formatted,
       #s_humanity_overtime_hours_inserts.hours_regular,
       #s_humanity_overtime_hours_inserts.hours_overtime,
       #s_humanity_overtime_hours_inserts.hours_d_overtime,
       #s_humanity_overtime_hours_inserts.hours_position_id,
       #s_humanity_overtime_hours_inserts.hours_location_id,
       #s_humanity_overtime_hours_inserts.company_id,
       #s_humanity_overtime_hours_inserts.start_time,
       #s_humanity_overtime_hours_inserts.end_time,
       #s_humanity_overtime_hours_inserts.ltf_file_name,
       #s_humanity_overtime_hours_inserts.employee_name,
       #s_humanity_overtime_hours_inserts.dummy_modified_date_time,
       case when s_humanity_overtime_hours.s_humanity_overtime_hours_id is null then isnull(#s_humanity_overtime_hours_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       47,
       #s_humanity_overtime_hours_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_humanity_overtime_hours_inserts
  left join p_humanity_overtime_hours
    on #s_humanity_overtime_hours_inserts.bk_hash = p_humanity_overtime_hours.bk_hash
   and p_humanity_overtime_hours.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_humanity_overtime_hours
    on p_humanity_overtime_hours.bk_hash = s_humanity_overtime_hours.bk_hash
   and p_humanity_overtime_hours.s_humanity_overtime_hours_id = s_humanity_overtime_hours.s_humanity_overtime_hours_id
 where s_humanity_overtime_hours.s_humanity_overtime_hours_id is null
    or (s_humanity_overtime_hours.s_humanity_overtime_hours_id is not null
        and s_humanity_overtime_hours.dv_hash <> #s_humanity_overtime_hours_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_humanity_overtime_hours @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_humanity_overtime_hours @current_dv_batch_id

end
