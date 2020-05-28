CREATE PROC [dbo].[proc_etl_exerp_time_configuration] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_exerp_time_configuration

set @insert_date_time = getdate()
insert into dbo.stage_hash_exerp_time_configuration (
       bk_hash,
       id,
       name,
       part_from,
       part_from_unit,
       part_staff_stop,
       part_staff_stop_unit,
       part_cust_stop,
       part_cust_stop_unit,
       cancel_sanc_start,
       cancel_sanc_start_unit,
       cancel_stop_staff,
       cancel_stop_staff_unit,
       cancel_stop_cust,
       cancel_stop_cust_unit,
       recurrence_in_past,
       recurrence_in_past_unit,
       course_sign_start,
       course_sign_start_unit,
       course_stop,
       course_stop_unit,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       id,
       name,
       part_from,
       part_from_unit,
       part_staff_stop,
       part_staff_stop_unit,
       part_cust_stop,
       part_cust_stop_unit,
       cancel_sanc_start,
       cancel_sanc_start_unit,
       cancel_stop_staff,
       cancel_stop_staff_unit,
       cancel_stop_cust,
       cancel_stop_cust_unit,
       recurrence_in_past,
       recurrence_in_past_unit,
       course_sign_start,
       course_sign_start_unit,
       course_stop,
       course_stop_unit,
       dummy_modified_date_time,
       isnull(cast(stage_exerp_time_configuration.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_exerp_time_configuration
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_exerp_time_configuration @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_exerp_time_configuration (
       bk_hash,
       time_configuration_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_exerp_time_configuration.bk_hash,
       stage_hash_exerp_time_configuration.id time_configuration_id,
       isnull(cast(stage_hash_exerp_time_configuration.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       33,
       @insert_date_time,
       @user
  from stage_hash_exerp_time_configuration
  left join h_exerp_time_configuration
    on stage_hash_exerp_time_configuration.bk_hash = h_exerp_time_configuration.bk_hash
 where h_exerp_time_configuration_id is null
   and stage_hash_exerp_time_configuration.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current s_exerp_time_configuration
if object_id('tempdb..#s_exerp_time_configuration_inserts') is not null drop table #s_exerp_time_configuration_inserts
create table #s_exerp_time_configuration_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_time_configuration.bk_hash,
       stage_hash_exerp_time_configuration.id time_configuration_id,
       stage_hash_exerp_time_configuration.name name,
       stage_hash_exerp_time_configuration.part_from part_from,
       stage_hash_exerp_time_configuration.part_from_unit part_from_unit,
       stage_hash_exerp_time_configuration.part_staff_stop part_staff_stop,
       stage_hash_exerp_time_configuration.part_staff_stop_unit part_staff_stop_unit,
       stage_hash_exerp_time_configuration.part_cust_stop part_cust_stop,
       stage_hash_exerp_time_configuration.part_cust_stop_unit part_cust_stop_unit,
       stage_hash_exerp_time_configuration.cancel_sanc_start cancel_sanc_start,
       stage_hash_exerp_time_configuration.cancel_sanc_start_unit cancel_sanc_start_unit,
       stage_hash_exerp_time_configuration.cancel_stop_staff cancel_stop_staff,
       stage_hash_exerp_time_configuration.cancel_stop_staff_unit cancel_stop_staff_unit,
       stage_hash_exerp_time_configuration.cancel_stop_cust cancel_stop_cust,
       stage_hash_exerp_time_configuration.cancel_stop_cust_unit cancel_stop_cust_unit,
       stage_hash_exerp_time_configuration.recurrence_in_past recurrence_in_past,
       stage_hash_exerp_time_configuration.recurrence_in_past_unit recurrence_in_past_unit,
       stage_hash_exerp_time_configuration.course_sign_start course_sign_start,
       stage_hash_exerp_time_configuration.course_sign_start_unit course_sign_start_unit,
       stage_hash_exerp_time_configuration.course_stop course_stop,
       stage_hash_exerp_time_configuration.course_stop_unit course_stop_unit,
       stage_hash_exerp_time_configuration.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_exerp_time_configuration.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_exerp_time_configuration.id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_time_configuration.name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_time_configuration.part_from as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_time_configuration.part_from_unit,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_time_configuration.part_staff_stop as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_time_configuration.part_staff_stop_unit,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_time_configuration.part_cust_stop as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_time_configuration.part_cust_stop_unit,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_time_configuration.cancel_sanc_start as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_time_configuration.cancel_sanc_start_unit,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_time_configuration.cancel_stop_staff as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_time_configuration.cancel_stop_staff_unit,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_time_configuration.cancel_stop_cust as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_time_configuration.cancel_stop_cust_unit,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_time_configuration.recurrence_in_past as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_time_configuration.recurrence_in_past_unit,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_time_configuration.course_sign_start as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_time_configuration.course_sign_start_unit,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_time_configuration.course_stop as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_time_configuration.course_stop_unit,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_time_configuration
 where stage_hash_exerp_time_configuration.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_exerp_time_configuration records
set @insert_date_time = getdate()
insert into s_exerp_time_configuration (
       bk_hash,
       time_configuration_id,
       name,
       part_from,
       part_from_unit,
       part_staff_stop,
       part_staff_stop_unit,
       part_cust_stop,
       part_cust_stop_unit,
       cancel_sanc_start,
       cancel_sanc_start_unit,
       cancel_stop_staff,
       cancel_stop_staff_unit,
       cancel_stop_cust,
       cancel_stop_cust_unit,
       recurrence_in_past,
       recurrence_in_past_unit,
       course_sign_start,
       course_sign_start_unit,
       course_stop,
       course_stop_unit,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_exerp_time_configuration_inserts.bk_hash,
       #s_exerp_time_configuration_inserts.time_configuration_id,
       #s_exerp_time_configuration_inserts.name,
       #s_exerp_time_configuration_inserts.part_from,
       #s_exerp_time_configuration_inserts.part_from_unit,
       #s_exerp_time_configuration_inserts.part_staff_stop,
       #s_exerp_time_configuration_inserts.part_staff_stop_unit,
       #s_exerp_time_configuration_inserts.part_cust_stop,
       #s_exerp_time_configuration_inserts.part_cust_stop_unit,
       #s_exerp_time_configuration_inserts.cancel_sanc_start,
       #s_exerp_time_configuration_inserts.cancel_sanc_start_unit,
       #s_exerp_time_configuration_inserts.cancel_stop_staff,
       #s_exerp_time_configuration_inserts.cancel_stop_staff_unit,
       #s_exerp_time_configuration_inserts.cancel_stop_cust,
       #s_exerp_time_configuration_inserts.cancel_stop_cust_unit,
       #s_exerp_time_configuration_inserts.recurrence_in_past,
       #s_exerp_time_configuration_inserts.recurrence_in_past_unit,
       #s_exerp_time_configuration_inserts.course_sign_start,
       #s_exerp_time_configuration_inserts.course_sign_start_unit,
       #s_exerp_time_configuration_inserts.course_stop,
       #s_exerp_time_configuration_inserts.course_stop_unit,
       #s_exerp_time_configuration_inserts.dummy_modified_date_time,
       case when s_exerp_time_configuration.s_exerp_time_configuration_id is null then isnull(#s_exerp_time_configuration_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #s_exerp_time_configuration_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_exerp_time_configuration_inserts
  left join p_exerp_time_configuration
    on #s_exerp_time_configuration_inserts.bk_hash = p_exerp_time_configuration.bk_hash
   and p_exerp_time_configuration.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_exerp_time_configuration
    on p_exerp_time_configuration.bk_hash = s_exerp_time_configuration.bk_hash
   and p_exerp_time_configuration.s_exerp_time_configuration_id = s_exerp_time_configuration.s_exerp_time_configuration_id
 where s_exerp_time_configuration.s_exerp_time_configuration_id is null
    or (s_exerp_time_configuration.s_exerp_time_configuration_id is not null
        and s_exerp_time_configuration.dv_hash <> #s_exerp_time_configuration_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_exerp_time_configuration @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_exerp_time_configuration @current_dv_batch_id

end
