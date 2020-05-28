CREATE PROC [dbo].[proc_etl_exerp_activity] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_exerp_activity

set @insert_date_time = getdate()
insert into dbo.stage_hash_exerp_activity (
       bk_hash,
       id,
       name,
       state,
       type,
       activity_group_id,
       color,
       max_participants,
       max_waiting_list_participants,
       external_id,
       access_group_id,
       description,
       time_configuration_id,
       course_schedule_type,
       age_group_id,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       id,
       name,
       state,
       type,
       activity_group_id,
       color,
       max_participants,
       max_waiting_list_participants,
       external_id,
       access_group_id,
       description,
       time_configuration_id,
       course_schedule_type,
       age_group_id,
       dummy_modified_date_time,
       isnull(cast(stage_exerp_activity.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_exerp_activity
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_exerp_activity @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_exerp_activity (
       bk_hash,
       activity_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_exerp_activity.bk_hash,
       stage_hash_exerp_activity.id activity_id,
       isnull(cast(stage_hash_exerp_activity.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       33,
       @insert_date_time,
       @user
  from stage_hash_exerp_activity
  left join h_exerp_activity
    on stage_hash_exerp_activity.bk_hash = h_exerp_activity.bk_hash
 where h_exerp_activity_id is null
   and stage_hash_exerp_activity.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_exerp_activity
if object_id('tempdb..#l_exerp_activity_inserts') is not null drop table #l_exerp_activity_inserts
create table #l_exerp_activity_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_activity.bk_hash,
       stage_hash_exerp_activity.id activity_id,
       stage_hash_exerp_activity.activity_group_id activity_group_id,
       stage_hash_exerp_activity.external_id external_id,
       stage_hash_exerp_activity.access_group_id access_group_id,
       isnull(cast(stage_hash_exerp_activity.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_exerp_activity.id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_activity.activity_group_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_activity.external_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_activity.access_group_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_activity
 where stage_hash_exerp_activity.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_exerp_activity records
set @insert_date_time = getdate()
insert into l_exerp_activity (
       bk_hash,
       activity_id,
       activity_group_id,
       external_id,
       access_group_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_exerp_activity_inserts.bk_hash,
       #l_exerp_activity_inserts.activity_id,
       #l_exerp_activity_inserts.activity_group_id,
       #l_exerp_activity_inserts.external_id,
       #l_exerp_activity_inserts.access_group_id,
       case when l_exerp_activity.l_exerp_activity_id is null then isnull(#l_exerp_activity_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #l_exerp_activity_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_exerp_activity_inserts
  left join p_exerp_activity
    on #l_exerp_activity_inserts.bk_hash = p_exerp_activity.bk_hash
   and p_exerp_activity.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_exerp_activity
    on p_exerp_activity.bk_hash = l_exerp_activity.bk_hash
   and p_exerp_activity.l_exerp_activity_id = l_exerp_activity.l_exerp_activity_id
 where l_exerp_activity.l_exerp_activity_id is null
    or (l_exerp_activity.l_exerp_activity_id is not null
        and l_exerp_activity.dv_hash <> #l_exerp_activity_inserts.source_hash)

--calculate hash and lookup to current l_exerp_activity_1
if object_id('tempdb..#l_exerp_activity_1_inserts') is not null drop table #l_exerp_activity_1_inserts
create table #l_exerp_activity_1_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_activity.bk_hash,
       stage_hash_exerp_activity.id activity_id,
       stage_hash_exerp_activity.time_configuration_id time_configuration_id,
       isnull(cast(stage_hash_exerp_activity.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_exerp_activity.id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_activity.time_configuration_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_activity
 where stage_hash_exerp_activity.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_exerp_activity_1 records
set @insert_date_time = getdate()
insert into l_exerp_activity_1 (
       bk_hash,
       activity_id,
       time_configuration_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_exerp_activity_1_inserts.bk_hash,
       #l_exerp_activity_1_inserts.activity_id,
       #l_exerp_activity_1_inserts.time_configuration_id,
       case when l_exerp_activity_1.l_exerp_activity_1_id is null then isnull(#l_exerp_activity_1_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #l_exerp_activity_1_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_exerp_activity_1_inserts
  left join p_exerp_activity
    on #l_exerp_activity_1_inserts.bk_hash = p_exerp_activity.bk_hash
   and p_exerp_activity.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_exerp_activity_1
    on p_exerp_activity.bk_hash = l_exerp_activity_1.bk_hash
   and p_exerp_activity.l_exerp_activity_1_id = l_exerp_activity_1.l_exerp_activity_1_id
 where l_exerp_activity_1.l_exerp_activity_1_id is null
    or (l_exerp_activity_1.l_exerp_activity_1_id is not null
        and l_exerp_activity_1.dv_hash <> #l_exerp_activity_1_inserts.source_hash)

--calculate hash and lookup to current l_exerp_activity_2
if object_id('tempdb..#l_exerp_activity_2_inserts') is not null drop table #l_exerp_activity_2_inserts
create table #l_exerp_activity_2_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_activity.bk_hash,
       stage_hash_exerp_activity.id activity_id,
       stage_hash_exerp_activity.age_group_id age_group_id,
       isnull(cast(stage_hash_exerp_activity.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_exerp_activity.id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_activity.age_group_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_activity
 where stage_hash_exerp_activity.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_exerp_activity_2 records
set @insert_date_time = getdate()
insert into l_exerp_activity_2 (
       bk_hash,
       activity_id,
       age_group_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_exerp_activity_2_inserts.bk_hash,
       #l_exerp_activity_2_inserts.activity_id,
       #l_exerp_activity_2_inserts.age_group_id,
       case when l_exerp_activity_2.l_exerp_activity_2_id is null then isnull(#l_exerp_activity_2_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #l_exerp_activity_2_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_exerp_activity_2_inserts
  left join p_exerp_activity
    on #l_exerp_activity_2_inserts.bk_hash = p_exerp_activity.bk_hash
   and p_exerp_activity.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_exerp_activity_2
    on p_exerp_activity.bk_hash = l_exerp_activity_2.bk_hash
   and p_exerp_activity.l_exerp_activity_2_id = l_exerp_activity_2.l_exerp_activity_2_id
 where l_exerp_activity_2.l_exerp_activity_2_id is null
    or (l_exerp_activity_2.l_exerp_activity_2_id is not null
        and l_exerp_activity_2.dv_hash <> #l_exerp_activity_2_inserts.source_hash)

--calculate hash and lookup to current s_exerp_activity
if object_id('tempdb..#s_exerp_activity_inserts') is not null drop table #s_exerp_activity_inserts
create table #s_exerp_activity_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_activity.bk_hash,
       stage_hash_exerp_activity.id activity_id,
       stage_hash_exerp_activity.name name,
       stage_hash_exerp_activity.state state,
       stage_hash_exerp_activity.type type,
       stage_hash_exerp_activity.color color,
       stage_hash_exerp_activity.max_participants max_participants,
       stage_hash_exerp_activity.max_waiting_list_participants max_waiting_list_participants,
       stage_hash_exerp_activity.description description,
       stage_hash_exerp_activity.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_exerp_activity.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_exerp_activity.id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_activity.name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_activity.state,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_activity.type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_activity.color,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_activity.max_participants as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_activity.max_waiting_list_participants as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_activity.description,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_activity
 where stage_hash_exerp_activity.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_exerp_activity records
set @insert_date_time = getdate()
insert into s_exerp_activity (
       bk_hash,
       activity_id,
       name,
       state,
       type,
       color,
       max_participants,
       max_waiting_list_participants,
       description,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_exerp_activity_inserts.bk_hash,
       #s_exerp_activity_inserts.activity_id,
       #s_exerp_activity_inserts.name,
       #s_exerp_activity_inserts.state,
       #s_exerp_activity_inserts.type,
       #s_exerp_activity_inserts.color,
       #s_exerp_activity_inserts.max_participants,
       #s_exerp_activity_inserts.max_waiting_list_participants,
       #s_exerp_activity_inserts.description,
       #s_exerp_activity_inserts.dummy_modified_date_time,
       case when s_exerp_activity.s_exerp_activity_id is null then isnull(#s_exerp_activity_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #s_exerp_activity_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_exerp_activity_inserts
  left join p_exerp_activity
    on #s_exerp_activity_inserts.bk_hash = p_exerp_activity.bk_hash
   and p_exerp_activity.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_exerp_activity
    on p_exerp_activity.bk_hash = s_exerp_activity.bk_hash
   and p_exerp_activity.s_exerp_activity_id = s_exerp_activity.s_exerp_activity_id
 where s_exerp_activity.s_exerp_activity_id is null
    or (s_exerp_activity.s_exerp_activity_id is not null
        and s_exerp_activity.dv_hash <> #s_exerp_activity_inserts.source_hash)

--calculate hash and lookup to current s_exerp_activity_1
if object_id('tempdb..#s_exerp_activity_1_inserts') is not null drop table #s_exerp_activity_1_inserts
create table #s_exerp_activity_1_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_activity.bk_hash,
       stage_hash_exerp_activity.id activity_id,
       stage_hash_exerp_activity.course_schedule_type course_schedule_type,
       stage_hash_exerp_activity.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_exerp_activity.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_exerp_activity.id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_activity.course_schedule_type,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_activity
 where stage_hash_exerp_activity.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_exerp_activity_1 records
set @insert_date_time = getdate()
insert into s_exerp_activity_1 (
       bk_hash,
       activity_id,
       course_schedule_type,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_exerp_activity_1_inserts.bk_hash,
       #s_exerp_activity_1_inserts.activity_id,
       #s_exerp_activity_1_inserts.course_schedule_type,
       #s_exerp_activity_1_inserts.dummy_modified_date_time,
       case when s_exerp_activity_1.s_exerp_activity_1_id is null then isnull(#s_exerp_activity_1_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #s_exerp_activity_1_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_exerp_activity_1_inserts
  left join p_exerp_activity
    on #s_exerp_activity_1_inserts.bk_hash = p_exerp_activity.bk_hash
   and p_exerp_activity.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_exerp_activity_1
    on p_exerp_activity.bk_hash = s_exerp_activity_1.bk_hash
   and p_exerp_activity.s_exerp_activity_1_id = s_exerp_activity_1.s_exerp_activity_1_id
 where s_exerp_activity_1.s_exerp_activity_1_id is null
    or (s_exerp_activity_1.s_exerp_activity_1_id is not null
        and s_exerp_activity_1.dv_hash <> #s_exerp_activity_1_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_exerp_activity @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_exerp_activity @current_dv_batch_id

end
