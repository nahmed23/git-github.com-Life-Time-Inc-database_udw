CREATE PROC [dbo].[proc_etl_exerp_participation] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_exerp_participation

set @insert_date_time = getdate()
insert into dbo.stage_hash_exerp_participation (
       bk_hash,
       id,
       booking_id,
       center_id,
       person_id,
       creation_datetime,
       state,
       user_interface_type,
       show_up_datetime,
       show_up_interface_type,
       showup_using_card,
       cancel_datetime,
       cancel_interface_type,
       cancel_reason,
       was_on_waiting_list,
       ets,
       seat_obtained_datetime,
       participant_number,
       seat_id,
       seat_state,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(id,'z#@$k%&P'))),2) bk_hash,
       id,
       booking_id,
       center_id,
       person_id,
       creation_datetime,
       state,
       user_interface_type,
       show_up_datetime,
       show_up_interface_type,
       showup_using_card,
       cancel_datetime,
       cancel_interface_type,
       cancel_reason,
       was_on_waiting_list,
       ets,
       seat_obtained_datetime,
       participant_number,
       seat_id,
       seat_state,
       isnull(cast(stage_exerp_participation.creation_datetime as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_exerp_participation
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_exerp_participation @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_exerp_participation (
       bk_hash,
       participation_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_exerp_participation.bk_hash,
       stage_hash_exerp_participation.id participation_id,
       isnull(cast(stage_hash_exerp_participation.creation_datetime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       33,
       @insert_date_time,
       @user
  from stage_hash_exerp_participation
  left join h_exerp_participation
    on stage_hash_exerp_participation.bk_hash = h_exerp_participation.bk_hash
 where h_exerp_participation_id is null
   and stage_hash_exerp_participation.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_exerp_participation
if object_id('tempdb..#l_exerp_participation_inserts') is not null drop table #l_exerp_participation_inserts
create table #l_exerp_participation_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_participation.bk_hash,
       stage_hash_exerp_participation.id participation_id,
       stage_hash_exerp_participation.booking_id booking_id,
       stage_hash_exerp_participation.center_id center_id,
       stage_hash_exerp_participation.person_id person_id,
       isnull(cast(stage_hash_exerp_participation.creation_datetime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_exerp_participation.id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_participation.booking_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_participation.center_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_participation.person_id,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_participation
 where stage_hash_exerp_participation.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_exerp_participation records
set @insert_date_time = getdate()
insert into l_exerp_participation (
       bk_hash,
       participation_id,
       booking_id,
       center_id,
       person_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_exerp_participation_inserts.bk_hash,
       #l_exerp_participation_inserts.participation_id,
       #l_exerp_participation_inserts.booking_id,
       #l_exerp_participation_inserts.center_id,
       #l_exerp_participation_inserts.person_id,
       case when l_exerp_participation.l_exerp_participation_id is null then isnull(#l_exerp_participation_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #l_exerp_participation_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_exerp_participation_inserts
  left join p_exerp_participation
    on #l_exerp_participation_inserts.bk_hash = p_exerp_participation.bk_hash
   and p_exerp_participation.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_exerp_participation
    on p_exerp_participation.bk_hash = l_exerp_participation.bk_hash
   and p_exerp_participation.l_exerp_participation_id = l_exerp_participation.l_exerp_participation_id
 where l_exerp_participation.l_exerp_participation_id is null
    or (l_exerp_participation.l_exerp_participation_id is not null
        and l_exerp_participation.dv_hash <> #l_exerp_participation_inserts.source_hash)

--calculate hash and lookup to current s_exerp_participation
if object_id('tempdb..#s_exerp_participation_inserts') is not null drop table #s_exerp_participation_inserts
create table #s_exerp_participation_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_participation.bk_hash,
       stage_hash_exerp_participation.id participation_id,
       stage_hash_exerp_participation.creation_datetime creation_datetime,
       stage_hash_exerp_participation.state state,
       stage_hash_exerp_participation.user_interface_type user_interface_type,
       stage_hash_exerp_participation.show_up_datetime show_up_datetime,
       stage_hash_exerp_participation.show_up_interface_type show_up_interface_type,
       stage_hash_exerp_participation.showup_using_card show_up_using_card,
       stage_hash_exerp_participation.cancel_datetime cancel_datetime,
       stage_hash_exerp_participation.cancel_interface_type cancel_interface_type,
       stage_hash_exerp_participation.cancel_reason cancel_reason,
       stage_hash_exerp_participation.was_on_waiting_list was_on_waiting_list,
       stage_hash_exerp_participation.ets ets,
       isnull(cast(stage_hash_exerp_participation.creation_datetime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_exerp_participation.id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exerp_participation.creation_datetime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_participation.state,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_participation.user_interface_type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exerp_participation.show_up_datetime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_participation.show_up_interface_type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_participation.showup_using_card as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exerp_participation.cancel_datetime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_participation.cancel_interface_type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_participation.cancel_reason,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_participation.was_on_waiting_list as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_participation.ets as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_participation
 where stage_hash_exerp_participation.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_exerp_participation records
set @insert_date_time = getdate()
insert into s_exerp_participation (
       bk_hash,
       participation_id,
       creation_datetime,
       state,
       user_interface_type,
       show_up_datetime,
       show_up_interface_type,
       show_up_using_card,
       cancel_datetime,
       cancel_interface_type,
       cancel_reason,
       was_on_waiting_list,
       ets,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_exerp_participation_inserts.bk_hash,
       #s_exerp_participation_inserts.participation_id,
       #s_exerp_participation_inserts.creation_datetime,
       #s_exerp_participation_inserts.state,
       #s_exerp_participation_inserts.user_interface_type,
       #s_exerp_participation_inserts.show_up_datetime,
       #s_exerp_participation_inserts.show_up_interface_type,
       #s_exerp_participation_inserts.show_up_using_card,
       #s_exerp_participation_inserts.cancel_datetime,
       #s_exerp_participation_inserts.cancel_interface_type,
       #s_exerp_participation_inserts.cancel_reason,
       #s_exerp_participation_inserts.was_on_waiting_list,
       #s_exerp_participation_inserts.ets,
       case when s_exerp_participation.s_exerp_participation_id is null then isnull(#s_exerp_participation_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #s_exerp_participation_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_exerp_participation_inserts
  left join p_exerp_participation
    on #s_exerp_participation_inserts.bk_hash = p_exerp_participation.bk_hash
   and p_exerp_participation.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_exerp_participation
    on p_exerp_participation.bk_hash = s_exerp_participation.bk_hash
   and p_exerp_participation.s_exerp_participation_id = s_exerp_participation.s_exerp_participation_id
 where s_exerp_participation.s_exerp_participation_id is null
    or (s_exerp_participation.s_exerp_participation_id is not null
        and s_exerp_participation.dv_hash <> #s_exerp_participation_inserts.source_hash)

--calculate hash and lookup to current s_exerp_participation_1
if object_id('tempdb..#s_exerp_participation_1_inserts') is not null drop table #s_exerp_participation_1_inserts
create table #s_exerp_participation_1_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_participation.bk_hash,
       stage_hash_exerp_participation.id participation_id,
       stage_hash_exerp_participation.seat_obtained_datetime seat_obtained_datetime,
       stage_hash_exerp_participation.participant_number participant_number,
       stage_hash_exerp_participation.seat_id seat_id,
       stage_hash_exerp_participation.seat_state seat_state,
       isnull(cast(stage_hash_exerp_participation.creation_datetime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_exerp_participation.id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exerp_participation.seat_obtained_datetime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_participation.participant_number as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_participation.seat_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_participation.seat_state,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_participation
 where stage_hash_exerp_participation.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_exerp_participation_1 records
set @insert_date_time = getdate()
insert into s_exerp_participation_1 (
       bk_hash,
       participation_id,
       seat_obtained_datetime,
       participant_number,
       seat_id,
       seat_state,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_exerp_participation_1_inserts.bk_hash,
       #s_exerp_participation_1_inserts.participation_id,
       #s_exerp_participation_1_inserts.seat_obtained_datetime,
       #s_exerp_participation_1_inserts.participant_number,
       #s_exerp_participation_1_inserts.seat_id,
       #s_exerp_participation_1_inserts.seat_state,
       case when s_exerp_participation_1.s_exerp_participation_1_id is null then isnull(#s_exerp_participation_1_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #s_exerp_participation_1_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_exerp_participation_1_inserts
  left join p_exerp_participation
    on #s_exerp_participation_1_inserts.bk_hash = p_exerp_participation.bk_hash
   and p_exerp_participation.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_exerp_participation_1
    on p_exerp_participation.bk_hash = s_exerp_participation_1.bk_hash
   and p_exerp_participation.s_exerp_participation_1_id = s_exerp_participation_1.s_exerp_participation_1_id
 where s_exerp_participation_1.s_exerp_participation_1_id is null
    or (s_exerp_participation_1.s_exerp_participation_1_id is not null
        and s_exerp_participation_1.dv_hash <> #s_exerp_participation_1_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_exerp_participation @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_exerp_participation @current_dv_batch_id

end
