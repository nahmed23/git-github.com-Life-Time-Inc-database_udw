CREATE PROC [dbo].[proc_etl_exerp_booking] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_exerp_booking

set @insert_date_time = getdate()
insert into dbo.stage_hash_exerp_booking (
       bk_hash,
       id,
       name,
       color,
       start_datetime,
       activity_id,
       stop_datetime,
       creation_datetime,
       state,
       center_id,
       ets,
       class_capacity,
       waiting_list_capacity,
       cancel_datetime,
       cancel_reason,
       main_booking_id,
       max_capacity_override,
       description,
       comment,
       single_cancellation,
       strict_age_limit,
       minimum_age,
       maximum_age,
       minimum_age_unit,
       maximum_age_unit,
       age_text,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(id,'z#@$k%&P'))),2) bk_hash,
       id,
       name,
       color,
       start_datetime,
       activity_id,
       stop_datetime,
       creation_datetime,
       state,
       center_id,
       ets,
       class_capacity,
       waiting_list_capacity,
       cancel_datetime,
       cancel_reason,
       main_booking_id,
       max_capacity_override,
       description,
       comment,
       single_cancellation,
       strict_age_limit,
       minimum_age,
       maximum_age,
       minimum_age_unit,
       maximum_age_unit,
       age_text,
       isnull(cast(stage_exerp_booking.creation_datetime as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_exerp_booking
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_exerp_booking @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_exerp_booking (
       bk_hash,
       booking_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_exerp_booking.bk_hash,
       stage_hash_exerp_booking.id booking_id,
       isnull(cast(stage_hash_exerp_booking.creation_datetime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       33,
       @insert_date_time,
       @user
  from stage_hash_exerp_booking
  left join h_exerp_booking
    on stage_hash_exerp_booking.bk_hash = h_exerp_booking.bk_hash
 where h_exerp_booking_id is null
   and stage_hash_exerp_booking.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_exerp_booking
if object_id('tempdb..#l_exerp_booking_inserts') is not null drop table #l_exerp_booking_inserts
create table #l_exerp_booking_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_booking.bk_hash,
       stage_hash_exerp_booking.id booking_id,
       stage_hash_exerp_booking.activity_id activity_id,
       stage_hash_exerp_booking.center_id center_id,
       stage_hash_exerp_booking.main_booking_id main_booking_id,
       isnull(cast(stage_hash_exerp_booking.creation_datetime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_exerp_booking.id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_booking.activity_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_booking.center_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_booking.main_booking_id,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_booking
 where stage_hash_exerp_booking.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_exerp_booking records
set @insert_date_time = getdate()
insert into l_exerp_booking (
       bk_hash,
       booking_id,
       activity_id,
       center_id,
       main_booking_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_exerp_booking_inserts.bk_hash,
       #l_exerp_booking_inserts.booking_id,
       #l_exerp_booking_inserts.activity_id,
       #l_exerp_booking_inserts.center_id,
       #l_exerp_booking_inserts.main_booking_id,
       case when l_exerp_booking.l_exerp_booking_id is null then isnull(#l_exerp_booking_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #l_exerp_booking_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_exerp_booking_inserts
  left join p_exerp_booking
    on #l_exerp_booking_inserts.bk_hash = p_exerp_booking.bk_hash
   and p_exerp_booking.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_exerp_booking
    on p_exerp_booking.bk_hash = l_exerp_booking.bk_hash
   and p_exerp_booking.l_exerp_booking_id = l_exerp_booking.l_exerp_booking_id
 where l_exerp_booking.l_exerp_booking_id is null
    or (l_exerp_booking.l_exerp_booking_id is not null
        and l_exerp_booking.dv_hash <> #l_exerp_booking_inserts.source_hash)

--calculate hash and lookup to current s_exerp_booking
if object_id('tempdb..#s_exerp_booking_inserts') is not null drop table #s_exerp_booking_inserts
create table #s_exerp_booking_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_booking.bk_hash,
       stage_hash_exerp_booking.id booking_id,
       stage_hash_exerp_booking.name name,
       stage_hash_exerp_booking.color color,
       stage_hash_exerp_booking.start_datetime start_datetime,
       stage_hash_exerp_booking.stop_datetime stop_datetime,
       stage_hash_exerp_booking.creation_datetime creation_datetime,
       stage_hash_exerp_booking.state state,
       stage_hash_exerp_booking.ets ets,
       stage_hash_exerp_booking.class_capacity class_capacity,
       stage_hash_exerp_booking.waiting_list_capacity waiting_list_capacity,
       stage_hash_exerp_booking.cancel_datetime cancel_datetime,
       stage_hash_exerp_booking.cancel_reason cancel_reason,
       stage_hash_exerp_booking.max_capacity_override max_capacity_override,
       stage_hash_exerp_booking.description description,
       stage_hash_exerp_booking.comment comment,
       isnull(cast(stage_hash_exerp_booking.creation_datetime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_exerp_booking.id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_booking.name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_booking.color,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exerp_booking.start_datetime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exerp_booking.stop_datetime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exerp_booking.creation_datetime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_booking.state,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_booking.ets as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_booking.class_capacity as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_booking.waiting_list_capacity as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_exerp_booking.cancel_datetime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_booking.cancel_reason,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_booking.max_capacity_override as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_booking.description,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_booking.comment,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_booking
 where stage_hash_exerp_booking.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_exerp_booking records
set @insert_date_time = getdate()
insert into s_exerp_booking (
       bk_hash,
       booking_id,
       name,
       color,
       start_datetime,
       stop_datetime,
       creation_datetime,
       state,
       ets,
       class_capacity,
       waiting_list_capacity,
       cancel_datetime,
       cancel_reason,
       max_capacity_override,
       description,
       comment,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_exerp_booking_inserts.bk_hash,
       #s_exerp_booking_inserts.booking_id,
       #s_exerp_booking_inserts.name,
       #s_exerp_booking_inserts.color,
       #s_exerp_booking_inserts.start_datetime,
       #s_exerp_booking_inserts.stop_datetime,
       #s_exerp_booking_inserts.creation_datetime,
       #s_exerp_booking_inserts.state,
       #s_exerp_booking_inserts.ets,
       #s_exerp_booking_inserts.class_capacity,
       #s_exerp_booking_inserts.waiting_list_capacity,
       #s_exerp_booking_inserts.cancel_datetime,
       #s_exerp_booking_inserts.cancel_reason,
       #s_exerp_booking_inserts.max_capacity_override,
       #s_exerp_booking_inserts.description,
       #s_exerp_booking_inserts.comment,
       case when s_exerp_booking.s_exerp_booking_id is null then isnull(#s_exerp_booking_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #s_exerp_booking_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_exerp_booking_inserts
  left join p_exerp_booking
    on #s_exerp_booking_inserts.bk_hash = p_exerp_booking.bk_hash
   and p_exerp_booking.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_exerp_booking
    on p_exerp_booking.bk_hash = s_exerp_booking.bk_hash
   and p_exerp_booking.s_exerp_booking_id = s_exerp_booking.s_exerp_booking_id
 where s_exerp_booking.s_exerp_booking_id is null
    or (s_exerp_booking.s_exerp_booking_id is not null
        and s_exerp_booking.dv_hash <> #s_exerp_booking_inserts.source_hash)

--calculate hash and lookup to current s_exerp_booking_1
if object_id('tempdb..#s_exerp_booking_1_inserts') is not null drop table #s_exerp_booking_1_inserts
create table #s_exerp_booking_1_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_booking.bk_hash,
       stage_hash_exerp_booking.id booking_id,
       stage_hash_exerp_booking.single_cancellation single_cancellation,
       stage_hash_exerp_booking.strict_age_limit strict_age_limit,
       stage_hash_exerp_booking.minimum_age minimum_age,
       stage_hash_exerp_booking.maximum_age maximum_age,
       stage_hash_exerp_booking.minimum_age_unit minimum_age_unit,
       stage_hash_exerp_booking.maximum_age_unit maximum_age_unit,
       stage_hash_exerp_booking.age_text age_text,
       isnull(cast(stage_hash_exerp_booking.creation_datetime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_exerp_booking.id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_booking.single_cancellation as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_booking.strict_age_limit as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_booking.minimum_age as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_booking.maximum_age as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_booking.minimum_age_unit,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_booking.maximum_age_unit,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_booking.age_text,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_booking
 where stage_hash_exerp_booking.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_exerp_booking_1 records
set @insert_date_time = getdate()
insert into s_exerp_booking_1 (
       bk_hash,
       booking_id,
       single_cancellation,
       strict_age_limit,
       minimum_age,
       maximum_age,
       minimum_age_unit,
       maximum_age_unit,
       age_text,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_exerp_booking_1_inserts.bk_hash,
       #s_exerp_booking_1_inserts.booking_id,
       #s_exerp_booking_1_inserts.single_cancellation,
       #s_exerp_booking_1_inserts.strict_age_limit,
       #s_exerp_booking_1_inserts.minimum_age,
       #s_exerp_booking_1_inserts.maximum_age,
       #s_exerp_booking_1_inserts.minimum_age_unit,
       #s_exerp_booking_1_inserts.maximum_age_unit,
       #s_exerp_booking_1_inserts.age_text,
       case when s_exerp_booking_1.s_exerp_booking_1_id is null then isnull(#s_exerp_booking_1_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #s_exerp_booking_1_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_exerp_booking_1_inserts
  left join p_exerp_booking
    on #s_exerp_booking_1_inserts.bk_hash = p_exerp_booking.bk_hash
   and p_exerp_booking.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_exerp_booking_1
    on p_exerp_booking.bk_hash = s_exerp_booking_1.bk_hash
   and p_exerp_booking.s_exerp_booking_1_id = s_exerp_booking_1.s_exerp_booking_1_id
 where s_exerp_booking_1.s_exerp_booking_1_id is null
    or (s_exerp_booking_1.s_exerp_booking_1_id is not null
        and s_exerp_booking_1.dv_hash <> #s_exerp_booking_1_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_exerp_booking @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_exerp_booking @current_dv_batch_id

end
