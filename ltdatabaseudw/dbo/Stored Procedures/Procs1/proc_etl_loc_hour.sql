CREATE PROC [dbo].[proc_etl_loc_hour] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_loc_hour

set @insert_date_time = getdate()
insert into dbo.stage_hash_loc_hour (
       bk_hash,
       hour_id,
       location_id,
       val_hour_type_id,
       day_of_week,
       start_time,
       end_time,
       hour_24,
       sunrise,
       sunset,
       closed,
       by_appointment_only,
       created_date_time,
       created_by,
       deleted_date_time,
       deleted_by,
       last_updated_date_time,
       last_updated_by,
       udw_dim_location_key,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(hour_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       hour_id,
       location_id,
       val_hour_type_id,
       day_of_week,
       start_time,
       end_time,
       hour_24,
       sunrise,
       sunset,
       closed,
       by_appointment_only,
       created_date_time,
       created_by,
       deleted_date_time,
       deleted_by,
       last_updated_date_time,
       last_updated_by,
       udw_dim_location_key,
       isnull(cast(stage_loc_hour.created_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_loc_hour
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_loc_hour @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_loc_hour (
       bk_hash,
       hour_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_loc_hour.bk_hash,
       stage_hash_loc_hour.hour_id hour_id,
       isnull(cast(stage_hash_loc_hour.created_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       50,
       @insert_date_time,
       @user
  from stage_hash_loc_hour
  left join h_loc_hour
    on stage_hash_loc_hour.bk_hash = h_loc_hour.bk_hash
 where h_loc_hour_id is null
   and stage_hash_loc_hour.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_loc_hour
if object_id('tempdb..#l_loc_hour_inserts') is not null drop table #l_loc_hour_inserts
create table #l_loc_hour_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_loc_hour.bk_hash,
       stage_hash_loc_hour.hour_id hour_id,
       stage_hash_loc_hour.location_id location_id,
       stage_hash_loc_hour.val_hour_type_id val_hour_type_id,
       stage_hash_loc_hour.udw_dim_location_key udw_dim_location_key,
       isnull(cast(stage_hash_loc_hour.created_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_loc_hour.hour_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_loc_hour.location_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_loc_hour.val_hour_type_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_loc_hour.udw_dim_location_key,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_loc_hour
 where stage_hash_loc_hour.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_loc_hour records
set @insert_date_time = getdate()
insert into l_loc_hour (
       bk_hash,
       hour_id,
       location_id,
       val_hour_type_id,
       udw_dim_location_key,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_loc_hour_inserts.bk_hash,
       #l_loc_hour_inserts.hour_id,
       #l_loc_hour_inserts.location_id,
       #l_loc_hour_inserts.val_hour_type_id,
       #l_loc_hour_inserts.udw_dim_location_key,
       case when l_loc_hour.l_loc_hour_id is null then isnull(#l_loc_hour_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       50,
       #l_loc_hour_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_loc_hour_inserts
  left join p_loc_hour
    on #l_loc_hour_inserts.bk_hash = p_loc_hour.bk_hash
   and p_loc_hour.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_loc_hour
    on p_loc_hour.bk_hash = l_loc_hour.bk_hash
   and p_loc_hour.l_loc_hour_id = l_loc_hour.l_loc_hour_id
 where l_loc_hour.l_loc_hour_id is null
    or (l_loc_hour.l_loc_hour_id is not null
        and l_loc_hour.dv_hash <> #l_loc_hour_inserts.source_hash)

--calculate hash and lookup to current s_loc_hour
if object_id('tempdb..#s_loc_hour_inserts') is not null drop table #s_loc_hour_inserts
create table #s_loc_hour_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_loc_hour.bk_hash,
       stage_hash_loc_hour.hour_id hour_id,
       stage_hash_loc_hour.day_of_week day_of_week,
       stage_hash_loc_hour.start_time start_time,
       stage_hash_loc_hour.end_time end_time,
       stage_hash_loc_hour.hour_24 hour_24,
       stage_hash_loc_hour.sunrise sunrise,
       stage_hash_loc_hour.sunset sunset,
       stage_hash_loc_hour.closed closed,
       stage_hash_loc_hour.by_appointment_only by_appointment_only,
       stage_hash_loc_hour.created_date_time created_date_time,
       stage_hash_loc_hour.created_by created_by,
       stage_hash_loc_hour.deleted_date_time deleted_date_time,
       stage_hash_loc_hour.deleted_by deleted_by,
       stage_hash_loc_hour.last_updated_date_time last_updated_date_time,
       stage_hash_loc_hour.last_updated_by last_updated_by,
       isnull(cast(stage_hash_loc_hour.created_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_loc_hour.hour_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_loc_hour.day_of_week,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_loc_hour.start_time as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_loc_hour.end_time as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_loc_hour.hour_24 as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_loc_hour.sunrise as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_loc_hour.sunset as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_loc_hour.closed as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_loc_hour.by_appointment_only as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_loc_hour.created_date_time,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_loc_hour.created_by,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_loc_hour.deleted_date_time,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_loc_hour.deleted_by,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_loc_hour.last_updated_date_time,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_loc_hour.last_updated_by,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_loc_hour
 where stage_hash_loc_hour.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_loc_hour records
set @insert_date_time = getdate()
insert into s_loc_hour (
       bk_hash,
       hour_id,
       day_of_week,
       start_time,
       end_time,
       hour_24,
       sunrise,
       sunset,
       closed,
       by_appointment_only,
       created_date_time,
       created_by,
       deleted_date_time,
       deleted_by,
       last_updated_date_time,
       last_updated_by,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_loc_hour_inserts.bk_hash,
       #s_loc_hour_inserts.hour_id,
       #s_loc_hour_inserts.day_of_week,
       #s_loc_hour_inserts.start_time,
       #s_loc_hour_inserts.end_time,
       #s_loc_hour_inserts.hour_24,
       #s_loc_hour_inserts.sunrise,
       #s_loc_hour_inserts.sunset,
       #s_loc_hour_inserts.closed,
       #s_loc_hour_inserts.by_appointment_only,
       #s_loc_hour_inserts.created_date_time,
       #s_loc_hour_inserts.created_by,
       #s_loc_hour_inserts.deleted_date_time,
       #s_loc_hour_inserts.deleted_by,
       #s_loc_hour_inserts.last_updated_date_time,
       #s_loc_hour_inserts.last_updated_by,
       case when s_loc_hour.s_loc_hour_id is null then isnull(#s_loc_hour_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       50,
       #s_loc_hour_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_loc_hour_inserts
  left join p_loc_hour
    on #s_loc_hour_inserts.bk_hash = p_loc_hour.bk_hash
   and p_loc_hour.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_loc_hour
    on p_loc_hour.bk_hash = s_loc_hour.bk_hash
   and p_loc_hour.s_loc_hour_id = s_loc_hour.s_loc_hour_id
 where s_loc_hour.s_loc_hour_id is null
    or (s_loc_hour.s_loc_hour_id is not null
        and s_loc_hour.dv_hash <> #s_loc_hour_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_loc_hour @current_dv_batch_id

end
