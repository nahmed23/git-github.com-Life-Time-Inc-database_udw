CREATE PROC [dbo].[proc_etl_ec_workout_histories] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_ec_WorkoutHistories

set @insert_date_time = getdate()
insert into dbo.stage_hash_ec_WorkoutHistories (
       bk_hash,
       WorkoutHistoryId,
       WorkoutId,
       PartyId,
       IsCustom,
       DateCreated,
       DateStarted,
       DateEnded,
       IsStarted,
       Completed,
       Comments,
       Rating,
       [key],
       Type,
       IsActive,
       FatCalories,
       TotalCalories,
       SourceName,
       Description,
       ActivityType,
       Scheduled,
       Tracked,
       SourceWorkoutId,
       HeartRateZoneOneSeconds,
       HeartRateZoneTwoSeconds,
       HeartRateZoneThreeSeconds,
       HeartRateZoneFourSeconds,
       HeartRateZoneFiveSeconds,
       DistanceInMiles,
       AverageMilesPerHour,
       AverageWatts,
       AverageHeartRate,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(WorkoutHistoryId as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       WorkoutHistoryId,
       WorkoutId,
       PartyId,
       IsCustom,
       DateCreated,
       DateStarted,
       DateEnded,
       IsStarted,
       Completed,
       Comments,
       Rating,
       [key],
       Type,
       IsActive,
       FatCalories,
       TotalCalories,
       SourceName,
       Description,
       ActivityType,
       Scheduled,
       Tracked,
       SourceWorkoutId,
       HeartRateZoneOneSeconds,
       HeartRateZoneTwoSeconds,
       HeartRateZoneThreeSeconds,
       HeartRateZoneFourSeconds,
       HeartRateZoneFiveSeconds,
       DistanceInMiles,
       AverageMilesPerHour,
       AverageWatts,
       AverageHeartRate,
       isnull(cast(stage_ec_WorkoutHistories.datecreated as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_ec_WorkoutHistories
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_ec_workout_histories @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_ec_workout_histories (
       bk_hash,
       workout_history_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_ec_WorkoutHistories.bk_hash,
       stage_hash_ec_WorkoutHistories.WorkoutHistoryId workout_history_id,
       isnull(cast(stage_hash_ec_WorkoutHistories.datecreated as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       34,
       @insert_date_time,
       @user
  from stage_hash_ec_WorkoutHistories
  left join h_ec_workout_histories
    on stage_hash_ec_WorkoutHistories.bk_hash = h_ec_workout_histories.bk_hash
 where h_ec_workout_histories_id is null
   and stage_hash_ec_WorkoutHistories.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_ec_workout_histories
if object_id('tempdb..#l_ec_workout_histories_inserts') is not null drop table #l_ec_workout_histories_inserts
create table #l_ec_workout_histories_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ec_WorkoutHistories.bk_hash,
       stage_hash_ec_WorkoutHistories.WorkoutHistoryId workout_history_id,
       stage_hash_ec_WorkoutHistories.WorkoutId workout_id,
       stage_hash_ec_WorkoutHistories.PartyId party_id,
       isnull(cast(stage_hash_ec_WorkoutHistories.datecreated as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ec_WorkoutHistories.WorkoutHistoryId as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ec_WorkoutHistories.WorkoutId as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ec_WorkoutHistories.PartyId as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ec_WorkoutHistories
 where stage_hash_ec_WorkoutHistories.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_ec_workout_histories records
set @insert_date_time = getdate()
insert into l_ec_workout_histories (
       bk_hash,
       workout_history_id,
       workout_id,
       party_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_ec_workout_histories_inserts.bk_hash,
       #l_ec_workout_histories_inserts.workout_history_id,
       #l_ec_workout_histories_inserts.workout_id,
       #l_ec_workout_histories_inserts.party_id,
       case when l_ec_workout_histories.l_ec_workout_histories_id is null then isnull(#l_ec_workout_histories_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       34,
       #l_ec_workout_histories_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_ec_workout_histories_inserts
  left join p_ec_workout_histories
    on #l_ec_workout_histories_inserts.bk_hash = p_ec_workout_histories.bk_hash
   and p_ec_workout_histories.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_ec_workout_histories
    on p_ec_workout_histories.bk_hash = l_ec_workout_histories.bk_hash
   and p_ec_workout_histories.l_ec_workout_histories_id = l_ec_workout_histories.l_ec_workout_histories_id
 where l_ec_workout_histories.l_ec_workout_histories_id is null
    or (l_ec_workout_histories.l_ec_workout_histories_id is not null
        and l_ec_workout_histories.dv_hash <> #l_ec_workout_histories_inserts.source_hash)

--calculate hash and lookup to current s_ec_workout_histories
if object_id('tempdb..#s_ec_workout_histories_inserts') is not null drop table #s_ec_workout_histories_inserts
create table #s_ec_workout_histories_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ec_WorkoutHistories.bk_hash,
       stage_hash_ec_WorkoutHistories.WorkoutHistoryId workout_history_id,
       stage_hash_ec_WorkoutHistories.IsCustom is_custom,
       stage_hash_ec_WorkoutHistories.DateCreated date_created,
       stage_hash_ec_WorkoutHistories.DateStarted date_started,
       stage_hash_ec_WorkoutHistories.DateEnded date_ended,
       stage_hash_ec_WorkoutHistories.IsStarted is_started,
       stage_hash_ec_WorkoutHistories.Completed completed,
       stage_hash_ec_WorkoutHistories.Comments comments,
       stage_hash_ec_WorkoutHistories.Rating rating,
       stage_hash_ec_WorkoutHistories.[key] [key],
       stage_hash_ec_WorkoutHistories.Type type,
       stage_hash_ec_WorkoutHistories.IsActive is_active,
       stage_hash_ec_WorkoutHistories.FatCalories fat_calories,
       stage_hash_ec_WorkoutHistories.TotalCalories total_calories,
       stage_hash_ec_WorkoutHistories.SourceName source_name,
       stage_hash_ec_WorkoutHistories.Description description,
       stage_hash_ec_WorkoutHistories.ActivityType activity_type,
       stage_hash_ec_WorkoutHistories.Scheduled scheduled,
       stage_hash_ec_WorkoutHistories.Tracked tracked,
       stage_hash_ec_WorkoutHistories.SourceWorkoutId source_workout_id,
       stage_hash_ec_WorkoutHistories.HeartRateZoneOneSeconds heart_rate_zone_one_seconds,
       stage_hash_ec_WorkoutHistories.HeartRateZoneTwoSeconds heart_rate_zone_two_seconds,
       stage_hash_ec_WorkoutHistories.HeartRateZoneThreeSeconds heart_rate_zone_three_seconds,
       stage_hash_ec_WorkoutHistories.HeartRateZoneFourSeconds heart_rate_zone_four_seconds,
       stage_hash_ec_WorkoutHistories.HeartRateZoneFiveSeconds heart_rate_zone_five_seconds,
       stage_hash_ec_WorkoutHistories.DistanceInMiles distance_in_miles,
       stage_hash_ec_WorkoutHistories.AverageMilesPerHour average_miles_per_hour,
       stage_hash_ec_WorkoutHistories.AverageWatts average_watts,
       stage_hash_ec_WorkoutHistories.AverageHeartRate average_heart_rate,
       isnull(cast(stage_hash_ec_WorkoutHistories.datecreated as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_ec_WorkoutHistories.WorkoutHistoryId as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ec_WorkoutHistories.IsCustom as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_ec_WorkoutHistories.DateCreated,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_ec_WorkoutHistories.DateStarted,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_ec_WorkoutHistories.DateEnded,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ec_WorkoutHistories.IsStarted as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ec_WorkoutHistories.Completed as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ec_WorkoutHistories.Comments,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ec_WorkoutHistories.Rating as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ec_WorkoutHistories.[key],'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ec_WorkoutHistories.Type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ec_WorkoutHistories.IsActive as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ec_WorkoutHistories.FatCalories as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ec_WorkoutHistories.TotalCalories as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ec_WorkoutHistories.SourceName,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ec_WorkoutHistories.Description,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ec_WorkoutHistories.ActivityType,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ec_WorkoutHistories.Scheduled as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ec_WorkoutHistories.Tracked as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ec_WorkoutHistories.SourceWorkoutId,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ec_WorkoutHistories.HeartRateZoneOneSeconds as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ec_WorkoutHistories.HeartRateZoneTwoSeconds as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ec_WorkoutHistories.HeartRateZoneThreeSeconds as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ec_WorkoutHistories.HeartRateZoneFourSeconds as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ec_WorkoutHistories.HeartRateZoneFiveSeconds as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ec_WorkoutHistories.DistanceInMiles as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ec_WorkoutHistories.AverageMilesPerHour as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ec_WorkoutHistories.AverageWatts as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ec_WorkoutHistories.AverageHeartRate as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ec_WorkoutHistories
 where stage_hash_ec_WorkoutHistories.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_ec_workout_histories records
set @insert_date_time = getdate()
insert into s_ec_workout_histories (
       bk_hash,
       workout_history_id,
       is_custom,
       date_created,
       date_started,
       date_ended,
       is_started,
       completed,
       comments,
       rating,
       [key],
       type,
       is_active,
       fat_calories,
       total_calories,
       source_name,
       description,
       activity_type,
       scheduled,
       tracked,
       source_workout_id,
       heart_rate_zone_one_seconds,
       heart_rate_zone_two_seconds,
       heart_rate_zone_three_seconds,
       heart_rate_zone_four_seconds,
       heart_rate_zone_five_seconds,
       distance_in_miles,
       average_miles_per_hour,
       average_watts,
       average_heart_rate,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_ec_workout_histories_inserts.bk_hash,
       #s_ec_workout_histories_inserts.workout_history_id,
       #s_ec_workout_histories_inserts.is_custom,
       #s_ec_workout_histories_inserts.date_created,
       #s_ec_workout_histories_inserts.date_started,
       #s_ec_workout_histories_inserts.date_ended,
       #s_ec_workout_histories_inserts.is_started,
       #s_ec_workout_histories_inserts.completed,
       #s_ec_workout_histories_inserts.comments,
       #s_ec_workout_histories_inserts.rating,
       #s_ec_workout_histories_inserts.[key],
       #s_ec_workout_histories_inserts.type,
       #s_ec_workout_histories_inserts.is_active,
       #s_ec_workout_histories_inserts.fat_calories,
       #s_ec_workout_histories_inserts.total_calories,
       #s_ec_workout_histories_inserts.source_name,
       #s_ec_workout_histories_inserts.description,
       #s_ec_workout_histories_inserts.activity_type,
       #s_ec_workout_histories_inserts.scheduled,
       #s_ec_workout_histories_inserts.tracked,
       #s_ec_workout_histories_inserts.source_workout_id,
       #s_ec_workout_histories_inserts.heart_rate_zone_one_seconds,
       #s_ec_workout_histories_inserts.heart_rate_zone_two_seconds,
       #s_ec_workout_histories_inserts.heart_rate_zone_three_seconds,
       #s_ec_workout_histories_inserts.heart_rate_zone_four_seconds,
       #s_ec_workout_histories_inserts.heart_rate_zone_five_seconds,
       #s_ec_workout_histories_inserts.distance_in_miles,
       #s_ec_workout_histories_inserts.average_miles_per_hour,
       #s_ec_workout_histories_inserts.average_watts,
       #s_ec_workout_histories_inserts.average_heart_rate,
       case when s_ec_workout_histories.s_ec_workout_histories_id is null then isnull(#s_ec_workout_histories_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       34,
       #s_ec_workout_histories_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_ec_workout_histories_inserts
  left join p_ec_workout_histories
    on #s_ec_workout_histories_inserts.bk_hash = p_ec_workout_histories.bk_hash
   and p_ec_workout_histories.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_ec_workout_histories
    on p_ec_workout_histories.bk_hash = s_ec_workout_histories.bk_hash
   and p_ec_workout_histories.s_ec_workout_histories_id = s_ec_workout_histories.s_ec_workout_histories_id
 where s_ec_workout_histories.s_ec_workout_histories_id is null
    or (s_ec_workout_histories.s_ec_workout_histories_id is not null
        and s_ec_workout_histories.dv_hash <> #s_ec_workout_histories_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_ec_workout_histories @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_ec_workout_histories @current_dv_batch_id

end
