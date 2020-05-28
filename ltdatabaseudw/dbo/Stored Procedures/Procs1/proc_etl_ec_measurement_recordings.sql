CREATE PROC [dbo].[proc_etl_ec_measurement_recordings] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_ec_MeasurementRecordings

set @insert_date_time = getdate()
insert into dbo.stage_hash_ec_MeasurementRecordings (
       bk_hash,
       MeasurementRecordingId,
       PartyId,
       ClubId,
       UserProgramStatusId,
       MeasureDate,
       Notes,
       Source,
       Active,
       Certified,
       CreatedBy,
       CreatedDate,
       ModifiedBy,
       ModifiedDate,
       MetaData,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(MeasurementRecordingId,'z#@$k%&P'))),2) bk_hash,
       MeasurementRecordingId,
       PartyId,
       ClubId,
       UserProgramStatusId,
       MeasureDate,
       Notes,
       Source,
       Active,
       Certified,
       CreatedBy,
       CreatedDate,
       ModifiedBy,
       ModifiedDate,
       MetaData,
       isnull(cast(stage_ec_MeasurementRecordings.CreatedDate as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_ec_MeasurementRecordings
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_ec_measurement_recordings @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_ec_measurement_recordings (
       bk_hash,
       measurement_recording_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_ec_MeasurementRecordings.bk_hash,
       stage_hash_ec_MeasurementRecordings.MeasurementRecordingId measurement_recording_id,
       isnull(cast(stage_hash_ec_MeasurementRecordings.CreatedDate as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       34,
       @insert_date_time,
       @user
  from stage_hash_ec_MeasurementRecordings
  left join h_ec_measurement_recordings
    on stage_hash_ec_MeasurementRecordings.bk_hash = h_ec_measurement_recordings.bk_hash
 where h_ec_measurement_recordings_id is null
   and stage_hash_ec_MeasurementRecordings.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_ec_measurement_recordings
if object_id('tempdb..#l_ec_measurement_recordings_inserts') is not null drop table #l_ec_measurement_recordings_inserts
create table #l_ec_measurement_recordings_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ec_MeasurementRecordings.bk_hash,
       stage_hash_ec_MeasurementRecordings.MeasurementRecordingId measurement_recording_id,
       stage_hash_ec_MeasurementRecordings.PartyId party_id,
       stage_hash_ec_MeasurementRecordings.ClubId club_id,
       stage_hash_ec_MeasurementRecordings.UserProgramStatusId user_program_status_id,
       isnull(cast(stage_hash_ec_MeasurementRecordings.CreatedDate as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_ec_MeasurementRecordings.MeasurementRecordingId,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ec_MeasurementRecordings.PartyId as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ec_MeasurementRecordings.ClubId as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ec_MeasurementRecordings.UserProgramStatusId,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ec_MeasurementRecordings
 where stage_hash_ec_MeasurementRecordings.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_ec_measurement_recordings records
set @insert_date_time = getdate()
insert into l_ec_measurement_recordings (
       bk_hash,
       measurement_recording_id,
       party_id,
       club_id,
       user_program_status_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_ec_measurement_recordings_inserts.bk_hash,
       #l_ec_measurement_recordings_inserts.measurement_recording_id,
       #l_ec_measurement_recordings_inserts.party_id,
       #l_ec_measurement_recordings_inserts.club_id,
       #l_ec_measurement_recordings_inserts.user_program_status_id,
       case when l_ec_measurement_recordings.l_ec_measurement_recordings_id is null then isnull(#l_ec_measurement_recordings_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       34,
       #l_ec_measurement_recordings_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_ec_measurement_recordings_inserts
  left join p_ec_measurement_recordings
    on #l_ec_measurement_recordings_inserts.bk_hash = p_ec_measurement_recordings.bk_hash
   and p_ec_measurement_recordings.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_ec_measurement_recordings
    on p_ec_measurement_recordings.bk_hash = l_ec_measurement_recordings.bk_hash
   and p_ec_measurement_recordings.l_ec_measurement_recordings_id = l_ec_measurement_recordings.l_ec_measurement_recordings_id
 where l_ec_measurement_recordings.l_ec_measurement_recordings_id is null
    or (l_ec_measurement_recordings.l_ec_measurement_recordings_id is not null
        and l_ec_measurement_recordings.dv_hash <> #l_ec_measurement_recordings_inserts.source_hash)

--calculate hash and lookup to current s_ec_measurement_recordings
if object_id('tempdb..#s_ec_measurement_recordings_inserts') is not null drop table #s_ec_measurement_recordings_inserts
create table #s_ec_measurement_recordings_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ec_MeasurementRecordings.bk_hash,
       stage_hash_ec_MeasurementRecordings.MeasurementRecordingId measurement_recording_id,
       stage_hash_ec_MeasurementRecordings.MeasureDate measure_date,
       stage_hash_ec_MeasurementRecordings.Notes notes,
       stage_hash_ec_MeasurementRecordings.Source source,
       stage_hash_ec_MeasurementRecordings.Active active,
       stage_hash_ec_MeasurementRecordings.Certified certified,
       stage_hash_ec_MeasurementRecordings.CreatedBy created_by,
       stage_hash_ec_MeasurementRecordings.CreatedDate created_date,
       stage_hash_ec_MeasurementRecordings.ModifiedBy modified_by,
       stage_hash_ec_MeasurementRecordings.ModifiedDate modified_date,
       stage_hash_ec_MeasurementRecordings.MetaData metadata,
       isnull(cast(stage_hash_ec_MeasurementRecordings.CreatedDate as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_ec_MeasurementRecordings.MeasurementRecordingId,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_ec_MeasurementRecordings.MeasureDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ec_MeasurementRecordings.Notes,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ec_MeasurementRecordings.Source,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ec_MeasurementRecordings.Active as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ec_MeasurementRecordings.Certified as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ec_MeasurementRecordings.CreatedBy as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_ec_MeasurementRecordings.CreatedDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ec_MeasurementRecordings.ModifiedBy as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_ec_MeasurementRecordings.ModifiedDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ec_MeasurementRecordings.MetaData,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ec_MeasurementRecordings
 where stage_hash_ec_MeasurementRecordings.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_ec_measurement_recordings records
set @insert_date_time = getdate()
insert into s_ec_measurement_recordings (
       bk_hash,
       measurement_recording_id,
       measure_date,
       notes,
       source,
       active,
       certified,
       created_by,
       created_date,
       modified_by,
       modified_date,
       metadata,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_ec_measurement_recordings_inserts.bk_hash,
       #s_ec_measurement_recordings_inserts.measurement_recording_id,
       #s_ec_measurement_recordings_inserts.measure_date,
       #s_ec_measurement_recordings_inserts.notes,
       #s_ec_measurement_recordings_inserts.source,
       #s_ec_measurement_recordings_inserts.active,
       #s_ec_measurement_recordings_inserts.certified,
       #s_ec_measurement_recordings_inserts.created_by,
       #s_ec_measurement_recordings_inserts.created_date,
       #s_ec_measurement_recordings_inserts.modified_by,
       #s_ec_measurement_recordings_inserts.modified_date,
       #s_ec_measurement_recordings_inserts.metadata,
       case when s_ec_measurement_recordings.s_ec_measurement_recordings_id is null then isnull(#s_ec_measurement_recordings_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       34,
       #s_ec_measurement_recordings_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_ec_measurement_recordings_inserts
  left join p_ec_measurement_recordings
    on #s_ec_measurement_recordings_inserts.bk_hash = p_ec_measurement_recordings.bk_hash
   and p_ec_measurement_recordings.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_ec_measurement_recordings
    on p_ec_measurement_recordings.bk_hash = s_ec_measurement_recordings.bk_hash
   and p_ec_measurement_recordings.s_ec_measurement_recordings_id = s_ec_measurement_recordings.s_ec_measurement_recordings_id
 where s_ec_measurement_recordings.s_ec_measurement_recordings_id is null
    or (s_ec_measurement_recordings.s_ec_measurement_recordings_id is not null
        and s_ec_measurement_recordings.dv_hash <> #s_ec_measurement_recordings_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_ec_measurement_recordings @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_ec_measurement_recordings @current_dv_batch_id

end
