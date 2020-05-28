CREATE PROC [dbo].[proc_etl_ec_measurements] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_ec_Measurements

set @insert_date_time = getdate()
insert into dbo.stage_hash_ec_Measurements (
       bk_hash,
       MeasurementId,
       MeasurementRecordingId,
       MeasureValue,
       MeasuresId,
       Unit,
       jan_one,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(MeasurementId,'z#@$k%&P'))),2) bk_hash,
       MeasurementId,
       MeasurementRecordingId,
       MeasureValue,
       MeasuresId,
       Unit,
       jan_one,
       isnull(cast(stage_ec_Measurements.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_ec_Measurements
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_ec_measurements @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_ec_measurements (
       bk_hash,
       measurement_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_ec_Measurements.bk_hash,
       stage_hash_ec_Measurements.MeasurementId measurement_id,
       isnull(cast(stage_hash_ec_Measurements.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       34,
       @insert_date_time,
       @user
  from stage_hash_ec_Measurements
  left join h_ec_measurements
    on stage_hash_ec_Measurements.bk_hash = h_ec_measurements.bk_hash
 where h_ec_measurements_id is null
   and stage_hash_ec_Measurements.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_ec_measurements
if object_id('tempdb..#l_ec_measurements_inserts') is not null drop table #l_ec_measurements_inserts
create table #l_ec_measurements_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ec_Measurements.bk_hash,
       stage_hash_ec_Measurements.MeasurementId measurement_id,
       stage_hash_ec_Measurements.MeasurementRecordingId measurement_recording_id,
       stage_hash_ec_Measurements.MeasuresId measures_id,
       isnull(cast(stage_hash_ec_Measurements.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_ec_Measurements.MeasurementId,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ec_Measurements.MeasurementRecordingId,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ec_Measurements.MeasuresId,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ec_Measurements
 where stage_hash_ec_Measurements.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_ec_measurements records
set @insert_date_time = getdate()
insert into l_ec_measurements (
       bk_hash,
       measurement_id,
       measurement_recording_id,
       measures_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_ec_measurements_inserts.bk_hash,
       #l_ec_measurements_inserts.measurement_id,
       #l_ec_measurements_inserts.measurement_recording_id,
       #l_ec_measurements_inserts.measures_id,
       case when l_ec_measurements.l_ec_measurements_id is null then isnull(#l_ec_measurements_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       34,
       #l_ec_measurements_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_ec_measurements_inserts
  left join p_ec_measurements
    on #l_ec_measurements_inserts.bk_hash = p_ec_measurements.bk_hash
   and p_ec_measurements.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_ec_measurements
    on p_ec_measurements.bk_hash = l_ec_measurements.bk_hash
   and p_ec_measurements.l_ec_measurements_id = l_ec_measurements.l_ec_measurements_id
 where l_ec_measurements.l_ec_measurements_id is null
    or (l_ec_measurements.l_ec_measurements_id is not null
        and l_ec_measurements.dv_hash <> #l_ec_measurements_inserts.source_hash)

--calculate hash and lookup to current s_ec_measurements
if object_id('tempdb..#s_ec_measurements_inserts') is not null drop table #s_ec_measurements_inserts
create table #s_ec_measurements_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ec_Measurements.bk_hash,
       stage_hash_ec_Measurements.MeasurementId measurement_id,
       stage_hash_ec_Measurements.MeasureValue measure_value,
       stage_hash_ec_Measurements.Unit unit,
       stage_hash_ec_Measurements.jan_one jan_one,
       isnull(cast(stage_hash_ec_Measurements.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_ec_Measurements.MeasurementId,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ec_Measurements.MeasureValue,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ec_Measurements.Unit,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_ec_Measurements.jan_one,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ec_Measurements
 where stage_hash_ec_Measurements.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_ec_measurements records
set @insert_date_time = getdate()
insert into s_ec_measurements (
       bk_hash,
       measurement_id,
       measure_value,
       unit,
       jan_one,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_ec_measurements_inserts.bk_hash,
       #s_ec_measurements_inserts.measurement_id,
       #s_ec_measurements_inserts.measure_value,
       #s_ec_measurements_inserts.unit,
       #s_ec_measurements_inserts.jan_one,
       case when s_ec_measurements.s_ec_measurements_id is null then isnull(#s_ec_measurements_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       34,
       #s_ec_measurements_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_ec_measurements_inserts
  left join p_ec_measurements
    on #s_ec_measurements_inserts.bk_hash = p_ec_measurements.bk_hash
   and p_ec_measurements.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_ec_measurements
    on p_ec_measurements.bk_hash = s_ec_measurements.bk_hash
   and p_ec_measurements.s_ec_measurements_id = s_ec_measurements.s_ec_measurements_id
 where s_ec_measurements.s_ec_measurements_id is null
    or (s_ec_measurements.s_ec_measurements_id is not null
        and s_ec_measurements.dv_hash <> #s_ec_measurements_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_ec_measurements @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_ec_measurements @current_dv_batch_id

end
