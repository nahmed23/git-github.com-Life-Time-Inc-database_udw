CREATE PROC [dbo].[proc_etl_ec_measures] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_ec_Measures

set @insert_date_time = getdate()
insert into dbo.stage_hash_ec_Measures (
       bk_hash,
       MeasuresId,
       Slug,
       Title,
       Tags,
       Description,
       Unit,
       MeasureValueType,
       ExtendedMetadata,
       Gender,
       OptimumRangeMale,
       OptimumRangeFemale,
       DiagonosticRangeMale,
       DiagonosticRangeFemale,
       CreatedBy,
       CreatedDate,
       ModifiedBy,
       ModifiedDate,
       MeasurementType,
       MeasurementInstructionsLocation,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(MeasuresId,'z#@$k%&P'))),2) bk_hash,
       MeasuresId,
       Slug,
       Title,
       Tags,
       Description,
       Unit,
       MeasureValueType,
       ExtendedMetadata,
       Gender,
       OptimumRangeMale,
       OptimumRangeFemale,
       DiagonosticRangeMale,
       DiagonosticRangeFemale,
       CreatedBy,
       CreatedDate,
       ModifiedBy,
       ModifiedDate,
       MeasurementType,
       MeasurementInstructionsLocation,
       isnull(cast(stage_ec_Measures.CreatedDate as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_ec_Measures
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_ec_measures @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_ec_measures (
       bk_hash,
       measures_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_ec_Measures.bk_hash,
       stage_hash_ec_Measures.MeasuresId measures_id,
       isnull(cast(stage_hash_ec_Measures.CreatedDate as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       34,
       @insert_date_time,
       @user
  from stage_hash_ec_Measures
  left join h_ec_measures
    on stage_hash_ec_Measures.bk_hash = h_ec_measures.bk_hash
 where h_ec_measures_id is null
   and stage_hash_ec_Measures.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current s_ec_measures
if object_id('tempdb..#s_ec_measures_inserts') is not null drop table #s_ec_measures_inserts
create table #s_ec_measures_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_ec_Measures.bk_hash,
       stage_hash_ec_Measures.MeasuresId measures_id,
       stage_hash_ec_Measures.Slug slug,
       stage_hash_ec_Measures.Title title,
       stage_hash_ec_Measures.Tags tags,
       stage_hash_ec_Measures.Description description,
       stage_hash_ec_Measures.Unit unit,
       stage_hash_ec_Measures.MeasureValueType measure_value_type,
       stage_hash_ec_Measures.ExtendedMetadata extended_metadata,
       stage_hash_ec_Measures.Gender gender,
       stage_hash_ec_Measures.OptimumRangeMale optimum_range_male,
       stage_hash_ec_Measures.OptimumRangeFemale optimum_range_female,
       stage_hash_ec_Measures.DiagonosticRangeMale diagonostic_range_male,
       stage_hash_ec_Measures.DiagonosticRangeFemale diagonostic_range_female,
       stage_hash_ec_Measures.CreatedBy created_by,
       stage_hash_ec_Measures.CreatedDate created_date,
       stage_hash_ec_Measures.ModifiedBy modified_by,
       stage_hash_ec_Measures.ModifiedDate modified_date,
       stage_hash_ec_Measures.MeasurementType measurement_type,
       stage_hash_ec_Measures.MeasurementInstructionsLocation measurement_instructions_location,
       isnull(cast(stage_hash_ec_Measures.CreatedDate as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_ec_Measures.MeasuresId,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ec_Measures.Slug,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ec_Measures.Title,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ec_Measures.Tags,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ec_Measures.Description,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ec_Measures.Unit,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ec_Measures.MeasureValueType as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ec_Measures.ExtendedMetadata,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ec_Measures.Gender,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ec_Measures.OptimumRangeMale,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ec_Measures.OptimumRangeFemale,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ec_Measures.DiagonosticRangeMale,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ec_Measures.DiagonosticRangeFemale,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ec_Measures.CreatedBy as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_ec_Measures.CreatedDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ec_Measures.ModifiedBy as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_ec_Measures.ModifiedDate,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_ec_Measures.MeasurementType as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_ec_Measures.MeasurementInstructionsLocation,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_ec_Measures
 where stage_hash_ec_Measures.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_ec_measures records
set @insert_date_time = getdate()
insert into s_ec_measures (
       bk_hash,
       measures_id,
       slug,
       title,
       tags,
       description,
       unit,
       measure_value_type,
       extended_metadata,
       gender,
       optimum_range_male,
       optimum_range_female,
       diagonostic_range_male,
       diagonostic_range_female,
       created_by,
       created_date,
       modified_by,
       modified_date,
       measurement_type,
       measurement_instructions_location,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_ec_measures_inserts.bk_hash,
       #s_ec_measures_inserts.measures_id,
       #s_ec_measures_inserts.slug,
       #s_ec_measures_inserts.title,
       #s_ec_measures_inserts.tags,
       #s_ec_measures_inserts.description,
       #s_ec_measures_inserts.unit,
       #s_ec_measures_inserts.measure_value_type,
       #s_ec_measures_inserts.extended_metadata,
       #s_ec_measures_inserts.gender,
       #s_ec_measures_inserts.optimum_range_male,
       #s_ec_measures_inserts.optimum_range_female,
       #s_ec_measures_inserts.diagonostic_range_male,
       #s_ec_measures_inserts.diagonostic_range_female,
       #s_ec_measures_inserts.created_by,
       #s_ec_measures_inserts.created_date,
       #s_ec_measures_inserts.modified_by,
       #s_ec_measures_inserts.modified_date,
       #s_ec_measures_inserts.measurement_type,
       #s_ec_measures_inserts.measurement_instructions_location,
       case when s_ec_measures.s_ec_measures_id is null then isnull(#s_ec_measures_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       34,
       #s_ec_measures_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_ec_measures_inserts
  left join p_ec_measures
    on #s_ec_measures_inserts.bk_hash = p_ec_measures.bk_hash
   and p_ec_measures.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_ec_measures
    on p_ec_measures.bk_hash = s_ec_measures.bk_hash
   and p_ec_measures.s_ec_measures_id = s_ec_measures.s_ec_measures_id
 where s_ec_measures.s_ec_measures_id is null
    or (s_ec_measures.s_ec_measures_id is not null
        and s_ec_measures.dv_hash <> #s_ec_measures_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_ec_measures @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_ec_measures @current_dv_batch_id

end
