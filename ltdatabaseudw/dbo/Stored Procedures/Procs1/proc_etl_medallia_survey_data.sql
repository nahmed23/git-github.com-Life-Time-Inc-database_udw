CREATE PROC [dbo].[proc_etl_medallia_survey_data] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_medallia_survey_data

set @insert_date_time = getdate()
insert into dbo.stage_hash_medallia_survey_data (
       bk_hash,
       survey_id,
       field_name,
       field_value,
       file_name,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(survey_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(field_name,'z#@$k%&P'))),2) bk_hash,
       survey_id,
       field_name,
       field_value,
       file_name,
       dummy_modified_date_time,
       isnull(cast(stage_medallia_survey_data.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_medallia_survey_data
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_medallia_survey_data @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_medallia_survey_data (
       bk_hash,
       survey_id,
       field_name,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_medallia_survey_data.bk_hash,
       stage_hash_medallia_survey_data.survey_id survey_id,
       stage_hash_medallia_survey_data.field_name field_name,
       isnull(cast(stage_hash_medallia_survey_data.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       44,
       @insert_date_time,
       @user
  from stage_hash_medallia_survey_data
  left join h_medallia_survey_data
    on stage_hash_medallia_survey_data.bk_hash = h_medallia_survey_data.bk_hash
 where h_medallia_survey_data_id is null
   and stage_hash_medallia_survey_data.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current s_medallia_survey_data
if object_id('tempdb..#s_medallia_survey_data_inserts') is not null drop table #s_medallia_survey_data_inserts
create table #s_medallia_survey_data_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_medallia_survey_data.bk_hash,
       stage_hash_medallia_survey_data.survey_id survey_id,
       stage_hash_medallia_survey_data.field_name field_name,
       stage_hash_medallia_survey_data.field_value field_value,
       stage_hash_medallia_survey_data.file_name file_name,
       stage_hash_medallia_survey_data.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_medallia_survey_data.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_medallia_survey_data.survey_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_medallia_survey_data.field_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_medallia_survey_data.field_value,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_medallia_survey_data.file_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_medallia_survey_data.dummy_modified_date_time,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_medallia_survey_data
 where stage_hash_medallia_survey_data.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_medallia_survey_data records
set @insert_date_time = getdate()
insert into s_medallia_survey_data (
       bk_hash,
       survey_id,
       field_name,
       field_value,
       file_name,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_medallia_survey_data_inserts.bk_hash,
       #s_medallia_survey_data_inserts.survey_id,
       #s_medallia_survey_data_inserts.field_name,
       #s_medallia_survey_data_inserts.field_value,
       #s_medallia_survey_data_inserts.file_name,
       #s_medallia_survey_data_inserts.dummy_modified_date_time,
       case when s_medallia_survey_data.s_medallia_survey_data_id is null then isnull(#s_medallia_survey_data_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       44,
       #s_medallia_survey_data_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_medallia_survey_data_inserts
  left join p_medallia_survey_data
    on #s_medallia_survey_data_inserts.bk_hash = p_medallia_survey_data.bk_hash
   and p_medallia_survey_data.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_medallia_survey_data
    on p_medallia_survey_data.bk_hash = s_medallia_survey_data.bk_hash
   and p_medallia_survey_data.s_medallia_survey_data_id = s_medallia_survey_data.s_medallia_survey_data_id
 where s_medallia_survey_data.s_medallia_survey_data_id is null
    or (s_medallia_survey_data.s_medallia_survey_data_id is not null
        and s_medallia_survey_data.dv_hash <> #s_medallia_survey_data_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_medallia_survey_data @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_medallia_survey_data @current_dv_batch_id

end
