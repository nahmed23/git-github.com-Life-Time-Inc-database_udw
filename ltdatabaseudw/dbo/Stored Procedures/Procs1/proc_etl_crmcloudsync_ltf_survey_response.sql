CREATE PROC [dbo].[proc_etl_crmcloudsync_ltf_survey_response] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_crmcloudsync_LTF_SurveyResponse

set @insert_date_time = getdate()
insert into dbo.stage_hash_crmcloudsync_LTF_SurveyResponse (
       bk_hash,
       ltf_surveyresponseid,
       ltf_survey_response,
       ltf_survey,
       ltf_sequence,
       ltf_question,
       ltf_response,
       statecode,
       statuscode,
       createdon,
       createdby,
       modifiedon,
       modifiedby,
       InsertedDateTime,
       InsertUser,
       UpdatedDateTime,
       UpdateUser,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(ltf_surveyresponseid,'z#@$k%&P'))),2) bk_hash,
       ltf_surveyresponseid,
       ltf_survey_response,
       ltf_survey,
       ltf_sequence,
       ltf_question,
       ltf_response,
       statecode,
       statuscode,
       createdon,
       createdby,
       modifiedon,
       modifiedby,
       InsertedDateTime,
       InsertUser,
       UpdatedDateTime,
       UpdateUser,
       isnull(cast(stage_crmcloudsync_LTF_SurveyResponse.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_crmcloudsync_LTF_SurveyResponse
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_crmcloudsync_ltf_survey_response @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_crmcloudsync_ltf_survey_response (
       bk_hash,
       ltf_survey_response_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_crmcloudsync_LTF_SurveyResponse.bk_hash,
       stage_hash_crmcloudsync_LTF_SurveyResponse.ltf_surveyresponseid ltf_survey_response_id,
       isnull(cast(stage_hash_crmcloudsync_LTF_SurveyResponse.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       3,
       @insert_date_time,
       @user
  from stage_hash_crmcloudsync_LTF_SurveyResponse
  left join h_crmcloudsync_ltf_survey_response
    on stage_hash_crmcloudsync_LTF_SurveyResponse.bk_hash = h_crmcloudsync_ltf_survey_response.bk_hash
 where h_crmcloudsync_ltf_survey_response_id is null
   and stage_hash_crmcloudsync_LTF_SurveyResponse.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_crmcloudsync_ltf_survey_response
if object_id('tempdb..#l_crmcloudsync_ltf_survey_response_inserts') is not null drop table #l_crmcloudsync_ltf_survey_response_inserts
create table #l_crmcloudsync_ltf_survey_response_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_crmcloudsync_LTF_SurveyResponse.bk_hash,
       stage_hash_crmcloudsync_LTF_SurveyResponse.ltf_surveyresponseid ltf_survey_response_id,
       stage_hash_crmcloudsync_LTF_SurveyResponse.ltf_survey ltf_survey,
       stage_hash_crmcloudsync_LTF_SurveyResponse.createdby created_by,
       stage_hash_crmcloudsync_LTF_SurveyResponse.modifiedby modified_by,
       isnull(cast(stage_hash_crmcloudsync_LTF_SurveyResponse.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_SurveyResponse.ltf_surveyresponseid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_SurveyResponse.ltf_survey,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_SurveyResponse.createdby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_SurveyResponse.modifiedby,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_crmcloudsync_LTF_SurveyResponse
 where stage_hash_crmcloudsync_LTF_SurveyResponse.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_crmcloudsync_ltf_survey_response records
set @insert_date_time = getdate()
insert into l_crmcloudsync_ltf_survey_response (
       bk_hash,
       ltf_survey_response_id,
       ltf_survey,
       created_by,
       modified_by,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_crmcloudsync_ltf_survey_response_inserts.bk_hash,
       #l_crmcloudsync_ltf_survey_response_inserts.ltf_survey_response_id,
       #l_crmcloudsync_ltf_survey_response_inserts.ltf_survey,
       #l_crmcloudsync_ltf_survey_response_inserts.created_by,
       #l_crmcloudsync_ltf_survey_response_inserts.modified_by,
       case when l_crmcloudsync_ltf_survey_response.l_crmcloudsync_ltf_survey_response_id is null then isnull(#l_crmcloudsync_ltf_survey_response_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       3,
       #l_crmcloudsync_ltf_survey_response_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_crmcloudsync_ltf_survey_response_inserts
  left join p_crmcloudsync_ltf_survey_response
    on #l_crmcloudsync_ltf_survey_response_inserts.bk_hash = p_crmcloudsync_ltf_survey_response.bk_hash
   and p_crmcloudsync_ltf_survey_response.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_crmcloudsync_ltf_survey_response
    on p_crmcloudsync_ltf_survey_response.bk_hash = l_crmcloudsync_ltf_survey_response.bk_hash
   and p_crmcloudsync_ltf_survey_response.l_crmcloudsync_ltf_survey_response_id = l_crmcloudsync_ltf_survey_response.l_crmcloudsync_ltf_survey_response_id
 where l_crmcloudsync_ltf_survey_response.l_crmcloudsync_ltf_survey_response_id is null
    or (l_crmcloudsync_ltf_survey_response.l_crmcloudsync_ltf_survey_response_id is not null
        and l_crmcloudsync_ltf_survey_response.dv_hash <> #l_crmcloudsync_ltf_survey_response_inserts.source_hash)

--calculate hash and lookup to current s_crmcloudsync_ltf_survey_response
if object_id('tempdb..#s_crmcloudsync_ltf_survey_response_inserts') is not null drop table #s_crmcloudsync_ltf_survey_response_inserts
create table #s_crmcloudsync_ltf_survey_response_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_crmcloudsync_LTF_SurveyResponse.bk_hash,
       stage_hash_crmcloudsync_LTF_SurveyResponse.ltf_surveyresponseid ltf_survey_response_id,
       stage_hash_crmcloudsync_LTF_SurveyResponse.ltf_survey_response ltf_survey_response,
       stage_hash_crmcloudsync_LTF_SurveyResponse.ltf_sequence ltf_sequence,
       stage_hash_crmcloudsync_LTF_SurveyResponse.ltf_question ltf_question,
       stage_hash_crmcloudsync_LTF_SurveyResponse.ltf_response ltf_response,
       stage_hash_crmcloudsync_LTF_SurveyResponse.createdon created_on,
       stage_hash_crmcloudsync_LTF_SurveyResponse.modifiedon modified_on,
       stage_hash_crmcloudsync_LTF_SurveyResponse.InsertedDateTime inserted_date_time,
       stage_hash_crmcloudsync_LTF_SurveyResponse.InsertUser insert_user,
       stage_hash_crmcloudsync_LTF_SurveyResponse.UpdatedDateTime updated_date_time,
       stage_hash_crmcloudsync_LTF_SurveyResponse.UpdateUser update_user,
       stage_hash_crmcloudsync_LTF_SurveyResponse.statecode state_code,
       stage_hash_crmcloudsync_LTF_SurveyResponse.statuscode status_code,
       isnull(cast(stage_hash_crmcloudsync_LTF_SurveyResponse.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_SurveyResponse.ltf_surveyresponseid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_SurveyResponse.ltf_survey_response,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_SurveyResponse.ltf_sequence as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_SurveyResponse.ltf_question,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_SurveyResponse.ltf_response,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_SurveyResponse.createdon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_SurveyResponse.modifiedon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_SurveyResponse.InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_SurveyResponse.InsertUser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_SurveyResponse.UpdatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_SurveyResponse.UpdateUser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_SurveyResponse.statecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_SurveyResponse.statuscode as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_crmcloudsync_LTF_SurveyResponse
 where stage_hash_crmcloudsync_LTF_SurveyResponse.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_crmcloudsync_ltf_survey_response records
set @insert_date_time = getdate()
insert into s_crmcloudsync_ltf_survey_response (
       bk_hash,
       ltf_survey_response_id,
       ltf_survey_response,
       ltf_sequence,
       ltf_question,
       ltf_response,
       created_on,
       modified_on,
       inserted_date_time,
       insert_user,
       updated_date_time,
       update_user,
       state_code,
       status_code,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_crmcloudsync_ltf_survey_response_inserts.bk_hash,
       #s_crmcloudsync_ltf_survey_response_inserts.ltf_survey_response_id,
       #s_crmcloudsync_ltf_survey_response_inserts.ltf_survey_response,
       #s_crmcloudsync_ltf_survey_response_inserts.ltf_sequence,
       #s_crmcloudsync_ltf_survey_response_inserts.ltf_question,
       #s_crmcloudsync_ltf_survey_response_inserts.ltf_response,
       #s_crmcloudsync_ltf_survey_response_inserts.created_on,
       #s_crmcloudsync_ltf_survey_response_inserts.modified_on,
       #s_crmcloudsync_ltf_survey_response_inserts.inserted_date_time,
       #s_crmcloudsync_ltf_survey_response_inserts.insert_user,
       #s_crmcloudsync_ltf_survey_response_inserts.updated_date_time,
       #s_crmcloudsync_ltf_survey_response_inserts.update_user,
       #s_crmcloudsync_ltf_survey_response_inserts.state_code,
       #s_crmcloudsync_ltf_survey_response_inserts.status_code,
       case when s_crmcloudsync_ltf_survey_response.s_crmcloudsync_ltf_survey_response_id is null then isnull(#s_crmcloudsync_ltf_survey_response_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       3,
       #s_crmcloudsync_ltf_survey_response_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_crmcloudsync_ltf_survey_response_inserts
  left join p_crmcloudsync_ltf_survey_response
    on #s_crmcloudsync_ltf_survey_response_inserts.bk_hash = p_crmcloudsync_ltf_survey_response.bk_hash
   and p_crmcloudsync_ltf_survey_response.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_crmcloudsync_ltf_survey_response
    on p_crmcloudsync_ltf_survey_response.bk_hash = s_crmcloudsync_ltf_survey_response.bk_hash
   and p_crmcloudsync_ltf_survey_response.s_crmcloudsync_ltf_survey_response_id = s_crmcloudsync_ltf_survey_response.s_crmcloudsync_ltf_survey_response_id
 where s_crmcloudsync_ltf_survey_response.s_crmcloudsync_ltf_survey_response_id is null
    or (s_crmcloudsync_ltf_survey_response.s_crmcloudsync_ltf_survey_response_id is not null
        and s_crmcloudsync_ltf_survey_response.dv_hash <> #s_crmcloudsync_ltf_survey_response_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_crmcloudsync_ltf_survey_response @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_crmcloudsync_ltf_survey_response @current_dv_batch_id

end
