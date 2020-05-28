CREATE PROC [dbo].[proc_etl_crmcloudsync_ltf_survey] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_crmcloudsync_LTF_Survey

set @insert_date_time = getdate()
insert into dbo.stage_hash_crmcloudsync_LTF_Survey (
       bk_hash,
       ltf_surveyid,
       ltf_name,
       ltf_surveytype,
       ltf_membernumber,
       ltf_employeeid,
       ltf_subscriber,
       ltf_surveytoolid,
       ltf_submittedon,
       ltf_source,
       statecode,
       statuscode,
       ltf_submittedby,
       createdon,
       createdby,
       modifiedon,
       modifiedby,
       InsertedDateTime,
       InsertUser,
       UpdatedDateTime,
       UpdateUser,
       ltf_connectmember,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(ltf_surveyid,'z#@$k%&P'))),2) bk_hash,
       ltf_surveyid,
       ltf_name,
       ltf_surveytype,
       ltf_membernumber,
       ltf_employeeid,
       ltf_subscriber,
       ltf_surveytoolid,
       ltf_submittedon,
       ltf_source,
       statecode,
       statuscode,
       ltf_submittedby,
       createdon,
       createdby,
       modifiedon,
       modifiedby,
       InsertedDateTime,
       InsertUser,
       UpdatedDateTime,
       UpdateUser,
       ltf_connectmember,
       isnull(cast(stage_crmcloudsync_LTF_Survey.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_crmcloudsync_LTF_Survey
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_crmcloudsync_ltf_survey @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_crmcloudsync_ltf_survey (
       bk_hash,
       ltf_survey_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_crmcloudsync_LTF_Survey.bk_hash,
       stage_hash_crmcloudsync_LTF_Survey.ltf_surveyid ltf_survey_id,
       isnull(cast(stage_hash_crmcloudsync_LTF_Survey.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       3,
       @insert_date_time,
       @user
  from stage_hash_crmcloudsync_LTF_Survey
  left join h_crmcloudsync_ltf_survey
    on stage_hash_crmcloudsync_LTF_Survey.bk_hash = h_crmcloudsync_ltf_survey.bk_hash
 where h_crmcloudsync_ltf_survey_id is null
   and stage_hash_crmcloudsync_LTF_Survey.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_crmcloudsync_ltf_survey
if object_id('tempdb..#l_crmcloudsync_ltf_survey_inserts') is not null drop table #l_crmcloudsync_ltf_survey_inserts
create table #l_crmcloudsync_ltf_survey_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_crmcloudsync_LTF_Survey.bk_hash,
       stage_hash_crmcloudsync_LTF_Survey.ltf_surveyid ltf_survey_id,
       stage_hash_crmcloudsync_LTF_Survey.ltf_subscriber ltf_subscriber,
       stage_hash_crmcloudsync_LTF_Survey.ltf_employeeid ltf_employee_id,
       stage_hash_crmcloudsync_LTF_Survey.ltf_surveytoolid ltf_survey_tool_id,
       stage_hash_crmcloudsync_LTF_Survey.createdby created_by,
       stage_hash_crmcloudsync_LTF_Survey.modifiedby modified_by,
       stage_hash_crmcloudsync_LTF_Survey.ltf_connectmember ltf_connect_member,
       stage_hash_crmcloudsync_LTF_Survey.ltf_membernumber ltf_member_number,
       stage_hash_crmcloudsync_LTF_Survey.ltf_submittedby ltf_submitted_by,
       isnull(cast(stage_hash_crmcloudsync_LTF_Survey.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Survey.ltf_surveyid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Survey.ltf_subscriber,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Survey.ltf_employeeid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Survey.ltf_surveytoolid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Survey.createdby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Survey.modifiedby,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Survey.ltf_connectmember,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Survey.ltf_membernumber,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Survey.ltf_submittedby,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_crmcloudsync_LTF_Survey
 where stage_hash_crmcloudsync_LTF_Survey.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_crmcloudsync_ltf_survey records
set @insert_date_time = getdate()
insert into l_crmcloudsync_ltf_survey (
       bk_hash,
       ltf_survey_id,
       ltf_subscriber,
       ltf_employee_id,
       ltf_survey_tool_id,
       created_by,
       modified_by,
       ltf_connect_member,
       ltf_member_number,
       ltf_submitted_by,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_crmcloudsync_ltf_survey_inserts.bk_hash,
       #l_crmcloudsync_ltf_survey_inserts.ltf_survey_id,
       #l_crmcloudsync_ltf_survey_inserts.ltf_subscriber,
       #l_crmcloudsync_ltf_survey_inserts.ltf_employee_id,
       #l_crmcloudsync_ltf_survey_inserts.ltf_survey_tool_id,
       #l_crmcloudsync_ltf_survey_inserts.created_by,
       #l_crmcloudsync_ltf_survey_inserts.modified_by,
       #l_crmcloudsync_ltf_survey_inserts.ltf_connect_member,
       #l_crmcloudsync_ltf_survey_inserts.ltf_member_number,
       #l_crmcloudsync_ltf_survey_inserts.ltf_submitted_by,
       case when l_crmcloudsync_ltf_survey.l_crmcloudsync_ltf_survey_id is null then isnull(#l_crmcloudsync_ltf_survey_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       3,
       #l_crmcloudsync_ltf_survey_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_crmcloudsync_ltf_survey_inserts
  left join p_crmcloudsync_ltf_survey
    on #l_crmcloudsync_ltf_survey_inserts.bk_hash = p_crmcloudsync_ltf_survey.bk_hash
   and p_crmcloudsync_ltf_survey.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_crmcloudsync_ltf_survey
    on p_crmcloudsync_ltf_survey.bk_hash = l_crmcloudsync_ltf_survey.bk_hash
   and p_crmcloudsync_ltf_survey.l_crmcloudsync_ltf_survey_id = l_crmcloudsync_ltf_survey.l_crmcloudsync_ltf_survey_id
 where l_crmcloudsync_ltf_survey.l_crmcloudsync_ltf_survey_id is null
    or (l_crmcloudsync_ltf_survey.l_crmcloudsync_ltf_survey_id is not null
        and l_crmcloudsync_ltf_survey.dv_hash <> #l_crmcloudsync_ltf_survey_inserts.source_hash)

--calculate hash and lookup to current s_crmcloudsync_ltf_survey
if object_id('tempdb..#s_crmcloudsync_ltf_survey_inserts') is not null drop table #s_crmcloudsync_ltf_survey_inserts
create table #s_crmcloudsync_ltf_survey_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_crmcloudsync_LTF_Survey.bk_hash,
       stage_hash_crmcloudsync_LTF_Survey.ltf_surveyid ltf_survey_id,
       stage_hash_crmcloudsync_LTF_Survey.ltf_name ltf_name,
       stage_hash_crmcloudsync_LTF_Survey.ltf_submittedon ltf_submitted_on,
       stage_hash_crmcloudsync_LTF_Survey.createdon created_on,
       stage_hash_crmcloudsync_LTF_Survey.modifiedon modified_on,
       stage_hash_crmcloudsync_LTF_Survey.InsertUser insert_user,
       stage_hash_crmcloudsync_LTF_Survey.InsertedDateTime inserted_date_time,
       stage_hash_crmcloudsync_LTF_Survey.UpdatedDateTime updated_date_time,
       stage_hash_crmcloudsync_LTF_Survey.UpdateUser update_user,
       stage_hash_crmcloudsync_LTF_Survey.ltf_source ltf_source,
       stage_hash_crmcloudsync_LTF_Survey.statuscode status_code,
       stage_hash_crmcloudsync_LTF_Survey.statecode state_code,
       stage_hash_crmcloudsync_LTF_Survey.ltf_surveytype ltf_survey_type,
       isnull(cast(stage_hash_crmcloudsync_LTF_Survey.InsertedDateTime as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Survey.ltf_surveyid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Survey.ltf_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_Survey.ltf_submittedon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_Survey.createdon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_Survey.modifiedon,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Survey.InsertUser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_Survey.InsertedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(convert(varchar,stage_hash_crmcloudsync_LTF_Survey.UpdatedDateTime,120),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_crmcloudsync_LTF_Survey.UpdateUser,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Survey.ltf_source as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Survey.statuscode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Survey.statecode as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_crmcloudsync_LTF_Survey.ltf_surveytype as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_crmcloudsync_LTF_Survey
 where stage_hash_crmcloudsync_LTF_Survey.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_crmcloudsync_ltf_survey records
set @insert_date_time = getdate()
insert into s_crmcloudsync_ltf_survey (
       bk_hash,
       ltf_survey_id,
       ltf_name,
       ltf_submitted_on,
       created_on,
       modified_on,
       insert_user,
       inserted_date_time,
       updated_date_time,
       update_user,
       ltf_source,
       status_code,
       state_code,
       ltf_survey_type,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_crmcloudsync_ltf_survey_inserts.bk_hash,
       #s_crmcloudsync_ltf_survey_inserts.ltf_survey_id,
       #s_crmcloudsync_ltf_survey_inserts.ltf_name,
       #s_crmcloudsync_ltf_survey_inserts.ltf_submitted_on,
       #s_crmcloudsync_ltf_survey_inserts.created_on,
       #s_crmcloudsync_ltf_survey_inserts.modified_on,
       #s_crmcloudsync_ltf_survey_inserts.insert_user,
       #s_crmcloudsync_ltf_survey_inserts.inserted_date_time,
       #s_crmcloudsync_ltf_survey_inserts.updated_date_time,
       #s_crmcloudsync_ltf_survey_inserts.update_user,
       #s_crmcloudsync_ltf_survey_inserts.ltf_source,
       #s_crmcloudsync_ltf_survey_inserts.status_code,
       #s_crmcloudsync_ltf_survey_inserts.state_code,
       #s_crmcloudsync_ltf_survey_inserts.ltf_survey_type,
       case when s_crmcloudsync_ltf_survey.s_crmcloudsync_ltf_survey_id is null then isnull(#s_crmcloudsync_ltf_survey_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       3,
       #s_crmcloudsync_ltf_survey_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_crmcloudsync_ltf_survey_inserts
  left join p_crmcloudsync_ltf_survey
    on #s_crmcloudsync_ltf_survey_inserts.bk_hash = p_crmcloudsync_ltf_survey.bk_hash
   and p_crmcloudsync_ltf_survey.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_crmcloudsync_ltf_survey
    on p_crmcloudsync_ltf_survey.bk_hash = s_crmcloudsync_ltf_survey.bk_hash
   and p_crmcloudsync_ltf_survey.s_crmcloudsync_ltf_survey_id = s_crmcloudsync_ltf_survey.s_crmcloudsync_ltf_survey_id
 where s_crmcloudsync_ltf_survey.s_crmcloudsync_ltf_survey_id is null
    or (s_crmcloudsync_ltf_survey.s_crmcloudsync_ltf_survey_id is not null
        and s_crmcloudsync_ltf_survey.dv_hash <> #s_crmcloudsync_ltf_survey_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_crmcloudsync_ltf_survey @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_crmcloudsync_ltf_survey @current_dv_batch_id

end
