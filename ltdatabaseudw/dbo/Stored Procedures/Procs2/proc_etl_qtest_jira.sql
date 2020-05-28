CREATE PROC [dbo].[proc_etl_qtest_jira] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_qtest_jira

set @insert_date_time = getdate()
insert into dbo.stage_hash_qtest_jira (
       bk_hash,
       project_id,
       project_name,
       project_description,
       project_status_id,
       project_start_date,
       project_sample,
       project_x_explorer_access_level,
       project_automation,
       project_template_id,
       project_uuid,
       release_project_id,
       release_id,
       release_name,
       release_order,
       release_pid,
       release_created_date,
       release_last_modified_date,
       release_start_date,
       release_end_date,
       release_properties_start_date,
       release_properties_end_date,
       requirement_project_id,
       requirement_jira_issue,
       requirement_jira_priority,
       requirement_jira_status,
       requirement_jira_story_point,
       requirement_jira_defect_id,
       requirement_id,
       requirement_name,
       requirement_pid,
       requirement_created_date,
       requirement_parent_id,
       test_case_id,
       test_case_name,
       test_case_order,
       test_case_pid,
       test_case_created_date,
       test_case_last_modified_date,
       test_case_parent_id,
       test_case_test_case_version_id,
       test_case_version,
       test_case_description,
       test_case_creator_id,
       test_case_properties_automation_field_value_name,
       test_case_properties_automation_content_field_value,
       test_case_properties_status_field_value_name,
       test_case_properties_type_field_value_name,
       test_case_properties_assigned_to_field_value_name,
       test_case_properties_description_field_value,
       test_case_properties_precondition_field_value,
       test_case_properties_test_tier_field_value_name,
       test_case_properties_zephyr_issue_key_field_value,
       test_case_properties_candidate_for_automation_field_value_name,
       test_case_properties_platform_field_value_name,
       test_case_properties_test_data_field_value,
       test_case_project_id,
       test_case_properties_zephyr_test_steps_field_value,
       test_case_properties_shared_field_value,
       dummy_modified_date_time,
       project_date_format,
       release_properties_status,
       requirement_last_modified_date,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(project_id,'z#@$k%&P')+'P%#&z$@k'+isnull(release_id,'z#@$k%&P')+'P%#&z$@k'+isnull(requirement_id,'z#@$k%&P')+'P%#&z$@k'+isnull(test_case_id,'z#@$k%&P'))),2) bk_hash,
       project_id,
       project_name,
       project_description,
       project_status_id,
       project_start_date,
       project_sample,
       project_x_explorer_access_level,
       project_automation,
       project_template_id,
       project_uuid,
       release_project_id,
       release_id,
       release_name,
       release_order,
       release_pid,
       release_created_date,
       release_last_modified_date,
       release_start_date,
       release_end_date,
       release_properties_start_date,
       release_properties_end_date,
       requirement_project_id,
       requirement_jira_issue,
       requirement_jira_priority,
       requirement_jira_status,
       requirement_jira_story_point,
       requirement_jira_defect_id,
       requirement_id,
       requirement_name,
       requirement_pid,
       requirement_created_date,
       requirement_parent_id,
       test_case_id,
       test_case_name,
       test_case_order,
       test_case_pid,
       test_case_created_date,
       test_case_last_modified_date,
       test_case_parent_id,
       test_case_test_case_version_id,
       test_case_version,
       test_case_description,
       test_case_creator_id,
       test_case_properties_automation_field_value_name,
       test_case_properties_automation_content_field_value,
       test_case_properties_status_field_value_name,
       test_case_properties_type_field_value_name,
       test_case_properties_assigned_to_field_value_name,
       test_case_properties_description_field_value,
       test_case_properties_precondition_field_value,
       test_case_properties_test_tier_field_value_name,
       test_case_properties_zephyr_issue_key_field_value,
       test_case_properties_candidate_for_automation_field_value_name,
       test_case_properties_platform_field_value_name,
       test_case_properties_test_data_field_value,
       test_case_project_id,
       test_case_properties_zephyr_test_steps_field_value,
       test_case_properties_shared_field_value,
       dummy_modified_date_time,
       project_date_format,
       release_properties_status,
       requirement_last_modified_date,
       isnull(cast(stage_qtest_jira.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_qtest_jira
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_qtest_jira @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_qtest_jira (
       bk_hash,
       project_id,
       release_id,
       requirement_id,
       test_case_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_qtest_jira.bk_hash,
       stage_hash_qtest_jira.project_id project_id,
       stage_hash_qtest_jira.release_id release_id,
       stage_hash_qtest_jira.requirement_id requirement_id,
       stage_hash_qtest_jira.test_case_id test_case_id,
       isnull(cast(stage_hash_qtest_jira.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       48,
       @insert_date_time,
       @user
  from stage_hash_qtest_jira
  left join h_qtest_jira
    on stage_hash_qtest_jira.bk_hash = h_qtest_jira.bk_hash
 where h_qtest_jira_id is null
   and stage_hash_qtest_jira.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_qtest_jira
if object_id('tempdb..#l_qtest_jira_inserts') is not null drop table #l_qtest_jira_inserts
create table #l_qtest_jira_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_qtest_jira.bk_hash,
       stage_hash_qtest_jira.project_id project_id,
       stage_hash_qtest_jira.project_status_id project_status_id,
       stage_hash_qtest_jira.project_template_id project_template_id,
       stage_hash_qtest_jira.release_project_id release_project_id,
       stage_hash_qtest_jira.release_id release_id,
       stage_hash_qtest_jira.release_pid release_pid,
       stage_hash_qtest_jira.requirement_project_id requirement_project_id,
       stage_hash_qtest_jira.requirement_jira_defect_id requirement_jira_defect_id,
       stage_hash_qtest_jira.requirement_id requirement_id,
       stage_hash_qtest_jira.requirement_pid requirement_pid,
       stage_hash_qtest_jira.requirement_parent_id requirement_parent_id,
       stage_hash_qtest_jira.test_case_id test_case_id,
       stage_hash_qtest_jira.test_case_pid test_case_pid,
       stage_hash_qtest_jira.test_case_parent_id test_case_parent_id,
       stage_hash_qtest_jira.test_case_test_case_version_id test_case_test_case_version_id,
       stage_hash_qtest_jira.test_case_creator_id test_case_creator_id,
       stage_hash_qtest_jira.test_case_project_id test_case_project_id,
       isnull(cast(stage_hash_qtest_jira.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_qtest_jira.project_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.project_status_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.project_template_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.release_project_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.release_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.release_pid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.requirement_project_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.requirement_jira_defect_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.requirement_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.requirement_pid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.requirement_parent_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.test_case_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.test_case_pid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.test_case_parent_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.test_case_test_case_version_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.test_case_creator_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.test_case_project_id,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_qtest_jira
 where stage_hash_qtest_jira.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_qtest_jira records
set @insert_date_time = getdate()
insert into l_qtest_jira (
       bk_hash,
       project_id,
       project_status_id,
       project_template_id,
       release_project_id,
       release_id,
       release_pid,
       requirement_project_id,
       requirement_jira_defect_id,
       requirement_id,
       requirement_pid,
       requirement_parent_id,
       test_case_id,
       test_case_pid,
       test_case_parent_id,
       test_case_test_case_version_id,
       test_case_creator_id,
       test_case_project_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_qtest_jira_inserts.bk_hash,
       #l_qtest_jira_inserts.project_id,
       #l_qtest_jira_inserts.project_status_id,
       #l_qtest_jira_inserts.project_template_id,
       #l_qtest_jira_inserts.release_project_id,
       #l_qtest_jira_inserts.release_id,
       #l_qtest_jira_inserts.release_pid,
       #l_qtest_jira_inserts.requirement_project_id,
       #l_qtest_jira_inserts.requirement_jira_defect_id,
       #l_qtest_jira_inserts.requirement_id,
       #l_qtest_jira_inserts.requirement_pid,
       #l_qtest_jira_inserts.requirement_parent_id,
       #l_qtest_jira_inserts.test_case_id,
       #l_qtest_jira_inserts.test_case_pid,
       #l_qtest_jira_inserts.test_case_parent_id,
       #l_qtest_jira_inserts.test_case_test_case_version_id,
       #l_qtest_jira_inserts.test_case_creator_id,
       #l_qtest_jira_inserts.test_case_project_id,
       case when l_qtest_jira.l_qtest_jira_id is null then isnull(#l_qtest_jira_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       48,
       #l_qtest_jira_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_qtest_jira_inserts
  left join p_qtest_jira
    on #l_qtest_jira_inserts.bk_hash = p_qtest_jira.bk_hash
   and p_qtest_jira.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_qtest_jira
    on p_qtest_jira.bk_hash = l_qtest_jira.bk_hash
   and p_qtest_jira.l_qtest_jira_id = l_qtest_jira.l_qtest_jira_id
 where l_qtest_jira.l_qtest_jira_id is null
    or (l_qtest_jira.l_qtest_jira_id is not null
        and l_qtest_jira.dv_hash <> #l_qtest_jira_inserts.source_hash)

--calculate hash and lookup to current s_qtest_jira
if object_id('tempdb..#s_qtest_jira_inserts') is not null drop table #s_qtest_jira_inserts
create table #s_qtest_jira_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_qtest_jira.bk_hash,
       stage_hash_qtest_jira.project_id project_id,
       stage_hash_qtest_jira.project_name project_name,
       stage_hash_qtest_jira.project_description project_description,
       stage_hash_qtest_jira.project_start_date project_start_date,
       stage_hash_qtest_jira.project_sample project_sample,
       stage_hash_qtest_jira.project_x_explorer_access_level project_x_explorer_access_level,
       stage_hash_qtest_jira.project_automation project_automation,
       stage_hash_qtest_jira.project_uuid project_uuid,
       stage_hash_qtest_jira.release_id release_id,
       stage_hash_qtest_jira.release_name release_name,
       stage_hash_qtest_jira.release_order release_order,
       stage_hash_qtest_jira.release_created_date release_created_date,
       stage_hash_qtest_jira.release_last_modified_date release_last_modified_date,
       stage_hash_qtest_jira.release_start_date release_start_date,
       stage_hash_qtest_jira.release_end_date release_end_date,
       stage_hash_qtest_jira.release_properties_start_date release_properties_start_date,
       stage_hash_qtest_jira.release_properties_end_date release_properties_end_date,
       stage_hash_qtest_jira.requirement_jira_issue requirement_jira_issue,
       stage_hash_qtest_jira.requirement_jira_priority requirement_jira_priority,
       stage_hash_qtest_jira.requirement_jira_status requirement_jira_status,
       stage_hash_qtest_jira.requirement_id requirement_id,
       stage_hash_qtest_jira.requirement_jira_story_point requirement_jira_story_point,
       stage_hash_qtest_jira.requirement_name requirement_name,
       stage_hash_qtest_jira.test_case_id test_case_id,
       stage_hash_qtest_jira.requirement_created_date requirement_created_date,
       stage_hash_qtest_jira.test_case_name test_case_name,
       stage_hash_qtest_jira.test_case_order test_case_order,
       stage_hash_qtest_jira.test_case_created_date test_case_created_date,
       stage_hash_qtest_jira.test_case_last_modified_date test_case_last_modified_date,
       stage_hash_qtest_jira.test_case_version test_case_version,
       stage_hash_qtest_jira.test_case_description test_case_description,
       stage_hash_qtest_jira.test_case_properties_automation_field_value_name test_case_properties_automation_field_value_name,
       stage_hash_qtest_jira.test_case_properties_automation_content_field_value test_case_properties_automation_content_field_value,
       stage_hash_qtest_jira.test_case_properties_status_field_value_name test_case_properties_status_field_value_name,
       stage_hash_qtest_jira.test_case_properties_type_field_value_name test_case_properties_type_field_value_name,
       stage_hash_qtest_jira.test_case_properties_assigned_to_field_value_name test_case_properties_assigned_to_field_value_name,
       stage_hash_qtest_jira.test_case_properties_description_field_value test_case_properties_description_field_value,
       stage_hash_qtest_jira.test_case_properties_precondition_field_value test_case_properties_precondition_field_value,
       stage_hash_qtest_jira.test_case_properties_test_tier_field_value_name test_case_properties_test_tier_field_value_name,
       stage_hash_qtest_jira.test_case_properties_zephyr_issue_key_field_value test_case_properties_zephyr_issue_key_field_value,
       stage_hash_qtest_jira.test_case_properties_candidate_for_automation_field_value_name test_case_properties_candidate_for_automation_field_value_name,
       stage_hash_qtest_jira.test_case_properties_platform_field_value_name test_case_properties_platform_field_value_name,
       stage_hash_qtest_jira.test_case_properties_test_data_field_value test_case_properties_test_data_field_value,
       stage_hash_qtest_jira.test_case_properties_zephyr_test_steps_field_value test_case_properties_zephyr_test_steps_field_value,
       stage_hash_qtest_jira.test_case_properties_shared_field_value test_case_properties_shared_field_value,
       stage_hash_qtest_jira.dummy_modified_date_time dummy_modified_date_time,
       stage_hash_qtest_jira.project_date_format project_date_format,
       stage_hash_qtest_jira.release_properties_status release_properties_status,
       stage_hash_qtest_jira.requirement_last_modified_date requirement_last_modified_date,
       isnull(cast(stage_hash_qtest_jira.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_qtest_jira.project_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.project_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.project_description,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.project_start_date,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.project_sample,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.project_x_explorer_access_level,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.project_automation,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.project_uuid,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.release_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.release_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.release_order,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.release_created_date,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.release_last_modified_date,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.release_start_date,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.release_end_date,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.release_properties_start_date,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.release_properties_end_date,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.requirement_jira_issue,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.requirement_jira_priority,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.requirement_jira_status,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.requirement_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.requirement_jira_story_point,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.requirement_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.test_case_id,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.requirement_created_date,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.test_case_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.test_case_order,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.test_case_created_date,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.test_case_last_modified_date,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.test_case_version,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.test_case_description,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.test_case_properties_automation_field_value_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.test_case_properties_automation_content_field_value,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.test_case_properties_status_field_value_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.test_case_properties_type_field_value_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.test_case_properties_assigned_to_field_value_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.test_case_properties_description_field_value,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.test_case_properties_precondition_field_value,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.test_case_properties_test_tier_field_value_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.test_case_properties_zephyr_issue_key_field_value,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.test_case_properties_candidate_for_automation_field_value_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.test_case_properties_platform_field_value_name,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.test_case_properties_test_data_field_value,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.test_case_properties_zephyr_test_steps_field_value,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.test_case_properties_shared_field_value,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.project_date_format,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.release_properties_status,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_qtest_jira.requirement_last_modified_date,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_qtest_jira
 where stage_hash_qtest_jira.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_qtest_jira records
set @insert_date_time = getdate()
insert into s_qtest_jira (
       bk_hash,
       project_id,
       project_name,
       project_description,
       project_start_date,
       project_sample,
       project_x_explorer_access_level,
       project_automation,
       project_uuid,
       release_id,
       release_name,
       release_order,
       release_created_date,
       release_last_modified_date,
       release_start_date,
       release_end_date,
       release_properties_start_date,
       release_properties_end_date,
       requirement_jira_issue,
       requirement_jira_priority,
       requirement_jira_status,
       requirement_id,
       requirement_jira_story_point,
       requirement_name,
       test_case_id,
       requirement_created_date,
       test_case_name,
       test_case_order,
       test_case_created_date,
       test_case_last_modified_date,
       test_case_version,
       test_case_description,
       test_case_properties_automation_field_value_name,
       test_case_properties_automation_content_field_value,
       test_case_properties_status_field_value_name,
       test_case_properties_type_field_value_name,
       test_case_properties_assigned_to_field_value_name,
       test_case_properties_description_field_value,
       test_case_properties_precondition_field_value,
       test_case_properties_test_tier_field_value_name,
       test_case_properties_zephyr_issue_key_field_value,
       test_case_properties_candidate_for_automation_field_value_name,
       test_case_properties_platform_field_value_name,
       test_case_properties_test_data_field_value,
       test_case_properties_zephyr_test_steps_field_value,
       test_case_properties_shared_field_value,
       dummy_modified_date_time,
       project_date_format,
       release_properties_status,
       requirement_last_modified_date,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_qtest_jira_inserts.bk_hash,
       #s_qtest_jira_inserts.project_id,
       #s_qtest_jira_inserts.project_name,
       #s_qtest_jira_inserts.project_description,
       #s_qtest_jira_inserts.project_start_date,
       #s_qtest_jira_inserts.project_sample,
       #s_qtest_jira_inserts.project_x_explorer_access_level,
       #s_qtest_jira_inserts.project_automation,
       #s_qtest_jira_inserts.project_uuid,
       #s_qtest_jira_inserts.release_id,
       #s_qtest_jira_inserts.release_name,
       #s_qtest_jira_inserts.release_order,
       #s_qtest_jira_inserts.release_created_date,
       #s_qtest_jira_inserts.release_last_modified_date,
       #s_qtest_jira_inserts.release_start_date,
       #s_qtest_jira_inserts.release_end_date,
       #s_qtest_jira_inserts.release_properties_start_date,
       #s_qtest_jira_inserts.release_properties_end_date,
       #s_qtest_jira_inserts.requirement_jira_issue,
       #s_qtest_jira_inserts.requirement_jira_priority,
       #s_qtest_jira_inserts.requirement_jira_status,
       #s_qtest_jira_inserts.requirement_id,
       #s_qtest_jira_inserts.requirement_jira_story_point,
       #s_qtest_jira_inserts.requirement_name,
       #s_qtest_jira_inserts.test_case_id,
       #s_qtest_jira_inserts.requirement_created_date,
       #s_qtest_jira_inserts.test_case_name,
       #s_qtest_jira_inserts.test_case_order,
       #s_qtest_jira_inserts.test_case_created_date,
       #s_qtest_jira_inserts.test_case_last_modified_date,
       #s_qtest_jira_inserts.test_case_version,
       #s_qtest_jira_inserts.test_case_description,
       #s_qtest_jira_inserts.test_case_properties_automation_field_value_name,
       #s_qtest_jira_inserts.test_case_properties_automation_content_field_value,
       #s_qtest_jira_inserts.test_case_properties_status_field_value_name,
       #s_qtest_jira_inserts.test_case_properties_type_field_value_name,
       #s_qtest_jira_inserts.test_case_properties_assigned_to_field_value_name,
       #s_qtest_jira_inserts.test_case_properties_description_field_value,
       #s_qtest_jira_inserts.test_case_properties_precondition_field_value,
       #s_qtest_jira_inserts.test_case_properties_test_tier_field_value_name,
       #s_qtest_jira_inserts.test_case_properties_zephyr_issue_key_field_value,
       #s_qtest_jira_inserts.test_case_properties_candidate_for_automation_field_value_name,
       #s_qtest_jira_inserts.test_case_properties_platform_field_value_name,
       #s_qtest_jira_inserts.test_case_properties_test_data_field_value,
       #s_qtest_jira_inserts.test_case_properties_zephyr_test_steps_field_value,
       #s_qtest_jira_inserts.test_case_properties_shared_field_value,
       #s_qtest_jira_inserts.dummy_modified_date_time,
       #s_qtest_jira_inserts.project_date_format,
       #s_qtest_jira_inserts.release_properties_status,
       #s_qtest_jira_inserts.requirement_last_modified_date,
       case when s_qtest_jira.s_qtest_jira_id is null then isnull(#s_qtest_jira_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       48,
       #s_qtest_jira_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_qtest_jira_inserts
  left join p_qtest_jira
    on #s_qtest_jira_inserts.bk_hash = p_qtest_jira.bk_hash
   and p_qtest_jira.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_qtest_jira
    on p_qtest_jira.bk_hash = s_qtest_jira.bk_hash
   and p_qtest_jira.s_qtest_jira_id = s_qtest_jira.s_qtest_jira_id
 where s_qtest_jira.s_qtest_jira_id is null
    or (s_qtest_jira.s_qtest_jira_id is not null
        and s_qtest_jira.dv_hash <> #s_qtest_jira_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_qtest_jira @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_qtest_jira @current_dv_batch_id

end
