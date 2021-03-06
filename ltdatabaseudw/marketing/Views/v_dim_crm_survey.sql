﻿CREATE VIEW [marketing].[v_dim_crm_survey]
AS select d_crmcloudsync_ltf_survey.dim_crm_ltf_survey_key dim_crm_ltf_survey_key,
       d_crmcloudsync_ltf_survey.ltf_survey_id ltf_survey_id,
       d_crmcloudsync_ltf_survey.created_by_dim_crm_system_user_key created_by_dim_crm_system_user_key,
       d_crmcloudsync_ltf_survey.created_dim_date_key created_dim_date_key,
       d_crmcloudsync_ltf_survey.created_dim_time_key created_dim_time_key,
       d_crmcloudsync_ltf_survey.created_on created_on,
       d_crmcloudsync_ltf_survey.insert_user insert_user,
       d_crmcloudsync_ltf_survey.inserted_date_time inserted_date_time,
       d_crmcloudsync_ltf_survey.inserted_dim_date_key inserted_dim_date_key,
       d_crmcloudsync_ltf_survey.inserted_dim_time_key inserted_dim_time_key,
       d_crmcloudsync_ltf_survey.ltf_connect_member ltf_connect_member,
       d_crmcloudsync_ltf_survey.ltf_employee_id ltf_employee_id,
       d_crmcloudsync_ltf_survey.ltf_member_number ltf_member_number,
       d_crmcloudsync_ltf_survey.ltf_name ltf_name,
       d_crmcloudsync_ltf_survey.ltf_source ltf_source,
       d_crmcloudsync_ltf_survey.ltf_submitted_by_dim_crm_system_user_key ltf_submitted_by_dim_crm_system_user_key,
       d_crmcloudsync_ltf_survey.ltf_submitted_dim_date_key ltf_submitted_dim_date_key,
       d_crmcloudsync_ltf_survey.ltf_submitted_dim_time_key ltf_submitted_dim_time_key,
       d_crmcloudsync_ltf_survey.ltf_submitted_on ltf_submitted_on,
       d_crmcloudsync_ltf_survey.ltf_subscriber ltf_subscriber,
       d_crmcloudsync_ltf_survey.ltf_survey_tool_id ltf_survey_tool_id,
       d_crmcloudsync_ltf_survey.ltf_survey_type ltf_survey_type,
       d_crmcloudsync_ltf_survey.modified_by_dim_crm_system_user_key modified_by_dim_crm_system_user_key,
       d_crmcloudsync_ltf_survey.modified_dim_date_key modified_dim_date_key,
       d_crmcloudsync_ltf_survey.modified_dim_time_key modified_dim_time_key,
       d_crmcloudsync_ltf_survey.modified_on modified_on,
       d_crmcloudsync_ltf_survey.state_code state_code,
       d_crmcloudsync_ltf_survey.status_code status_code,
       d_crmcloudsync_ltf_survey.update_user update_user,
       d_crmcloudsync_ltf_survey.updated_date_time updated_date_time,
       d_crmcloudsync_ltf_survey.updated_dim_date_key updated_dim_date_key,
       d_crmcloudsync_ltf_survey.updated_dim_time_key updated_dim_time_key
  from dbo.d_crmcloudsync_ltf_survey;