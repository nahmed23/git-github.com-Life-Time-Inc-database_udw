CREATE VIEW [marketing].[v_fact_medallia_survey_data]
AS select fact_medallia_survey_data.dim_club_key dim_club_key,
       fact_medallia_survey_data.dim_medallia_field_key dim_medallia_field_key,
       fact_medallia_survey_data.dim_mms_member_key dim_mms_member_key,
       fact_medallia_survey_data.dim_mms_membership_key dim_mms_membership_key,
       fact_medallia_survey_data.dim_survey_created_dim_date_key dim_survey_created_dim_date_key,
       fact_medallia_survey_data.dim_survey_created_dim_time_key dim_survey_created_dim_time_key,
       fact_medallia_survey_data.fact_medallia_survey_data_key fact_medallia_survey_data_key,
       fact_medallia_survey_data.field_name field_name,
       fact_medallia_survey_data.file_name file_name,
       fact_medallia_survey_data.survey_data survey_data,
       fact_medallia_survey_data.survey_data_converted_to_dim_date_key survey_data_converted_to_dim_date_key,
       fact_medallia_survey_data.survey_data_converted_to_dim_time_key survey_data_converted_to_dim_time_key,
       fact_medallia_survey_data.survey_id survey_id,
       fact_medallia_survey_data.survey_status survey_status,
       fact_medallia_survey_data.survey_type survey_type
  from dbo.fact_medallia_survey_data;