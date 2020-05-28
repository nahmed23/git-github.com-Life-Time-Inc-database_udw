CREATE VIEW [marketing].[v_medallia_nps_detractors_expected_values]
AS WITH detractors_expected_values
AS (
	SELECT fact_mart_seg_member_expected_value.dim_mms_member_key
		,medallia_nps_detractors.member_id
		,medallia_nps_detractors.club_code
		,medallia_nps_detractors.current_survey_date
		,medallia_nps_detractors.current_nps_score
		,medallia_nps_detractors.current_comment
		,medallia_nps_detractors.previous_survey_date
		,medallia_nps_detractors.previous_nps_score
		,medallia_nps_detractors.current_segment
		,fact_mart_seg_member_expected_value.expected_value_60_months
		,fact_mart_seg_member_expected_value.row_add_dim_date_key
	FROM marketing.v_fact_mart_seg_member_expected_value fact_mart_seg_member_expected_value
	JOIN marketing.v_medallia_nps_detractors medallia_nps_detractors 
	ON fact_mart_seg_member_expected_value.dim_mms_member_key = medallia_nps_detractors.member_key
		AND fact_mart_seg_member_expected_value.active_flag = 'Y'
	/*WHERE fact_mart_seg_member_expected_value.expected_value_60_months > 50000 --Enter values here*/
	/*	AND medallia_nps_detractors.current_nps_score < 7 --Enter values here*/
	)
SELECT nps_detractors_expected_values.dim_mms_member_key
	,nps_detractors_expected_values.member_id
	,nps_detractors_expected_values.club_code
	,nps_detractors_expected_values.current_survey_date
	,nps_detractors_expected_values.current_nps_score
	,nps_detractors_expected_values.current_comment
	,nps_detractors_expected_values.previous_survey_date
	,nps_detractors_expected_values.previous_nps_score
	,nps_detractors_expected_values.current_segment
	,nps_detractors_expected_values.expected_value_60_months
FROM (
	SELECT dim_mms_member_key
		,member_id
		,club_code
		,current_survey_date
		,current_nps_score
		,current_comment
		,previous_survey_date
		,previous_nps_score
		,current_segment
		,expected_value_60_months
		,row_add_dim_date_key
	FROM detractors_expected_values
	) nps_detractors_expected_values
JOIN (
	SELECT dim_mms_member_key
		,max(row_add_dim_date_key) AS row_add_dim_date_key
	FROM detractors_expected_values
	GROUP BY dim_mms_member_key
	) dim_member_active ON nps_detractors_expected_values.dim_mms_member_key = dim_member_active.dim_mms_member_key
	AND nps_detractors_expected_values.row_add_dim_date_key = dim_member_active.row_add_dim_date_key;