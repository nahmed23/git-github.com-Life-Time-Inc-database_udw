CREATE VIEW [marketing].[v_fact_mms_child_center_usage_activity_area] AS select fact_mms_child_center_usage_activity_area.activity_area_dim_description_key activity_area_dim_description_key,
       fact_mms_child_center_usage_activity_area.check_in_dim_date_key check_in_dim_date_key,
       fact_mms_child_center_usage_activity_area.check_in_dim_mms_member_key check_in_dim_mms_member_key,
       fact_mms_child_center_usage_activity_area.check_in_dim_time_key check_in_dim_time_key,
       fact_mms_child_center_usage_activity_area.check_out_dim_date_key check_out_dim_date_key,
       fact_mms_child_center_usage_activity_area.check_out_dim_mms_member_key check_out_dim_mms_member_key,
       fact_mms_child_center_usage_activity_area.check_out_dim_time_key check_out_dim_time_key,
       fact_mms_child_center_usage_activity_area.child_center_usage_activity_area_id child_center_usage_activity_area_id,
       fact_mms_child_center_usage_activity_area.dim_club_key dim_club_key,
       fact_mms_child_center_usage_activity_area.dim_mms_membership_key dim_mms_membership_key,
       fact_mms_child_center_usage_activity_area.fact_mms_child_center_usage_activity_area_key fact_mms_child_center_usage_activity_area_key,
       fact_mms_child_center_usage_activity_area.fact_mms_child_center_usage_key fact_mms_child_center_usage_key,
       fact_mms_child_center_usage_activity_area.length_of_stay_minutes length_of_stay_minutes
  from dbo.fact_mms_child_center_usage_activity_area;