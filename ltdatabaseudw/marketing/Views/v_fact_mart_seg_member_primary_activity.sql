CREATE VIEW [marketing].[v_fact_mart_seg_member_primary_activity]
AS select d_mart_fact_seg_member_primary_activity.fact_seg_member_primary_activity_key fact_seg_member_primary_activity_key,
       d_mart_fact_seg_member_primary_activity.fact_seg_member_primary_activity_id fact_seg_member_primary_activity_id,
       d_mart_fact_seg_member_primary_activity.active_flag active_flag,
       d_mart_fact_seg_member_primary_activity.confidence_score confidence_score,
       d_mart_fact_seg_member_primary_activity.dim_mms_member_key dim_mms_member_key,
       d_mart_fact_seg_member_primary_activity.member_id member_id,
       d_mart_fact_seg_member_primary_activity.primary_activity_segment primary_activity_segment,
       d_mart_fact_seg_member_primary_activity.row_add_date row_add_date,
       d_mart_fact_seg_member_primary_activity.row_add_dim_date_key row_add_dim_date_key,
       d_mart_fact_seg_member_primary_activity.row_add_dim_time_key row_add_dim_time_key,
       d_mart_fact_seg_member_primary_activity.row_deactivation_date row_deactivation_date,
       d_mart_fact_seg_member_primary_activity.row_deactivation_dim_date_key row_deactivation_dim_date_key,
       d_mart_fact_seg_member_primary_activity.row_deactivation_dim_time_key row_deactivation_dim_time_key
  from dbo.d_mart_fact_seg_member_primary_activity;