CREATE VIEW [marketing].[v_dim_mart_seg_member_primary_activity]
AS select d_mart_dim_seg_member_primary_activity.dim_seg_member_primary_activity_key dim_seg_member_primary_activity_key,
       d_mart_dim_seg_member_primary_activity.dim_seg_member_primary_activity_id dim_seg_member_primary_activity_id,
       d_mart_dim_seg_member_primary_activity.active_flag active_flag,
       d_mart_dim_seg_member_primary_activity.primary_activity primary_activity,
       d_mart_dim_seg_member_primary_activity.primary_activity_segment primary_activity_segment,
       d_mart_dim_seg_member_primary_activity.row_add_date row_add_date,
       d_mart_dim_seg_member_primary_activity.row_add_dim_date_key row_add_dim_date_key,
       d_mart_dim_seg_member_primary_activity.row_add_dim_time_key row_add_dim_time_key
  from dbo.d_mart_dim_seg_member_primary_activity;