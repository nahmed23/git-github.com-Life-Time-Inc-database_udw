CREATE VIEW [marketing].[v_dim_mart_seg_member_usage_group]
AS select d_mart_seg_member_usage_group_history.dim_mart_seg_member_usage_group_key dim_mart_seg_member_usage_group_key,
       d_mart_seg_member_usage_group_history.usage_group_segment_id usage_group_segment_id,
       d_mart_seg_member_usage_group_history.effective_date_time effective_date_time,
       d_mart_seg_member_usage_group_history.expiration_date_time expiration_date_time,
       d_mart_seg_member_usage_group_history.active_flag active_flag,
       d_mart_seg_member_usage_group_history.max_swipes_week max_swipes_week,
       d_mart_seg_member_usage_group_history.min_swipes_week min_swipes_week,
       d_mart_seg_member_usage_group_history.usage_group usage_group
  from dbo.d_mart_seg_member_usage_group_history;