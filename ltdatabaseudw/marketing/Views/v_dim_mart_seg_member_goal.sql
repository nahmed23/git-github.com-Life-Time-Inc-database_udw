CREATE VIEW [marketing].[v_dim_mart_seg_member_goal]
AS select d_mart_dim_seg_member_goal.dim_seg_member_goal_key dim_seg_member_goal_key,
       d_mart_dim_seg_member_goal.dim_seg_member_goal_id dim_seg_member_goal_id,
       d_mart_dim_seg_member_goal.active_flag active_flag,
       d_mart_dim_seg_member_goal.goal goal,
       d_mart_dim_seg_member_goal.goal_segment goal_segment,
       d_mart_dim_seg_member_goal.row_add_date row_add_date,
       d_mart_dim_seg_member_goal.row_add_dim_date_key row_add_dim_date_key,
       d_mart_dim_seg_member_goal.row_add_dim_time_key row_add_dim_time_key
  from dbo.d_mart_dim_seg_member_goal;