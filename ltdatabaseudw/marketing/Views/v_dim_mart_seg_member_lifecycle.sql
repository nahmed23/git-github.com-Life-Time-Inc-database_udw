CREATE VIEW [marketing].[v_dim_mart_seg_member_lifecycle]
AS select d_mart_dim_seg_member_lifecycle.dim_seg_member_lifecycle_key dim_seg_member_lifecycle_key,
       d_mart_dim_seg_member_lifecycle.dim_seg_member_lifecycle_id dim_seg_member_lifecycle_id,
       d_mart_dim_seg_member_lifecycle.active_flag active_flag,
       d_mart_dim_seg_member_lifecycle.lifecycle lifecycle,
       d_mart_dim_seg_member_lifecycle.lifecycle_segment  lifecycle_segment ,
       d_mart_dim_seg_member_lifecycle.row_add_date row_add_date,
       d_mart_dim_seg_member_lifecycle.row_add_dim_date_key row_add_dim_date_key,
       d_mart_dim_seg_member_lifecycle.row_add_dim_time_key row_add_dim_time_key
  from dbo.d_mart_dim_seg_member_lifecycle;