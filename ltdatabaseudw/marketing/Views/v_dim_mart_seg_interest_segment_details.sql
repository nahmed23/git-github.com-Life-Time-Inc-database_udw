CREATE VIEW [marketing].[v_dim_mart_seg_interest_segment_details]
AS select d_mart_dim_interest_segment_details.dim_interest_segment_details_key dim_interest_segment_details_key,
       d_mart_dim_interest_segment_details.interest_id interest_id,
       d_mart_dim_interest_segment_details.active_flag active_flag,
       d_mart_dim_interest_segment_details.dim_interest_segment_details_id dim_interest_segment_details_id,
       d_mart_dim_interest_segment_details.interest_display_name interest_display_name,
       d_mart_dim_interest_segment_details.interest_name interest_name,
       d_mart_dim_interest_segment_details.row_add_date row_add_date,
       d_mart_dim_interest_segment_details.row_add_dim_date_key row_add_dim_date_key,
       d_mart_dim_interest_segment_details.row_add_dim_time_key row_add_dim_time_key
  from dbo.d_mart_dim_interest_segment_details;