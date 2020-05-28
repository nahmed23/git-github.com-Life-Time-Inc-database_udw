CREATE VIEW [marketing].[v_fact_mart_seg_member_expected_value]
AS select d_mart_fact_seg_member_expected_value.fact_seg_member_expected_value_id fact_seg_member_expected_value_id,
       d_mart_fact_seg_member_expected_value.active_flag active_flag,
       d_mart_fact_seg_member_expected_value.dim_mms_member_key dim_mms_member_key,
       d_mart_fact_seg_member_expected_value.expected_value_60_months expected_value_60_months,
       d_mart_fact_seg_member_expected_value.member_id member_id,
       d_mart_fact_seg_member_expected_value.past_spend_last_3_years past_spend_last_3_years,
       d_mart_fact_seg_member_expected_value.row_add_date row_add_date,
       d_mart_fact_seg_member_expected_value.row_add_dim_date_key row_add_dim_date_key,
       d_mart_fact_seg_member_expected_value.row_add_dim_time_key row_add_dim_time_key,
       d_mart_fact_seg_member_expected_value.row_deactivation_date row_deactivation_date,
       d_mart_fact_seg_member_expected_value.row_deactivation_dim_date_key row_deactivation_dim_date_key
  from dbo.d_mart_fact_seg_member_expected_value;