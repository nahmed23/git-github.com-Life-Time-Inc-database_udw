CREATE VIEW [marketing].[v_fact_mart_member_interests]
AS select d_mart_fact_member_interests.fact_member_interests_key fact_member_interests_key,
       d_mart_fact_member_interests.fact_member_interests_id fact_member_interests_id,
       d_mart_fact_member_interests.active_flag active_flag,
       d_mart_fact_member_interests.dim_mms_member_key dim_mms_member_key,
       d_mart_fact_member_interests.interest_confidence interest_confidence,
       d_mart_fact_member_interests.interest_id interest_id,
       d_mart_fact_member_interests.member_id member_id,
       d_mart_fact_member_interests.row_add_date row_add_date,
       d_mart_fact_member_interests.row_add_dim_date_key row_add_dim_date_key,
       d_mart_fact_member_interests.row_add_dim_time_key row_add_dim_time_key,
       d_mart_fact_member_interests.row_deactivation_date row_deactivation_date,
       d_mart_fact_member_interests.row_deactivation_dim_date_key row_deactivation_dim_date_key,
       d_mart_fact_member_interests.row_deactivation_dim_time_key row_deactivation_dim_time_key
  from dbo.d_mart_fact_member_interests;