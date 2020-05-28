CREATE VIEW [marketing].[v_fact_mart_seg_membership_term_risk]
AS select d_mart_fact_seg_membership_term_risk.fact_seg_membership_term_risk_key fact_seg_membership_term_risk_key,
       d_mart_fact_seg_membership_term_risk.fact_seg_membership_term_risk_id fact_seg_membership_term_risk_id,
       d_mart_fact_seg_membership_term_risk.active_flag active_flag,
       d_mart_fact_seg_membership_term_risk.dim_mms_membership_key dim_mms_membership_key,
       d_mart_fact_seg_membership_term_risk.membership_id membership_id,
       d_mart_fact_seg_membership_term_risk.row_add_date row_add_date,
       d_mart_fact_seg_membership_term_risk.row_add_dim_date_key row_add_dim_date_key,
       d_mart_fact_seg_membership_term_risk.row_add_dim_time_key row_add_dim_time_key,
       d_mart_fact_seg_membership_term_risk.row_deactivation_date row_deactivation_date,
       d_mart_fact_seg_membership_term_risk.row_deactivation_dim_date_key row_deactivation_dim_date_key,
       d_mart_fact_seg_membership_term_risk.row_deactivation_dim_time_key row_deactivation_dim_time_key,
       d_mart_fact_seg_membership_term_risk.term_risk_segment  term_risk_segment 
  from dbo.d_mart_fact_seg_membership_term_risk;