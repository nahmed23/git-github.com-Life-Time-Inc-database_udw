CREATE VIEW [marketing].[v_dim_mart_seg_membership_term_risk]
AS select d_mart_dim_seg_membership_term_risk.dim_seg_membership_term_risk_key dim_seg_membership_term_risk_key,
       d_mart_dim_seg_membership_term_risk.dim_seg_term_risk_id dim_seg_term_risk_id,
       d_mart_dim_seg_membership_term_risk.active_flag active_flag,
       d_mart_dim_seg_membership_term_risk.row_add_date row_add_date,
       d_mart_dim_seg_membership_term_risk.row_add_dim_date_key row_add_dim_date_key,
       d_mart_dim_seg_membership_term_risk.row_add_dim_time_key row_add_dim_time_key,
       d_mart_dim_seg_membership_term_risk.term_risk  term_risk ,
       d_mart_dim_seg_membership_term_risk.term_risk_segment  term_risk_segment 
  from dbo.d_mart_dim_seg_membership_term_risk;