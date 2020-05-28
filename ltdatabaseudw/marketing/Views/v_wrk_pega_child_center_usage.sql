CREATE VIEW [marketing].[v_wrk_pega_child_center_usage]
AS select wrk_pega_child_center_usage.check_in_date check_in_date,
       wrk_pega_child_center_usage.check_in_member_id check_in_member_id,
       wrk_pega_child_center_usage.check_in_time check_in_time,
       wrk_pega_child_center_usage.check_out_date check_out_date,
       wrk_pega_child_center_usage.check_out_member_id check_out_member_id,
       wrk_pega_child_center_usage.check_out_time check_out_time,
       wrk_pega_child_center_usage.child_age_months child_age_months,
       wrk_pega_child_center_usage.child_center_usage_id child_center_usage_id,
       wrk_pega_child_center_usage.child_member_id child_member_id,
       wrk_pega_child_center_usage.club_id club_id,
       wrk_pega_child_center_usage.fact_mms_child_center_usage_key fact_mms_child_center_usage_key,
       wrk_pega_child_center_usage.membership_id membership_id,
       wrk_pega_child_center_usage.sequence_number sequence_number
  from dbo.wrk_pega_child_center_usage;