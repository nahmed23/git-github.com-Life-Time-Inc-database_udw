CREATE VIEW [marketing].[v_fact_guest_usage_summary]
AS select fact_guest_usage_summary.club_id club_id,
       fact_guest_usage_summary.data_received_late_flag data_received_late_flag,
       fact_guest_usage_summary.dim_club_key dim_club_key,
       fact_guest_usage_summary.fact_guest_usage_summary_key fact_guest_usage_summary_key,
       fact_guest_usage_summary.fact_mms_guest_count_dim_date_key fact_mms_guest_count_dim_date_key,
       fact_guest_usage_summary.guest_count_date guest_count_date,
       fact_guest_usage_summary.guest_count_id guest_count_id,
       fact_guest_usage_summary.inserted_date_time inserted_date_time,
       fact_guest_usage_summary.member_child_count member_child_count,
       fact_guest_usage_summary.member_count member_count,
       fact_guest_usage_summary.non_member_child_count non_member_child_count,
       fact_guest_usage_summary.non_member_count non_member_count
  from dbo.fact_guest_usage_summary;