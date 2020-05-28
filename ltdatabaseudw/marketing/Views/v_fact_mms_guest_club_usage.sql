CREATE VIEW [marketing].[v_fact_mms_guest_club_usage] AS select fact_mms_guest_club_usage.fact_mms_guest_club_usage_key fact_mms_guest_club_usage_key,
       fact_mms_guest_club_usage.guest_visit_id guest_visit_id,
       fact_mms_guest_club_usage.check_in_dim_date_key check_in_dim_date_key,
       fact_mms_guest_club_usage.check_in_dim_time_key check_in_dim_time_key,
       fact_mms_guest_club_usage.dim_club_guest_key dim_club_guest_key,
       fact_mms_guest_club_usage.dim_club_key dim_club_key,
       fact_mms_guest_club_usage.dim_mms_membership_guest_privilege_rule_key dim_mms_membership_guest_privilege_rule_key,
       fact_mms_guest_club_usage.guest_of_dim_mms_member_key guest_of_dim_mms_member_key,
       fact_mms_guest_club_usage.membership_id membership_id
  from dbo.fact_mms_guest_club_usage;