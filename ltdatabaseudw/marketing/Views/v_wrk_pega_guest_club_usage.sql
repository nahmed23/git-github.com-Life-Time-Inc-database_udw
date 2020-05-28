CREATE VIEW [marketing].[v_wrk_pega_guest_club_usage]
AS select wrk_pega_guest_club_usage.check_in_date_time check_in_date_time,
       wrk_pega_guest_club_usage.club_id club_id,
       wrk_pega_guest_club_usage.guest_id guest_id,
       wrk_pega_guest_club_usage.guest_of_dim_mms_member_key guest_of_dim_mms_member_key,
       wrk_pega_guest_club_usage.guest_of_member_id guest_of_member_id,
       wrk_pega_guest_club_usage.guest_privilege_rule_id guest_privilege_rule_id,
       wrk_pega_guest_club_usage.guest_visit_id guest_visit_id,
       wrk_pega_guest_club_usage.max_number_of_guests max_number_of_guests,
       wrk_pega_guest_club_usage.membership_id membership_id,
       wrk_pega_guest_club_usage.sequence_number sequence_number
  from dbo.wrk_pega_guest_club_usage;