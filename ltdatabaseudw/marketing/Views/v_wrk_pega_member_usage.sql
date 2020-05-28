CREATE VIEW [marketing].[v_wrk_pega_member_usage]
AS select wrk_pega_member_usage.check_in_date_time check_in_date_time,
       wrk_pega_member_usage.club_id club_id,
       wrk_pega_member_usage.dim_mms_member_key dim_mms_member_key,
       wrk_pega_member_usage.member_id member_id,
       wrk_pega_member_usage.member_usage_id member_usage_id,
       wrk_pega_member_usage.sequence_number sequence_number
  from dbo.wrk_pega_member_usage;