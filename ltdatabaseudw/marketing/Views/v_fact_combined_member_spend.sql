CREATE VIEW [marketing].[v_fact_combined_member_spend]
AS select fact_combined_member_spend.dim_mms_member_key dim_mms_member_key,
       fact_combined_member_spend.dim_mms_membership_key dim_mms_membership_key,
       fact_combined_member_spend.first_name first_name,
       fact_combined_member_spend.home_club home_club,
       fact_combined_member_spend.home_dim_club_key home_dim_club_key,
       fact_combined_member_spend.last_12_month_spend_amount last_12_month_spend_amount,
       fact_combined_member_spend.last_name last_name,
       fact_combined_member_spend.total_spend_amount total_spend_amount
  from dbo.fact_combined_member_spend;