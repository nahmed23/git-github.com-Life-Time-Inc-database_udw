CREATE VIEW [marketing].[v_fact_magento_member_spend]
AS select fact_magento_member_spend.dim_mms_member_key dim_mms_member_key,
       fact_magento_member_spend.dim_mms_membership_key dim_mms_membership_key,
       fact_magento_member_spend.last_12_month_spend_amount last_12_month_spend_amount,
       fact_magento_member_spend.total_spend_amount total_spend_amount
  from dbo.fact_magento_member_spend;