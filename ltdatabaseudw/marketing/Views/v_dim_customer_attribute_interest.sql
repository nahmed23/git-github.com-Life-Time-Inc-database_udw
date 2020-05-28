CREATE VIEW [marketing].[v_dim_customer_attribute_interest]
AS select wrk_pega_customer_attribute_interest.dim_mms_member_key dim_mms_member_key,
       wrk_pega_customer_attribute_interest.interest_confidence interest_confidence,
       wrk_pega_customer_attribute_interest.interest_id interest_id,
       wrk_pega_customer_attribute_interest.interest_name interest_name,
       wrk_pega_customer_attribute_interest.member_id member_id,
       wrk_pega_customer_attribute_interest.sequence_number sequence_number
  from dbo.wrk_pega_customer_attribute_interest;