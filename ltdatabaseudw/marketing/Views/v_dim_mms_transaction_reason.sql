CREATE VIEW [marketing].[v_dim_mms_transaction_reason] AS select d_mms_reason_code.dim_mms_transaction_reason_key dim_mms_transaction_reason_key,
       d_mms_reason_code.reason_code_id reason_code_id,
       d_mms_reason_code.description description
  from dbo.d_mms_reason_code;