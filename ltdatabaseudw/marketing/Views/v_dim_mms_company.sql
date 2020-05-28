CREATE VIEW [marketing].[v_dim_mms_company] AS select d_mms_company.dim_mms_company_key dim_mms_company_key,
       d_mms_company.company_id company_id,
       d_mms_company.account_rep_name account_rep_name,
       d_mms_company.company_name company_name,
       d_mms_company.corporate_code corporate_code,
       d_mms_company.eft_account_number_on_file_flag eft_account_number_on_file_flag,
       d_mms_company.invoice_flag invoice_flag,
       d_mms_company.report_to_email_address report_to_email_address,
       d_mms_company.small_business_flag small_business_flag,
       d_mms_company.usage_report_flag usage_report_flag,
       d_mms_company.usage_report_member_type usage_report_member_type
  from dbo.d_mms_company;