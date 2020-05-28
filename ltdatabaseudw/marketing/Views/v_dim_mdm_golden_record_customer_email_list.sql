CREATE VIEW [marketing].[v_dim_mdm_golden_record_customer_email_list] AS select dim_mdm_golden_record_customer_email_list.dim_mdm_golden_record_customer_email_list_key dim_mdm_golden_record_customer_email_list_key,
       dim_mdm_golden_record_customer_email_list.entity_id entity_id,
       dim_mdm_golden_record_customer_email_list.email email,
       dim_mdm_golden_record_customer_email_list.type type
  from dbo.dim_mdm_golden_record_customer_email_list;