CREATE VIEW [marketing].[v_dim_mdm_golden_record_customer_phone_list] AS select dim_mdm_golden_record_customer_phone_list.dim_mdm_golden_record_customer_phone_list_key dim_mdm_golden_record_customer_phone_list_key,
       dim_mdm_golden_record_customer_phone_list.entity_id entity_id,
       dim_mdm_golden_record_customer_phone_list.phone phone,
       dim_mdm_golden_record_customer_phone_list.type type
  from dbo.dim_mdm_golden_record_customer_phone_list;