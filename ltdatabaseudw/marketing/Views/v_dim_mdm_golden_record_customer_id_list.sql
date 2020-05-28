CREATE VIEW [marketing].[v_dim_mdm_golden_record_customer_id_list]
AS select dim_mdm_golden_record_customer_id_list.dim_mdm_golden_record_customer_id_list_key dim_mdm_golden_record_customer_id_list_key,
       dim_mdm_golden_record_customer_id_list.entity_id entity_id,
       dim_mdm_golden_record_customer_id_list.dim_description_key dim_description_key,
       dim_mdm_golden_record_customer_id_list.id id,
       dim_mdm_golden_record_customer_id_list.id_type id_type,
       dim_mdm_golden_record_customer_id_list.mdm_load_date_time mdm_load_date_time,
       dim_mdm_golden_record_customer_id_list.udw_load_date_time udw_load_date_time
  from dbo.dim_mdm_golden_record_customer_id_list;