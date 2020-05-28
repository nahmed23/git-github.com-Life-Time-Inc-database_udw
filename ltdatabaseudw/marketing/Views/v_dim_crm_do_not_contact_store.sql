CREATE VIEW [marketing].[v_dim_crm_do_not_contact_store]
AS select d_crmcloudsync_ltf_do_not_contact_store.dim_crm_ltf_do_not_contact_store_key dim_crm_ltf_do_not_contact_store_key,
       d_crmcloudsync_ltf_do_not_contact_store.ltf_do_not_contact_store_id ltf_do_not_contact_store_id,
       d_crmcloudsync_ltf_do_not_contact_store.created_dim_date_key created_dim_date_key,
       d_crmcloudsync_ltf_do_not_contact_store.created_dim_time_key created_dim_time_key,
       d_crmcloudsync_ltf_do_not_contact_store.created_on created_on,
       d_crmcloudsync_ltf_do_not_contact_store.dim_crm_contact_key dim_crm_contact_key,
       d_crmcloudsync_ltf_do_not_contact_store.dim_crm_lead_key dim_crm_lead_key,
       d_crmcloudsync_ltf_do_not_contact_store.ltf_email_address1 ltf_email_address1
  from dbo.d_crmcloudsync_ltf_do_not_contact_store;