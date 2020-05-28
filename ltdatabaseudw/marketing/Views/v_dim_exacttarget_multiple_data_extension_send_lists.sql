CREATE VIEW [marketing].[v_dim_exacttarget_multiple_data_extension_send_lists] AS select p_exacttarget_multiple_data_extension_send_lists.bk_hash dim_exacttarget_multiple_data_extension_send_lists_key,
       s_exacttarget_multiple_data_extension_send_lists.client_id client_id,
       s_exacttarget_multiple_data_extension_send_lists.send_id send_id,
       s_exacttarget_multiple_data_extension_send_lists.list_id list_id,
       s_exacttarget_multiple_data_extension_send_lists.de_client_id de_client_id,
       s_exacttarget_multiple_data_extension_send_lists.data_extension_name data_extension_name,
       s_exacttarget_multiple_data_extension_send_lists.date_created date_created,
       s_exacttarget_multiple_data_extension_send_lists.status status
  from dbo.p_exacttarget_multiple_data_extension_send_lists
  join dbo.s_exacttarget_multiple_data_extension_send_lists
    on p_exacttarget_multiple_data_extension_send_lists.bk_hash = s_exacttarget_multiple_data_extension_send_lists.bk_hash 
   and p_exacttarget_multiple_data_extension_send_lists.s_exacttarget_multiple_data_extension_send_lists_id = s_exacttarget_multiple_data_extension_send_lists.s_exacttarget_multiple_data_extension_send_lists_id;