CREATE VIEW [marketing].[v_dim_exacttarget_lists] AS select p_exacttarget_lists.bk_hash dim_exacttarget_lists_key,
       s_exacttarget_lists.client_id client_id,
       s_exacttarget_lists.list_id list_id,
       s_exacttarget_lists.date_created date_created,
       s_exacttarget_lists.description description,
       s_exacttarget_lists.list_type list_type,
       s_exacttarget_lists.name name,
       s_exacttarget_lists.status status
  from dbo.p_exacttarget_lists
  join dbo.s_exacttarget_lists
    on p_exacttarget_lists.bk_hash = s_exacttarget_lists.bk_hash 
   and p_exacttarget_lists.s_exacttarget_lists_id = s_exacttarget_lists.s_exacttarget_lists_id;