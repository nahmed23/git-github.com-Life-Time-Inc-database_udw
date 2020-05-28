CREATE VIEW [marketing].[v_dim_exacttarget_list_membership] AS select p_exacttarget_list_membership.bk_hash dim_exacttarget_list_membership_key,
       s_exacttarget_list_membership.client_id client_id,
       s_exacttarget_list_membership.subscriber_id subscriber_id,
       s_exacttarget_list_membership.list_id list_id,
       s_exacttarget_list_membership.date_joined date_joined,
       s_exacttarget_list_membership.date_unsubscribed date_unsubscribed,
       s_exacttarget_list_membership.email_address email_address,
       s_exacttarget_list_membership.join_type join_type,
       s_exacttarget_list_membership.list_name list_name,
       s_exacttarget_list_membership.subscriber_key subscriber_key,
       s_exacttarget_list_membership.unsubscribe_reason unsubscribe_reason
  from dbo.p_exacttarget_list_membership
  join dbo.s_exacttarget_list_membership
    on p_exacttarget_list_membership.bk_hash = s_exacttarget_list_membership.bk_hash 
   and p_exacttarget_list_membership.s_exacttarget_list_membership_id = s_exacttarget_list_membership.s_exacttarget_list_membership_id;