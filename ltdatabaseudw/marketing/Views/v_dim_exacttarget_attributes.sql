CREATE VIEW [marketing].[v_dim_exacttarget_attributes] AS select p_exacttarget_attributes.bk_hash dim_exacttarget_attributes_key,
       s_exacttarget_attributes.client_id client_id,
       s_exacttarget_attributes.subscriber_id subscriber_id,
       s_exacttarget_attributes.email_address email_address,
       s_exacttarget_attributes.subscriber_key subscriber_key
  from dbo.p_exacttarget_attributes
  join dbo.s_exacttarget_attributes
    on p_exacttarget_attributes.bk_hash = s_exacttarget_attributes.bk_hash 
   and p_exacttarget_attributes.s_exacttarget_attributes_id = s_exacttarget_attributes.s_exacttarget_attributes_id;