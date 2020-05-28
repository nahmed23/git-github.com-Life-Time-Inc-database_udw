CREATE VIEW [marketing].[v_dim_mms_ltf_key]
AS select d_mms_ltf_key.ltf_key_id ltf_key_id,
       d_mms_ltf_key.inserted_date_time inserted_date_time,
       d_mms_ltf_key.inserted_dim_date_key inserted_dim_date_key,
       d_mms_ltf_key.inserted_dim_time_key inserted_dim_time_key,
       d_mms_ltf_key.ltf_key_identifier ltf_key_identifier,
       d_mms_ltf_key.ltf_key_name ltf_key_name,
       d_mms_ltf_key.updated_date_time updated_date_time,
       d_mms_ltf_key.updated_dim_date_key updated_dim_date_key,
       d_mms_ltf_key.updated_dim_time_key updated_dim_time_key
  from dbo.d_mms_ltf_key;