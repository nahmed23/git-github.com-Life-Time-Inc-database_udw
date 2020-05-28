CREATE VIEW [marketing].[v_dim_mms_membership_communication_preference]
AS select d_mms_membership_communication_preference.membership_communication_preference_id membership_communication_preference_id,
       d_mms_membership_communication_preference.active_flag active_flag,
       d_mms_membership_communication_preference.communication_preference_dim_description_key communication_preference_dim_description_key,
       d_mms_membership_communication_preference.dim_mms_membership_key dim_mms_membership_key,
       d_mms_membership_communication_preference.inserted_date_time inserted_date_time,
       d_mms_membership_communication_preference.inserted_dim_date_key inserted_dim_date_key,
       d_mms_membership_communication_preference.inserted_dim_time_key inserted_dim_time_key,
       d_mms_membership_communication_preference.membership_id membership_id,
       d_mms_membership_communication_preference.updated_date_time updated_date_time,
       d_mms_membership_communication_preference.updated_dim_date_key updated_dim_date_key,
       d_mms_membership_communication_preference.updated_dim_time_key updated_dim_time_key,
       d_mms_membership_communication_preference.val_communication_preference_id val_communication_preference_id
  from dbo.d_mms_membership_communication_preference;