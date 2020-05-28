CREATE VIEW [marketing].[v_dim_mms_member_30_day_rejoin]
AS select dim_mms_member_30_day_rejoin.date_of_birth date_of_birth,
       dim_mms_member_30_day_rejoin.dim_mms_member_key dim_mms_member_key,
       dim_mms_member_30_day_rejoin.email_address email_address,
       dim_mms_member_30_day_rejoin.entity_id entity_id,
       dim_mms_member_30_day_rejoin.first_name first_name,
       dim_mms_member_30_day_rejoin.join_date join_date,
       dim_mms_member_30_day_rejoin.last_name last_name,
       dim_mms_member_30_day_rejoin.member_active_flag member_active_flag,
       dim_mms_member_30_day_rejoin.member_id member_id,
       dim_mms_member_30_day_rejoin.member_type member_type,
       dim_mms_member_30_day_rejoin.phone_number phone_number,
       dim_mms_member_30_day_rejoin.previous_join_date previous_join_date,
       dim_mms_member_30_day_rejoin.sex sex,
       dim_mms_member_30_day_rejoin.termination_reason termination_reason
  from dbo.dim_mms_member_30_day_rejoin;