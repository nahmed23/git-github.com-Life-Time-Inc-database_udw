CREATE VIEW [marketing].[v_fact_mms_member_usage]
AS select fact_mms_member_usage.fact_mms_member_usage_key fact_mms_member_usage_key,
       fact_mms_member_usage.member_usage_id member_usage_id,
       fact_mms_member_usage.check_in_dim_date_time checkin_date_time,
       fact_mms_member_usage.dim_mms_checkin_member_key checkin_dim_mms_member_key,
       fact_mms_member_usage.delinquent_checkin_flag delinquent_checkin_flag,
       fact_mms_member_usage.department_dim_mms_description_key department_dim_mms_description_key,
       fact_mms_member_usage.dim_club_key dim_club_key,
       fact_mms_member_usage.dim_mms_membership_key dim_mms_membership_key,
       fact_mms_member_usage.dim_mms_primary_member_key dim_mms_primary_member_key,
       fact_mms_member_usage.dv_inserted_date_time dv_inserted_date_time,
       fact_mms_member_usage.gender_abbreviation gender_abbreviation,
       fact_mms_member_usage.member_age_years member_age_years,
       fact_mms_member_usage.p_mms_member_usage_id p_mms_member_usage_id
  from dbo.fact_mms_member_usage;