CREATE VIEW [marketing].[v_fact_mms_kids_play_usage] AS select fact_mms_kids_play_usage.fact_mms_kids_play_usage_key fact_mms_kids_play_usage_key,
       fact_mms_kids_play_usage.kids_play_check_in_id kids_play_check_in_id,
       fact_mms_kids_play_usage.check_in_dim_date_key check_in_dim_date_key,
       fact_mms_kids_play_usage.check_in_dim_time_key check_in_dim_time_key,
       fact_mms_kids_play_usage.child_age_months child_age_months,
       fact_mms_kids_play_usage.child_age_years child_age_years,
       fact_mms_kids_play_usage.child_dim_mms_member_key child_dim_mms_member_key,
       fact_mms_kids_play_usage.child_gender_abbreviation child_gender_abbreviation,
       fact_mms_kids_play_usage.dim_club_key dim_club_key
  from dbo.fact_mms_kids_play_usage;