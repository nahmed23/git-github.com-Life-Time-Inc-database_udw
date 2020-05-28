CREATE VIEW [marketing].[v_fact_mms_club_activity_area_member_usage]
AS select d_mms_club_activity_area_member_usage.fact_mms_club_activity_area_member_usage_key fact_mms_club_activity_area_member_usage_key,
       d_mms_club_activity_area_member_usage.club_activity_area_member_usage_id club_activity_area_member_usage_id,
       d_mms_club_activity_area_member_usage.club_activity_area_member_usage_dim_description_key club_activity_area_member_usage_dim_description_key,
       d_mms_club_activity_area_member_usage.dim_club_key dim_club_key,
       d_mms_club_activity_area_member_usage.dim_mms_member_key dim_mms_member_key,
       d_mms_club_activity_area_member_usage.inserted_date_time inserted_date_time,
       d_mms_club_activity_area_member_usage.updated_date_time updated_date_time,
       d_mms_club_activity_area_member_usage.val_activity_area_id val_activity_area_id
  from dbo.d_mms_club_activity_area_member_usage;