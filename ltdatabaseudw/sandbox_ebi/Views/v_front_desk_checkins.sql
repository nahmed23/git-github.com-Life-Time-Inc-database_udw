CREATE VIEW [sandbox_ebi].[v_front_desk_checkins] AS SELECT vLocation.club_id,
       Usage.check_in_dim_date_time,
       DimDate.calendar_date,
	   DimDate.week_number_in_year,
	   DimDate.day_number_in_week,
	   DimTime.hour,
	   DimTime.minute,
	   DimTime.display_24_hour_time,
	   Member.member_id,
	   MembershipHistory.membership_id,
       Usage.gender_abbreviation,
	   Usage.member_age_years,
	   Member.description_member AS member_type,
	   MembershipHistory.club_id AS member_home_club_id,
	   MembershipHistory.membership_type_id,
	   MembershipHistory.membership_type,
	   MembershipHistory.membership_status,
	   Usage.delinquent_checkin_flag,
	   vLocation.club_name,
	   vLocation.club_code,
	   vLocation.club_type,
       vLocation.club_open_date,
	   vLocation.club_status,
       vLocation.current_operations_status,
	   vLocation.area,
	   vLocation.region,
	   vLocation.city,
       vLocation.state_abbreviation,
	   vLocation.club_re_open_date
FROM  [dbo].[fact_mms_member_usage] Usage
 JOIN [dbo].[dim_date] DimDate
   ON Usage.check_in_dim_date_key  = DimDate.dim_date_key
 JOIN [sandbox_ebi].[v_location] vLocation
   ON Usage.dim_club_key = vLocation.dim_club_key
 JOIN [dbo].[dim_time] DimTime
   ON Usage.check_in_dim_time_key = DimTime.dim_time_key
 LEFT JOIN [sandbox_ebi].[v_member] Member
   ON Usage.dim_mms_checkin_member_key = Member.dim_mms_member_key
 LEFT JOIN [sandbox_ebi].[v_dim_mms_membership_v2] MembershipHistory
   ON Member.dim_mms_membership_key = MembershipHistory.dim_mms_membership_key
   AND MembershipHistory.effective_date_time <= Usage.check_in_dim_date_time
   AND MembershipHistory.expiration_date_time > Usage.check_in_dim_date_time;