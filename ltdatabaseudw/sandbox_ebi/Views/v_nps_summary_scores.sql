CREATE VIEW [sandbox_ebi].[v_nps_summary_scores]
AS SELECT
CASE WHEN nps_score <> 'INACTIVE' THEN CONVERT(INT, REPLACE(nps_score, CHAR(0), '')) END AS nps_score
,date.calendar_date as calendar_date
,segment
,loc.city as survey_city
,loc.club_id as survey_club_id
,nps.club_code as survey_club_code
,loc.club_name as survey_club_name
,loc.club_type as survey_club_type
,loc.club_status as survey_club_status
,loc.postal_code as survey_postal_code
,loc.state_abbreviation as state_abbreviation
FROM
marketing.v_medallia_nps_member_summary_scores nps
join sandbox_ebi.v_location loc
on loc.dim_club_key = nps.home_club_key
join dbo.dim_date date
on date.dim_date_key = nps.survey_date
where nps_score <> 'INACTIVE';