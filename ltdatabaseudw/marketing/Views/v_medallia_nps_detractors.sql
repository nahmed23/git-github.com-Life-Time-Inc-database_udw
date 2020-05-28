CREATE VIEW [marketing].[v_medallia_nps_detractors]
AS WITH RankedValues AS
(
    SELECT
        fct.dim_mms_member_key, fct.dim_mms_membership_key, fct.dim_club_key, fct.survey_type, fct.survey_id , fct.survey_data_converted_to_dim_date_key survey_response_date
        , DENSE_RANK() OVER(partition by fct.dim_mms_member_key ORDER BY fct.dim_mms_member_key, fct.survey_data_converted_to_dim_date_key desc, fct.survey_id desc) AS FieldSRank
		, fct1.survey_data ltr_score, fct2.survey_data ltr_comment
		, max(case when fct.survey_data in ('9','10') and fct.dim_medallia_field_key = 'member_ltr_sc11' then 'Promoter'
			   when fct.survey_data in ('7','8')  and fct.dim_medallia_field_key = 'member_ltr_sc11' then 'Passive'
			   when fct.survey_data in ('0','1','2','3','4','5','6')  and fct.dim_medallia_field_key = 'member_ltr_sc11' then 'Detractor'
			   else NULL
		  end) as member_nps_type
	FROM
        fact_medallia_survey_data fct
		left join fact_medallia_survey_data fct1 on isnull(fct.survey_id,0)=isnull(fct1.survey_id,0) and fct1.dim_medallia_field_key in ('member_ltr_sc11') /*and fct1.survey_data is not NULL*/
		left join fact_medallia_survey_data fct2 on isnull(fct.survey_id,0)=isnull(fct2.survey_id,0) and fct2.dim_medallia_field_key in ( 'member_ltr_comment_comment')
	where fct.dim_medallia_field_key in ('member_ltr_sc11', 'member_ltr_comment_comment')
		and fct.dim_mms_member_key != '-998' and fct1.survey_data is not NULL
		/*and fct.dim_mms_member_key in ('7DCC31E2FB91A7241FE1900CDDEE6599', 'C1E9C571E1CF74FC75EEB489031B16EB', 'F267A78A3F469A490866A742556B82F2','B60F37B3C97AECAD111B6E451C10537B', '01F88953CCBB1685CC96969A771A6213', '00977B7901083C86276A081E955A42E6', '022BFCA32F550169CA3EBC314AE00CE7', '21F4CAF60D13D3C7E3625835C634CFA6', '13752C4F471363888FBF26A318CC6C48')*/
	group by fct.dim_mms_member_key, fct.dim_club_key, fct.survey_type, fct.survey_data_converted_to_dim_date_key, fct.survey_data_converted_to_dim_time_key
		, fct.survey_id, fct1.survey_data, fct2.survey_data
		, fct.dim_mms_membership_key
	/*order by fct.dim_mms_member_key, fct.survey_data_converted_to_dim_date_key desc, fct.survey_id desc*/
)
SELECT
    mbr.member_id, mbr.first_name, mbr.last_name
/*	, clb.club_id*/
	, clb.club_code
    , MAX((CASE WHEN FieldSRank = 1 THEN ltr_score ELSE NULL END)) AS current_nps_score
    , MAX((CASE WHEN FieldSRank = 1 THEN ltr_comment ELSE NULL END)) AS current_comment
    , MAX((CASE WHEN FieldSRank = 1 THEN member_NPS_type ELSE NULL END)) AS current_segment
    , MAX((CASE WHEN FieldSRank = 1 THEN Survey_Response_Date ELSE '-998' END)) AS current_survey_date
    , MAX((CASE WHEN FieldSRank = 1 THEN survey_id ELSE NULL END)) AS current_survey_id

    , MAX((CASE WHEN FieldSRank = 2 THEN ltr_score ELSE NULL END)) AS previous_nps_score
    , MAX((CASE WHEN FieldSRank = 2 THEN ltr_comment ELSE NULL END)) AS previous_comment
    , MAX((CASE WHEN FieldSRank = 2 THEN member_NPS_type ELSE NULL END)) AS previous_segment
    , MAX((CASE WHEN FieldSRank = 2 THEN Survey_Response_Date ELSE '-998' END)) AS previous_survey_date
	, MAX((CASE WHEN FieldSRank = 2 THEN survey_id ELSE NULL END)) AS previous_survey_id
	, Main.dim_mms_member_key member_key/*, Main.dim_club_key club_key, Main.survey_type*/
/*	, mbrs.club_id home_club_id*/
	, CASE WHEN mbrs.home_dim_club_key is NULL THEN '-998' ELSE mbrs.home_dim_club_key END home_club_key
FROM RankedValues Main
	left join d_mms_member mbr on mbr.dim_mms_member_key= Main.dim_mms_member_key
	left join dim_club clb on clb.dim_club_key=Main.dim_club_key
	left join d_mms_membership mbrs on mbrs.dim_mms_membership_key=Main.dim_mms_membership_key
where Main.dim_mms_member_key in (select dim_mms_member_key from RankedValues where (CASE WHEN FieldSRank = 1 THEN member_NPS_type ELSE NULL END) in ('Detractor'))
	and Main.dim_mms_member_key <> '-998'
	and FieldSRank in (1,2)
GROUP BY
    Main.dim_mms_member_key, Main.dim_club_key, Main.survey_type
	, mbr.member_id, mbr.first_name, mbr.last_name, clb.club_id, clb.club_code
	, mbrs.club_id, CASE WHEN mbrs.home_dim_club_key is NULL THEN '-998' ELSE mbrs.home_dim_club_key END;