CREATE VIEW [sandbox_ebi].[v_member]
AS SELECT
	Member.dim_mms_member_key dim_mms_member_key,   
	Member.member_id member_id,
	Member.assess_junior_member_dues_flag assess_junior_member_dues_flag,
/*PPI
	d_mms_member.customer_name customer_name,  
	d_mms_member.customer_name_last_first customer_name_last_first,
*/
	Member.date_of_birth date_of_birth,
	Member.description_member description_member,
	Member.dim_mms_membership_key dim_mms_membership_key, 
/*PPI
	d_mms_member.email_address email_address,
	d_mms_member.first_name first_name,
*/
	Member.gender_abbreviation gender_abbreviation, 
	Member.join_date join_date, 
/*PPI
	d_mms_member.last_name last_name, 
*/
	Member.member_active_flag member_active_flag, 
	Member.member_type_dim_description_key member_type_dim_description_key,  --
	Member.membership_id membership_id,
	Member.val_member_type_id val_member_type_id, 
--d_mms_member
	Member.party_id party_id,
--Program
	--Member.member_id,
	RankedPrograms.Program1_ProgramID,
	RankedPrograms.Program1_ProgramName,
	RankedPrograms.Program2_ProgramID,
	RankedPrograms.Program2_ProgramName,
	RankedPrograms.Program3_ProgramID,
	RankedPrograms.Program3_ProgramName,
	RankedPrograms.Program4_ProgramID,
	RankedPrograms.Program4_ProgramName,  
--dim_mms_membership
	Membership.current_price,
	Membership.dim_mms_company_key,
	Membership.dim_mms_membership_type_key,
	Membership.membership_type,
	Membership.home_dim_club_key,
	Membership.membership_cancellation_request_date,
	Membership.membership_sales_channel_dim_description_key,
	Membership.membership_status,
	Membership.revenue_reporting_category_description,
	Membership.membership_expiration_date,
--dim_club
	dim_club.state state,
	dim_club.state_or_province state_or_province,
	dim_club.club_status club_status,
	dim_club.club_id club_id,
--dim_description
	dbo.dim_description.description as Region  --region contains the area.... "Region" = <areaDirector>-<area>, everyhwere else it's called MMS_region, workday_region
	FROM d_mms_member Member
    JOIN dim_mms_membership Membership
	ON Member.membership_id = Membership.membership_id
	LEFT JOIN (SELECT dim_mms_member_key, 
	                   MAX(CASE WHEN ProgramRanking = 1
					        THEN reimbursement_program_id
							END) Program1_ProgramID,
                       MAX(CASE WHEN ProgramRanking = 1
					        THEN program_name
							END) Program1_ProgramName,
					   MAX(CASE WHEN ProgramRanking = 2
					        THEN reimbursement_program_id
							END) Program2_ProgramID,
                       MAX(CASE WHEN ProgramRanking = 2
					        THEN program_name
							END) Program2_ProgramName,
					   MAX(CASE WHEN ProgramRanking = 3
					        THEN reimbursement_program_id
							END) Program3_ProgramID,
                       MAX(CASE WHEN ProgramRanking = 3
					        THEN program_name
							END) Program3_ProgramName,
					   MAX(CASE WHEN ProgramRanking = 4
					        THEN reimbursement_program_id
							END) Program4_ProgramID,
                       MAX(CASE WHEN ProgramRanking = 4
					        THEN program_name
							END) Program4_ProgramName
	             FROM (SELECT DISTINCT Member.member_id,
                       Member.dim_mms_member_key,
                       Member.membership_id,
				       FactMemberReimbursementProgram.enrollment_dim_date_key,
                       DimReimbursementProgram.program_name,
				       DimReimbursementProgram.reimbursement_program_id,
                       RANK() OVER (PARTITION BY Member.member_id
                                 ORDER BY FactMemberReimbursementProgram.enrollment_dim_date_key) ProgramRanking

                      FROM fact_mms_member_reimbursement_program FactMemberReimbursementProgram
                      JOIN d_mms_member Member
                        ON FactmemberReimbursementProgram.dim_mms_member_key = Member.dim_mms_member_key
                      JOIN dim_mms_reimbursement_program DimReimbursementProgram
                        ON FactMemberReimbursementProgram.dim_mms_reimbursement_program_key = DimReimbursementProgram.dim_mms_reimbursement_program_key
                      JOIN d_mms_membership Membership
	                    ON Member.membership_id = Membership.membership_id 
                     WHERE FactMemberReimbursementProgram.enrollment_date <= getdate()
                       AND FactMemberReimbursementProgram.termination_date > getdate()
                       AND Member.member_active_flag = 'Y'
                       AND (Membership.membership_expiration_date > getdate()
                            OR Membership.membership_expiration_date is null)) RankedPartnerProgramMembers
                  GROUP BY dim_mms_member_key)  RankedPrograms
	  ON Member.dim_mms_member_key = RankedPrograms.dim_mms_member_key
  	LEFT JOIN dim_club
	on membership.home_dim_club_key = dim_club.dim_club_key
	LEFT Join dbo.dim_description
	ON dim_club.region_dim_description_key=dbo.dim_description.dim_description_key
	WHERE member.dim_mms_member_key > '0';