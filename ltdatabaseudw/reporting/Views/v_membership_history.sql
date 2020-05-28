CREATE VIEW [reporting].[v_membership_history]
AS SELECT
        Member.[member_id],   --MemberID
		Member.[customer_name] customer_name,  
		Member.[first_name],  
		Member.[last_name],  
		Member.[date_of_birth],
		Member.[join_date],		---Is this the needed join date
		-------------Club------------------
		Club.state state,
		Club.[city] city,
		Club.[state_or_province] state_or_province,
		Club.club_id club_id,
		Club.club_status club_status,
		Club.[club_membership_level] club_membership_level,
		Club.[club_name] club_name,
		Club.[club_type] club_type,
	
	   Membership.dim_mms_membership_key dim_mms_membership_key,
       Membership.membership_id membership_id,   --MembershipID
       Membership.advisor_employee_id advisor_employee_id,
       Membership.attrition_date attrition_date,
       Membership.created_date_time created_date_time,
       Membership.created_date_time_key created_date_time_key,
       Membership.crm_opportunity_id crm_opportunity_id,
       Membership.current_price current_price,
       Membership.dim_crm_opportunity_key dim_crm_opportunity_key,
       Membership.dim_mms_company_key dim_mms_company_key,    ---Can be used for reimbursement program - might need to limit 
       Membership.eft_option eft_option,
       Membership.home_dim_club_key home_dim_club_key,        ---might join to dimmmsclub 
       Membership.membership_activation_date membership_activation_date, --Membership ActivationDate
       Membership.membership_address_city membership_address_city,
       Membership.membership_address_country membership_address_country,
       Membership.membership_address_line_1 membership_address_line_1,
       Membership.membership_address_line_2 membership_address_line_2,
       Membership.membership_address_postal_code membership_address_postal_code,
       Membership.membership_address_state_abbreviation membership_address_state_abbreviation,
       Membership.membership_cancellation_request_date membership_cancellation_request_date, --Membership canceling request date
       Membership.membership_expiration_date membership_expiration_date,  --Membership Expiratin Date: typically this would be 30days after unless requested at 
       Membership.membership_sales_channel_dim_description_key membership_sales_channel_dim_description_key,
       Membership.membership_source membership_source,
       Membership.membership_status membership_status,              ---Membership Status
       Membership.membership_type membership_type,					--Membership type  
       Membership.membership_type_id membership_type_id,			--Membership type ID
       Membership.money_back_cancellation_flag money_back_cancellation_flag,
       Membership.non_payment_termination_flag non_payment_termination_flag,
       Membership.original_sales_dim_team_member_key original_sales_dim_team_member_key,
       Membership.p_mms_membership_id p_mms_membership_id,
       Membership.prior_plus_membership_type prior_plus_membership_type,
       Membership.prior_plus_membership_type_key prior_plus_membership_type_key,
       Membership.prior_plus_price prior_plus_price,
       Membership.prior_plus_undiscounted_price prior_plus_undiscounted_price,
       Membership.revenue_reporting_category_description revenue_reporting_category_description,
       Membership.sales_reporting_category_description sales_reporting_category_description,
       Membership.termination_reason termination_reason,       --termination reason
       Membership.undiscounted_price undiscounted_price,
       Membership.val_country_id val_country_id,
       Membership.val_eft_option_id val_eft_option_id,
       Membership.val_membership_source_id val_membership_source_id,
       Membership.val_membership_status_id val_membership_status_id,
       Membership.val_revenue_reporting_category_id val_revenue_reporting_category_id,
       Membership.val_sales_reporting_category_id val_sales_reporting_category_id,
       Membership.val_state_id val_state_id,
       Membership.val_termination_reason_id val_termination_reason_id,
      
      MembershipHistory.effective_date_time,
      MembershipHistory.expiration_date_time,


       [dMembershipAttribute].membership_type_attribute_id,
	   [dMembershipAttribute].dim_mms_membership_type_key,
	   [rMembershipAttribute].val_membership_type_attribute_id,
	   [rMembershipAttribute].description    ---revenue category 




FROM [dbo].[dim_mms_membership_history] MembershipHistory


JOIN [dbo].[dim_mms_membership] Membership
    ON Membership.dim_mms_membership_key = MembershipHistory.dim_mms_membership_key

JOIN [dbo].[d_mms_member] Member			 --base table dbo.d_mms_member 	
	ON Membership.[dim_mms_membership_key] = Member.[dim_mms_membership_key]

JOIN [dbo].d_mms_membership_type_attribute dMembershipAttribute
	ON [Membership].[dim_mms_membership_type_key] = [dMembershipAttribute].[dim_mms_membership_type_key]

JOIN [dbo].r_mms_val_membership_type_attribute rMembershipAttribute
	ON [dMembershipAttribute].[val_membership_type_attribute_id] = [rMembershipAttribute].[val_membership_type_attribute_id]


JOIN [dbo].[dim_club] Club 
	ON Membership.[home_dim_club_key] = Club.[dim_club_key]

where Member.[val_member_type_id]= '1' -- As the primary individual
	AND [rMembershipAttribute].[Description] like 'membership status%'
	AND [dMembershipAttribute].deleted_flag = 0;