CREATE VIEW [sandbox].[v_mart_sw_member_info]
AS SELECT d_mms_member.[first_name]
       , d_mms_member.[middle_name]
       , d_mms_member.[last_name]
       , d_mms_member.[member_name]
       , [member_type_description] = r_mms_val_member_type.[description]
       , d_mms_member.[active_flag]

       , [enrollment_type_description]           = r_mms_val_enrollment_type.[description]
       , [membership_source_description]         = r_mms_val_membership_source.[description]
       , [membership_status_description]         = r_mms_val_membership_status.[description]
       , [membership_type_display_name]          = vMTP.[membership_type_display_name]
       , [membership_family_status_display_name] = vMTP.[family_status_display_name]
       , [termination_reason_description]        = r_mms_val_termination_reason.[description]
       , [membership_type_product_description]   = vMTP.[product_description]
       , [membership_type_group_description]     = vMTP.[membership_type_group_description]

       , d_mms_membership.[current_price]
       , d_mms_membership.[undiscounted_price]
       , d_mms_membership.[join_fee_paid]
       , d_mms_membership.[prior_plus_price]
       , d_mms_membership.[prior_plus_undiscounted_price]
       , d_mms_membership.[prior_plus_membership_type_id]
       , d_mms_membership.[product_tier_price]

       , [access_by_price_paid_flag] = ISNULL(vMTP.[access_by_price_paid_flag],0)

       --, ClubAssessJrMemberDuesFlag = ISNULL(vCD.[assess_jr_member_dues_flag],1)
       , [member_assess_jr_member_dues_flag]          = ISNULL(d_mms_member.[assess_jr_member_dues_flag],1)
       , [membership_type_assess_jr_member_dues_flag] = ISNULL(vMTP.[assess_jr_member_dues_flag],1)

       , d_mms_member.[age]
       , d_mms_member.[birthday]
       , d_mms_member.[dob]
       , d_mms_member.[gender]
       , d_mms_member.[join_date]
       , Tenure = CAST(ISNULL(DATEDIFF(MM, ISNULL(d_mms_member.[join_date], d_mms_membership.[created_date_time]), ISNULL(d_mms_membership.[expiration_date], GETDATE())), 0) AS int)
       , CreatedDate = ISNULL(d_mms_membership.[created_date_no_time], d_mms_member.[join_date])

       , d_mms_membership.[created_date_no_time]
       , d_mms_membership.[created_date_time]
       , d_mms_membership.[activation_date_no_time]
       , d_mms_membership.[activation_date]
       , d_mms_membership.[cancellation_request_date_no_time]
       , d_mms_membership.[cancellation_request_date]
       , d_mms_membership.[expiration_date_no_time]
       , d_mms_membership.[expiration_date]
       , d_mms_membership.[updated_date_time]

       , d_mms_member.[member_id]
       , d_mms_membership.[membership_id]
       , d_mms_member.[val_member_type_id]
       , d_mms_membership.[membership_type_id]
       , d_mms_membership.[advisor_employee_id]
       , d_mms_membership.[club_id]
       , d_mms_membership.[company_id]
       , d_mms_membership.[val_enrollment_type_id]
       , d_mms_membership.[val_membership_source_id]
       , d_mms_membership.[val_membership_status_id]
       , vMTP.[val_membership_type_family_status_id]
       , d_mms_membership.[val_termination_reason_id]
       , d_mms_membership.[qualified_sales_promotion_id]
       , vMTP.[product_id]
       , [product_department_id] = vMTP.[department_id]
       , vMTP.[val_check_in_group_id]
       , vMTP.[val_membership_type_group_id]
       , vMTP.[val_pricing_method_id]
       , d_mms_membership.[crm_opportunity_id]
       , d_mms_member.[crm_contact_id]

       --, vCD.ValPreSaleID
       --, vCD.ClubCode
       --, vCD.ClubName
       --, vCD.ClubOpate
       --, vCD.ClubCloseDate
       --, vCD.SalesAreaName
       --, vCD.SalesAreaRegionName
       --, vCD.ValSalesAreaID
       , [advisor_club_id] = d_mms_employee.[club_id]
       , [advisor_name]    = d_mms_employee.[employee_name]
       --, d_mms_member.MemberImagePath   --= CAST(CAST(d_mms_member.[member_id] / 1000 AS int) * 1000 AS varchar) + '/' + CAST(d_mms_member.[member_id] AS varchar) + '.jpg'
       , [member_type_sort_order] = r_mms_val_member_type.[sort_order]

       , d_mms_member.[dim_mms_member_key]
       , d_mms_membership.[dim_mms_membership_key]
       , d_mms_membership.[dim_mms_membership_type_key]
       , d_mms_membership.[dim_advisor_employee_key]
       , d_mms_membership.[dim_club_key]
       , d_mms_membership.[dim_mms_company_key]
    FROM [sandbox].[v_mart_mms_membership] d_mms_membership
         INNER JOIN [sandbox].[v_mart_mms_member] d_mms_member
           ON d_mms_member.[membership_id] = d_mms_membership.[membership_id]
         INNER JOIN [sandbox].[v_mart_mms_val_member_type] r_mms_val_member_type
           ON r_mms_val_member_type.[val_member_type_id] = d_mms_member.[val_member_type_id]
         LEFT OUTER JOIN [sandbox].[v_mart_mms_val_membership_source] r_mms_val_membership_source
           ON r_mms_val_membership_source.[val_membership_source_id] = d_mms_membership.[val_membership_source_id]
         INNER JOIN [sandbox].[v_mart_mms_val_membership_status] r_mms_val_membership_status
           ON r_mms_val_membership_status.[val_membership_status_id] = d_mms_membership.[val_membership_status_id]
         INNER JOIN [sandbox].[v_mart_sw_membership_type_product] vMTP
           ON vMTP.[membership_type_id] = d_mms_membership.[membership_type_id]
         LEFT OUTER JOIN [sandbox].[v_mart_mms_val_enrollment_type] r_mms_val_enrollment_type
           ON r_mms_val_enrollment_type.[val_enrollment_type_id] = d_mms_membership.[val_enrollment_type_id]
         LEFT OUTER JOIN [sandbox].[v_mart_mms_val_termination_reason] r_mms_val_termination_reason
           ON r_mms_val_termination_reason.[val_termination_reason_id] = d_mms_membership.[val_termination_reason_id]
         --INNER JOIN [sandbox].[mart_vClub_Detail] vCD
         --  ON vCD.ClubID = d_mms_membership.ClubID
         LEFT OUTER JOIN [sandbox].[v_mart_mms_employee] d_mms_employee
           ON d_mms_employee.[employee_id] = d_mms_membership.[advisor_employee_id];