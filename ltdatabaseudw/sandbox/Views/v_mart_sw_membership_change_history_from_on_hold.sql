CREATE VIEW [sandbox].[v_mart_sw_membership_change_history_from_on_hold]
AS SELECT vMCH.[membership_id]
       , vMCH.[created_date_no_time]
       , vMCH.[created_date_time]
       , vMCH.[new_club_id]
       , vMCH.[new_advisor_employee_id]
       , vMCH.[new_company_id]
       , vMCH.[new_membership_type_id]
       , vMCH.[new_val_enrollment_type_id]
       , vMCH.[new_val_membership_source_id]
       , vMCH.[new_val_membership_status_id]
       , vMCH.[new_val_termination_reason_id]
       , vMCH.[new_current_price]
       , vMCH.[new_activation_date_no_time]
       , vMCH.[new_activation_date]
       , vMCH.[new_cancellation_request_date_no_time]
       , vMCH.[new_cancellation_request_date]
       , vMCH.[new_expiration_date_no_time]
       , vMCH.[new_expiration_date]
       , vMCH.[new_effective_date_no_time]
       , vMCH.[new_effective_date_time]
       , [effective_month_starting_date]       = DATEADD(MM, DATEDIFF(MM, 0, vMCH.[new_effective_date_time]), 0)
       , [effective_month_ending_date_time]    = DATEADD(SS, -1, DATEADD(MM, 1, DATEADD(MM, DATEDIFF(MM, 0, vMCH.[new_effective_date_time]), 0)))
       , [effective_month_ending_date_no_time] = DATEADD(DD, -1, DATEADD(MM, 1, DATEADD(MM, DATEDIFF(MM, 0, vMCH.[new_effective_date_time]), 0)))
       , vMCH.[old_membership_type_id]
       , vMCH.[old_current_price]
       , vMCH.[old_effective_date_no_time]
       , vMCH.[old_effective_date_time]
       , [old_val_membership_type_attribute_id] = vMTA_Old.[val_membership_type_attribute_id]
       , [old_membership_type_attribute_description] = vVMTA_Old.[description]
       , [old_membership_type_attribute_group_description] = 'On Hold'
       , [price_difference] = (vMCH.[new_current_price] - vMCH.[old_current_price])
       , DDate.[month_starting_date]
       , DDate.[calendar_date]
       , vMCH.[dim_new_club_key]
       , vMCH.[dim_new_advisor_employee_key]
       , vMCH.[dim_new_company_key]
       , vMCH.[dim_new_membership_type_key]
       , vMCH.[dim_old_membership_type_key]
    FROM [sandbox].[v_mart_sw_membership_change_history] vMCH
         CROSS JOIN [dbo].[dim_date] DDate
         INNER JOIN [sandbox].[v_mart_mms_membership_type_attribute] vMTA_Old
           ON vMTA_Old.[membership_type_id] = vMCH.[old_membership_type_id]
         INNER JOIN [sandbox].[v_mart_mms_val_membership_type_attribute] vVMTA_Old
           ON vVMTA_Old.[val_membership_type_attribute_id] = vMTA_Old.[val_membership_type_attribute_id]
    WHERE vMCH.[old_effective_date_time] = ( SELECT MAX(vMSH_Max.effective_date_time)
                                               FROM [sandbox].[v_mart_mms_membership_history] vMSH_Max
                                               WHERE vMSH_Max.[membership_id] = vMCH.[membership_id]
                                                 AND vMSH_Max.[effective_date_no_time] < DDate.[month_starting_date] )
      AND vMTA_Old.[val_membership_type_attribute_id] = 57
      --AND EXISTS ( SELECT vMTP.[membership_type_id] FROM [sandbox].[mart_vMembershipTypeProduct_OnHold] vMTP WHERE vMTP.[membership_type_id] = vMCH.[old_membership_type_id] )
      AND vMCH.[new_effective_date_time] = ( SELECT MAX(vMSH_Max.effective_date_time)
                                               FROM [sandbox].[v_mart_mms_membership_history] vMSH_Max
                                               WHERE vMSH_Max.[membership_id] = vMCH.[membership_id]
                                                 AND vMSH_Max.[effective_date_no_time] <= DDate.[calendar_date]
                                                 AND vMSH_Max.[effective_date_no_time] >= DDate.[month_starting_date] )
      --AND NOT EXISTS ( SELECT vMTP.[membership_type_id] FROM [sandbox].[mart_vMembershipTypeProduct_House_Account] vMTP WHERE vMTP.[membership_type_id] = vMCH.[new_membership_type_id] )
      AND EXISTS ( SELECT vMTP.[membership_type_id] FROM [sandbox].[v_mart_sw_membership_type_product_on_hold_eligible] vMTP WHERE vMTP.[membership_type_id] = vMCH.[new_membership_type_id] )
      --AND NOT EXISTS
      --  ( SELECT vMTA.[membership_type_id]
      --      FROM [sandbox].[vMembershipTypeAttribute] vMTA
      --      WHERE vMTA.[membership_type_id] = vMCH.[new_membership_type_id]
      --        AND vMTA.[val_membership_type_attribute_id] IN (29,57,58,59) )
      AND (vMCH.[new_val_termination_reason_id] Is Null OR NOT EXISTS ( SELECT VTR.[val_termination_reason_id] FROM [sandbox].[v_mart_mms_val_termination_reason_money_back] VTR WHERE VTR.[val_termination_reason_id] = vMCH.[new_val_termination_reason_id] ));