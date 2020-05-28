CREATE VIEW [sandbox].[v_mart_sw_membership_type_product]
AS SELECT [membership_type_id]                   = d_mms_membership_type.[membership_type_id]
       , [product_id]                           = d_mms_product.[product_id]
       , [membership_type_display_name]         = d_mms_membership_type.[display_name]
       , [product_description]                  = d_mms_product.[product_description]
       , [family_status_display_name]           = LEFT(CASE WHEN CHARINDEX(' ', vVMTFS.[description]) = 0 THEN vVMTFS.[description] ELSE LEFT(vVMTFS.[description], CHARINDEX(' ', vVMTFS.[description]) - 1) END, 6)
       , [family_status_description]            = vVMTFS.[description]
       , [card_level_description]               = vVCL.[description]
       , [membership_type_group_description]    = vVMTG.[description]
       , [access_by_price_paid_flag]            = CAST(CASE WHEN d_mms_product.[access_by_price_paid_flag] = 'Y' THEN 1 ELSE 0 END AS bit)
       , [assess_jr_member_dues_flag]           = CAST(CASE WHEN d_mms_product.[junior_member_dues_flag] = 'Y' THEN 1 ELSE 0 END AS bit)
       , [revenue_category]                     = d_mms_product.[revenue_category]
       , [gl_account_number]                    = d_mms_product.[gl_account_number]
       , [gl_department_code]                   = d_mms_product.[gl_department_code]
       , [gl_product_code]                      = d_mms_product.[gl_product_code]
       --, [gl_sub_account_number]              = d_mms_product.[gl_sub_account_number]
       , [department_id]                        = d_mms_product.[department_id]
       , [val_membership_type_family_status_id] = d_mms_membership_type.[val_membership_type_family_status_id]
       , [val_pricing_method_id]                = d_mms_membership_type.[val_pricing_method_id]
       , [val_pricing_rule_id]                  = d_mms_membership_type.[val_pricing_rule_id]
       , [val_product_status_id]                = r_mms_val_product_status.[val_product_status_id]
       , [val_card_level_id]                    = vVMTG.[val_card_level_id]
       , [val_check_in_group_id]                = d_mms_membership_type.[val_check_in_group_id]
       , [val_membership_type_group_id]         = d_mms_membership_type.[val_membership_type_group_id]
       , [min_unit_type]                        = d_mms_membership_type.[unit_type_minimum]
       , [max_unit_type]                        = d_mms_membership_type.[unit_type_maximum]
       , [dim_mms_membership_type_key]          = d_mms_membership_type.[dim_mms_membership_type_key]
       , [dim_mms_product_key]                  = d_mms_membership_type.[dim_mms_product_key]
       , [dim_mms_val_product_status_key]       = d_mms_product.[r_mms_val_product_status_bk_hash]
    FROM [dbo].[d_mms_membership_type] d_mms_membership_type
         INNER JOIN [dbo].[d_mms_product] d_mms_product
           ON d_mms_product.[dim_mms_product_key] = d_mms_membership_type.[dim_mms_product_key]
         INNER JOIN [dbo].[r_mms_val_product_status]
           ON r_mms_val_product_status.[bk_hash] = d_mms_product.[r_mms_val_product_status_bk_hash]
         INNER JOIN [dbo].[r_mms_val_membership_type_family_status] vVMTFS
           ON vVMTFS.[val_membership_type_family_status_id] = d_mms_membership_type.[val_membership_type_family_status_id]
         LEFT OUTER JOIN [dbo].[r_mms_val_membership_type_group] vVMTG
           ON vVMTG.[val_membership_type_group_id] = d_mms_membership_type.[val_membership_type_group_id]
         LEFT OUTER JOIN [dbo].[r_mms_val_card_level] vVCL
           ON vVCL.val_card_level_id = vVMTG.val_card_level_id
    WHERE d_mms_product.[department_id] = 1;