CREATE VIEW [sandbox].[v_mart_sw_membership_type_product_active_fit]
AS SELECT [membership_type_id]           = d_mms_membership_type.[membership_type_id]
       , [product_id]                   = d_mms_membership_type.[product_id]
       , [membership_type_display_name] = d_mms_membership_type.[display_name]
       , [product_description]          = d_mms_product.[product_description]
       , [revenue_category]             = d_mms_product.[revenue_category]
       , [gl_account_number]            = d_mms_product.[gl_account_number]
       , [gl_department_code]           = d_mms_product.[gl_department_code]
       , [gl_product_code]              = d_mms_product.[gl_product_code]
       --, [gl_sub_account_number]              = d_mms_product.[gl_sub_account_number]
       , [dim_mms_membership_type_key]  = d_mms_membership_type.[dim_mms_membership_type_key]
       , [dim_mms_product_key]          = d_mms_membership_type.[dim_mms_product_key]
    FROM [dbo].[d_mms_membership_type] d_mms_membership_type
         INNER JOIN [dbo].[d_mms_product] d_mms_product
           ON d_mms_product.[product_id] = d_mms_membership_type.[product_id]
    WHERE d_mms_product.[department_id] = 1
      AND EXISTS
          ( SELECT d_mms_membership_type_attribute.*
              FROM [dbo].[d_mms_membership_type_attribute] d_mms_membership_type_attribute
              WHERE d_mms_membership_type_attribute.[membership_type_id] = d_mms_membership_type.[membership_type_id]
                AND d_mms_membership_type_attribute.[val_membership_type_attribute_id] IN (58,59) )
      AND d_mms_product.[product_description] LIKE '%_Active&Fit';