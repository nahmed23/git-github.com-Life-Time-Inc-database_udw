CREATE VIEW [sandbox].[v_mart_sw_membership_sales_promotion_code]
AS SELECT d_mms_membership_sales_promotion_code.[membership_sales_promotion_code_id]
       , d_mms_membership_sales_promotion_code.[membership_id]
       , d_mms_membership_sales_promotion_code.[member_id]
       , d_mms_membership_sales_promotion_code.[sales_promotion_code_id]
       , d_mms_membership_sales_promotion_code.[sales_advisor_employee_id]
       , [inserted_date_no_time] = DATEADD(DD, DATEDIFF(DD, 0, d_mms_membership_sales_promotion_code.[inserted_date_time]), 0)
       , d_mms_membership_sales_promotion_code.[inserted_date_time]
       , d_mms_sales_promotion_code.[sales_promotion_id]
       , [referral_member_id] = d_mms_sales_promotion_code.[member_id]
       , d_mms_sales_promotion_code.[promotion_code]
       , d_mms_sales_promotion_code.[expiration_date]
       , d_mms_sales_promotion.[effective_from_date_time]
       , d_mms_sales_promotion.[effective_thru_date_time]
       , d_mms_sales_promotion.[display_text]
       , d_mms_sales_promotion.[receipt_text]
       , d_mms_sales_promotion.[val_sales_promotion_type_id]
       , d_mms_sales_promotion.[available_for_all_clubs_flag]
       , [sales_promotion_code] = d_mms_sales_promotion.[display_text]
       , d_mms_qualified_sales_promotion.[qualified_sales_promotion_id]
       , d_mms_qualified_sales_promotion.[val_qualified_sales_promotion_type_id]
       , d_mms_qualified_sales_promotion.[promotion_name]
       , [qualified_sales_promotion_description]      = d_mms_qualified_sales_promotion.[description]
       , [qualified_sales_promotion_type_description] = r_mms_val_qualified_sales_promotion_type.[description]
       , d_mms_membership_sales_promotion_code.[dim_mms_membership_key]
       , d_mms_membership_sales_promotion_code.[dim_mms_member_key]
       , d_mms_membership_sales_promotion_code.[dim_mms_sales_promotion_code_key]
       , d_mms_sales_promotion_code.[dim_mms_sales_promotion_key]
    FROM [sandbox].[v_mart_mms_membership_sales_promotion_code] d_mms_membership_sales_promotion_code
         INNER JOIN [sandbox].[v_mart_mms_sales_promotion_code] d_mms_sales_promotion_code
           ON d_mms_sales_promotion_code.[sales_promotion_code_id] = d_mms_membership_sales_promotion_code.[sales_promotion_code_id]
         INNER JOIN [sandbox].[v_mart_mms_sales_promotion] d_mms_sales_promotion
           ON d_mms_sales_promotion.[sales_promotion_id] = d_mms_sales_promotion_code.[sales_promotion_id]
         LEFT OUTER JOIN [sandbox].[v_mart_mms_qualified_sales_promotion] d_mms_qualified_sales_promotion
           ON d_mms_qualified_sales_promotion.[sales_promotion_id] = d_mms_sales_promotion.[sales_promotion_id]
         LEFT OUTER JOIN [sandbox].[v_mart_mms_val_qualified_sales_promotion_type] r_mms_val_qualified_sales_promotion_type
           ON r_mms_val_qualified_sales_promotion_type.[val_qualified_sales_promotion_type_id] = d_mms_qualified_sales_promotion.[val_qualified_sales_promotion_type_id];