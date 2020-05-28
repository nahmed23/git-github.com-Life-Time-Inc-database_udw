CREATE VIEW [sandbox].[v_mart_mms_sales_promotion]
AS SELECT PIT.[sales_promotion_id]
     , LNK.[val_sales_promotion_type_id]
     , LNK.[promotion_owner_employee_id]
     , LNK.[company_id]
     , LNK.[val_revenue_reporting_category_id]
     , LNK.[val_sales_reporting_category_id]
     , SAT.[effective_from_date_time]
     , SAT.[effective_thru_date_time]
     , SAT.[display_text]
     , SAT.[receipt_text]
     , SAT.[available_for_all_sales_channels_flag]
     , SAT.[available_for_all_clubs_flag]
     , SAT.[available_for_all_customers_flag]
     , SAT.[inserted_date_time]
     , SAT.[updated_date_time]
     , SAT.[promotion_code_usage_limit]
     , SAT.[promotion_code_required_flag]
     , SAT.[promotion_code_issuer_create_limit]
     , SAT.[promotion_code_overall_create_limit]
     , SAT.[exclude_my_health_check_flag]
     , SAT.[exclude_from_attrition_reporting_flag]
     , [dim_mms_sales_promotion_key] = PIT.[bk_hash]
     , [dim_mms_val_sales_promotion_type_key] = CASE WHEN NOT DIM.[val_sales_promotion_type_id] Is Null THEN CONVERT(char(32), HASHBYTES('MD5', ('P%#&z$@k' + CONVERT(varchar, DIM.[val_sales_promotion_type_id]))),2) ELSE CONVERT(char(32),'-998',2) END
     , [dim_mms_company_key] = CASE WHEN NOT LNK.[company_id] Is Null THEN CONVERT(char(32), HASHBYTES('MD5', ('P%#&z$@k' + CONVERT(varchar, LNK.[company_id]))),2) ELSE CONVERT(char(32),'-998',2) END
     , PIT.[bk_hash]
     , PIT.[p_mms_sales_promotion_id]
     , PIT.[dv_load_date_time]
     , PIT.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (LNK.[dv_hash] + SAT.[dv_hash])),2)
     --, [l_hash] = LNK.[dv_hash]
     --, [s_hash] = SAT.[dv_hash]
  FROM [dbo].[p_mms_sales_promotion] PIT
       INNER JOIN [dbo].[d_mms_sales_promotion] DIM
         ON DIM.[bk_hash] = PIT.[bk_hash]
            AND DIM.[p_mms_sales_promotion_id] = PIT.[p_mms_sales_promotion_id]
       INNER JOIN [dbo].[l_mms_sales_promotion] LNK
         ON LNK.[bk_hash] = PIT.[bk_hash]
            AND LNK.[l_mms_sales_promotion_id] = PIT.[l_mms_sales_promotion_id]
       INNER JOIN [dbo].[s_mms_sales_promotion] SAT
         ON SAT.[bk_hash] = PIT.[bk_hash]
            AND SAT.[s_mms_sales_promotion_id] = PIT.[s_mms_sales_promotion_id];