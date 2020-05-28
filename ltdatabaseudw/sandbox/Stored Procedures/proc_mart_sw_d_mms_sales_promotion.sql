CREATE PROC [sandbox].[proc_mart_sw_d_mms_sales_promotion] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT PIT.[sales_promotion_id]
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
     , PIT.[bk_hash]
     , PIT.[p_mms_sales_promotion_id]
     , PIT.[dv_load_date_time]
     , PIT.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (LNK.[dv_hash] + SAT.[dv_hash])),2)
     --, [l_hash] = LNK.[dv_hash]
     --, [s_hash] = SAT.[dv_hash]
  FROM [dbo].[p_mms_sales_promotion] PIT
       INNER JOIN [dbo].[l_mms_sales_promotion] LNK
         ON LNK.[bk_hash] = PIT.[bk_hash]
            AND LNK.[l_mms_sales_promotion_id] = PIT.[l_mms_sales_promotion_id]
       INNER JOIN [dbo].[s_mms_sales_promotion] SAT
         ON SAT.[bk_hash] = PIT.[bk_hash]
            AND SAT.[s_mms_sales_promotion_id] = PIT.[s_mms_sales_promotion_id]
       INNER JOIN
         ( SELECT PIT.[p_mms_sales_promotion_id]
                , RowRank = RANK() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
                , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
             FROM [dbo].[p_mms_sales_promotion] PIT
             WHERE ( PIT.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
                 AND PIT.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END )
         ) PITU
         ON PITU.[p_mms_sales_promotion_id] = PIT.[p_mms_sales_promotion_id]
            AND PITU.RowRank = 1 AND PITU.RowNumber = 1
  WHERE NOT PIT.[sales_promotion_id] Is Null
ORDER BY PIT.[dv_batch_id] ASC, PIT.[dv_load_date_time] ASC, ISNULL(SAT.[updated_date_time], SAT.[inserted_date_time]) ASC;

END
