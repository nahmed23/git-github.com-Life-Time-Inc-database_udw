CREATE PROC [sandbox].[proc_mart_sw_d_mms_membership_recurrent_product] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT PIT.[membership_recurrent_product_id]
     , LNK.[membership_id]
     , LNK.[product_id]
     , LNK.[val_recurrent_product_termination_reason_id]
     , LNK.[club_id]
     , LNK.[last_updated_employee_id]
     , LNK.[commission_employee_id]
     , LNK.[member_id]
     , LNK.[val_recurrent_product_source_id]
     , LNK.[val_assessment_day_id]
     , LNK.[pricing_discount_id]
     , LNK.[val_discount_reason_id]
     , SAT.[activation_date]
     , SAT.[cancellation_request_date]
     , SAT.[termination_date]
     , SAT.[inserted_date_time]
     , SAT.[updated_date_time]
     , SAT.[price]
     , SAT.[created_date_time]
     , SAT.[utc_created_date_time]
     , SAT.[created_date_time_zone]
     , SAT.[last_updated_date_time]
     , SAT.[utc_last_updated_date_time]
     , SAT.[last_updated_date_time_zone]
     , SAT.[product_assessed_date_time]
     , SAT.[comments]
     , SAT.[number_of_sessions]
     , SAT.[price_per_session]
     , SAT.[product_hold_begin_date]
     , SAT.[product_hold_end_date]
     , SAT.[sold_not_serviced_flag]
     , SAT.[retail_price]
     , SAT.[retail_price_per_session]
     , SAT.[promotion_code]
     , SAT.[display_only_flag]
     , PIT.[bk_hash]
     , PIT.[p_mms_membership_recurrent_product_id]
     , PIT.[dv_load_date_time]
     , PIT.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (LNK.[dv_hash] + SAT.[dv_hash])),2)
     --, [l_hash] = LNK.[dv_hash]
     --, [s_hash] = SAT.[dv_hash]
  FROM [dbo].[p_mms_membership_recurrent_product] PIT
       INNER JOIN [dbo].[l_mms_membership_recurrent_product] LNK
         ON LNK.[bk_hash] = PIT.[bk_hash]
            AND LNK.[l_mms_membership_recurrent_product_id] = PIT.[l_mms_membership_recurrent_product_id]
       INNER JOIN[dbo].[s_mms_membership_recurrent_product] SAT
         ON SAT.[bk_hash] = PIT.[bk_hash]
            AND SAT.[s_mms_membership_recurrent_product_id] = PIT.[s_mms_membership_recurrent_product_id]
       INNER JOIN
         ( SELECT PIT.[p_mms_membership_recurrent_product_id]
                , RowRank = RANK() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
                , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
             FROM [dbo].[p_mms_membership_recurrent_product] PIT
             WHERE ( PIT.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
                 AND PIT.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END )
         ) PITU
         ON PITU.[p_mms_membership_recurrent_product_id] = PIT.[p_mms_membership_recurrent_product_id]
            AND PITU.RowRank = 1 AND PITU.RowNumber = 1
  WHERE NOT PIT.[membership_recurrent_product_id] Is Null
    AND NOT EXISTS
      ( SELECT PIT_Child.[product_id]
          FROM [dbo].[p_mms_product] PIT_Child
               INNER JOIN [dbo].[l_mms_product] LNK_Child
                 ON LNK_Child.[bk_hash] = PIT_Child.[bk_hash]
                    AND LNK_Child.[l_mms_product_id] = PIT_Child.[l_mms_product_id]
          WHERE NOT PIT_Child.[product_id] Is Null
            AND PIT_Child.[dv_load_end_date_time] = '9999-12-31 00:00:00.000'
            AND LNK_Child.[department_id] = 3
            AND PIT_Child.[product_id] = LNK.[product_id] )
ORDER BY PIT.[dv_batch_id] ASC, PIT.[dv_load_date_time] ASC, ISNULL(SAT.[updated_date_time], SAT.[inserted_date_time]) ASC;

END
