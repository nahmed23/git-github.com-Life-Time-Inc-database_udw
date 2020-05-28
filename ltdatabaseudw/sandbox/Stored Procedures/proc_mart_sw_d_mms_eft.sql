CREATE PROC [sandbox].[proc_mart_sw_d_mms_eft] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT PIT.[eft_id]
     , LNK.[membership_id]
     , LNK.[val_eft_status_id]
     , LNK.[val_payment_type_id]
     , LNK.[eft_return_code_id]
     , SAT.[eft_date]
     , SAT.[return_code]
     , SAT.[eft_amount]
     , SAT.[inserted_date_time]
     , SAT.[updated_date_time]
     , payment_type_description = d_mms_val_payment_type.[description]
     , return_code_description = d_mms_eft_return_code.[description]
     , PIT.[bk_hash]
     , PIT.[p_mms_eft_id]
     , PIT.[dv_load_date_time]
     , PIT.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (LNK.[dv_hash] + SAT.[dv_hash])),2)
     --, [l_hash] = LNK.[dv_hash]
     --, [s_hash] = SAT.[dv_hash]
     , RowRank = RANK() OVER (PARTITION BY LNK.[membership_id] ORDER BY SAT.[eft_date] DESC)
     , RowNumber = ROW_NUMBER() OVER (PARTITION BY LNK.[membership_id] ORDER BY SAT.[eft_date] DESC)
     , eft_attempt_count = COUNT(*) OVER (PARTITION BY LNK.[membership_id])
  FROM [dbo].[p_mms_eft] PIT
       INNER JOIN [dbo].[l_mms_eft] LNK
         ON LNK.[bk_hash] = PIT.[bk_hash]
            AND LNK.[l_mms_eft_id] = PIT.[l_mms_eft_id]
       INNER JOIN [dbo].[s_mms_eft] SAT
         ON SAT.[bk_hash] = PIT.[bk_hash]
            AND SAT.[s_mms_eft_id] = PIT.[s_mms_eft_id]
       INNER JOIN
         ( SELECT PIT.[p_mms_eft_id]
                , RowRank = RANK() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
                , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
             FROM [dbo].[p_mms_eft] PIT
             --WHERE PIT.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
             --  AND PIT.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END + " 
         ) PITU
         ON PITU.[p_mms_eft_id] = PIT.[p_mms_eft_id]
            AND PITU.RowRank = 1 AND PITU.RowNumber = 1
       INNER JOIN  --d_mms_val_payment_type
         ( SELECT Ref_Child.[val_payment_type_id]
                , Ref_Child.[description]
                , Ref_Child.[view_bank_account_type_flag]
             FROM [dbo].[r_mms_val_payment_type] Ref_Child
             WHERE NOT Ref_Child.[val_payment_type_id] Is Null
               AND Ref_Child.[dv_load_end_date_time] = '9999-12-31 00:00:00.000'
               AND Ref_Child.[view_bank_account_type_flag] = 1
         ) d_mms_val_payment_type
         ON d_mms_val_payment_type.[val_payment_type_id] = LNK.[val_payment_type_id]
       INNER JOIN  --d_mms_eft_return_code
         ( SELECT PIT_Child.[eft_return_code_id]
                , SAT_Child.[description]
             FROM [dbo].[p_mms_eft_return_code] PIT_Child
                  INNER JOIN [dbo].[s_mms_eft_return_code] SAT_Child
                    ON SAT_Child.[bk_hash] = PIT_Child.[bk_hash]
                       AND SAT_Child.[s_mms_eft_return_code_id] = PIT_Child.[s_mms_eft_return_code_id]
             WHERE NOT PIT_Child.[eft_return_code_id] Is Null
               AND PIT_Child.[dv_load_end_date_time] = '9999-12-31 00:00:00.000'
         ) d_mms_eft_return_code
         ON d_mms_eft_return_code.[eft_return_code_id] = LNK.[eft_return_code_id]
  WHERE NOT PIT.[eft_id] Is Null
    AND LNK.[val_eft_status_id] <> 3
    AND (NOT SAT.[return_code] Is null AND SAT.[return_code] <> 100)
    AND SAT.[eft_date] >= '2018-03-18'
ORDER BY PIT.[dv_batch_id] ASC, PIT.[dv_load_date_time] ASC, ISNULL(SAT.[updated_date_time], SAT.[inserted_date_time]) ASC;

END
