CREATE PROC [sandbox].[proc_mart_sw_d_mms_membership_modification_flex_reason] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT PIT.[membership_modification_flex_reason_id]
     , LNK.[membership_modification_request_id]
     , LNK.[val_flex_reason_id]
     , SAT.[inserted_date_time]
     , SAT.[updated_date_time]
     , PIT.[bk_hash]
     , PIT.[p_mms_membership_modification_flex_reason_id]
     , PIT.[dv_load_date_time]
     , PIT.[dv_batch_id]
     , [dv_hash] = CONVERT(char(32), HASHBYTES('MD5', (LNK.[dv_hash] + SAT.[dv_hash])),2)
     --, [l_hash] = LNK.[dv_hash]
     --, [s_hash] = SAT.[dv_hash]
  FROM [dbo].[p_mms_membership_modification_flex_reason] PIT
       INNER JOIN [dbo].[l_mms_membership_modification_flex_reason] LNK
         ON LNK.[bk_hash] = PIT.[bk_hash]
            AND LNK.[l_mms_membership_modification_flex_reason_id] = PIT.[l_mms_membership_modification_flex_reason_id]
       INNER JOIN[dbo].[s_mms_membership_modification_flex_reason] SAT
         ON SAT.[bk_hash] = PIT.[bk_hash]
            AND SAT.[s_mms_membership_modification_flex_reason_id] = PIT.[s_mms_membership_modification_flex_reason_id]
       INNER JOIN
         ( SELECT PIT.[p_mms_membership_modification_flex_reason_id]
                , RowRank = RANK() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
                , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
             FROM [dbo].[p_mms_membership_modification_flex_reason] PIT
             WHERE ( PIT.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
                 AND PIT.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END )
         ) PITU
         ON PITU.[p_mms_membership_modification_flex_reason_id] = PIT.[p_mms_membership_modification_flex_reason_id]
            AND PITU.RowRank = 1 AND PITU.RowNumber = 1
  WHERE NOT PIT.[membership_modification_flex_reason_id] Is Null
ORDER BY PIT.[dv_batch_id] ASC, PIT.[dv_load_date_time] ASC, ISNULL(SAT.[updated_date_time], SAT.[inserted_date_time]) ASC;

END
