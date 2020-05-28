CREATE PROC [sandbox].[proc_mart_sw_d_mms_membership_phone] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT PIT.[membership_phone_id]
     , LNK.[membership_id]
     , LNK.[val_phone_type_id]
     , SAT.[area_code]
     , SAT.[number]
     , SAT.[inserted_date_time]
     , SAT.[updated_date_time]
     , PIT.[bk_hash]
     , PIT.[p_mms_membership_phone_id]
     , PIT.[dv_load_date_time]
     , PIT.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (LNK.[dv_hash] + SAT.[dv_hash])),2)
     --, [l_hash] = LNK.[dv_hash]
     --, [s_hash] = SAT.[dv_hash]
     , PITU.[dv_deleted]
  FROM [dbo].[p_mms_membership_phone] PIT
       INNER JOIN [dbo].[l_mms_membership_phone] LNK
         ON LNK.[bk_hash] = PIT.[bk_hash]
            AND LNK.[l_mms_membership_phone_id] = PIT.[l_mms_membership_phone_id]
       INNER JOIN[dbo].[s_mms_membership_phone] SAT
         ON SAT.[bk_hash] = PIT.[bk_hash]
            AND SAT.[s_mms_membership_phone_id] = PIT.[s_mms_membership_phone_id]
       INNER JOIN
         ( SELECT PIT.[p_mms_membership_phone_id]
                , RowRank = RANK() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
                , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
                , HUB.[dv_inserted_date_time]
                , HUB.[dv_updated_date_time]
                , HUB.[dv_batch_id]
                , HUB.[dv_deleted]
             FROM [dbo].[p_mms_membership_phone] PIT
                  INNER JOIN
                    ( SELECT HUB.[bk_hash]
                           , HUB.[dv_inserted_date_time]
                           , HUB.[dv_updated_date_time]
                           , [dv_batch_id] = CONVERT(bigint, CONVERT(VARCHAR(8), ISNULL(HUB.[dv_updated_date_time],HUB.[dv_inserted_date_time]), 112) + REPLACE(CONVERT(varchar(8), ISNULL(HUB.[dv_updated_date_time],HUB.[dv_inserted_date_time]), 114), ':',''))
                           , HUB.[dv_deleted]
                        FROM [dbo].[h_mms_membership_phone] HUB
                    ) HUB
                    ON HUB.[bk_hash] = PIT.[bk_hash]
             WHERE ( (HUB.[dv_deleted] = 0
                      AND ( PIT.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
                        AND PIT.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END ) )
                  OR (HUB.[dv_deleted] = 1) )
                      --AND HUB.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END
                      --AND HUB.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END + ") )
         ) PITU
         ON PITU.[p_mms_membership_phone_id] = PIT.[p_mms_membership_phone_id]
            AND ((PITU.[dv_deleted] = 0 AND PITU.RowRank = 1 AND PITU.RowNumber = 1) OR PITU.[dv_deleted] = 1)
  WHERE NOT PIT.[membership_phone_id] Is Null
ORDER BY PIT.[dv_batch_id] ASC, PIT.[dv_load_date_time] ASC, ISNULL(SAT.[updated_date_time], SAT.[inserted_date_time]) ASC;

END
