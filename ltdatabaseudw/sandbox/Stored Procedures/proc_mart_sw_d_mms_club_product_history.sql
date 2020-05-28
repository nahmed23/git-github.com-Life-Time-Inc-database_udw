CREATE PROC [sandbox].[proc_mart_sw_d_mms_club_product_history] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT d_mms_club_product_history.[club_product_id]
     , LNK.[club_id]
     , LNK.[product_id]
     , d_mms_club_product_history.[price]
     , SAT.[inserted_date_time]
     , SAT.[updated_date_time]
     , d_mms_club_product_history.[effective_date_time]
     , d_mms_club_product_history.[expiration_date_time]
     , PIT.[bk_hash]
     , d_mms_club_product_history.[p_mms_club_product_id]
     , PIT.[dv_load_date_time]
     , PIT.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (batch_info.[l_hash] + batch_info.[s_hash])),2)
     --, [dv_deleted] = CAST(0 AS bit)
  FROM [dbo].[d_mms_club_product_history] d_mms_club_product_history
       INNER JOIN
         ( SELECT PIT.[d_mms_club_product_history_id]
                , PIT.[effective_date_time]
                , RowRank = RANK() OVER (PARTITION BY PIT.[dim_club_key], PIT.[dim_mms_product_key], DATEADD(DD, DATEDIFF(DD, 0, PIT.[effective_date_time]), 0) ORDER BY PIT.[effective_date_time] DESC)
                , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[dim_club_key], PIT.[dim_mms_product_key], DATEADD(DD, DATEDIFF(DD, 0, PIT.[effective_date_time]), 0) ORDER BY PIT.[effective_date_time] DESC)
             FROM [dbo].[d_mms_club_product_history] PIT
             WHERE ( PIT.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
                 AND PIT.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END )
               AND NOT EXISTS
                 ( SELECT *
                     FROM [dbo].[d_mms_club_product_history] PIT_Prior
                     WHERE ( PIT.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
                         AND PIT.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END )
                       AND PIT_Prior.[dim_club_key] = PIT.[dim_club_key]
                       AND PIT_Prior.[dim_mms_product_key] = PIT.[dim_mms_product_key]
                       AND PIT_Prior.[dim_club_bridge_dim_mms_product_key] = PIT.[dim_club_bridge_dim_mms_product_key]
                       AND PIT_Prior.[price] = PIT.[price]
                       AND DATEADD(DD, DATEDIFF(DD, 0, PIT_Prior.[effective_date_time]), 0) < DATEADD(DD, DATEDIFF(DD, 0, PIT.[effective_date_time]), 0) )
         ) PITU
         ON PITU.[d_mms_club_product_history_id] = d_mms_club_product_history.[d_mms_club_product_history_id]
            AND PITU.RowRank = 1 AND PITU.RowNumber = 1
       INNER JOIN [dbo].[p_mms_club_product] PIT
         ON PIT.[bk_hash] = d_mms_club_product_history.[bk_hash]
            AND PIT.[p_mms_club_product_id] = d_mms_club_product_history.[p_mms_club_product_id]
       INNER JOIN [dbo].[l_mms_club_product] LNK
         ON LNK.[bk_hash] = PIT.[bk_hash]
            AND LNK.[l_mms_club_product_id] = PIT.[l_mms_club_product_id]
       INNER JOIN[dbo].[s_mms_club_product] SAT
         ON SAT.[bk_hash] = PIT.[bk_hash]
            AND SAT.[s_mms_club_product_id] = PIT.[s_mms_club_product_id]
       CROSS APPLY
         ( SELECT [l_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, PIT.[club_product_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, LNK.[club_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, LNK.[product_id]),'z#@$k%&P'))),2)

                , [s_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, PIT.[club_product_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, SAT.[price]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_club_product_history.[effective_date_time], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_club_product_history.[expiration_date_time], 120),'z#@$k%&P'))),2)
         
         ) batch_info
  WHERE NOT ( PIT.[club_product_id] Is Null
           OR LNK.[club_id] Is Null
           OR LNK.[product_id] Is Null )
ORDER BY PIT.[dv_batch_id] ASC, PIT.[dv_load_date_time] ASC, PITU.[effective_date_time] ASC, PIT.[club_product_id] ASC;

END
