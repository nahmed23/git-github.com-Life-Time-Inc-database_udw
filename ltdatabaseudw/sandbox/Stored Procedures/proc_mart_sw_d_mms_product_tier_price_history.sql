CREATE PROC [sandbox].[proc_mart_sw_d_mms_product_tier_price_history] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT d_mms_product_tier_price_history.[product_tier_price_id]
     , d_mms_product_tier_price_history.[product_tier_id]
     , d_mms_product_tier_price_history.[val_card_level_id]
     , d_mms_product_tier_price_history.[val_membership_type_group_id]
     , d_mms_product_tier_price_history.[price]
     , d_mms_product_tier_price_history.[inserted_date_time]
     , d_mms_product_tier_price_history.[updated_date_time]
     , d_mms_product_tier.[product_id]
     , d_mms_product_tier.[val_product_tier_type_id]
     , d_mms_product_tier_price_history.[effective_date_time]
     , d_mms_product_tier_price_history.[bk_hash]
     , d_mms_product_tier_price_history.[p_mms_product_tier_price_id]
     , d_mms_product_tier_price_history.[dv_load_date_time]
     , d_mms_product_tier_price_history.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (batch_info.[l_hash] + batch_info.[s_hash])),2)
     --, [l_hash] = LNK.[dv_hash]
     --, [s_hash] = SAT.[dv_hash]
  FROM [dbo].[d_mms_product_tier_price_history]
       INNER JOIN
         ( SELECT PIT.[d_mms_product_tier_price_history_id]
                , PIT.[effective_date_time]
                , RowRank = RANK() OVER (PARTITION BY PIT.[bk_hash], DATEADD(DD, DATEDIFF(DD, 0, PIT.[effective_date_time]), 0) ORDER BY PIT.[effective_date_time] DESC)
                , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[bk_hash], DATEADD(DD, DATEDIFF(DD, 0, PIT.[effective_date_time]), 0) ORDER BY PIT.[effective_date_time] DESC)
             FROM [dbo].[d_mms_product_tier_price_history] PIT
             WHERE ( PIT.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
                 AND PIT.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END )
         ) PITU
         ON PITU.[d_mms_product_tier_price_history_id] = d_mms_product_tier_price_history.[d_mms_product_tier_price_history_id]
            AND PITU.RowRank = 1 AND PITU.RowNumber = 1
       INNER JOIN [dbo].[p_mms_product_tier_price] PIT
         ON PIT.[bk_hash] = d_mms_product_tier_price_history.[bk_hash]
            AND PIT.[p_mms_product_tier_price_id] = d_mms_product_tier_price_history.[p_mms_product_tier_price_id]
       INNER JOIN [dbo].[l_mms_product_tier_price] LNK
         ON LNK.[bk_hash] = PIT.[bk_hash]
            AND LNK.[l_mms_product_tier_price_id] = PIT.[l_mms_product_tier_price_id]
       INNER JOIN [dbo].[l_mms_product_tier_price_1] LNK_1
         ON LNK_1.[bk_hash] = PIT.[bk_hash]
            AND LNK_1.[l_mms_product_tier_price_1_id] = PIT.[l_mms_product_tier_price_1_id]
       INNER JOIN [dbo].[s_mms_product_tier_price] SAT
         ON SAT.[bk_hash] = PIT.[bk_hash]
            AND SAT.[s_mms_product_tier_price_id] = PIT.[s_mms_product_tier_price_id]
       INNER JOIN
         ( SELECT PIT_Ref.[product_tier_id]
               ,  LNK_Ref.[product_id]
               ,  LNK_Ref.[val_product_tier_type_id]
             FROM [dbo].[p_mms_product_tier] PIT_Ref
                  INNER JOIN [dbo].[l_mms_product_tier] LNK_Ref
                    ON LNK_Ref.[bk_hash] = PIT_Ref.[bk_hash]
                       AND LNK_Ref.[l_mms_product_tier_id] = PIT_Ref.[l_mms_product_tier_id]
             WHERE NOT PIT_Ref.[product_tier_id] Is Null
               AND PIT_Ref.[dv_load_end_date_time] = '9999-12-31 00:00:00.000'
         ) d_mms_product_tier
         ON d_mms_product_tier.[product_tier_id] = LNK.[product_tier_id]
       CROSS APPLY
         ( SELECT [l_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_product_tier_price_history.[product_tier_price_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_product_tier_price_history.[product_tier_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_product_tier_price_history.[val_card_level_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_product_tier_price_history.[val_membership_type_group_id]),'z#@$k%&P'))),2)

                , [s_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_product_tier_price_history.[product_tier_price_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_product_tier_price_history.[price]),'z#@$k%&P'))),2)
         ) batch_info
  WHERE NOT PIT.[product_tier_price_id] Is Null
ORDER BY PIT.[dv_batch_id] ASC, PIT.[dv_load_date_time] ASC, ISNULL(SAT.[updated_date_time], SAT.[inserted_date_time]) ASC;

END
