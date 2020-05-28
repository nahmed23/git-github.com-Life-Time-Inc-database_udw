CREATE VIEW [sandbox].[v_mart_mms_product_tier_price_history]
AS SELECT DIM.[product_tier_price_id]
     , DIM.[product_tier_id]
     , DIM.[val_card_level_id]
     , DIM.[val_membership_type_group_id]
     , DIM.[price]
     , DIM.[inserted_date_time]
     , DIM.[updated_date_time]
     , d_mms_product_tier.[product_id]
     , d_mms_product_tier.[val_product_tier_type_id]
     , [effective_date_no_time] = DATEADD(DD, DATEDIFF(DD, 0, DIM.[effective_date_time]), 0)
     , DIM.[effective_date_time]
     , DIM.[bk_hash]
     , DIM.[p_mms_product_tier_price_id]
     , DIM.[dv_load_date_time]
     , DIM.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (batch_info.[l_hash] + batch_info.[s_hash])),2)
  FROM [dbo].[d_mms_product_tier_price_history] DIM
       --INNER JOIN [dbo].[p_mms_product_tier_price] PIT
       --  ON PIT.[bk_hash] = DIM.[bk_hash]
       --     AND PIT.[p_mms_product_tier_price_id] = DIM.[p_mms_product_tier_price_id]
       --INNER JOIN [dbo].[l_mms_product_tier_price] LNK
       --  ON LNK.[bk_hash] = PIT.[bk_hash]
       --     AND LNK.[l_mms_product_tier_price_id] = PIT.[l_mms_product_tier_price_id]
       --INNER JOIN [dbo].[l_mms_product_tier_price_1] LNK_1
       --  ON LNK_1.[bk_hash] = PIT.[bk_hash]
       --     AND LNK_1.[l_mms_product_tier_price_1_id] = PIT.[l_mms_product_tier_price_1_id]
       --INNER JOIN [dbo].[s_mms_product_tier_price] SAT
       --  ON SAT.[bk_hash] = PIT.[bk_hash]
       --     AND SAT.[s_mms_product_tier_price_id] = PIT.[s_mms_product_tier_price_id]
        --INNER JOIN
        --  ( SELECT PIT.[DIM_id]
        --         , PIT.[effective_date_time]
        --         , RowRank = RANK() OVER (PARTITION BY PIT.[bk_hash], DATEADD(DD, DATEDIFF(DD, 0, PIT.[effective_date_time]), 0) ORDER BY PIT.[effective_date_time] DESC)
        --         , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[bk_hash], DATEADD(DD, DATEDIFF(DD, 0, PIT.[effective_date_time]), 0) ORDER BY PIT.[effective_date_time] DESC)
        --      FROM [dbo].[DIM] PIT
        --  ) PITU
        --  ON PITU.[DIM_id] = DIM.[DIM_id]
        --     AND PITU.RowRank = 1 AND PITU.RowNumber = 1
       INNER JOIN
         ( SELECT PIT_Ref.[product_tier_id]
                , LNK_Ref.[product_id]
                , LNK_Ref.[val_product_tier_type_id]
             FROM [dbo].[p_mms_product_tier] PIT_Ref
                  INNER JOIN [dbo].[l_mms_product_tier] LNK_Ref
                    ON LNK_Ref.[bk_hash] = PIT_Ref.[bk_hash]
                       AND LNK_Ref.[l_mms_product_tier_id] = PIT_Ref.[l_mms_product_tier_id]
             WHERE NOT PIT_Ref.[product_tier_id] Is Null
               AND PIT_Ref.[dv_load_end_date_time] = '9999-12-31 00:00:00.000'
         ) d_mms_product_tier
         ON d_mms_product_tier.[product_tier_id] = DIM.[product_tier_id]
       CROSS APPLY
         ( SELECT [l_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[product_tier_price_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[product_tier_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[val_card_level_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[val_membership_type_group_id]),'z#@$k%&P'))),2)

                , [s_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[product_tier_price_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[price]),'z#@$k%&P'))),2)
         ) batch_info;