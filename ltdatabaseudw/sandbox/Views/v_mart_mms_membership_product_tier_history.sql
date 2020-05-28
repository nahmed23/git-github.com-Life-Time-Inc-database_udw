CREATE VIEW [sandbox].[v_mart_mms_membership_product_tier_history]
AS SELECT PIT.[membership_product_tier_id]
     , LNK.[membership_id]
     , LNK.[product_tier_id]
     , SAT.[inserted_date_time]
     , SAT.[updated_date_time]
     , [effective_date_no_time] = DATEADD(DD, DATEDIFF(DD, 0, PITU.[effective_date_time]), 0)
     , PITU.[effective_date_time]
     , d_mms_product_tier.[product_id]
     , d_mms_product_tier.[val_product_tier_type_id]
     , DIM.[dim_mms_membership_key]
     , DIM.[dim_mms_product_tier_key]
     , d_mms_product_tier.[dim_mms_product_key]
     , PIT.[bk_hash]
     , PIT.[p_mms_membership_product_tier_id]
     , PIT.[dv_load_date_time]
     , PIT.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (batch_info.[l_hash] + batch_info.[s_hash])),2)
  FROM [dbo].[p_mms_membership_product_tier] PIT
       INNER JOIN [dbo].[d_mms_membership_product_tier] DIM
         ON DIM.[bk_hash] = PIT.[bk_hash]
            AND DIM.[p_mms_membership_product_tier_id] = PIT.[p_mms_membership_product_tier_id]
       INNER JOIN [dbo].[l_mms_membership_product_tier] LNK
         ON LNK.[bk_hash] = PIT.[bk_hash]
            AND LNK.[l_mms_membership_product_tier_id] = PIT.[l_mms_membership_product_tier_id]
       INNER JOIN[dbo].[s_mms_membership_product_tier] SAT
         ON SAT.[bk_hash] = PIT.[bk_hash]
            AND SAT.[s_mms_membership_product_tier_id] = PIT.[s_mms_membership_product_tier_id]
       INNER JOIN
         ( SELECT PIT.[bk_hash]
                , PIT.[p_mms_membership_product_tier_id]
                , PIT_Timestamp.[effective_date_time]
                , RowRank = RANK() OVER (PARTITION BY LNK.[membership_id], DATEADD(DD, DATEDIFF(DD, 0, PIT_Timestamp.[effective_date_time]), 0) ORDER BY PIT.[dv_load_end_date_time] DESC, PIT_Timestamp.[effective_date_time] DESC)
                , RowNumber = ROW_NUMBER() OVER (PARTITION BY LNK.[membership_id], DATEADD(DD, DATEDIFF(DD, 0, PIT_Timestamp.[effective_date_time]), 0) ORDER BY PIT.[dv_load_end_date_time] DESC, PIT_Timestamp.[effective_date_time] DESC)
             FROM [dbo].[p_mms_membership_product_tier] PIT
                  INNER JOIN [dbo].[l_mms_membership_product_tier] LNK
                    ON LNK.[bk_hash] = PIT.[bk_hash]
                       AND LNK.[l_mms_membership_product_tier_id] = PIT.[l_mms_membership_product_tier_id]
                  INNER JOIN[dbo].[s_mms_membership_product_tier] SAT
                    ON SAT.[bk_hash] = PIT.[bk_hash]
                       AND SAT.[s_mms_membership_product_tier_id] = PIT.[s_mms_membership_product_tier_id]
                  CROSS APPLY
                    ( SELECT [effective_date_time] = ISNULL(ISNULL(PIT.[dv_greatest_satellite_date_time]
                                                                 , CASE WHEN PIT.[dv_first_in_key_series] = 1
                                                                        THEN SAT.[inserted_date_time]
                                                                        ELSE ISNULL(SAT.[updated_date_time],SAT.[inserted_date_time]) END)
                                                          , CAST('2000-01-01' AS datetime))
                    ) PIT_Timestamp
         ) PITU
         ON PITU.[bk_hash] = PIT.[bk_hash]
            AND PITU.[p_mms_membership_product_tier_id] = PIT.[p_mms_membership_product_tier_id]
            AND PITU.RowRank = 1 AND PITU.RowNumber = 1
       INNER JOIN
         ( SELECT PIT_Ref.[product_tier_id]
                , LNK_Ref.[product_id]
                , LNK_Ref.[val_product_tier_type_id]
                , DIM_Ref.[dim_product_tier_key]
                , DIM_Ref.[dim_mms_product_key]
             FROM [dbo].[p_mms_product_tier] PIT_Ref
                  INNER JOIN [dbo].[d_mms_product_tier] DIM_Ref
                    ON DIM_Ref.[bk_hash] = PIT_Ref.[bk_hash]
                       AND DIM_Ref.[p_mms_product_tier_id] = PIT_Ref.[p_mms_product_tier_id]
                  INNER JOIN [dbo].[l_mms_product_tier] LNK_Ref
                    ON LNK_Ref.[bk_hash] = PIT_Ref.[bk_hash]
                       AND LNK_Ref.[l_mms_product_tier_id] = PIT_Ref.[l_mms_product_tier_id]
             WHERE NOT PIT_Ref.[product_tier_id] Is Null
               AND PIT_Ref.[dv_load_end_date_time] = '9999-12-31 00:00:00.000'
         ) d_mms_product_tier
           ON d_mms_product_tier.[product_tier_id] = LNK.[product_tier_id]
       CROSS APPLY
         ( SELECT [l_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, PIT.[membership_product_tier_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, LNK.[membership_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, LNK.[product_tier_id]),'z#@$k%&P'))),2)

                , [s_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, PIT.[membership_product_tier_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, SAT.[inserted_date_time], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, SAT.[updated_date_time], 120),'z#@$k%&P'))),2)
         ) batch_info;