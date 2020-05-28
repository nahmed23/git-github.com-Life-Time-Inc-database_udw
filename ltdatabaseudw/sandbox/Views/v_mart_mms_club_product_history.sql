CREATE VIEW [sandbox].[v_mart_mms_club_product_history]
AS SELECT DIM.[club_product_id]
     , LNK.[club_id]
     , LNK.[product_id]
     , DIM.[price]
     , SAT.[inserted_date_time]
     , SAT.[updated_date_time]
     , [effective_date_no_time] = DATEADD(DD, DATEDIFF(DD, 0, DIM.[effective_date_time]), 0)
     , DIM.[effective_date_time]
     , [expiration_date_no_time] = DATEADD(DD, DATEDIFF(DD, 0, DIM.[expiration_date_time]), 0)
     , DIM.[expiration_date_time]
     , DIM.[dim_club_key]
     , DIM.[dim_mms_product_key]
     , PIT.[bk_hash]
     , DIM.[p_mms_club_product_id]
     , PIT.[dv_load_date_time]
     , PIT.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (batch_info.[l_hash] + batch_info.[s_hash])),2)
  FROM [dbo].[p_mms_club_product] PIT
       INNER JOIN [dbo].[d_mms_club_product_history] DIM
         ON DIM.[bk_hash] = PIT.[bk_hash]
            AND DIM.[p_mms_club_product_id] = PIT.[p_mms_club_product_id]
       INNER JOIN [dbo].[l_mms_club_product] LNK
         ON LNK.[bk_hash] = PIT.[bk_hash]
            AND LNK.[l_mms_club_product_id] = PIT.[l_mms_club_product_id]
       INNER JOIN[dbo].[s_mms_club_product] SAT
         ON SAT.[bk_hash] = PIT.[bk_hash]
            AND SAT.[s_mms_club_product_id] = PIT.[s_mms_club_product_id]
       INNER JOIN
         ( SELECT PIT.[bk_hash]
                , PIT.[d_mms_club_product_history_id]
                , PIT.[effective_date_time]
                , RowRank = RANK() OVER (PARTITION BY PIT.[dim_club_key], PIT.[dim_mms_product_key], DATEADD(DD, DATEDIFF(DD, 0, PIT.[effective_date_time]), 0) ORDER BY PIT.[effective_date_time] DESC)
                , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[dim_club_key], PIT.[dim_mms_product_key], DATEADD(DD, DATEDIFF(DD, 0, PIT.[effective_date_time]), 0) ORDER BY PIT.[effective_date_time] DESC)
             FROM [dbo].[d_mms_club_product_history] PIT
             WHERE NOT EXISTS
                   ( SELECT *
                       FROM [dbo].[d_mms_club_product_history] PIT_Prior
                       WHERE PIT_Prior.[dim_club_key] = PIT.[dim_club_key]
                         AND PIT_Prior.[dim_mms_product_key] = PIT.[dim_mms_product_key]
                         AND PIT_Prior.[dim_club_bridge_dim_mms_product_key] = PIT.[dim_club_bridge_dim_mms_product_key]
                         AND PIT_Prior.[price] = PIT.[price]
                         AND DATEADD(DD, DATEDIFF(DD, 0, PIT_Prior.[effective_date_time]), 0) < DATEADD(DD, DATEDIFF(DD, 0, PIT.[effective_date_time]), 0) )
         ) PITU
         ON PITU.[bk_hash] = DIM.[bk_hash]
            AND PITU.[d_mms_club_product_history_id] = DIM.[d_mms_club_product_history_id]
            AND PITU.RowRank = 1 AND PITU.RowNumber = 1
       CROSS APPLY
         ( SELECT [l_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, PIT.[club_product_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, LNK.[club_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, LNK.[product_id]),'z#@$k%&P'))),2)

                , [s_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, PIT.[club_product_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, SAT.[price]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[effective_date_time], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[expiration_date_time], 120),'z#@$k%&P'))),2)
         
         ) batch_info;