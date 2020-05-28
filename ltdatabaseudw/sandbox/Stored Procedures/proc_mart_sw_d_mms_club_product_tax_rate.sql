CREATE PROC [sandbox].[proc_mart_sw_d_mms_club_product_tax_rate] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT d_mms_club_product_tax_rate.[club_product_tax_rate_id]
     , d_mms_club_product_tax_rate.[club_id]
     , d_mms_club_product_tax_rate.[product_id]
     , d_mms_club_product_tax_rate.[tax_rate_id]
     , d_mms_club_product_tax_rate.[start_date]
     , d_mms_club_product_tax_rate.[end_date]
     , d_mms_club_product_tax_rate.[inserted_date_time]
     , d_mms_club_product_tax_rate.[updated_date_time]
     , d_mms_tax_rate.[val_tax_type_id]
     , d_mms_tax_rate.[tax_percentage]
     , d_mms_tax_rate.[tax_rate]
     , d_mms_club_product_tax_rate.[bk_hash]
     , d_mms_club_product_tax_rate.[p_mms_club_product_tax_rate_id]
     , d_mms_club_product_tax_rate.[dv_load_date_time]
     , d_mms_club_product_tax_rate.[dv_batch_id]
     , d_mms_club_product_tax_rate.[dv_hash]
  FROM ( SELECT PIT.[club_product_tax_rate_id]
              , LNK.[club_id]
              , LNK.[product_id]
              , LNK.[tax_rate_id]
              , SAT.[start_date]
              , SAT.[end_date]
              , SAT.[inserted_date_time]
              , SAT.[updated_date_time]
              , PIT.[bk_hash]
              , PIT.[p_mms_club_product_tax_rate_id]
              , PIT.[dv_load_date_time]
              , PIT.[dv_batch_id]
              , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (LNK.[dv_hash] + SAT.[dv_hash])),2)
              --, [l_hash] = LNK.[dv_hash]
              --, [s_hash] = SAT.[dv_hash]
           FROM [dbo].[p_mms_club_product_tax_rate] PIT
                INNER JOIN [dbo].[l_mms_club_product_tax_rate] LNK
                  ON LNK.[bk_hash] = PIT.[bk_hash]
                     AND LNK.[l_mms_club_product_tax_rate_id] = PIT.[l_mms_club_product_tax_rate_id]
                INNER JOIN [dbo].[s_mms_club_product_tax_rate] SAT
                  ON SAT.[bk_hash] = PIT.[bk_hash]
                     AND SAT.[s_mms_club_product_tax_rate_id] = PIT.[s_mms_club_product_tax_rate_id]
                INNER JOIN
                  ( SELECT PIT.[p_mms_club_product_tax_rate_id]
                         , RowRank = RANK() OVER (PARTITION BY LNK.[club_id], LNK.[product_id] ORDER BY PIT.[dv_load_end_date_time] DESC)
                         , RowNumber = ROW_NUMBER() OVER (PARTITION BY LNK.[club_id], LNK.[product_id] ORDER BY PIT.[dv_load_end_date_time] DESC)
                      FROM [dbo].[p_mms_club_product_tax_rate] PIT
                           INNER JOIN [dbo].[l_mms_club_product_tax_rate] LNK
                             ON LNK.[bk_hash] = PIT.[bk_hash]
                                AND LNK.[l_mms_club_product_tax_rate_id] = PIT.[l_mms_club_product_tax_rate_id]
                      WHERE ( PIT.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
                          AND PIT.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END )
                  ) PITU
                  ON PITU.[p_mms_club_product_tax_rate_id] = PIT.[p_mms_club_product_tax_rate_id]
                     AND PITU.RowRank = 1 AND PITU.RowNumber = 1
           WHERE NOT PIT.[club_product_tax_rate_id] Is Null
       ) d_mms_club_product_tax_rate
       INNER JOIN
         ( SELECT PIT.[tax_rate_id]
                , LNK.[val_tax_type_id]
                , SAT.[tax_percentage]
                , [tax_rate] = ISNULL(SAT.[tax_percentage],0) / 100
                , SAT.[inserted_date_time]
                , SAT.[updated_date_time]
                , PIT.[bk_hash]
                , PIT.[p_mms_tax_rate_id]
                , PIT.[dv_load_date_time]
                , PIT.[dv_batch_id]
                , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (LNK.[dv_hash] + SAT.[dv_hash])),2)
                --, [l_hash] = LNK.[dv_hash]
                --, [s_hash] = SAT.[dv_hash]
             FROM [dbo].[p_mms_tax_rate] PIT
                  INNER JOIN [dbo].[l_mms_tax_rate] LNK
                    ON LNK.[bk_hash] = PIT.[bk_hash]
                       AND LNK.[l_mms_tax_rate_id] = PIT.[l_mms_tax_rate_id]
                  INNER JOIN [dbo].[s_mms_tax_rate] SAT
                    ON SAT.[bk_hash] = PIT.[bk_hash]
                       AND SAT.[s_mms_tax_rate_id] = PIT.[s_mms_tax_rate_id]
                  INNER JOIN
                    ( SELECT PIT.[p_mms_tax_rate_id]
                           , RowRank = RANK() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
                           , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
                        FROM [dbo].[p_mms_tax_rate] PIT
                        --WHERE PIT.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
                        --  AND PIT.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END + " 
                    ) PITU
                    ON PITU.[p_mms_tax_rate_id] = PIT.[p_mms_tax_rate_id]
                       AND PITU.RowRank = 1 AND PITU.RowNumber = 1
             WHERE NOT PIT.[tax_rate_id] Is Null
         ) d_mms_tax_rate
         ON d_mms_tax_rate.tax_rate_id = d_mms_club_product_tax_rate.tax_rate_id
ORDER BY d_mms_club_product_tax_rate.[dv_batch_id] ASC, d_mms_club_product_tax_rate.[dv_load_date_time] ASC, ISNULL(d_mms_club_product_tax_rate.[updated_date_time], d_mms_club_product_tax_rate.[inserted_date_time]) ASC;

END
