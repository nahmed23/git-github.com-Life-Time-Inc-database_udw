CREATE PROC [sandbox].[proc_mart_sw_d_boss_asi_reserv] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT d_boss_asi_reserv.[reservation]
     , d_boss_asi_reserv.[instructor]
     , d_boss_asi_reserv.[reserve_type]
     , d_boss_asi_reserv.[create_date]
     , d_boss_asi_reserv.[last_modified]
     , d_boss_asi_reserv.[employee_id]
     , d_boss_asi_reserv.[product_id]
     , d_boss_asi_player.[asi_player_id]
     , d_boss_asi_player.[cancel_date]
     , d_boss_asi_player.[check_in_date]
     , d_boss_asi_player.[created_at]
     , d_boss_asi_player.[date_used]
     , d_boss_asi_player.[start_date]
     , d_boss_asi_player.[updated_at]
     , d_boss_asi_player.[member_id]
     , batch_info.[bk_hash]
     , d_boss_asi_reserv.[p_boss_asi_reserv_id]
     , d_boss_asi_player.[p_boss_asi_player_id]
     , batch_info.[dv_load_date_time]
     , batch_info.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (batch_info.[l_hash] + batch_info.[s_hash])),2)
     , [dv_deleted] = CAST(CASE WHEN (d_boss_asi_reserv.[dv_deleted] = 1 OR d_boss_asi_player.[dv_deleted] = 1) THEN 1 WHEN NOT d_boss_asi_player.[cancel_date] Is Null THEN 1 ELSE 0 END AS bit)
  FROM ( SELECT PIT.[reservation]
              , PIT.[bk_hash]
              , LNK.[trainer_cust_code]
              , LNK.[mms_product_id]
              , SAT.[instructor]
              , SAT.[reserve_type]
              --, SAT.[start_date]
              --, SAT.[end_date]
              , SAT.[create_date]
              , SAT.[last_modified]
              , PIT.[p_boss_asi_reserv_id]
              , PIT.[dv_load_date_time]
              , PIT.[dv_batch_id]
              --, SAT.[dv_hash]
              , PITU.[dv_deleted]
              , [employee_id] = CAST(CASE WHEN (ISNUMERIC(LNK.[trainer_cust_code]) = 1 AND CONVERT(bigint, LNK.[trainer_cust_code]) <= 2147483647) THEN CONVERT(int, LNK.[trainer_cust_code]) ELSE Null END AS int)
              , [product_id] = CAST(CASE WHEN (ISNUMERIC(LNK.[mms_product_id]) = 1 AND CONVERT(bigint, LNK.[mms_product_id]) <= 2147483647) THEN CONVERT(int, LNK.[mms_product_id]) ELSE Null END AS int)
           FROM [dbo].[p_boss_asi_reserv] PIT
                INNER JOIN [dbo].[l_boss_asi_reserv] LNK
                  ON LNK.[bk_hash] = PIT.[bk_hash]
                     AND LNK.[l_boss_asi_reserv_id] = PIT.[l_boss_asi_reserv_id]
                INNER JOIN [dbo].[s_boss_asi_reserv] SAT
                  ON SAT.[bk_hash] = PIT.[bk_hash]
                     AND SAT.[s_boss_asi_reserv_id] = PIT.[s_boss_asi_reserv_id]
                INNER JOIN
                  ( SELECT PIT.[p_boss_asi_reserv_id]
                         , RowRank = RANK() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
                         , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
                         , HUB.[dv_inserted_date_time]
                         , HUB.[dv_updated_date_time]
                         , HUB.[dv_batch_id]
                         , HUB.[dv_deleted]
                      FROM [dbo].[p_boss_asi_reserv] PIT
                           INNER JOIN
                             ( SELECT HUB.[bk_hash]
                                    , HUB.[dv_inserted_date_time]
                                    , HUB.[dv_updated_date_time]
                                    , [dv_batch_id] = CONVERT(bigint, CONVERT(VARCHAR(8), ISNULL(HUB.[dv_updated_date_time],HUB.[dv_inserted_date_time]), 112) + REPLACE(CONVERT(varchar(8), ISNULL(HUB.[dv_updated_date_time],HUB.[dv_inserted_date_time]), 114), ':',''))
                                    , HUB.[dv_deleted]
                                 FROM [dbo].[h_boss_asi_reserv] HUB
                             ) HUB
                             ON HUB.[bk_hash] = PIT.[bk_hash]
                      WHERE ( (HUB.[dv_deleted] = 0
                               AND PIT.[dv_load_end_date_time] = '9999-12-31 00:00:00.000')
                               --AND PIT.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
                               --AND PIT.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END
                           OR (HUB.[dv_deleted] = 1) )
                               --AND HUB.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END
                               --AND HUB.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END + ") )
                  ) PITU
                  ON PITU.[p_boss_asi_reserv_id] = PIT.[p_boss_asi_reserv_id]
                     AND ((PITU.[dv_deleted] = 0 AND PITU.RowRank = 1 AND PITU.RowNumber = 1) OR PITU.[dv_deleted] = 1)
           WHERE NOT PIT.[reservation] Is Null
             AND (NOT NullIf(LNK.[trainer_cust_code],'') Is Null AND NOT LNK.[trainer_cust_code] IN ('0'))
             AND (NOT NullIf(LNK.[mms_product_id],'') Is Null AND NOT LNK.[mms_product_id] IN ('0','86','1859','4944','2533','4946','1949','4342','11114','14292','2647','1427','4102','2534','1858','1947','1948','2744','3145','14266')) --'2785', '11914'
       ) d_boss_asi_reserv
       INNER JOIN
         ( SELECT PIT.[asi_player_id]
                , PIT.[bk_hash]
                , LNK.[mbr_code]
                , LNK.[reservation]
                , SAT.[date_used]
                , SAT.[start_date]
                , SAT.[created_at]
                , SAT.[updated_at]
                , SAT.[cancel_date]
                , SAT.[check_in_date]
                , PIT.[p_boss_asi_player_id]
                , PIT.[dv_load_date_time]
                , PIT.[dv_batch_id]
                --, SAT.[dv_hash]
                , [dv_deleted] = CAST(CASE WHEN PITU.[dv_deleted] = 1 OR NOT SAT.[cancel_date] Is Null THEN 1 ELSE 0 END AS bit)
                , [member_id] = CAST(CASE WHEN (ISNUMERIC(LNK.[mbr_code]) = 1 AND CONVERT(bigint, LNK.[mbr_code]) <= 2147483647) THEN CONVERT(int, LNK.[mbr_code]) ELSE Null END AS int)
             FROM [dbo].[p_boss_asi_player] PIT
                  INNER JOIN [dbo].[l_boss_asi_player] LNK
                    ON LNK.[bk_hash] = PIT.[bk_hash]
                       AND LNK.[l_boss_asi_player_id] = PIT.[l_boss_asi_player_id]
                  INNER JOIN [dbo].[s_boss_asi_player] SAT
                    ON SAT.[bk_hash] = PIT.[bk_hash]
                       AND SAT.[s_boss_asi_player_id] = PIT.[s_boss_asi_player_id]
                  INNER JOIN
                    ( SELECT PIT.[p_boss_asi_player_id]
                           , RowRank = RANK() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
                           , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
                           , HUB.[dv_inserted_date_time]
                           , HUB.[dv_updated_date_time]
                           , HUB.[dv_batch_id]
                           , HUB.[dv_deleted]
                        FROM [dbo].[p_boss_asi_player] PIT
                           INNER JOIN
                             ( SELECT HUB.[bk_hash]
                                    , HUB.[dv_inserted_date_time]
                                    , HUB.[dv_updated_date_time]
                                    , [dv_batch_id] = CONVERT(bigint, CONVERT(VARCHAR(8), ISNULL(HUB.[dv_updated_date_time],HUB.[dv_inserted_date_time]), 112) + REPLACE(CONVERT(varchar(8), ISNULL(HUB.[dv_updated_date_time],HUB.[dv_inserted_date_time]), 114), ':',''))
                                    , HUB.[dv_deleted]
                                 FROM [dbo].[h_boss_asi_player] HUB
                             ) HUB
                             ON HUB.[bk_hash] = PIT.[bk_hash]
                      WHERE ( (HUB.[dv_deleted] = 0
                               AND PIT.[dv_load_end_date_time] = '9999-12-31 00:00:00.000')
                               --AND PIT.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
                               --AND PIT.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END
                           OR (HUB.[dv_deleted] = 1) )
                               --AND HUB.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END
                               --AND HUB.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END + ") )
                    ) PITU
                    ON PITU.[p_boss_asi_player_id] = PIT.[p_boss_asi_player_id]
                       AND ((PITU.[dv_deleted] = 0 AND PITU.RowRank = 1 AND PITU.RowNumber = 1) OR PITU.[dv_deleted] = 1)
             WHERE NOT PIT.[asi_player_id] Is Null
               --AND SAT.[cancel_date] Is Null
         ) d_boss_asi_player
         ON d_boss_asi_player.[reservation] = d_boss_asi_reserv.[reservation]
       CROSS APPLY
         ( SELECT [dv_load_date_time] = CASE WHEN d_boss_asi_reserv.[dv_load_date_time] >= d_boss_asi_player.[dv_load_date_time] THEN d_boss_asi_reserv.[dv_load_date_time] ELSE d_boss_asi_player.[dv_load_date_time] END
                , [dv_batch_id] = CASE WHEN d_boss_asi_reserv.[dv_batch_id] >= d_boss_asi_player.[dv_batch_id] THEN d_boss_asi_reserv.[dv_batch_id] ELSE d_boss_asi_player.[dv_batch_id] END
                , [bk_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, d_boss_asi_reserv.[reservation]),'z#@$k%&P')
                                                                + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_boss_asi_player.[asi_player_id]),'z#@$k%&P'))),2)
                , [l_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, d_boss_asi_reserv.[reservation]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_boss_asi_reserv.[instructor]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_boss_asi_reserv.[trainer_cust_code]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_boss_asi_reserv.[mms_product_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_boss_asi_player.[asi_player_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_boss_asi_player.[mbr_code]),'z#@$k%&P'))),2)
                , [s_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, d_boss_asi_reserv.[reservation]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_boss_asi_reserv.[create_date], 120),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_boss_asi_player.[check_in_date], 120),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_boss_asi_player.[created_at], 120),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_boss_asi_player.[date_used], 120),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_boss_asi_player.[start_date], 120),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_boss_asi_player.[updated_at], 120),'z#@$k%&P'))),2)
         ) batch_info
  WHERE ( ( d_boss_asi_reserv.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
        AND d_boss_asi_reserv.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END )
        OR ( d_boss_asi_player.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
         AND d_boss_asi_player.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END ) )
ORDER BY d_boss_asi_reserv.[dv_batch_id] ASC, d_boss_asi_reserv.[dv_load_date_time] ASC, d_boss_asi_player.[member_id], d_boss_asi_player.[created_at]

END
