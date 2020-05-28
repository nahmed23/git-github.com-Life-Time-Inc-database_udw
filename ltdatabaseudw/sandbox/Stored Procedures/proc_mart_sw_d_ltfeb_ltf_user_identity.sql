CREATE PROC [sandbox].[proc_mart_sw_d_ltfeb_ltf_user_identity] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT * FROM (
SELECT d_ltfeb_ltf_user_identity.[party_id]
     , d_ltfeb_party_relationship_role_assignment.[member_id]
     , d_ltfeb_ltf_user_identity.[ltf_user_name]
     , d_ltfeb_ltf_user_identity.[lui_identity_status]
     , d_ltfeb_ltf_user_identity.[lui_identity_status_from_date_time]
     , d_ltfeb_ltf_user_identity.[lui_identity_status_thru_date_time]
     , d_ltfeb_ltf_user_identity.[update_date_time]
     , [bk_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, d_ltfeb_ltf_user_identity.[party_id]),'z#@$k%&P')
                                                     + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_ltfeb_party_relationship_role_assignment.[member_id]),'z#@$k%&P'))),2)
     , d_ltfeb_ltf_user_identity.[p_ltfeb_ltf_user_identity_id]
     , d_ltfeb_party_relationship_role_assignment.[p_ltfeb_party_relationship_role_assignment_id]
     , d_ltfeb_ltf_user_identity.[dv_load_date_time]
     , d_ltfeb_ltf_user_identity.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, d_ltfeb_ltf_user_identity.[party_id]),'z#@$k%&P')
                                                     + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_ltfeb_party_relationship_role_assignment.[member_id]),'z#@$k%&P')
                                                     + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_ltfeb_ltf_user_identity.[ltf_user_name]),'z#@$k%&P')
                                                     + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_ltfeb_ltf_user_identity.[lui_identity_status]),'z#@$k%&P')
                                                     + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_ltfeb_ltf_user_identity.[lui_identity_status_from_date_time]),'z#@$k%&P')
                                                     + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_ltfeb_ltf_user_identity.[lui_identity_status_thru_date_time]),'z#@$k%&P')
                                                     + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_ltfeb_ltf_user_identity.[update_date_time]),'z#@$k%&P'))),2)
     , [dv_deleted] = CAST(CASE WHEN (d_ltfeb_party_relationship_role_assignment.[dv_deleted] = 1 OR d_ltfeb_party_relationship.[dv_deleted] = 1 OR d_ltfeb_ltf_user_identity.[dv_deleted] = 1) THEN 1 ELSE 0 END AS bit)
     , RowRank = RANK() OVER (PARTITION BY d_ltfeb_party_relationship.[from_party_role_id] ORDER BY d_ltfeb_party_relationship.[party_relationship_thru_date] DESC, d_ltfeb_party_relationship.[from_date_in_effect] ASC)
     , RowNumber = ROW_NUMBER() OVER (PARTITION BY d_ltfeb_party_relationship.[from_party_role_id] ORDER BY d_ltfeb_party_relationship.[party_relationship_thru_date] DESC, d_ltfeb_party_relationship.[from_date_in_effect] ASC)
  FROM ( SELECT PIT.[bk_hash]
              , PIT.[party_relationship_id]
              , LNK.[assigned_id]
              , [member_id] = CAST(CASE WHEN (ISNUMERIC(LNK.[assigned_id]) = 1 AND CONVERT(bigint, LNK.[assigned_id]) <= 2147483647) THEN CONVERT(int, LNK.[assigned_id]) ELSE Null END AS int)
              , SAT.[update_date_time]
              , PIT.[p_ltfeb_party_relationship_role_assignment_id]
              , PIT.[dv_load_date_time]
              , PIT.[dv_batch_id]
              , PITU.[dv_deleted]
           FROM [dbo].[p_ltfeb_party_relationship_role_assignment] PIT
                INNER JOIN [dbo].[l_ltfeb_party_relationship_role_assignment] LNK
                  ON LNK.[bk_hash] = PIT.[bk_hash]
                     AND LNK.[l_ltfeb_party_relationship_role_assignment_id] = PIT.[l_ltfeb_party_relationship_role_assignment_id]
                INNER JOIN [dbo].[s_ltfeb_party_relationship_role_assignment] SAT
                  ON SAT.[bk_hash] = PIT.[bk_hash]
                     AND SAT.[s_ltfeb_party_relationship_role_assignment_id] = PIT.[s_ltfeb_party_relationship_role_assignment_id]
                INNER JOIN
                  ( SELECT PIT.[p_ltfeb_party_relationship_role_assignment_id]
                         , RowRank = RANK() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
                         , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
                         , HUB.[dv_inserted_date_time]
                         , HUB.[dv_updated_date_time]
                         , HUB.[dv_batch_id]
                         , HUB.[dv_deleted]
                      FROM [dbo].[p_ltfeb_party_relationship_role_assignment] PIT
                           INNER JOIN
                             ( SELECT HUB.[bk_hash]
                                    , HUB.[dv_inserted_date_time]
                                    , HUB.[dv_updated_date_time]
                                    , [dv_batch_id] = CONVERT(bigint, CONVERT(VARCHAR(8), ISNULL(HUB.[dv_updated_date_time],HUB.[dv_inserted_date_time]), 112) + REPLACE(CONVERT(varchar(8), ISNULL(HUB.[dv_updated_date_time],HUB.[dv_inserted_date_time]), 114), ':',''))
                                    , HUB.[dv_deleted]
                                 FROM [dbo].[h_ltfeb_party_relationship_role_assignment] HUB
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
                  ON PITU.[p_ltfeb_party_relationship_role_assignment_id] = PIT.[p_ltfeb_party_relationship_role_assignment_id]
                     AND ((PITU.[dv_deleted] = 0 AND PITU.RowRank = 1 AND PITU.RowNumber = 1) OR PITU.[dv_deleted] = 1)
           WHERE NOT PIT.[party_relationship_id] Is Null
             AND SAT.party_relationship_role_type = 'MMS Member'
       ) d_ltfeb_party_relationship_role_assignment

       INNER JOIN
         ( SELECT PIT.[bk_hash]
                , PIT.[party_relationship_id]
                , LNK.[from_party_role_id]
                , LNK.[to_party_role_id]
                , LNK.[party_relationship_type_id]
                , SAT.[from_date_in_effect]
                , SAT.[party_relationship_thru_date]
                , SAT.[update_date_time]
                , PIT.[dv_load_date_time]
                , PIT.[dv_batch_id]
                , PITU.[dv_deleted]
                , d_ltfeb_party_role_from.[pr_party_id]
                --, RowRank = RANK() OVER (PARTITION BY LNK.[from_party_role_id] ORDER BY SAT.[party_relationship_thru_date] DESC)
                --, RowNumber = ROW_NUMBER() OVER (PARTITION BY d_ltfeb_party_role_from.[pr_party_id] ORDER BY SAT.[party_relationship_thru_date] DESC)
             FROM [dbo].[p_ltfeb_party_relationship] PIT
                  INNER JOIN [dbo].[l_ltfeb_party_relationship] LNK
                    ON LNK.[bk_hash] = PIT.[bk_hash]
                       AND LNK.[l_ltfeb_party_relationship_id] = PIT.[l_ltfeb_party_relationship_id]
                  INNER JOIN [dbo].[s_ltfeb_party_relationship] SAT
                    ON SAT.[bk_hash] = PIT.[bk_hash]
                       AND SAT.[s_ltfeb_party_relationship_id] = PIT.[s_ltfeb_party_relationship_id]
                INNER JOIN
                  ( SELECT PIT.[p_ltfeb_party_relationship_id]
                         , RowRank = RANK() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
                         , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
                         , HUB.[dv_inserted_date_time]
                         , HUB.[dv_updated_date_time]
                         , HUB.[dv_batch_id]
                         , HUB.[dv_deleted]
                      FROM [dbo].[p_ltfeb_party_relationship] PIT
                           INNER JOIN
                             ( SELECT HUB.[bk_hash]
                                    , HUB.[dv_inserted_date_time]
                                    , HUB.[dv_updated_date_time]
                                    , [dv_batch_id] = CONVERT(bigint, CONVERT(VARCHAR(8), ISNULL(HUB.[dv_updated_date_time],HUB.[dv_inserted_date_time]), 112) + REPLACE(CONVERT(varchar(8), ISNULL(HUB.[dv_updated_date_time],HUB.[dv_inserted_date_time]), 114), ':',''))
                                    , HUB.[dv_deleted]
                                 FROM [dbo].[h_ltfeb_party_relationship] HUB
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
                  ON PITU.[p_ltfeb_party_relationship_id] = PIT.[p_ltfeb_party_relationship_id]
                     AND ((PITU.[dv_deleted] = 0 AND PITU.RowRank = 1 AND PITU.RowNumber = 1) OR PITU.[dv_deleted] = 1)

                  INNER JOIN
                    ( SELECT PIT_Child.[bk_hash]
                           , PIT_Child.[party_role_id]
                           , LNK_Child.[pr_party_id]
                           --, SAT_Child.[update_date_time]
                           --, PIT_Child.[dv_load_date_time]
                           --, PIT_Child.[dv_batch_id]
                        FROM [dbo].[p_ltfeb_party_role] PIT_Child
                             INNER JOIN [dbo].[l_ltfeb_party_role] LNK_Child
                               ON LNK_Child.[bk_hash] = PIT_Child.[bk_hash]
                                  AND LNK_Child.[l_ltfeb_party_role_id] = PIT_Child.[l_ltfeb_party_role_id]
                             INNER JOIN [dbo].[s_ltfeb_party_role] SAT_Child
                               ON SAT_Child.[bk_hash] = PIT_Child.[bk_hash]
                                  AND SAT_Child.[s_ltfeb_party_role_id] = PIT_Child.[s_ltfeb_party_role_id]
                        WHERE NOT PIT_Child.[party_role_id] Is Null
                          AND PIT_Child.[dv_load_end_date_time] = '9999-12-31 00:00:00.000'
                          AND SAT_Child.party_role_type = 'Customer'
                    ) d_ltfeb_party_role_from
                    ON d_ltfeb_party_role_from.[party_role_id] = LNK.[from_party_role_id]

                  INNER JOIN
                    ( SELECT PIT_Child.[bk_hash]
                           , PIT_Child.[party_role_id]
                           --, LNK_Child.[pr_party_id]
                           --, SAT_Child.[update_date_time]
                           --, PIT_Child.[dv_load_date_time]
                           --, PIT_Child.[dv_batch_id]
                        FROM [dbo].[p_ltfeb_party_role] PIT_Child
                             --INNER JOIN [dbo].[l_ltfeb_party_role] LNK_Child
                             --  ON LNK_Child.[bk_hash] = PIT_Child.[bk_hash]
                             --     AND LNK_Child.[l_ltfeb_party_role_id] = PIT_Child.[l_ltfeb_party_role_id]
                             INNER JOIN [dbo].[s_ltfeb_party_role] SAT_Child
                               ON SAT_Child.[bk_hash] = PIT_Child.[bk_hash]
                                  AND SAT_Child.[s_ltfeb_party_role_id] = PIT_Child.[s_ltfeb_party_role_id]
                        WHERE NOT PIT_Child.[party_role_id] Is Null
                          AND PIT_Child.[dv_load_end_date_time] = '9999-12-31 00:00:00.000'
                          AND SAT_Child.party_role_type = 'Club'
                    ) d_ltfeb_party_role_to
                    ON d_ltfeb_party_role_to.[party_role_id] = LNK.[to_party_role_id]
             WHERE NOT PIT.[party_relationship_id] Is Null
         ) d_ltfeb_party_relationship
         ON d_ltfeb_party_relationship.[party_relationship_id] = d_ltfeb_party_relationship_role_assignment.[party_relationship_id]
            --AND RowRank = 1

       INNER JOIN
         ( SELECT PIT.[bk_hash]
                , PIT.[party_id]
                , SAT.[ltf_user_name]
                , SAT.[lui_identity_status]
                , SAT.[lui_identity_status_from_date_time]
                , SAT.[lui_identity_status_thru_date_time]
                , SAT.[update_date_time]
                --, SAT.[last_successful_login_datetime]
                , PIT.[p_ltfeb_ltf_user_identity_id]
                , PIT.[dv_load_date_time]
                , PIT.[dv_batch_id]
                , PITU.[dv_deleted]
             FROM [dbo].[p_ltfeb_ltf_user_identity] PIT
                  INNER JOIN [dbo].[s_ltfeb_ltf_user_identity] SAT
                    ON SAT.[bk_hash] = PIT.[bk_hash]
                       AND SAT.[s_ltfeb_ltf_user_identity_id] = PIT.[s_ltfeb_ltf_user_identity_id]
                  INNER JOIN
                    ( SELECT PIT.[p_ltfeb_ltf_user_identity_id]
                           , RowRank = RANK() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
                           , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
                           , HUB.[dv_inserted_date_time]
                           , HUB.[dv_updated_date_time]
                           , HUB.[dv_batch_id]
                           , HUB.[dv_deleted]
                        FROM [dbo].[p_ltfeb_ltf_user_identity] PIT
                             INNER JOIN
                               ( SELECT HUB.[bk_hash]
                                      , HUB.[dv_inserted_date_time]
                                      , HUB.[dv_updated_date_time]
                                      , [dv_batch_id] = CONVERT(bigint, CONVERT(VARCHAR(8), ISNULL(HUB.[dv_updated_date_time],HUB.[dv_inserted_date_time]), 112) + REPLACE(CONVERT(varchar(8), ISNULL(HUB.[dv_updated_date_time],HUB.[dv_inserted_date_time]), 114), ':',''))
                                      , HUB.[dv_deleted]
                                   FROM [dbo].[h_ltfeb_ltf_user_identity] HUB
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
                    ON PITU.[p_ltfeb_ltf_user_identity_id] = PIT.[p_ltfeb_ltf_user_identity_id]
                       AND ((PITU.[dv_deleted] = 0 AND PITU.RowRank = 1 AND PITU.RowNumber = 1) OR PITU.[dv_deleted] = 1)
             WHERE NOT PIT.[party_id] Is Null
               AND NOT SAT.[lui_identity_status] IN ('Temporary Probation', 'Temporary', 'Denied')
         ) d_ltfeb_ltf_user_identity
         ON d_ltfeb_ltf_user_identity.[party_id] = d_ltfeb_party_relationship.[pr_party_id]
--         CROSS APPLY
--           ( SELECT [update_date_time] = (SELECT MAX(ISNULL(v, CONVERT(datetime, '1900-01-01 00:00:00.000'))) FROM (VALUES (d_ltfeb_party_relationship_role_assignment.[update_date_time]), (d_ltfeb_party_relationship.[update_date_time]), (d_ltfeb_party_role_from.[update_date_time]), (d_ltfeb_party_role_to.[update_date_time]), (d_ltfeb_ltf_user_identity.[update_date_time])) AS value(v))
----(SELECT MAX(ISNULL(v, '1900-01-01 00:00:00.000')) FROM (VALUES (C.ModifiedDateTime), (CM.ModifiedDateTime)) AS value(v))

--                  --, [dv_load_date_time] = (SELECT MAX(ISNULL(v, '1900-01-01 00:00:00.000')) FROM (VALUES (d_ltfeb_party_relationship_role_assignment.[dv_load_date_time]), (d_ltfeb_party_relationship.[dv_load_date_time]), (d_ltfeb_party_role_from.[dv_load_date_time]), (d_ltfeb_party_role_to.[dv_load_date_time]), (d_ltfeb_ltf_user_identity.[dv_load_date_time])) AS value(v))
--                  --, [dv_batch_id] = (SELECT MAX(ISNULL(v, -1)) FROM (VALUES (d_ltfeb_party_relationship_role_assignment.[dv_batch_id]), (d_ltfeb_party_relationship.[dv_batch_id]), (d_ltfeb_party_role_from.[dv_batch_id]), (d_ltfeb_party_role_to.[dv_batch_id]), (d_ltfeb_ltf_user_identity.[dv_batch_id])) AS value(v))
--           ) d_max_version
  WHERE ( (d_ltfeb_party_relationship_role_assignment.[dv_deleted] = 0 AND d_ltfeb_party_relationship.[dv_deleted] = 0 AND d_ltfeb_ltf_user_identity.[dv_deleted] = 0
           AND ( d_ltfeb_ltf_user_identity.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
             AND d_ltfeb_ltf_user_identity.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END ) )
       OR (d_ltfeb_party_relationship_role_assignment.[dv_deleted] = 1 OR d_ltfeb_party_relationship.[dv_deleted] = 1 OR d_ltfeb_ltf_user_identity.[dv_deleted] = 1) )
  --GROUP BY d_ltfeb_ltf_user_identity.[party_id]
  --       , d_ltfeb_party_relationship_role_assignment.[member_id]
  --       , d_ltfeb_ltf_user_identity.[ltf_user_name]
  --       , d_ltfeb_ltf_user_identity.[lui_identity_status]
  --       , d_ltfeb_ltf_user_identity.[lui_identity_status_from_date_time]
  --       , d_ltfeb_ltf_user_identity.[lui_identity_status_thru_date_time]
  --       , d_ltfeb_ltf_user_identity.[update_date_time]
  --       , d_ltfeb_ltf_user_identity.[bk_hash]
  --       , d_ltfeb_ltf_user_identity.[p_ltfeb_ltf_user_identity_id]
  --       , d_ltfeb_party_relationship_role_assignment.[p_ltfeb_party_relationship_role_assignment_id]
  --       , d_ltfeb_ltf_user_identity.[dv_load_date_time]
  --       , d_ltfeb_ltf_user_identity.[dv_batch_id]
  --       , d_ltfeb_ltf_user_identity.[dv_deleted]
) RankedSource
WHERE RankedSource.RowRank = 1 AND RankedSource.RowNumber = 1
ORDER BY [update_date_time] ASC;

END
