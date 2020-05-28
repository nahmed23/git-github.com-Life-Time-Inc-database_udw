CREATE PROC [sandbox].[proc_mart_sw_d_boss_asi_club_res] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT [club_id] = PIT.[club]
     , [employee_id] = ISNULL(d_boss_employee.[employee_id],LNK.[employee_id])
     , SAT.[status]
     , SAT.[resource_type]
     , SAT.[resource]
     , [created_at] = ISNULL(SAT.[created_at],SAT.[updated_at])
     , SAT.[updated_at]
     , PIT.[bk_hash]
     , PIT.[p_boss_asi_club_res_id]
     , PIT.[dv_load_date_time]
     , PIT.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (LNK.[dv_hash] + SAT.[dv_hash])),2)
     --, [l_hash] = LNK.[dv_hash]
     --, [s_hash] = SAT.[dv_hash]
     , PITU.[dv_deleted]
  FROM [dbo].[p_boss_asi_club_res] PIT
       INNER JOIN [dbo].[l_boss_asi_club_res] LNK
         ON LNK.[bk_hash] = PIT.[bk_hash]
            AND LNK.[l_boss_asi_club_res_id] = PIT.[l_boss_asi_club_res_id]
       INNER JOIN [dbo].[s_boss_asi_club_res] SAT
         ON SAT.[bk_hash] = PIT.[bk_hash]
            AND SAT.[s_boss_asi_club_res_id] = PIT.[s_boss_asi_club_res_id]
       INNER JOIN
         ( SELECT PIT.[p_boss_asi_club_res_id]
                , RowRank = RANK() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
                , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
                , HUB.[dv_inserted_date_time]
                , HUB.[dv_updated_date_time]
                , HUB.[dv_batch_id]
                , HUB.[dv_deleted]
             FROM [dbo].[p_boss_asi_club_res] PIT
                  INNER JOIN
                    ( SELECT HUB.[bk_hash]
                           , HUB.[dv_inserted_date_time]
                           , HUB.[dv_updated_date_time]
                           , [dv_batch_id] = CONVERT(bigint, CONVERT(VARCHAR(8), ISNULL(HUB.[dv_updated_date_time],HUB.[dv_inserted_date_time]), 112) + REPLACE(CONVERT(varchar(8), ISNULL(HUB.[dv_updated_date_time],HUB.[dv_inserted_date_time]), 114), ':',''))
                           , HUB.[dv_deleted]
                        FROM [dbo].[h_boss_asi_club_res] HUB
                    ) HUB
                    ON HUB.[bk_hash] = PIT.[bk_hash]
             WHERE ( (HUB.[dv_deleted] = 0
                      AND ( PIT.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
                        AND PIT.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END ) )
                  OR (HUB.[dv_deleted] = 1) )
         ) PITU
         ON PITU.[p_boss_asi_club_res_id] = PIT.[p_boss_asi_club_res_id]
            AND ((PITU.[dv_deleted] = 0 AND PITU.RowRank = 1 AND PITU.RowNumber = 1) OR PITU.[dv_deleted] = 1)
       LEFT OUTER JOIN
         ( SELECT empl_id = CONVERT(char(6), LNK.[id])
                , LNK.[employee_id]
             FROM [dbo].[p_boss_employees] PIT
                  INNER JOIN [dbo].[l_boss_employees] LNK
                    ON LNK.[bk_hash] = PIT.[bk_hash]
                       AND LNK.[l_boss_employees_id] = PIT.[l_boss_employees_id]
             WHERE NOT PIT.[employee_id] Is Null
               AND PIT.[dv_load_end_date_time] = '9999-12-31 00:00:00.000'
         ) d_boss_employee
         ON d_boss_employee.[empl_id] = LNK.[empl_id]
  WHERE NOT PIT.[club] Is Null
    AND ( (NOT LNK.[employee_id] Is Null AND LNK.[employee_id] > 0)
       OR (NOT d_boss_employee.[employee_id] Is Null AND d_boss_employee.[employee_id] > 0) )
ORDER BY PIT.[dv_batch_id] ASC, PIT.[dv_load_date_time] ASC, LNK.[employee_id], PIT.[club], SAT.[created_at]

END
