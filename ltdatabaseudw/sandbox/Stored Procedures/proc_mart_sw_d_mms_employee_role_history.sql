CREATE PROC [sandbox].[proc_mart_sw_d_mms_employee_role_history] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT d_mms_employee_role_history.[employee_role_id]
     , d_mms_employee_role_history.[employee_id]
     , d_mms_employee_role_history.[val_employee_role_id]
     , d_mms_employee_role_history.[inserted_date_time]
     , d_mms_employee_role_history.[updated_date_time]
     , d_mms_employee_role_history.[primary_employee_role_flag]
     , d_mms_employee_role_history.[effective_date_time]
     , d_mms_employee_role_history.[expiration_date_time]
     , d_mms_employee_role_history.[bk_hash]
     , d_mms_employee_role_history.[p_mms_employee_role_id]
     , d_mms_employee_role_history.[dv_load_date_time]
     , d_mms_employee_role_history.[dv_batch_id]
     , d_mms_employee_role_history.[dv_hash]
     --, [l_hash] = LNK.[dv_hash]
     --, [s_hash] = SAT.[dv_hash]
  FROM ( SELECT PIT.[employee_role_id]
              , LNK.[employee_id]
              , LNK.[val_employee_role_id]
              , SAT.[inserted_date_time]
              , [updated_date_time] = CASE WHEN HUB.[dv_deleted] = 1 THEN PIT.[dv_load_date_time] ELSE Null END
              --, [updated_date_time] = CASE WHEN SAT.[updated_date_time] Is Null AND ISNULL(dv_next.[expiration_date_time], '9999-12-31 00:00:00.000') <> '9999-12-31 00:00:00.000' THEN ISNULL(dv_next.[expiration_date_time], '9999-12-31 00:00:00.000') ELSE SAT.[updated_date_time] END
              , SAT.[primary_employee_role_flag]
              , [effective_date_time] = ISNULL(SAT.[inserted_date_time], ISNULL(HUB.[dv_load_date_time], CAST('2000-01-01' AS datetime)))
              , [expiration_date_time] = CASE WHEN HUB.[dv_deleted] = 1 THEN PIT.[dv_load_date_time] ELSE '9999-12-31 00:00:00.000' END
              --, dv_effective.[effective_date_time]
              --, [expiration_date_time] = ISNULL(dv_next.[expiration_date_time], '9999-12-31 00:00:00.000')
              , SAT.[bk_hash]
              , PIT.[p_mms_employee_role_id]
              , PIT.[dv_load_date_time]
              , PIT.[dv_batch_id]
              , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (LNK.[dv_hash] + SAT.[dv_hash])),2)
           FROM [dbo].[p_mms_employee_role] PIT
                INNER JOIN [dbo].[h_mms_employee_role] HUB
                  ON HUB.[bk_hash] = PIT.[bk_hash]
                INNER JOIN [dbo].[l_mms_employee_role] LNK
                  ON LNK.[bk_hash] = PIT.[bk_hash]
                     AND LNK.[l_mms_employee_role_id] = PIT.[l_mms_employee_role_id]
                INNER JOIN[dbo].[s_mms_employee_role] SAT
                  ON SAT.[bk_hash] = PIT.[bk_hash]
                     AND SAT.[s_mms_employee_role_id] = PIT.[s_mms_employee_role_id]
                --CROSS APPLY
                --  ( SELECT [effective_date_time] = ISNULL(SAT.[inserted_date_time], ISNULL(PIT.[dv_load_date_time], CAST('2000-01-01' AS datetime)))
                --  ) dv_effective
                --OUTER APPLY
                --  ( SELECT TOP 1 dv_next_effective.[expiration_date_time]
                --      FROM [dbo].[p_mms_employee_role] PIT_Next
                --           INNER JOIN [dbo].[l_mms_employee_role] LNK_Next
                --             ON LNK_Next.[bk_hash] = PIT_Next.[bk_hash]
                --                AND LNK_Next.[l_mms_employee_role_id] = PIT_Next.[l_mms_employee_role_id]
                --           INNER JOIN [dbo].[s_mms_employee_role] SAT_Next
                --             ON SAT_Next.[bk_hash] = PIT_Next.[bk_hash]
                --                AND SAT_Next.[s_mms_employee_role_id] = PIT_Next.[s_mms_employee_role_id]
                --           CROSS APPLY
                --             ( SELECT [expiration_date_time] = ISNULL(SAT_Next.[inserted_date_time], ISNULL(PIT_Next.[dv_load_date_time], CAST('9999-12-31 00:00:00.000' AS datetime)))
                --             ) dv_next_effective
                --      WHERE LNK_Next.[employee_id] = LNK.[employee_id]
                --        AND dv_next_effective.[expiration_date_time] > dv_effective.[effective_date_time]
                --    ORDER BY 1 ASC
                --  ) dv_next
           WHERE NOT PIT.[employee_role_id] Is Null
             AND ( PIT.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
               AND PIT.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END )
       ) d_mms_employee_role_history
  --WHERE (d_mms_employee_role_history.[expiration_date_time] >= '2019-06-14' ) -- OR ISNULL(dv_next.[expiration_date_time], '9999-12-31 00:00:00.000') = '9999-12-31 00:00:00.000')
ORDER BY d_mms_employee_role_history.[dv_batch_id] ASC, d_mms_employee_role_history.[dv_load_date_time] ASC, ISNULL(d_mms_employee_role_history.[updated_date_time], d_mms_employee_role_history.[inserted_date_time]) ASC;

END
