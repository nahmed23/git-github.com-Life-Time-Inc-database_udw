CREATE VIEW [sandbox].[v_mart_mms_employee_role]
AS SELECT PIT.[employee_role_id]
     , LNK.[employee_id]
     , LNK.[val_employee_role_id]
     , SAT.[inserted_date_time]
     , SAT.[updated_date_time]
     , SAT.[primary_employee_role_flag]
     , DIM.[dim_employee_role_key]
     , DIM.[dim_employee_key]
     , [dim_mms_val_employee_role_key] = CASE WHEN NOT LNK.[val_employee_role_id] Is Null THEN CONVERT(char(32), HASHBYTES('MD5', ('P%#&z$@k' + CONVERT(varchar, LNK.[val_employee_role_id]))),2) ELSE CONVERT(char(32),'-998',2) END
     , SAT.[bk_hash]
     , PIT.[p_mms_employee_role_id]
     , PIT.[dv_load_date_time]
     , PIT.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (LNK.[dv_hash] + SAT.[dv_hash])),2)
     --, [l_hash] = LNK.[dv_hash]
     --, [s_hash] = SAT.[dv_hash]
  FROM [dbo].[p_mms_employee_role] PIT
       INNER JOIN [dbo].[d_mms_employee_role] DIM
         ON DIM.[bk_hash] = PIT.[bk_hash]
            AND DIM.[p_mms_employee_role_id] = PIT.[p_mms_employee_role_id]
       INNER JOIN [dbo].[l_mms_employee_role] LNK
         ON LNK.[bk_hash] = PIT.[bk_hash]
            AND LNK.[l_mms_employee_role_id] = PIT.[l_mms_employee_role_id]
       INNER JOIN[dbo].[s_mms_employee_role] SAT
         ON SAT.[bk_hash] = PIT.[bk_hash]
            AND SAT.[s_mms_employee_role_id] = PIT.[s_mms_employee_role_id]
       INNER JOIN
         ( SELECT PIT.[p_mms_employee_role_id]
                , RowRank = RANK() OVER (PARTITION BY PIT.[employee_role_id], LNK.[employee_id], LNK.[val_employee_role_id] ORDER BY PIT.[dv_load_end_date_time] DESC, ISNULL(SAT.[updated_date_time], SAT.[inserted_date_time]) DESC)
                , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[employee_role_id], LNK.[employee_id], LNK.[val_employee_role_id] ORDER BY PIT.[dv_load_end_date_time] DESC, ISNULL(SAT.[updated_date_time], SAT.[inserted_date_time]) DESC)
             FROM [dbo].[p_mms_employee_role] PIT
                  INNER JOIN [dbo].[l_mms_employee_role] LNK
                    ON LNK.[bk_hash] = PIT.[bk_hash]
                       AND LNK.[l_mms_employee_role_id] = PIT.[l_mms_employee_role_id]
                  INNER JOIN[dbo].[s_mms_employee_role] SAT
                    ON SAT.[bk_hash] = PIT.[bk_hash]
                       AND SAT.[s_mms_employee_role_id] = PIT.[s_mms_employee_role_id]
         ) PITU
         ON PITU.[p_mms_employee_role_id] = PIT.[p_mms_employee_role_id]
            AND PITU.RowRank = 1 AND PITU.RowNumber = 1;