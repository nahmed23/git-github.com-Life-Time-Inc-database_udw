CREATE VIEW [sandbox].[v_mart_mms_employee_history]
AS SELECT PIT.[employee_id]
     , LNK.[club_id]
     , LNK.[member_id]
     , SAT.[active_status_flag]
     , SAT.[first_name]
     , SAT.[last_name]
     , SAT.[middle_int]
     , SAT.[hire_date]
     , SAT.[termination_date]
     , SAT.[inserted_date_time]
     , SAT.[updated_date_time]
     , [effective_date_no_time] = DATEADD(DD, DATEDIFF(DD, 0, PITU.[effective_date_time]), 0)
     , PITU.[effective_date_time]
     , [dim_mms_employee_key] = PIT.[bk_hash]
     , DIM.[dim_club_key] --= CASE WHEN NOT LNK.[club_id] Is Null THEN CONVERT(char(32), HASHBYTES('MD5', ('P%#&z$@k' + CONVERT(varchar, LNK.[club_id]))),2) ELSE CONVERT(char(32),'-998',2) END
     , [dim_mms_member_key] = CASE WHEN NOT LNK.[member_id] Is Null THEN CONVERT(char(32), HASHBYTES('MD5', ('P%#&z$@k' + CONVERT(varchar, LNK.[member_id]))),2) ELSE CONVERT(char(32),'-998',2) END
     , PIT.[bk_hash]
     , PIT.[p_mms_employee_id]
     , PIT.[dv_load_date_time]
     , PIT.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (batch_info.[l_hash] + batch_info.[s_hash])),2)
     , [employee_name]        = SAT.[first_name] + ' ' + SAT.[last_name]
     , [employee_lname_fname] = ISNULL(SAT.[last_name], '') + ', ' + ISNULL(SAT.[first_name], '')
  FROM [dbo].[p_mms_employee] PIT
       INNER JOIN [dbo].[d_mms_employee] DIM
         ON DIM.[bk_hash] = PIT.[bk_hash]
            AND DIM.[p_mms_employee_id] = PIT.[p_mms_employee_id]
       INNER JOIN [dbo].[l_mms_employee] LNK
         ON LNK.[bk_hash] = PIT.[bk_hash]
            AND LNK.[l_mms_employee_id] = PIT.[l_mms_employee_id]
       INNER JOIN[dbo].[s_mms_employee] SAT
         ON SAT.[bk_hash] = PIT.[bk_hash]
            AND SAT.[s_mms_employee_id] = PIT.[s_mms_employee_id]
       INNER JOIN
         ( SELECT PIT.[bk_hash]
                , PIT.[p_mms_employee_id]
                , PIT_Timestamp.[effective_date_time]
                , RowRank = RANK() OVER (PARTITION BY PIT.[bk_hash], DATEADD(DD, DATEDIFF(DD, 0, PIT_Timestamp.[effective_date_time]), 0) ORDER BY PIT.[dv_load_end_date_time] DESC, PIT_Timestamp.[effective_date_time] DESC)
                , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[bk_hash], DATEADD(DD, DATEDIFF(DD, 0, PIT_Timestamp.[effective_date_time]), 0) ORDER BY PIT.[dv_load_end_date_time] DESC, PIT_Timestamp.[effective_date_time] DESC)
             FROM [dbo].[p_mms_employee] PIT
                  INNER JOIN[dbo].[s_mms_employee] SAT
                    ON SAT.[bk_hash] = PIT.[bk_hash]
                       AND SAT.[s_mms_employee_id] = PIT.[s_mms_employee_id]
                  CROSS APPLY
                    ( SELECT [effective_date_time] = ISNULL(ISNULL(PIT.[dv_greatest_satellite_date_time]
                                                                 , CASE WHEN PIT.[dv_first_in_key_series] = 1
                                                                        THEN ISNULL(SAT.[inserted_date_time], SAT.[hire_date])
                                                                        ELSE ISNULL(SAT.[updated_date_time], ISNULL(SAT.[inserted_date_time], SAT.[hire_date])) END)
                                                          , CAST('2000-01-01' AS datetime))
                    ) PIT_Timestamp
         ) PITU
         ON PITU.[bk_hash] = PIT.[bk_hash]
            AND PITU.[p_mms_employee_id] = PIT.[p_mms_employee_id]
            AND PITU.RowRank = 1 AND PITU.RowNumber = 1
       CROSS APPLY
         ( SELECT [l_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, PIT.[employee_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, LNK.[club_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, LNK.[member_id]),'z#@$k%&P'))),2)

                , [s_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, PIT.[employee_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + CONVERT(varchar, ISNULL(SAT.[active_status_flag],0))
                                                                  + 'P%#&z$@k' + ISNULL(SAT.[first_name],'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(SAT.[last_name],'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(SAT.[middle_int],'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, SAT.[hire_date], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, SAT.[termination_date], 120),'z#@$k%&P'))),2)
         ) batch_info;