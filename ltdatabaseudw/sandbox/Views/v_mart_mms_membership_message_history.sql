CREATE VIEW [sandbox].[v_mart_mms_membership_message_history]
AS SELECT DIM.[membership_message_id]
     , DIM.[membership_id]
     , DIM.[close_club_id]
     , DIM.[close_employee_id]
     , DIM.[open_club_id]
     , DIM.[open_employee_id]
     , DIM.[val_membership_message_type_id]
     , DIM.[val_message_status_id]
     , [open_date_no_time] = DATEADD(DD, DATEDIFF(DD, 0, DIM.[open_date_time]), 0)
     , DIM.[open_date_time]
     , [close_date_no_time] = DATEADD(DD, DATEDIFF(DD, 0, DIM.[close_date_time]), 0)
     , DIM.[close_date_time]
     , DIM.[received_date_time]
     , DIM.[comment]
     , DIM.[utc_open_date_time]
     , DIM.[open_date_time_zone]
     , DIM.[utc_close_date_time]
     , DIM.[close_date_time_zone]
     , DIM.[utc_received_date_time]
     , DIM.[received_date_time_zone]
     , DIM.[inserted_date_time]
     , DIM.[updated_date_time]
     , [effective_date_no_time] = DATEADD(DD, DATEDIFF(DD, 0, PITU.[effective_date_time]), 0)
     , PITU.[effective_date_time]  -- = ISNULL(DIM.[updated_date_time],ISNULL(DIM.[inserted_date_time],DIM.[open_date_time]))
     , [dim_mms_membership_key] = CASE WHEN NOT DIM.[membership_id] Is Null THEN CONVERT(char(32), HASHBYTES('MD5', ('P%#&z$@k' + CONVERT(varchar, DIM.[membership_id]))),2) ELSE CONVERT(char(32),'-998',2) END
     , [dim_close_club_key] = CASE WHEN NOT DIM.[close_club_id] Is Null THEN CONVERT(char(32), HASHBYTES('MD5', ('P%#&z$@k' + CONVERT(varchar, DIM.[close_club_id]))),2) ELSE CONVERT(char(32),'-998',2) END
     , [dim_close_employee_key] = CASE WHEN NOT DIM.[close_employee_id] Is Null THEN CONVERT(char(32), HASHBYTES('MD5', ('P%#&z$@k' + CONVERT(varchar, DIM.[close_employee_id]))),2) ELSE CONVERT(char(32),'-998',2) END
     , [dim_open_club_key] = CASE WHEN NOT DIM.[open_club_id] Is Null THEN CONVERT(char(32), HASHBYTES('MD5', ('P%#&z$@k' + CONVERT(varchar, DIM.[open_club_id]))),2) ELSE CONVERT(char(32),'-998',2) END
     , [dim_open_employee_key] = CASE WHEN NOT DIM.[open_employee_id] Is Null THEN CONVERT(char(32), HASHBYTES('MD5', ('P%#&z$@k' + CONVERT(varchar, DIM.[open_employee_id]))),2) ELSE CONVERT(char(32),'-998',2) END
     , DIM.[bk_hash]
     , DIM.[p_mms_membership_message_id]
     , DIM.[dv_load_date_time]
     , DIM.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (batch_info.[l_hash] + batch_info.[s_hash])),2)
  FROM [dbo].[d_mms_membership_message_history] DIM
       INNER JOIN
         ( SELECT PIT.[bk_hash]
                , PIT.[p_mms_membership_message_id]
                , PIT_Timestamp.[effective_date_time]
                , RowRank = RANK() OVER (PARTITION BY PIT.[bk_hash], DATEADD(DD, DATEDIFF(DD, 0, PIT_Timestamp.[effective_date_time]), 0) ORDER BY PIT.[dv_load_end_date_time] DESC, PIT_Timestamp.[effective_date_time] DESC)
                , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[bk_hash], DATEADD(DD, DATEDIFF(DD, 0, PIT_Timestamp.[effective_date_time]), 0) ORDER BY PIT.[dv_load_end_date_time] DESC, PIT_Timestamp.[effective_date_time] DESC)
             FROM [dbo].[p_mms_membership_message] PIT
                  INNER JOIN[dbo].[s_mms_membership_message] SAT
                    ON SAT.[bk_hash] = PIT.[bk_hash]
                       AND SAT.[s_mms_membership_message_id] = PIT.[s_mms_membership_message_id]
                  CROSS APPLY
                    ( SELECT [effective_date_time] = ISNULL(ISNULL(PIT.[dv_greatest_satellite_date_time], 
                                                                   CASE WHEN PIT.[dv_first_in_key_series] = 1
                                                                        THEN ISNULL(SAT.[close_date_time], SAT.[open_date_time])
                                                                        ELSE ISNULL(SAT.[inserted_date_time], ISNULL(SAT.[close_date_time], SAT.[open_date_time])) END)
                                                          , CAST('2000-01-01' AS datetime))
                    ) PIT_Timestamp
         ) PITU
         ON PITU.[bk_hash] = DIM.[bk_hash]
            AND PITU.[p_mms_membership_message_id] = DIM.[p_mms_membership_message_id]
            AND PITU.RowRank = 1 AND PITU.RowNumber = 1
       CROSS APPLY
         ( SELECT [l_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[membership_message_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[membership_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[open_employee_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[close_employee_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[val_membership_message_type_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[val_message_status_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[open_club_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[close_club_id]),'z#@$k%&P'))),2)

                , [s_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[membership_message_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[open_date_time], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[close_date_time], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[received_date_time], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[utc_open_date_time], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[open_date_time_zone]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[utc_close_date_time], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[close_date_time_zone]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[utc_received_date_time], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[received_date_time_zone]),'z#@$k%&P'))),2)
                                                                  --+ 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[inserted_date_time], 120),'z#@$k%&P')
                                                                  --+ 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[updated_date_time], 120),'z#@$k%&P')
         ) batch_info;