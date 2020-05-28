CREATE PROC [sandbox].[proc_mart_sw_d_mms_membership_message_history] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT d_mms_mbrs_msg.[membership_message_id]
     , d_mms_mbrs_msg.[membership_id]
     , d_mms_mbrs_msg.[open_employee_id]
     , d_mms_mbrs_msg.[close_employee_id]
     , d_mms_mbrs_msg.[val_membership_message_type_id]
     , d_mms_mbrs_msg.[val_message_status_id]
     , d_mms_mbrs_msg.[open_club_id]
     , d_mms_mbrs_msg.[close_club_id]
     , d_mms_mbrs_msg.[open_date_time]
     , d_mms_mbrs_msg.[close_date_time]
     , d_mms_mbrs_msg.[received_date_time]
     , d_mms_mbrs_msg.[comment]
     , d_mms_mbrs_msg.[utc_open_date_time]
     , d_mms_mbrs_msg.[open_date_time_zone]
     , d_mms_mbrs_msg.[utc_close_date_time]
     , d_mms_mbrs_msg.[close_date_time_zone]
     , d_mms_mbrs_msg.[utc_received_date_time]
     , d_mms_mbrs_msg.[received_date_time_zone]
     , d_mms_mbrs_msg.[inserted_date_time]
     , d_mms_mbrs_msg.[updated_date_time]
     , PITU.[effective_date_time]  -- = ISNULL(d_mms_mbrs_msg.[updated_date_time],ISNULL(d_mms_mbrs_msg.[inserted_date_time],d_mms_mbrs_msg.[open_date_time]))
     , d_mms_mbrs_msg.[bk_hash]
     , d_mms_mbrs_msg.[p_mms_membership_message_id]
     , d_mms_mbrs_msg.[dv_load_date_time]
     , d_mms_mbrs_msg.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (batch_info.[l_hash] + batch_info.[s_hash])),2)
     --, [l_hash] = LNK.[dv_hash]
     --, [s_hash] = SAT.[dv_hash]
  FROM [dbo].[d_mms_membership_message_history] d_mms_mbrs_msg
       INNER JOIN
         ( SELECT PIT.[p_mms_membership_message_id]
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
             WHERE ( PIT.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
                 AND PIT.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END )
         ) PITU
         ON PITU.[p_mms_membership_message_id] = d_mms_mbrs_msg.[p_mms_membership_message_id]
            AND PITU.RowRank = 1 AND PITU.RowNumber = 1
       CROSS APPLY
         ( SELECT [l_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_msg.[membership_message_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_msg.[membership_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_msg.[open_employee_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_msg.[close_employee_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_msg.[val_membership_message_type_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_msg.[val_message_status_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_msg.[open_club_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_msg.[close_club_id]),'z#@$k%&P'))),2)

                , [s_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_msg.[membership_message_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_msg.[open_date_time], 120),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_msg.[close_date_time], 120),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_msg.[received_date_time], 120),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_msg.[utc_open_date_time], 120),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_msg.[open_date_time_zone]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_msg.[utc_close_date_time], 120),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_msg.[close_date_time_zone]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_msg.[utc_received_date_time], 120),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_msg.[received_date_time_zone]),'z#@$k%&P'))),2)
                                                               --+ 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_msg.[inserted_date_time], 120),'z#@$k%&P')
                                                               --+ 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_msg.[updated_date_time], 120),'z#@$k%&P')
         ) batch_info
  WHERE d_mms_mbrs_msg.[val_membership_message_type_id] IN (64,65,82,107,169,177,184,203)
    AND NOT d_mms_mbrs_msg.[membership_id] Is Null
ORDER BY d_mms_mbrs_msg.[dv_batch_id] ASC, d_mms_mbrs_msg.[dv_load_date_time] ASC, PITU.[effective_date_time] ASC;

END
