CREATE VIEW [sandbox].[v_mart_mms_membership_modification_request_history]
AS SELECT DIM.[membership_modification_request_id]
     , DIM.[membership_id]
     , DIM.[member_id]
     , DIM.[membership_type_id]
     , DIM.[club_id]
     , DIM.[commisioned_employee_id]
     , DIM.[employee_id]
     , DIM.[member_agreement_staging_id]
     , DIM.[previous_membership_type_id]
     , DIM.[val_flex_reason_id]
     , DIM.[val_membership_modification_request_type_id]
     , DIM.[val_membership_modification_request_status_id]
     , DIM.[val_membership_upgrade_date_range_id]
     , [request_date_no_time] = DATEADD(DD, DATEDIFF(DD, 0, DIM.[request_date_time]), 0)
     , DIM.[request_date_time]
     , DIM.[utc_request_date_time]
     , DIM.[request_date_time_zone]
     , DIM.[effective_date]
     , DIM.[inserted_date_time]
     , DIM.[updated_date_time]
     , [status_changed_date_no_time] = DATEADD(DD, DATEDIFF(DD, 0, DIM.[status_changed_date_time]), 0)
     , DIM.[status_changed_date_time]
     , DIM.[last_eft_month]
     , DIM.[future_membership_upgrade_flag]
     , DIM.[first_months_dues]
     , DIM.[total_monthly_amount]
     , DIM.[membership_upgrade_month_year]
     , DIM.[agreement_price]
     , DIM.[waive_service_fee_flag]
     , DIM.[full_access_date_extension_flag]
     , DIM.[new_members]
     , DIM.[add_on_fee]
     , DIM.[service_fee]
     , DIM.[diamond_fee]
     , DIM.[pro_rated_dues]
     , DIM.[deactivated_members]
     , DIM.[juniors_assessed]
     , DIM.[member_freeze_flag]
     , [effective_date_no_time] = DATEADD(DD, DATEDIFF(DD, 0, PITU.[effective_date_time]), 0)
     , PITU.[effective_date_time]  --= ISNULL(DIM.[updated_date_time],DIM.[inserted_date_time])
     , [dim_mms_membership_key] = CASE WHEN NOT DIM.[membership_id] Is Null THEN CONVERT(char(32), HASHBYTES('MD5', ('P%#&z$@k' + CONVERT(varchar, DIM.[membership_id]))),2) ELSE CONVERT(char(32),'-998',2) END
     , [dim_mms_member_key] = CASE WHEN NOT DIM.[member_id] Is Null THEN CONVERT(char(32), HASHBYTES('MD5', ('P%#&z$@k' + CONVERT(varchar, DIM.[member_id]))),2) ELSE CONVERT(char(32),'-998',2) END
     , [dim_mms_membership_type_key] = CASE WHEN NOT DIM.[membership_type_id] Is Null THEN CONVERT(char(32), HASHBYTES('MD5', ('P%#&z$@k' + CONVERT(varchar, DIM.[membership_type_id]))),2) ELSE CONVERT(char(32),'-998',2) END
     , [dim_club_key] = CASE WHEN NOT DIM.[club_id] Is Null THEN CONVERT(char(32), HASHBYTES('MD5', ('P%#&z$@k' + CONVERT(varchar, DIM.[club_id]))),2) ELSE CONVERT(char(32),'-998',2) END
     , [dim_employee_key] = CASE WHEN NOT DIM.[employee_id] Is Null THEN CONVERT(char(32), HASHBYTES('MD5', ('P%#&z$@k' + CONVERT(varchar, DIM.[employee_id]))),2) ELSE CONVERT(char(32),'-998',2) END
     , DIM.[bk_hash]
     , DIM.[p_mms_membership_modification_request_id]
     , DIM.[dv_load_date_time]
     , DIM.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (batch_info.[l_hash] + batch_info.[s_hash])),2)
  FROM [dbo].[d_mms_membership_modification_request_history] DIM
       INNER JOIN
         ( SELECT PIT.[bk_hash]
                , PIT.[p_mms_membership_modification_request_id]
                , PIT_Timestamp.[effective_date_time]
                , RowRank = RANK() OVER (PARTITION BY PIT.[bk_hash], DATEADD(DD, DATEDIFF(DD, 0, PIT_Timestamp.[effective_date_time]), 0) ORDER BY PIT.[dv_load_end_date_time] DESC, PIT_Timestamp.[effective_date_time] DESC)
                , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[bk_hash], DATEADD(DD, DATEDIFF(DD, 0, PIT_Timestamp.[effective_date_time]), 0) ORDER BY PIT.[dv_load_end_date_time] DESC, PIT_Timestamp.[effective_date_time] DESC)
             FROM [dbo].[p_mms_membership_modification_request] PIT
                  INNER JOIN[dbo].[s_mms_membership_modification_request] SAT
                    ON SAT.[bk_hash] = PIT.[bk_hash]
                       AND SAT.[s_mms_membership_modification_request_id] = PIT.[s_mms_membership_modification_request_id]
                  CROSS APPLY
                    ( SELECT [effective_date_time] = ISNULL(ISNULL(PIT.[dv_greatest_satellite_date_time], 
                                                                   CASE WHEN PIT.[dv_first_in_key_series] = 1
                                                                        THEN SAT.[inserted_date_time]
                                                                        ELSE ISNULL(SAT.[updated_date_time],SAT.[inserted_date_time]) END)
                                                          , CAST('2000-01-01' AS datetime))
                    ) PIT_Timestamp
         ) PITU
           ON PITU.[bk_hash] = DIM.[bk_hash]
              AND PITU.[p_mms_membership_modification_request_id] = DIM.[p_mms_membership_modification_request_id]
              AND PITU.RowRank = 1 AND PITU.RowNumber = 1
       CROSS APPLY
         ( SELECT [l_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[membership_modification_request_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[membership_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[member_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[val_membership_modification_request_type_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[val_flex_reason_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[membership_type_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[val_membership_modification_request_status_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[employee_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[val_membership_upgrade_date_range_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[club_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[commisioned_employee_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[member_agreement_staging_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[previous_membership_type_id]),'z#@$k%&P'))),2)

                , [s_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[membership_modification_request_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[request_date_time], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[utc_request_date_time], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(DIM.[request_date_time_zone],'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[effective_date], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[inserted_date_time], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[updated_date_time], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[status_changed_date_time], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(DIM.[last_eft_month],'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[future_membership_upgrade_flag]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[first_months_dues]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[total_monthly_amount]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[membership_upgrade_month_year]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[agreement_price]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[waive_service_fee_flag]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[full_access_date_extension_flag]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(DIM.[new_members],'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[add_on_fee]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[service_fee]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[diamond_fee]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[pro_rated_dues]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(DIM.[deactivated_members],'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[juniors_assessed]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[member_freeze_flag]),'z#@$k%&P'))),2)
         ) batch_info;