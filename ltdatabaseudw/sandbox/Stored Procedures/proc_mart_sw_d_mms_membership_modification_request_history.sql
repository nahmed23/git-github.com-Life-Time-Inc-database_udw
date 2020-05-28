CREATE PROC [sandbox].[proc_mart_sw_d_mms_membership_modification_request_history] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT d_mms_mbrs_mod_req.[membership_modification_request_id]
     , d_mms_mbrs_mod_req.[membership_id]
     , d_mms_mbrs_mod_req.[member_id]
     , d_mms_mbrs_mod_req.[val_membership_modification_request_type_id]
     , d_mms_mbrs_mod_req.[val_flex_reason_id]
     , d_mms_mbrs_mod_req.[membership_type_id]
     , d_mms_mbrs_mod_req.[val_membership_modification_request_status_id]
     , d_mms_mbrs_mod_req.[employee_id]
     , d_mms_mbrs_mod_req.[val_membership_upgrade_date_range_id]
     , d_mms_mbrs_mod_req.[club_id]
     , d_mms_mbrs_mod_req.[commisioned_employee_id]
     , d_mms_mbrs_mod_req.[member_agreement_staging_id]
     , d_mms_mbrs_mod_req.[previous_membership_type_id]
     , d_mms_mbrs_mod_req.[request_date_time]
     , d_mms_mbrs_mod_req.[utc_request_date_time]
     , d_mms_mbrs_mod_req.[request_date_time_zone]
     , d_mms_mbrs_mod_req.[effective_date]
     , d_mms_mbrs_mod_req.[inserted_date_time]
     , d_mms_mbrs_mod_req.[updated_date_time]
     , d_mms_mbrs_mod_req.[status_changed_date_time]
     , d_mms_mbrs_mod_req.[last_eft_month]
     , d_mms_mbrs_mod_req.[future_membership_upgrade_flag]
     , d_mms_mbrs_mod_req.[first_months_dues]
     , d_mms_mbrs_mod_req.[total_monthly_amount]
     , d_mms_mbrs_mod_req.[membership_upgrade_month_year]
     , d_mms_mbrs_mod_req.[agreement_price]
     , d_mms_mbrs_mod_req.[waive_service_fee_flag]
     , d_mms_mbrs_mod_req.[full_access_date_extension_flag]
     , d_mms_mbrs_mod_req.[new_members]
     , d_mms_mbrs_mod_req.[add_on_fee]
     , d_mms_mbrs_mod_req.[service_fee]
     , d_mms_mbrs_mod_req.[diamond_fee]
     , d_mms_mbrs_mod_req.[pro_rated_dues]
     , d_mms_mbrs_mod_req.[deactivated_members]
     , d_mms_mbrs_mod_req.[juniors_assessed]
     , d_mms_mbrs_mod_req.[member_freeze_flag]
     , PITU.[effective_date_time]  --= ISNULL(d_mms_mbrs_mod_req.[updated_date_time],d_mms_mbrs_mod_req.[inserted_date_time])
     , d_mms_mbrs_mod_req.[bk_hash]
     , d_mms_mbrs_mod_req.[p_mms_membership_modification_request_id]
     , d_mms_mbrs_mod_req.[dv_load_date_time]
     , d_mms_mbrs_mod_req.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (batch_info.[l_hash] + batch_info.[s_hash])),2)
  FROM [dbo].[d_mms_membership_modification_request_history] d_mms_mbrs_mod_req
       INNER JOIN
         ( SELECT PIT.[p_mms_membership_modification_request_id]
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
             WHERE ( PIT.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
                 AND PIT.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END )
         ) PITU
         ON PITU.[p_mms_membership_modification_request_id] = d_mms_mbrs_mod_req.[p_mms_membership_modification_request_id]
            AND PITU.RowRank = 1 AND PITU.RowNumber = 1
       CROSS APPLY
         ( SELECT [l_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_mod_req.[membership_modification_request_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_mod_req.[membership_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_mod_req.[member_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_mod_req.[val_membership_modification_request_type_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_mod_req.[val_flex_reason_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_mod_req.[membership_type_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_mod_req.[val_membership_modification_request_status_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_mod_req.[employee_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_mod_req.[val_membership_upgrade_date_range_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_mod_req.[club_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_mod_req.[commisioned_employee_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_mod_req.[member_agreement_staging_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_mod_req.[previous_membership_type_id]),'z#@$k%&P'))),2)

                , [s_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_mod_req.[membership_modification_request_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_mod_req.[request_date_time], 120),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_mod_req.[utc_request_date_time], 120),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(d_mms_mbrs_mod_req.[request_date_time_zone],'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_mod_req.[effective_date], 120),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_mod_req.[inserted_date_time], 120),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_mod_req.[updated_date_time], 120),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_mod_req.[status_changed_date_time], 120),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(d_mms_mbrs_mod_req.[last_eft_month],'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_mod_req.[future_membership_upgrade_flag]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_mod_req.[first_months_dues]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_mod_req.[total_monthly_amount]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_mod_req.[membership_upgrade_month_year]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_mod_req.[agreement_price]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_mod_req.[waive_service_fee_flag]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_mod_req.[full_access_date_extension_flag]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(d_mms_mbrs_mod_req.[new_members],'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_mod_req.[add_on_fee]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_mod_req.[service_fee]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_mod_req.[diamond_fee]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_mod_req.[pro_rated_dues]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(d_mms_mbrs_mod_req.[deactivated_members],'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_mod_req.[juniors_assessed]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_mod_req.[member_freeze_flag]),'z#@$k%&P'))),2)
         ) batch_info
ORDER BY d_mms_mbrs_mod_req.[dv_batch_id] ASC, d_mms_mbrs_mod_req.[dv_load_date_time] ASC, PITU.[effective_date_time] ASC;

END
