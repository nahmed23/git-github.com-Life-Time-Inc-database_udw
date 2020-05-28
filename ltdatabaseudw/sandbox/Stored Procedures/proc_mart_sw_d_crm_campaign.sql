CREATE PROC [sandbox].[proc_mart_sw_d_crm_campaign] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT d_crm_campaign.[campaign_id]
     , d_crm_campaign.[dim_crm_campaign_key]
     , d_crm_campaign.[ltf_job_id]
     , d_crm_campaign.[code_name]
     , d_crm_campaign.[created_on]
     , d_crm_campaign.[description]
     , d_crm_campaign.[ltf_club_restriction_type]
     , d_crm_campaign.[ltf_club_restriction_type_name]
     , d_crm_campaign.[ltf_expiration_days]
     , d_crm_campaign.[ltf_expiration_type]
     , d_crm_campaign.[ltf_expiration_type_name]
     , d_crm_campaign.[ltf_guest_pass_type]
     , d_crm_campaign.[ltf_issuance_method]
     , d_crm_campaign.[ltf_member_referral]
     , d_crm_campaign.[ltf_pass_days]
     , d_crm_campaign.[ltf_qr_code_string]
     , d_crm_campaign.[ltf_qr_code_url]
     , d_crm_campaign.[ltf_restricted_by_policy]
     , d_crm_campaign.[ltf_reward_club]
     , d_crm_campaign.[ltf_reward_lt_bucks]
     , d_crm_campaign.[ltf_reward_type]
     , d_crm_campaign.[ltf_reward_wait_days]
     , d_crm_campaign.[ltf_send_id]
     , d_crm_campaign.[ltf_targeted_prospects]
     , d_crm_campaign.[ltf_targeted_issue_date]
     , d_crm_campaign.[ltf_user_defined_dates]
     , d_crm_campaign.[ltf_user_defined_dates_name]
     , d_crm_campaign.[modified_on]
     , d_crm_campaign.[name]
     , d_crm_campaign.[proposed_end]
     , d_crm_campaign.[proposed_start]
     , d_crm_campaign.[state_code]
     , d_crm_campaign.[state_code_name]
     , d_crm_campaign.[status_code]
     , d_crm_campaign.[status_code_name]
     , d_crm_campaign.[type_code]
     , d_crm_campaign.[type_code_name]
     , d_crm_campaign.[local_created_on]
     , d_crm_campaign.[local_modified_on]
     , d_crm_campaign.[local_proposed_end]
     , d_crm_campaign.[local_proposed_start]
     , d_crm_campaign.[inserted_date_time]
     , d_crm_campaign.[updated_date_time]
     , d_crm_campaign.[bk_hash]
     , d_crm_campaign.[p_crmcloudsync_campaign_id]
     , d_crm_campaign.[dv_load_date_time]
     , d_crm_campaign.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (batch_info.[l_hash] + batch_info.[s_hash])),2)
  FROM ( SELECT d_crm_campaign.[campaign_id]
              , [dim_crm_campaign_key] = d_crm_campaign.[bk_hash]
              , d_crm_campaign.[ltf_job_id]
              , SAT.[code_name]
              , d_crm_campaign.[created_on]
              , [description] = NullIf(NullIf(d_crm_campaign.[description],''),'x')
              , SAT.[ltf_club_restriction_type]
              , SAT.[ltf_club_restriction_type_name]
              , SAT.[ltf_expiration_days]
              , d_crm_campaign.[ltf_expiration_type]
              , SAT.[ltf_expiration_type_name]
              , SAT.[ltf_guest_pass_type]
              , d_crm_campaign.[ltf_issuance_method]
              , d_crm_campaign.[ltf_member_referral]
              , d_crm_campaign.[ltf_pass_days]
              , SAT.[ltf_qr_code_string]
              , SAT.[ltf_qr_code_url]
              , d_crm_campaign.[ltf_restricted_by_policy]
              , d_crm_campaign.[ltf_reward_club]
              , d_crm_campaign.[ltf_reward_lt_bucks]
              , d_crm_campaign.[ltf_reward_type]
              , d_crm_campaign.[ltf_reward_wait_days]
              , SAT.[ltf_send_id]
              , d_crm_campaign.[ltf_targeted_prospects]
              , SAT.[ltf_targeted_issue_date]
              , SAT.[ltf_user_defined_dates]
              , SAT.[ltf_user_defined_dates_name]
              , d_crm_campaign.[modified_on]
              , SAT.[name]
              , d_crm_campaign.[proposed_end]
              , d_crm_campaign.[proposed_start]
              , d_crm_campaign.[state_code]
              , SAT.[state_code_name]
              , d_crm_campaign.[status_code]
              , SAT.[status_code_name]
              , SAT.[type_code]
              , SAT.[type_code_name]
              , [local_created_on]     = DATEADD(hh, -(map_tz_created.[offset]), d_crm_campaign.[created_on])
              , [local_modified_on]    = DATEADD(hh, -(map_tz_modified.[offset]), d_crm_campaign.[modified_on])
              , [local_proposed_end]   = DATEADD(hh, -(map_tz_initial_use.[offset]), d_crm_campaign.[proposed_end])
              , [local_proposed_start] = DATEADD(hh, -(map_tz_issued.[offset]), d_crm_campaign.[proposed_start])
              , SAT.[inserted_date_time]
              , SAT.[updated_date_time]
              , d_crm_campaign.[bk_hash]
              , d_crm_campaign.[p_crmcloudsync_campaign_id]
              , d_crm_campaign.[dv_load_date_time]
              , d_crm_campaign.[dv_batch_id]
           FROM [dbo].[d_crmcloudsync_campaign] d_crm_campaign
                LEFT OUTER JOIN
                  ( SELECT PIT.[p_crmcloudsync_campaign_id]
                         , SAT.[code_name]
                         , SAT.[ltf_club_restriction_type]
                         , SAT.[ltf_club_restriction_type_name]
                         , SAT.[ltf_expiration_days]
                         , SAT.[ltf_expiration_type_name]
                         , SAT.[ltf_guest_pass_type]
                         , SAT.[ltf_qr_code_string]
                         , SAT.[ltf_qr_code_url]
                         , SAT.[ltf_send_id]
                         , SAT.[ltf_targeted_issue_date]
                         , SAT.[ltf_user_defined_dates]
                         , SAT.[ltf_user_defined_dates_name]
                         , SAT.[name]
                         , SAT.[state_code_name]
                         , SAT.[status_code_name]
                         , SAT.[type_code]
                         , SAT.[type_code_name]
                         , SAT.[inserted_date_time]
                         , SAT.[updated_date_time]
                      FROM [dbo].[p_crmcloudsync_campaign] PIT
                           INNER JOIN [dbo].[l_crmcloudsync_campaign] LNK
                             ON LNK.[bk_hash] = PIT.[bk_hash]
                                AND LNK.[l_crmcloudsync_campaign_id] = PIT.[l_crmcloudsync_campaign_id]
                           INNER JOIN [dbo].[s_crmcloudsync_campaign] SAT
                             ON SAT.[bk_hash] = PIT.[bk_hash]
                                AND SAT.[s_crmcloudsync_campaign_id] = PIT.[s_crmcloudsync_campaign_id]
                  ) SAT
                  ON SAT.[p_crmcloudsync_campaign_id] = d_crm_campaign.[p_crmcloudsync_campaign_id]
                LEFT OUTER JOIN [dbo].[map_utc_time_zone_conversion] map_tz_created
                  ON map_tz_created.[val_time_zone_id] = 3  --Central Time
                      AND (NOT d_crm_campaign.[created_on] Is Null AND (d_crm_campaign.[created_on] >= map_tz_created.[utc_start_date_time] AND d_crm_campaign.[created_on] < map_tz_created.[utc_end_date_time]))
                LEFT OUTER JOIN [dbo].[map_utc_time_zone_conversion] map_tz_modified
                  ON map_tz_modified.[val_time_zone_id] = 3  --Central Time
                      AND (NOT d_crm_campaign.[modified_on] Is Null AND (d_crm_campaign.[modified_on] >= map_tz_modified.[utc_start_date_time] AND d_crm_campaign.[modified_on] < map_tz_modified.[utc_end_date_time]))
                LEFT OUTER JOIN [dbo].[map_utc_time_zone_conversion] map_tz_initial_use
                  ON map_tz_initial_use.[val_time_zone_id] = 3  --Central Time
                      AND (NOT d_crm_campaign.[proposed_end] Is Null AND (d_crm_campaign.[proposed_end] >= map_tz_initial_use.[utc_start_date_time] AND d_crm_campaign.[proposed_end] < map_tz_initial_use.[utc_end_date_time]))
                LEFT OUTER JOIN [dbo].[map_utc_time_zone_conversion] map_tz_issued
                  ON map_tz_issued.[val_time_zone_id] = 3  --Central Time
                      AND (NOT d_crm_campaign.[proposed_start] Is Null AND (d_crm_campaign.[proposed_start] >= map_tz_issued.[utc_start_date_time] AND d_crm_campaign.[proposed_start] < map_tz_issued.[utc_end_date_time]))
           WHERE ( d_crm_campaign.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
               AND d_crm_campaign.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END )
       ) d_crm_campaign
       CROSS APPLY
         ( SELECT [l_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar(36), d_crm_campaign.[campaign_id]),'z#@$k%&P'))),2)

                , [s_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar(36), d_crm_campaign.[campaign_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_campaign.[ltf_job_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_campaign.[code_name]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_campaign.[created_on], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_campaign.[description]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_campaign.[ltf_club_restriction_type]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_campaign.[ltf_club_restriction_type_name]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_campaign.[ltf_expiration_days]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_campaign.[ltf_expiration_type]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_campaign.[ltf_expiration_type_name]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_campaign.[ltf_guest_pass_type]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_campaign.[ltf_issuance_method]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_campaign.[ltf_member_referral]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_campaign.[ltf_pass_days]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_campaign.[ltf_qr_code_string]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_campaign.[ltf_qr_code_url]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_campaign.[ltf_restricted_by_policy]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_campaign.[ltf_reward_club]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_campaign.[ltf_reward_lt_bucks]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_campaign.[ltf_reward_type]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_campaign.[ltf_reward_wait_days]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_campaign.[ltf_send_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_campaign.[ltf_targeted_prospects]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_campaign.[ltf_targeted_issue_date], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_campaign.[ltf_user_defined_dates]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_campaign.[ltf_user_defined_dates_name]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_campaign.[modified_on], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_campaign.[name]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_campaign.[proposed_end], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_campaign.[proposed_start], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_campaign.[state_code]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_campaign.[state_code_name]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_campaign.[status_code]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_campaign.[status_code_name]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_campaign.[type_code]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_campaign.[type_code_name]),'z#@$k%&P'))),2)
         ) batch_info
ORDER BY d_crm_campaign.[dv_batch_id] ASC, d_crm_campaign.[dv_load_date_time] ASC, ISNULL(d_crm_campaign.[modified_on],d_crm_campaign.[created_on]) ASC;

END
