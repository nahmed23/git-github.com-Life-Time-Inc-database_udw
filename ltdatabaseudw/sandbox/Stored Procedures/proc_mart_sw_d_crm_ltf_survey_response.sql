CREATE PROC [sandbox].[proc_mart_sw_d_crm_ltf_survey_response] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT d_crm_ltf_survey_response.[contact_id]
     , d_crm_ltf_survey_response.[ltf_club_id]
     , d_crm_ltf_survey_response.[ltf_connect_member_id]
     , d_crm_ltf_survey_response.[ltf_subscriber_id]
     , d_crm_ltf_survey_response.[ltf_survey_id]
     , d_crm_ltf_survey_response.[ltf_survey_response_id]
     , d_crm_ltf_survey_response.[opportunity_id]
     , d_crm_ltf_survey_response.[dim_crm_contact_key]
     , d_crm_ltf_survey_response.[dim_crm_ltf_club_key]
     , d_crm_ltf_survey_response.[dim_crm_ltf_connect_member_key]
     , d_crm_ltf_survey_response.[dim_crm_ltf_subscriber_key]
     , d_crm_ltf_survey_response.[dim_crm_ltf_survey_key]
     , d_crm_ltf_survey_response.[dim_crm_ltf_survey_response_key]
     , d_crm_ltf_survey_response.[dim_crm_opportunity_key]
     , d_crm_ltf_survey_response.[dim_mms_member_key]
     , d_crm_ltf_survey_response.[created_on]
     , d_crm_ltf_survey_response.[ltf_name]
     , d_crm_ltf_survey_response.[ltf_question]
     , d_crm_ltf_survey_response.[ltf_response]
     , d_crm_ltf_survey_response.[ltf_sequence]
     , d_crm_ltf_survey_response.[ltf_source]
     , d_crm_ltf_survey_response.[ltf_submitted_on]
     , d_crm_ltf_survey_response.[ltf_survey_type]
     , d_crm_ltf_survey_response.[modified_on]
     , d_crm_ltf_survey_response.[state_code]
     , d_crm_ltf_survey_response.[status_code]
     , d_crm_ltf_survey_response.[member_id]
     , d_crm_ltf_survey_response.[inserted_date_time]
     , d_crm_ltf_survey_response.[updated_date_time]
     , d_crm_ltf_survey_response.[bk_hash]
     , d_crm_ltf_survey_response.[p_crmcloudsync_ltf_survey_response_id]
     , d_crm_ltf_survey_response.[dv_load_date_time]
     , d_crm_ltf_survey_response.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (batch_info.[l_hash] + batch_info.[s_hash])),2)
     , d_crm_ltf_survey_response.[dv_deleted]
  FROM ( SELECT d_crm_contact.[contact_id]
              , d_crm_ltf_club.[ltf_club_id]
              , [ltf_connect_member_id] = d_crm_ltf_survey.[ltf_connect_member]
              , [ltf_subscriber_id]     = d_crm_ltf_survey.[ltf_subscriber]
              , d_crm_ltf_survey.[ltf_survey_id]
              , d_crm_ltf_survey_response.[ltf_survey_response_id]
              , d_crm_opportunity.[opportunity_id]
              , [dim_crm_contact_key]             = d_crm_contact.[dim_crm_contact_key]
              , [dim_crm_ltf_club_key]            = d_crm_ltf_club.[dim_crm_ltf_club_key]
              , [dim_crm_ltf_connect_member_key]  = d_crm_ltf_connect_member.[dim_crm_ltf_connect_member_key]
              , [dim_crm_ltf_subscriber_key]      = d_crm_ltf_subscriber.[dim_crm_ltf_subscriber_key]
              , [dim_crm_ltf_survey_key]          = d_crm_ltf_survey.[dim_crm_ltf_survey_key]
              , [dim_crm_ltf_survey_response_key] = d_crm_ltf_survey_response.[dim_crm_ltf_survey_response_key]
              , [dim_crm_opportunity_key]         = d_crm_opportunity.[dim_crm_opportunity_key]
              , [dim_mms_member_key]              = d_crm_ltf_subscriber.[dim_mms_member_key]
              , d_crm_ltf_survey_response.[created_on]
              , d_crm_ltf_survey.[ltf_name]
              , d_crm_ltf_survey_response.[ltf_question]
              , d_crm_ltf_survey_response.[ltf_response]
              , d_crm_ltf_survey_response.[ltf_sequence]
              , d_crm_ltf_survey.[ltf_source]
              , d_crm_ltf_survey.[ltf_submitted_on]
              , d_crm_ltf_survey.[ltf_survey_type]
              , d_crm_ltf_survey_response.[modified_on]
              , d_crm_ltf_survey_response.[state_code]
              , d_crm_ltf_survey_response.[status_code]
              , [member_id] = CASE WHEN (ISNUMERIC(d_crm_ltf_subscriber.[ltf_name]) = 1 AND CONVERT(bigint, d_crm_ltf_subscriber.[ltf_name]) <= 2147483647) THEN CONVERT(int, d_crm_ltf_subscriber.[ltf_name]) ELSE Null END
              , d_crm_ltf_survey_response.[inserted_date_time]
              , d_crm_ltf_survey_response.[updated_date_time]
              , d_crm_ltf_survey_response.[bk_hash]
              , d_crm_ltf_survey_response.[p_crmcloudsync_ltf_survey_response_id]
              , d_crm_ltf_survey_response.[dv_load_date_time]
              , d_crm_ltf_survey_response.[dv_batch_id]
              , [dv_deleted] = CAST(CASE WHEN d_crm_ltf_subscriber.[ltf_subscriber_id] Is Null THEN 1 WHEN d_crm_ltf_survey_response.[status_code] = 2 THEN 1 ELSE ISNULL(d_crm_ltf_survey_response.[deleted_flag],0) END AS bit)
           FROM [dbo].[d_crmcloudsync_ltf_survey_response] d_crm_ltf_survey_response
                INNER JOIN [dbo].[d_crmcloudsync_ltf_survey] d_crm_ltf_survey
                  ON d_crm_ltf_survey_response.[ltf_survey] = d_crm_ltf_survey.[ltf_survey_id]
                INNER JOIN [dbo].[d_crmcloudsync_ltf_subscriber] d_crm_ltf_subscriber
                  ON d_crm_ltf_subscriber.[ltf_subscriber_id] = d_crm_ltf_survey.[ltf_subscriber]
                INNER JOIN [dbo].[d_crmcloudsync_contact] d_crm_contact
                  ON d_crm_contact.[dim_crm_contact_key] = d_crm_ltf_subscriber.[ltf_contact_dim_crm_contact_key]
                INNER JOIN [dbo].[d_crmcloudsync_ltf_subscription] d_crm_ltf_subscription
                  ON d_crm_ltf_subscription.[dim_crm_ltf_subscription_key] = d_crm_ltf_subscriber.[dim_crm_ltf_subscription_key]
                INNER JOIN [dbo].[d_crmcloudsync_ltf_club] d_crm_ltf_club
                  ON d_crm_ltf_club.[dim_crm_ltf_club_key] = d_crm_ltf_subscription.[dim_crm_ltf_club_key]
                LEFT OUTER JOIN [dbo].[d_crmcloudsync_ltf_connect_member] d_crm_ltf_connect_member
                  ON d_crm_ltf_connect_member.[ltf_connect_member_id] = d_crm_ltf_survey.[ltf_connect_member]
                LEFT OUTER JOIN [dbo].[d_crmcloudsync_opportunity] d_crm_opportunity
                  ON d_crm_opportunity.[dim_crm_opportunity_key] = d_crm_ltf_connect_member.[dim_crm_opportunity_key]
           WHERE d_crm_ltf_survey_response.[inserted_date_time] >= DATEADD(YY, -2, DATEADD(YY, DATEDIFF(YY, 0, DATEADD(DD, -1, DATEADD(DD, DATEDIFF(DD, 0, GETDATE()), 0))), 0))
             AND ( d_crm_ltf_survey_response.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
               AND d_crm_ltf_survey_response.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END )
       ) d_crm_ltf_survey_response
       CROSS APPLY
         ( SELECT [l_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar(36), d_crm_ltf_survey_response.[ltf_survey_response_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar(36), d_crm_ltf_survey_response.[ltf_survey_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar(36), d_crm_ltf_survey_response.[ltf_connect_member_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar(36), d_crm_ltf_survey_response.[ltf_subscriber_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_ltf_survey_response.[member_id]),'z#@$k%&P'))),2)

                , [s_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar(36), d_crm_ltf_survey_response.[ltf_survey_response_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_ltf_survey_response.[created_on], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_ltf_survey_response.[ltf_question]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_ltf_survey_response.[ltf_response]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_ltf_survey_response.[ltf_sequence]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_ltf_survey_response.[modified_on], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_ltf_survey_response.[state_code]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_ltf_survey_response.[status_code]),'z#@$k%&P'))),2)
         ) batch_info
ORDER BY d_crm_ltf_survey_response.[dv_batch_id] ASC, d_crm_ltf_survey_response.[dv_load_date_time] ASC, ISNULL(d_crm_ltf_survey_response.[modified_on],d_crm_ltf_survey_response.[created_on]) ASC;

END
