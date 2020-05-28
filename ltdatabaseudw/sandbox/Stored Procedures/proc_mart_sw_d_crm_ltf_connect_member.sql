CREATE PROC [sandbox].[proc_mart_sw_d_crm_ltf_connect_member] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT d_crm_ltf_connect_member.[ltf_connect_member_id]
     , d_crm_ltf_connect_member.[contact_id]
     , d_crm_ltf_connect_member.[ltf_club_id]
     , d_crm_ltf_connect_member.[ltf_subscriber_id]
     , d_crm_ltf_connect_member.[opportunity_id]
     , d_crm_ltf_connect_member.[dim_crm_contact_key]
     , d_crm_ltf_connect_member.[dim_crm_ltf_club_key]
     , d_crm_ltf_connect_member.[dim_crm_ltf_connect_member_key]
     , d_crm_ltf_connect_member.[dim_crm_ltf_subscriber_key]
     , d_crm_ltf_connect_member.[dim_crm_opportunity_key]
     , d_crm_ltf_connect_member.[dim_mms_member_key]
     , d_crm_ltf_connect_member.[ltf_profile_notes]
     , d_crm_ltf_connect_member.[ltf_programs_of_interest_name]
     , d_crm_ltf_connect_member.[ltf_want_to_do_name]
     , d_crm_ltf_connect_member.[ltf_who_met_with]
     , d_crm_ltf_connect_member.[ltf_why_want_to_do_name]
     , d_crm_ltf_connect_member.[created_on]
     , d_crm_ltf_connect_member.[modified_on]
     , d_crm_ltf_connect_member.[club_id]
     , d_crm_ltf_connect_member.[member_id]
     , d_crm_ltf_connect_member.[local_created_on]
     , d_crm_ltf_connect_member.[local_modified_on]
     , d_crm_ltf_connect_member.[inserted_date_time]
     , d_crm_ltf_connect_member.[updated_date_time]
     , d_crm_ltf_connect_member.[bk_hash]
     , d_crm_ltf_connect_member.[p_crmcloudsync_ltf_connect_member_id]
     , d_crm_ltf_connect_member.[dv_load_date_time]
     , d_crm_ltf_connect_member.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (batch_info.[l_hash] + batch_info.[s_hash])),2)
  FROM ( SELECT d_crm_contact.[contact_id]
              , d_crm_ltf_club.[ltf_club_id]
              , d_crm_ltf_connect_member.[ltf_connect_member_id]
              , d_crm_ltf_subscriber.[ltf_subscriber_id]
              , d_crm_opportunity.[opportunity_id]
              , [dim_crm_contact_key]            = d_crm_contact.[dim_crm_contact_key]
              , [dim_crm_ltf_club_key]           = d_crm_ltf_club.[dim_crm_ltf_club_key]
              , [dim_crm_ltf_connect_member_key] = d_crm_ltf_connect_member.[dim_crm_ltf_connect_member_key]
              , [dim_crm_ltf_subscriber_key]     = d_crm_ltf_subscriber.[dim_crm_ltf_subscriber_key]
              , [dim_crm_opportunity_key]        = d_crm_opportunity.[dim_crm_opportunity_key]
              , [dim_mms_member_key]             = d_crm_ltf_subscriber.[dim_mms_member_key]
              , [ltf_profile_notes]              = CONVERT(nvarchar(2000), NullIf(d_crm_ltf_connect_member.[ltf_profile_notes],''))
              , [ltf_programs_of_interest_name]  = CONVERT(nvarchar(255), NullIf(d_crm_ltf_connect_member.[ltf_programs_of_interest_name],''))
              , [ltf_want_to_do_name]            = CONVERT(nvarchar(255), NullIf(d_crm_ltf_connect_member.[ltf_want_to_do_name],''))
              , [ltf_who_met_with]               = CONVERT(nvarchar(100), NullIf(d_crm_ltf_connect_member.[ltf_who_met_with],''))
              , [ltf_why_want_to_do_name]        = CONVERT(nvarchar(255), NullIf(d_crm_ltf_connect_member.[ltf_why_want_to_do_name],''))
              , d_crm_ltf_connect_member.[created_on]
              , d_crm_ltf_connect_member.[modified_on]
              , d_crm_ltf_club.[club_id]
              , d_batch_connect_member.[member_id]  -- = CASE WHEN (ISNUMERIC(d_crm_ltf_subscriber.[ltf_name]) = 1 AND CONVERT(bigint, d_crm_ltf_subscriber.[ltf_name]) <= 2147483647) THEN CONVERT(int, d_crm_ltf_subscriber.[ltf_name]) ELSE Null END
              , [sort_order]        = CASE ISNULL(d_crm_ltf_connect_member.[state_code], 0) WHEN 0 THEN 2 WHEN 1 THEN 1 WHEN 2 THEN 3 END
              , [local_created_on]  = DATEADD(hh, -(map_tz_created.[offset]), d_crm_ltf_connect_member.[created_on])
              , [local_modified_on] = DATEADD(hh, -(map_tz_modified.[offset]), d_crm_ltf_connect_member.[modified_on])
              , d_crm_ltf_connect_member.[inserted_date_time]
              , d_crm_ltf_connect_member.[updated_date_time]
              , d_crm_ltf_connect_member.[bk_hash]
              , d_crm_ltf_connect_member.[p_crmcloudsync_ltf_connect_member_id]
              , d_crm_ltf_connect_member.[dv_load_date_time]
              , d_crm_ltf_connect_member.[dv_batch_id]
              , RowRank = RANK() OVER (PARTITION BY d_batch_connect_member.[member_id] ORDER BY d_crm_ltf_connect_member.[p_crmcloudsync_ltf_connect_member_id] DESC)
              , RowNumber = ROW_NUMBER() OVER (PARTITION BY d_batch_connect_member.[member_id] ORDER BY d_crm_ltf_connect_member.[p_crmcloudsync_ltf_connect_member_id] DESC)
           FROM [dbo].[d_crmcloudsync_ltf_connect_member] d_crm_ltf_connect_member
                INNER JOIN [dbo].[d_crmcloudsync_opportunity] d_crm_opportunity
                  ON d_crm_opportunity.[dim_crm_opportunity_key] = d_crm_ltf_connect_member.[dim_crm_opportunity_key]
                INNER JOIN [dbo].[d_crmcloudsync_ltf_subscriber] d_crm_ltf_subscriber
                  ON d_crm_ltf_subscriber.[dim_crm_ltf_subscriber_key] = d_crm_ltf_connect_member.[dim_crm_ltf_subscriber_key]
                INNER JOIN [dbo].[d_crmcloudsync_contact] d_crm_contact
                  ON d_crm_contact.[dim_crm_contact_key] = d_crm_ltf_subscriber.[ltf_contact_dim_crm_contact_key]
                INNER JOIN [dbo].[d_crmcloudsync_ltf_subscription] d_crm_ltf_subscription
                  ON d_crm_ltf_subscription.[dim_crm_ltf_subscription_key] = d_crm_ltf_subscriber.[dim_crm_ltf_subscription_key]
                INNER JOIN [dbo].[d_crmcloudsync_ltf_club] d_crm_ltf_club
                  ON d_crm_ltf_club.[dim_crm_ltf_club_key] = d_crm_ltf_subscription.[dim_crm_ltf_club_key]
                INNER JOIN [dbo].[dim_club] dim_club
                  ON dim_club.[dim_club_key] = d_crm_ltf_club.[dim_club_key]
                LEFT OUTER JOIN [dbo].[map_utc_time_zone_conversion] map_tz_created
                  ON map_tz_created.[val_time_zone_id] = dim_club.[val_time_zone_id]
                      AND (NOT d_crm_ltf_connect_member.[created_on] Is Null AND (d_crm_ltf_connect_member.[created_on] >= map_tz_created.[utc_start_date_time] AND d_crm_ltf_connect_member.[created_on] < map_tz_created.[utc_end_date_time]))
                LEFT OUTER JOIN [dbo].[map_utc_time_zone_conversion] map_tz_modified
                  ON map_tz_modified.[val_time_zone_id] = dim_club.[val_time_zone_id]
                      AND (NOT d_crm_ltf_connect_member.[modified_on] Is Null AND (d_crm_ltf_connect_member.[modified_on] >= map_tz_modified.[utc_start_date_time] AND d_crm_ltf_connect_member.[modified_on] < map_tz_modified.[utc_end_date_time]))
                CROSS APPLY
                  ( SELECT [member_id] = CASE WHEN (ISNUMERIC(d_crm_ltf_subscriber.[ltf_name]) = 1 AND CONVERT(bigint, d_crm_ltf_subscriber.[ltf_name]) <= 2147483647) THEN CONVERT(int, d_crm_ltf_subscriber.[ltf_name]) ELSE Null END ) d_batch_connect_member
           WHERE d_crm_ltf_connect_member.[inserted_date_time] >= DATEADD(YY, -2, DATEADD(YY, DATEDIFF(YY, 0, DATEADD(DD, -1, DATEADD(DD, DATEDIFF(DD, 0, GETDATE()), 0))), 0))
             AND NOT d_batch_connect_member.[member_id] Is Null
             AND ( d_crm_ltf_connect_member.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
               AND d_crm_ltf_connect_member.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END )
       ) d_crm_ltf_connect_member
       CROSS APPLY
         ( SELECT [l_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar(36), d_crm_ltf_connect_member.[ltf_connect_member_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar(36), d_crm_ltf_connect_member.[opportunity_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar(36), d_crm_ltf_connect_member.[contact_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar(36), d_crm_ltf_connect_member.[ltf_subscriber_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar(36), d_crm_ltf_connect_member.[ltf_club_id]),'z#@$k%&P'))),2)

                , [s_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar(36), d_crm_ltf_connect_member.[ltf_connect_member_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_ltf_connect_member.[ltf_programs_of_interest_name]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_ltf_connect_member.[ltf_want_to_do_name]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_ltf_connect_member.[ltf_who_met_with]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_ltf_connect_member.[ltf_why_want_to_do_name]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_ltf_connect_member.[created_on], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_ltf_connect_member.[modified_on], 120),'z#@$k%&P'))),2)
         ) batch_info
  WHERE d_crm_ltf_connect_member.RowRank = 1 AND d_crm_ltf_connect_member.RowNumber = 1
ORDER BY d_crm_ltf_connect_member.[dv_batch_id] ASC, d_crm_ltf_connect_member.[dv_load_date_time] ASC, ISNULL(d_crm_ltf_connect_member.[modified_on],d_crm_ltf_connect_member.[created_on]) ASC;

END
