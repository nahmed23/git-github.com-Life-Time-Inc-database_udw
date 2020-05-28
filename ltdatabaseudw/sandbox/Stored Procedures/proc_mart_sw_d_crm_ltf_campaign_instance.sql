CREATE PROC [sandbox].[proc_mart_sw_d_crm_ltf_campaign_instance] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT d_crm_ltf_campaign_instance.[campaign_id]
     , d_crm_ltf_campaign_instance.[contact_id]
     , d_crm_ltf_campaign_instance.[lead_id]
     , d_crm_ltf_campaign_instance.[ltf_campaign_instance_id]
     , d_crm_ltf_campaign_instance.[ltf_club_id]
     , d_crm_ltf_campaign_instance.[ltf_referring_member_id]
     , d_crm_ltf_campaign_instance.[opportunity_id]
     , d_crm_ltf_campaign_instance.[dim_crm_campaign_key]
     , d_crm_ltf_campaign_instance.[dim_crm_contact_key]
     , d_crm_ltf_campaign_instance.[dim_crm_lead_key]
     , d_crm_ltf_campaign_instance.[dim_crm_ltf_campaign_instance_key]
     , d_crm_ltf_campaign_instance.[dim_crm_ltf_club_key]
     , d_crm_ltf_campaign_instance.[dim_crm_opportunity_key]
     , d_crm_ltf_campaign_instance.[created_on]
     , d_crm_ltf_campaign_instance.[email_address_1]
     , d_crm_ltf_campaign_instance.[first_name]
     , d_crm_ltf_campaign_instance.[last_name]
     , d_crm_ltf_campaign_instance.[ltf_campaign_name]
     , d_crm_ltf_campaign_instance.[ltf_do_not_email_address_1]
     , d_crm_ltf_campaign_instance.[ltf_initial_use_date]
     , d_crm_ltf_campaign_instance.[ltf_issued_date]
     , d_crm_ltf_campaign_instance.[ltf_lead_source]
     , d_crm_ltf_campaign_instance.[ltf_lead_source_name]
     , d_crm_ltf_campaign_instance.[ltf_lead_type]
     , d_crm_ltf_campaign_instance.[ltf_lead_type_name]
     , d_crm_ltf_campaign_instance.[modified_on]
     , d_crm_ltf_campaign_instance.[status_code]
     , d_crm_ltf_campaign_instance.[status_code_name]
     , d_crm_ltf_campaign_instance.[club_id]
     , d_crm_ltf_campaign_instance.[employee_id]
     , d_crm_ltf_campaign_instance.[local_created_on]
     , d_crm_ltf_campaign_instance.[local_modified_on]
     , d_crm_ltf_campaign_instance.[local_ltf_initial_use_date]
     , d_crm_ltf_campaign_instance.[local_ltf_issued_date]
     , d_crm_ltf_campaign_instance.[inserted_date_time]
     , d_crm_ltf_campaign_instance.[updated_date_time]
     , d_crm_ltf_campaign_instance.[bk_hash]
     , d_crm_ltf_campaign_instance.[p_crmcloudsync_ltf_campaign_instance_id]
     , d_crm_ltf_campaign_instance.[dv_load_date_time]
     , d_crm_ltf_campaign_instance.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (batch_info.[l_hash] + batch_info.[s_hash])),2)
  FROM ( SELECT d_crm_campaign.[campaign_id]
              , d_crm_contact.[contact_id]
              , d_crm_lead.[lead_id]
              , d_crm_ltf_campaign_instance.[ltf_campaign_instance_id]
              , d_crm_ltf_club.[ltf_club_id]
              , [ltf_referring_member_id] = d_mms_member.[member_id]
              , d_crm_opportunity.[opportunity_id]
              , [dim_crm_campaign_key]              = d_crm_campaign.[dim_crm_campaign_key]
              , [dim_crm_contact_key]               = d_crm_contact.[dim_crm_contact_key]
              , [dim_crm_lead_key]                  = d_crm_lead.[dim_crm_lead_key]
              , [dim_crm_ltf_campaign_instance_key] = d_crm_ltf_campaign_instance.[dim_crm_ltf_campaign_instance_key]
              , [dim_crm_ltf_club_key]              = d_crm_ltf_club.[dim_crm_ltf_club_key]
              , [dim_crm_opportunity_key]           = d_crm_opportunity.[dim_crm_opportunity_key]
              , d_crm_ltf_campaign_instance.[created_on]
              , [email_address_1] = NullIf(NullIf(d_crm_contact.[email_address_1],''),'********@*******.***')
              , [first_name] = NullIf(d_crm_contact.[first_name],'')
              , [last_name] = NullIf(d_crm_contact.[last_name],'')
              , d_crm_ltf_campaign_instance.[ltf_campaign_name]
              , d_crm_contact.[ltf_do_not_email_address_1]
              , d_crm_ltf_campaign_instance.[ltf_initial_use_date]
              , d_crm_ltf_campaign_instance.[ltf_issued_date]
              , [ltf_lead_source] = CASE WHEN NOT d_crm_lead.[lead_id] Is Null THEN d_crm_lead.[ltf_lead_source] ELSE d_crm_opportunity.[ltf_lead_source] END
              , [ltf_lead_source_name] = CASE WHEN NOT d_crm_lead.[lead_id] Is Null
                                              THEN CASE WHEN (NOT d_crm_lead.[ltf_lead_source_name] Is Null AND d_crm_lead.[ltf_lead_source_name] = 'OMS chat') THEN 'Web Chat' ELSE NullIf(d_crm_lead.[ltf_lead_source_name],'') END --Converting legacy to current for historical reporting consistancies.
                                              ELSE CASE WHEN (NOT d_crm_opportunity.[ltf_lead_source_name] Is Null AND d_crm_opportunity.[ltf_lead_source_name] = 'OMS chat') THEN 'Web Chat' ELSE NullIf(d_crm_opportunity.[ltf_lead_source_name],'') END --Converting legacy to current for historical reporting consistancies.
                                         END
              , [ltf_lead_type] = CASE WHEN NOT d_crm_lead.[lead_id] Is Null THEN d_crm_lead.[ltf_lead_type] ELSE d_crm_opportunity.[ltf_lead_type] END
              , [ltf_lead_type_name] = CASE WHEN NOT d_crm_lead.[lead_id] Is Null THEN d_crm_lead.[ltf_lead_type_name] ELSE d_crm_opportunity.[ltf_lead_type_name] END
              , d_crm_ltf_campaign_instance.[modified_on]
              , d_crm_ltf_campaign_instance.[status_code]
              , d_crm_ltf_campaign_instance.[status_code_name]
              , d_crm_ltf_club.[club_id]
              , d_crm_created_on_behalf_user.[employee_id]
              , [local_created_on]           = DATEADD(hh, -(map_tz_created.[offset]), d_crm_ltf_campaign_instance.[created_on])
              , [local_modified_on]          = DATEADD(hh, -(map_tz_modified.[offset]), d_crm_ltf_campaign_instance.[modified_on])
              , [local_ltf_initial_use_date] = DATEADD(hh, -(map_tz_initial_use.[offset]), d_crm_ltf_campaign_instance.[ltf_initial_use_date])
              , [local_ltf_issued_date]      = DATEADD(hh, -(map_tz_issued.[offset]), d_crm_ltf_campaign_instance.[ltf_issued_date])
              , d_crm_ltf_campaign_instance.[inserted_date_time]
              , d_crm_ltf_campaign_instance.[updated_date_time]
              , d_crm_ltf_campaign_instance.[bk_hash]
              , d_crm_ltf_campaign_instance.[p_crmcloudsync_ltf_campaign_instance_id]
              , d_crm_ltf_campaign_instance.[dv_load_date_time]
              , d_crm_ltf_campaign_instance.[dv_batch_id]
           FROM [dbo].[d_crmcloudsync_ltf_campaign_instance] d_crm_ltf_campaign_instance
                INNER JOIN [dbo].[d_crmcloudsync_campaign] d_crm_campaign
                  ON d_crm_campaign.[dim_crm_campaign_key] = d_crm_ltf_campaign_instance.[ltf_campaign_dim_crm_campaign_key]
                INNER JOIN [dbo].[d_crmcloudsync_ltf_club] d_crm_ltf_club
                  ON d_crm_ltf_club.[dim_crm_ltf_club_key] = d_crm_ltf_campaign_instance.[dim_crm_ltf_club_key]
                INNER JOIN [dbo].[dim_club] dim_club
                  ON dim_club.[dim_club_key] = d_crm_ltf_club.[dim_club_key]
                INNER JOIN [dbo].[d_crmcloudsync_system_user] d_crm_created_on_behalf_user
                  ON d_crm_created_on_behalf_user.[dim_crm_system_user_key] = d_crm_ltf_campaign_instance.[created_on_behalf_by_dim_crm_system_user_key]
                INNER JOIN [dbo].[d_crmcloudsync_contact] d_crm_contact
                  ON d_crm_contact.[dim_crm_contact_key] = d_crm_ltf_campaign_instance.[ltf_issuing_contact_dim_crm_contact_key]
                INNER JOIN [dbo].[d_crmcloudsync_opportunity] d_crm_opportunity
                  ON d_crm_opportunity.[dim_crm_opportunity_key] = d_crm_ltf_campaign_instance.[ltf_issuing_opportunity_dim_crm_opportunity_key]
                  --ON d_crm_contact.[dim_crm_contact_key] = d_crm_opportunity.[parent_contact_dim_crm_contact_key]
                INNER JOIN [dbo].[d_crmcloudsync_lead] d_crm_lead
                  ON d_crm_lead.[dim_crm_lead_key] = d_crm_ltf_campaign_instance.[ltf_issuing_lead_dim_crm_lead_key]
                INNER JOIN [dbo].[d_mms_member] d_mms_member
                  ON d_mms_member.[dim_mms_member_key] = d_crm_ltf_campaign_instance.[dim_mms_member_key]  --[ltf_referring_member_dim_mms_member_key]  wrong/bad data
                LEFT OUTER JOIN [dbo].[map_utc_time_zone_conversion] map_tz_created
                  ON map_tz_created.[val_time_zone_id] = dim_club.[val_time_zone_id]
                      AND (NOT d_crm_ltf_campaign_instance.[created_on] Is Null AND (d_crm_ltf_campaign_instance.[created_on] >= map_tz_created.[utc_start_date_time] AND d_crm_ltf_campaign_instance.[created_on] < map_tz_created.[utc_end_date_time]))
                LEFT OUTER JOIN [dbo].[map_utc_time_zone_conversion] map_tz_modified
                  ON map_tz_modified.[val_time_zone_id] = dim_club.[val_time_zone_id]
                      AND (NOT d_crm_ltf_campaign_instance.[modified_on] Is Null AND (d_crm_ltf_campaign_instance.[modified_on] >= map_tz_modified.[utc_start_date_time] AND d_crm_ltf_campaign_instance.[modified_on] < map_tz_modified.[utc_end_date_time]))
                LEFT OUTER JOIN [dbo].[map_utc_time_zone_conversion] map_tz_initial_use
                  ON map_tz_initial_use.[val_time_zone_id] = dim_club.[val_time_zone_id]
                      AND (NOT d_crm_ltf_campaign_instance.[ltf_initial_use_date] Is Null AND (d_crm_ltf_campaign_instance.[ltf_initial_use_date] >= map_tz_initial_use.[utc_start_date_time] AND d_crm_ltf_campaign_instance.[ltf_initial_use_date] < map_tz_initial_use.[utc_end_date_time]))
                LEFT OUTER JOIN [dbo].[map_utc_time_zone_conversion] map_tz_issued
                  ON map_tz_issued.[val_time_zone_id] = dim_club.[val_time_zone_id]
                      AND (NOT d_crm_ltf_campaign_instance.[ltf_issued_date] Is Null AND (d_crm_ltf_campaign_instance.[ltf_issued_date] >= map_tz_issued.[utc_start_date_time] AND d_crm_ltf_campaign_instance.[ltf_issued_date] < map_tz_issued.[utc_end_date_time]))
             WHERE ( ( d_crm_ltf_campaign_instance.[bk_hash] IN ('-999','-998','-997') )
                  OR ( d_crm_ltf_campaign_instance.[inserted_date_time] >= DATEADD(YY, -2, DATEADD(YY, DATEDIFF(YY, 0, DATEADD(DD, -1, DATEADD(DD, DATEDIFF(DD, 0, GETDATE()), 0))), 0))
                       AND ( d_crm_ltf_campaign_instance.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
                         AND d_crm_ltf_campaign_instance.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END ) ) )
             --AND d_crm_campaign.campaign_id = 'F487229C-068C-E711-80E5-3863BB34FC88'
       ) d_crm_ltf_campaign_instance
       CROSS APPLY
         ( SELECT [l_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar(36), d_crm_ltf_campaign_instance.[campaign_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar(36), d_crm_ltf_campaign_instance.[contact_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar(36), d_crm_ltf_campaign_instance.[lead_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar(36), d_crm_ltf_campaign_instance.[ltf_campaign_instance_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar(36), d_crm_ltf_campaign_instance.[ltf_club_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar(36), d_crm_ltf_campaign_instance.[opportunity_id]),'z#@$k%&P'))),2)

                , [s_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar(36), d_crm_ltf_campaign_instance.[ltf_campaign_instance_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_ltf_campaign_instance.[created_on], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_ltf_campaign_instance.[email_address_1]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_ltf_campaign_instance.[first_name]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_ltf_campaign_instance.[last_name]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_ltf_campaign_instance.[ltf_do_not_email_address_1]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_ltf_campaign_instance.[ltf_initial_use_date], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_ltf_campaign_instance.[ltf_issued_date], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_ltf_campaign_instance.[ltf_lead_source]),'z#@$k%&P')
                                                                  --+ 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_ltf_campaign_instance.[ltf_lead_source_name]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_ltf_campaign_instance.[ltf_lead_type]),'z#@$k%&P')
                                                                  --+ 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_ltf_campaign_instance.[ltf_lead_type_name]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_ltf_campaign_instance.[modified_on], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_ltf_campaign_instance.[status_code]),'z#@$k%&P'))),2)
                                                                  --+ 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_ltf_campaign_instance.[status_code_name]),'z#@$k%&P')
         ) batch_info
ORDER BY d_crm_ltf_campaign_instance.[dv_batch_id] ASC, d_crm_ltf_campaign_instance.[dv_load_date_time] ASC, ISNULL(d_crm_ltf_campaign_instance.[modified_on],d_crm_ltf_campaign_instance.[created_on]) ASC;

END
