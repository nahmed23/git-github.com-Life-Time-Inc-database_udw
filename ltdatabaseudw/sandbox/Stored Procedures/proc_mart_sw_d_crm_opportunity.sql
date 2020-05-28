CREATE PROC [sandbox].[proc_mart_sw_d_crm_opportunity] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT d_crm_opportunity.[campaign_id]
     , d_crm_opportunity.[contact_id]
     , d_crm_opportunity.[created_by_business_unit_id]
     , d_crm_opportunity.[created_by_system_user_id]
     , d_crm_opportunity.[lead_id]
     , d_crm_opportunity.[ltf_campaign_instance_id]
     , d_crm_opportunity.[ltf_club_id]
     , d_crm_opportunity.[ltf_guest_visit_id]
     , d_crm_opportunity.[ltf_referring_member_id]
     , d_crm_opportunity.[opportunity_id]
     , d_crm_opportunity.[owning_business_unit_id]
     , d_crm_opportunity.[owning_system_user_id]
     , d_crm_opportunity.[dim_crm_campaign_key]
     , d_crm_opportunity.[dim_crm_contact_key]
     , d_crm_opportunity.[dim_crm_lead_key]
     , d_crm_opportunity.[dim_crm_ltf_campaign_instance_key]
     , d_crm_opportunity.[dim_crm_ltf_club_key]
     , d_crm_opportunity.[dim_crm_ltf_guest_visit_key]
     , d_crm_opportunity.[dim_crm_opportunity_key]
     , d_crm_opportunity.[activity_type_code]
     , d_crm_opportunity.[activity_type_code_name]
     , d_crm_opportunity.[created_on]
     , d_crm_opportunity.[email_address_1]
     , d_crm_opportunity.[first_name]
     , d_crm_opportunity.[last_name]
     , d_crm_opportunity.[ltf_campaign_name]
     , d_crm_opportunity.[ltf_channel]
     , d_crm_opportunity.[ltf_channel_name]
     , d_crm_opportunity.[ltf_do_not_email_address_1]
     , d_crm_opportunity.[ltf_guest_type]
     , d_crm_opportunity.[ltf_guest_type_name]
     , d_crm_opportunity.[ltf_lead_source]
     , d_crm_opportunity.[ltf_lead_source_name]
     , d_crm_opportunity.[ltf_lead_type]
     , d_crm_opportunity.[ltf_lead_type_name]
     , d_crm_opportunity.[ltf_line_of_business]
     , d_crm_opportunity.[ltf_line_of_business_name]
     , d_crm_opportunity.[modified_on]
     , d_crm_opportunity.[owner_id_type]
     , d_crm_opportunity.[club_id]
     , d_crm_opportunity.[created_by_employee_id]
     , d_crm_opportunity.[owning_employee_id]
     , d_crm_opportunity.[local_created_on]
     , d_crm_opportunity.[local_modified_on]
     , d_crm_opportunity.[inserted_date_time]
     , d_crm_opportunity.[updated_date_time]
     , d_crm_opportunity.[bk_hash]
     , d_crm_opportunity.[p_crmcloudsync_opportunity_id]
     , d_crm_opportunity.[dv_load_date_time]
     , d_crm_opportunity.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (batch_info.[l_hash] + batch_info.[s_hash])),2)
  FROM ( SELECT d_crm_ltf_campaign_instance.[campaign_id]
              , d_crm_contact.[contact_id]
              , [created_by_business_unit_id] = d_crm_system_user_created_by.[business_unit_id]
              , [created_by_system_user_id]   = d_crm_system_user_created_by.[system_user_id]
              , d_crm_lead.[lead_id]
              , d_crm_ltf_campaign_instance.[ltf_campaign_instance_id]
              , d_crm_ltf_club.[ltf_club_id]
              , [ltf_guest_visit_id] = d_crm_ltf_guest_visit.[activity_id]
              , [ltf_referring_member_id] = CAST(CASE WHEN (NOT d_crm_opportunity.[ltf_referring_member_id] Is Null
                                                            AND ISNUMERIC(d_crm_opportunity.[ltf_referring_member_id]) = 1
                                                            AND CHARINDEX('.', d_crm_opportunity.[ltf_referring_member_id]) = 0
                                                            AND CHARINDEX('\\', d_crm_opportunity.[ltf_referring_member_id]) = 0
                                                            AND CONVERT(bigint, d_crm_opportunity.[ltf_referring_member_id]) <= 2147483647)
                                                      THEN d_crm_opportunity.[ltf_referring_member_id]
                                                      ELSE Null END AS int)
              , d_crm_opportunity.[opportunity_id]
              , [owning_business_unit_id]           = d_crm_opportunity.[owning_business_unit]
              , [owning_system_user_id]             = d_crm_system_user_owning.[system_user_id]
              , [dim_crm_campaign_key]              = d_crm_ltf_campaign_instance.[dim_crm_campaign_key]
              , [dim_crm_contact_key]               = d_crm_contact.[dim_crm_contact_key]
              , [dim_crm_lead_key]                  = d_crm_lead.[dim_crm_lead_key]
              , [dim_crm_ltf_campaign_instance_key] = d_crm_ltf_campaign_instance.[dim_crm_ltf_campaign_instance_key]
              , [dim_crm_ltf_club_key]              = d_crm_ltf_club.[dim_crm_ltf_club_key]
              , [dim_crm_ltf_guest_visit_key]       = d_crm_lead.[ltf_originating_guest_visit_dim_crm_ltf_guest_visit_key]
              , [dim_crm_opportunity_key]           = d_crm_opportunity.[dim_crm_opportunity_key]
              , [activity_type_code]      = NullIf(d_crm_ltf_guest_visit.[activity_type_code],'')
              , [activity_type_code_name] = NullIf(d_crm_ltf_guest_visit.[activity_type_code_name],'')
              , d_crm_opportunity.[created_on]
              , [email_address_1]   = NullIf(NullIf(d_crm_contact.[email_address_1],''),'********@*******.***')
              , [first_name]        = NullIf(d_crm_contact.[first_name],'')
              , [last_name]         = NullIf(d_crm_contact.[last_name],'')
              , [ltf_campaign_name] = NullIf(d_crm_ltf_campaign_instance.[ltf_campaign_name],'')
              , d_crm_opportunity.[ltf_channel]
              , d_crm_opportunity.[ltf_channel_name]
              , d_crm_contact.[ltf_do_not_email_address_1]
              , d_crm_ltf_guest_visit.[ltf_guest_type]
              , [ltf_guest_type_name] = NullIf(d_crm_ltf_guest_visit.[ltf_guest_type_name],'')
              , d_crm_opportunity.[ltf_lead_source]
              , [ltf_lead_source_name] = CASE WHEN (NOT d_crm_opportunity.[ltf_lead_source_name] Is Null AND d_crm_opportunity.[ltf_lead_source_name] = 'OMS chat') THEN 'Web Chat' ELSE NullIf(d_crm_opportunity.[ltf_lead_source_name],'') END --Converting legacy to current for historical reporting consistancies.
              , d_crm_opportunity.[ltf_lead_type]
              , [ltf_lead_type_name] = NullIf(d_crm_opportunity.[ltf_lead_type_name],'')
              , d_crm_opportunity.[ltf_line_of_business]
              , d_crm_opportunity.[ltf_line_of_business_name]
              , d_crm_opportunity.[modified_on]
              , d_crm_opportunity.[owner_id_type]
              , d_crm_ltf_club.[club_id]
              , [created_by_employee_id] = CASE WHEN (ISNUMERIC(d_crm_system_user_created_by.[employee_id]) = 1 AND CONVERT(bigint, d_crm_system_user_created_by.[employee_id]) <= 2147483647) THEN CONVERT(int, d_crm_system_user_created_by.[employee_id]) ELSE Null END
              , [owning_employee_id]     = CASE WHEN (ISNUMERIC(d_crm_system_user_owning.[employee_id]) = 1 AND CONVERT(bigint, d_crm_system_user_owning.[employee_id]) <= 2147483647) THEN CONVERT(int, d_crm_system_user_owning.[employee_id]) ELSE Null END
              , [local_created_on]       = DATEADD(hh, -(map_tz_created.[offset]), d_crm_opportunity.[created_on])
              , [local_modified_on]      = DATEADD(hh, -(map_tz_modified.[offset]), d_crm_opportunity.[modified_on])
              , d_crm_opportunity.[inserted_date_time]
              , d_crm_opportunity.[updated_date_time]
              , d_crm_opportunity.[bk_hash]
              , d_crm_opportunity.[p_crmcloudsync_opportunity_id]
              , d_crm_opportunity.[dv_load_date_time]
              , d_crm_opportunity.[dv_batch_id]
           FROM [dbo].[d_crmcloudsync_opportunity] d_crm_opportunity
                CROSS APPLY ( SELECT [dim_crm_owner_key] = CASE WHEN d_crm_opportunity.[owner_id_type] = 'systemuser' THEN d_crm_opportunity.[dim_crm_owner_key] ELSE d_crm_opportunity.[owning_user_dim_crm_system_user_key] END ) d_crm_opportunity_apply
                INNER JOIN [dbo].[d_crmcloudsync_ltf_club] d_crm_ltf_club
                  ON d_crm_ltf_club.[dim_crm_ltf_club_key] = d_crm_opportunity.[dim_crm_ltf_club_key]
                INNER JOIN [dbo].[dim_club] dim_club
                  ON dim_club.[dim_club_key] = d_crm_ltf_club.[dim_club_key]
                INNER JOIN [dbo].[d_crmcloudsync_system_user] d_crm_system_user_created_by
                  ON d_crm_system_user_created_by.[dim_crm_system_user_key] = d_crm_opportunity.[created_by_dim_crm_system_user_key]
                INNER JOIN [dbo].[d_crmcloudsync_system_user] d_crm_system_user_owning
                  ON d_crm_system_user_owning.[dim_crm_system_user_key] = d_crm_opportunity_apply.[dim_crm_owner_key]
                INNER JOIN [dbo].[d_crmcloudsync_lead] d_crm_lead
                  ON d_crm_lead.[dim_crm_lead_key] = d_crm_opportunity.[originating_lead_dim_crm_lead_key]
                INNER JOIN [dbo].[d_crmcloudsync_contact] d_crm_contact
                  ON d_crm_contact.[dim_crm_contact_key] = d_crm_opportunity.[parent_contact_dim_crm_contact_key]
                CROSS APPLY
                  ( SELECT DISTINCT d_crm_ltf_guest_visit.[bk_hash]
                                  , d_crm_ltf_guest_visit.[dim_crm_ltf_guest_visit_key]
                                  , d_crm_ltf_guest_visit.[activity_id]
                                  , d_crm_ltf_guest_visit.[dim_crm_ltf_campaign_instance_key]
                                  , d_crm_ltf_guest_visit.[activity_type_code]
                                  , d_crm_ltf_guest_visit.[activity_type_code_name]
                                  , d_crm_ltf_guest_visit.[ltf_guest_type]
                                  , d_crm_ltf_guest_visit.[ltf_guest_type_name]
                      FROM [dbo].[d_crmcloudsync_ltf_guest_visit] d_crm_ltf_guest_visit
                      WHERE d_crm_ltf_guest_visit.[dim_crm_ltf_guest_visit_key] = d_crm_lead.[ltf_originating_guest_visit_dim_crm_ltf_guest_visit_key]
                  ) d_crm_ltf_guest_visit
                LEFT OUTER JOIN
                  ( SELECT d_crm_ltf_campaign_instance.[ltf_campaign_instance_id]
                         , d_crm_ltf_campaign_instance.[dim_crm_ltf_campaign_instance_key]
                         , d_crm_ltf_campaign_instance.[ltf_campaign_name]
                         , d_crm_campaign.[campaign_id]
                         , d_crm_campaign.[dim_crm_campaign_key]
                      FROM [dbo].[d_crmcloudsync_ltf_campaign_instance] d_crm_ltf_campaign_instance
                           INNER JOIN [dbo].[d_crmcloudsync_campaign] d_crm_campaign
                             ON d_crm_campaign.[dim_crm_campaign_key] = d_crm_ltf_campaign_instance.[ltf_campaign_dim_crm_campaign_key]
                  ) d_crm_ltf_campaign_instance
                  ON d_crm_ltf_campaign_instance.[dim_crm_ltf_campaign_instance_key] = d_crm_ltf_guest_visit.[dim_crm_ltf_campaign_instance_key]
                LEFT OUTER JOIN [dbo].[map_utc_time_zone_conversion] map_tz_created
                  ON map_tz_created.[val_time_zone_id] = dim_club.[val_time_zone_id]
                     AND (NOT d_crm_opportunity.[created_on] Is Null AND (d_crm_opportunity.[created_on] >= map_tz_created.[utc_start_date_time] AND d_crm_opportunity.[created_on] < map_tz_created.[utc_end_date_time]))
                LEFT OUTER JOIN [dbo].[map_utc_time_zone_conversion] map_tz_modified
                  ON map_tz_modified.[val_time_zone_id] = dim_club.[val_time_zone_id]
                     AND (NOT d_crm_opportunity.[modified_on] Is Null AND (d_crm_opportunity.[modified_on] >= map_tz_modified.[utc_start_date_time] AND d_crm_opportunity.[modified_on] < map_tz_modified.[utc_end_date_time]))
             WHERE ( ( d_crm_opportunity.[bk_hash] IN ('-999','-998','-997') )
                  OR ( d_crm_opportunity.[inserted_date_time] >= DATEADD(YY, -2, DATEADD(YY, DATEDIFF(YY, 0, DATEADD(DD, -1, DATEADD(DD, DATEDIFF(DD, 0, GETDATE()), 0))), 0))
                       AND ( d_crm_opportunity.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
                         AND d_crm_opportunity.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END ) ) )
       ) d_crm_opportunity
       CROSS APPLY
         ( SELECT [l_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar(36), d_crm_opportunity.[campaign_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar(36), d_crm_opportunity.[contact_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar(36), d_crm_opportunity.[created_by_business_unit_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar(36), d_crm_opportunity.[created_by_system_user_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar(36), d_crm_opportunity.[lead_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar(36), d_crm_opportunity.[ltf_campaign_instance_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar(36), d_crm_opportunity.[ltf_club_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar(36), d_crm_opportunity.[ltf_guest_visit_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar(36), d_crm_opportunity.[opportunity_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar(36), d_crm_opportunity.[owning_business_unit_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar(36), d_crm_opportunity.[owning_system_user_id]),'z#@$k%&P'))),2)

                , [s_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar(36), d_crm_opportunity.[opportunity_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_opportunity.[activity_type_code]),'z#@$k%&P')
                                                                  --+ 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_opportunity.[activity_type_code_name]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_opportunity.[created_on], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_opportunity.[email_address_1]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_opportunity.[first_name]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_opportunity.[last_name]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_opportunity.[ltf_campaign_name]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_opportunity.[ltf_do_not_email_address_1]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_opportunity.[ltf_guest_type]),'z#@$k%&P')
                                                                  --+ 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_opportunity.[ltf_guest_type_name]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_opportunity.[ltf_lead_source]),'z#@$k%&P')
                                                                  --+ 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_opportunity.[ltf_lead_source_name]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_opportunity.[ltf_lead_type]),'z#@$k%&P')
                                                                  --+ 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_opportunity.[ltf_lead_type_name]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_opportunity.[modified_on], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_opportunity.[owner_id_type]),'z#@$k%&P'))),2)
         ) batch_info
ORDER BY d_crm_opportunity.[dv_batch_id] ASC, d_crm_opportunity.[dv_load_date_time] ASC, ISNULL(d_crm_opportunity.[modified_on],d_crm_opportunity.[created_on]) ASC;

END
