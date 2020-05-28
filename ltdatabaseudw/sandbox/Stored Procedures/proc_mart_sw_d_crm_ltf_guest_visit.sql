﻿CREATE PROC [sandbox].[proc_mart_sw_d_crm_ltf_guest_visit] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT d_crm_ltf_guest_visit.[campaign_id]
     , d_crm_ltf_guest_visit.[contact_id]
     , d_crm_ltf_guest_visit.[created_by_business_unit_id]
     , d_crm_ltf_guest_visit.[created_by_system_user_id]
     , d_crm_ltf_guest_visit.[lead_id]
     , d_crm_ltf_guest_visit.[ltf_campaign_instance_id]
     , d_crm_ltf_guest_visit.[ltf_club_id]
     , d_crm_ltf_guest_visit.[ltf_guest_visit_id]
     , d_crm_ltf_guest_visit.[opportunity_id]
     , d_crm_ltf_guest_visit.[owning_system_user_id]
     , d_crm_ltf_guest_visit.[owning_business_unit_id]
     , d_crm_ltf_guest_visit.[dim_crm_campaign_key]
     , d_crm_ltf_guest_visit.[dim_crm_contact_key]
     , d_crm_ltf_guest_visit.[dim_crm_lead_key]
     , d_crm_ltf_guest_visit.[dim_crm_ltf_campaign_instance_key]
     , d_crm_ltf_guest_visit.[dim_crm_ltf_club_key]
     , d_crm_ltf_guest_visit.[dim_crm_ltf_guest_visit_key]
     , d_crm_ltf_guest_visit.[dim_crm_opportunity_key]
     , d_crm_ltf_guest_visit.[activity_type_code]
     , d_crm_ltf_guest_visit.[activity_type_code_name]
     , d_crm_ltf_guest_visit.[created_on]
     , d_crm_ltf_guest_visit.[email_address_1]
     , d_crm_ltf_guest_visit.[first_name]
     , d_crm_ltf_guest_visit.[last_name]
     , d_crm_ltf_guest_visit.[ltf_campaign_name]
     , d_crm_ltf_guest_visit.[ltf_do_not_email_address_1]
     , d_crm_ltf_guest_visit.[ltf_guest_type]
     , d_crm_ltf_guest_visit.[ltf_guest_type_name]
     , d_crm_ltf_guest_visit.[ltf_lead_source]
     , d_crm_ltf_guest_visit.[ltf_lead_source_name]
     , d_crm_ltf_guest_visit.[ltf_lead_type]
     , d_crm_ltf_guest_visit.[ltf_lead_type_name]
     , d_crm_ltf_guest_visit.[ltf_line_of_business]
     , d_crm_ltf_guest_visit.[ltf_line_of_business_name]
     , d_crm_ltf_guest_visit.[modified_on]
     , d_crm_ltf_guest_visit.[owner_id_type]
     --, d_crm_ltf_guest_visit.[regarding_object_type_code]
     , d_crm_ltf_guest_visit.[club_id]
     , d_crm_ltf_guest_visit.[created_by_employee_id]
     , d_crm_ltf_guest_visit.[ltf_referring_employee_id]
     , d_crm_ltf_guest_visit.[ltf_referring_member_id]
     , d_crm_ltf_guest_visit.[owning_employee_id]
     , d_crm_ltf_guest_visit.[local_created_on]
     , d_crm_ltf_guest_visit.[local_modified_on]
     , d_crm_ltf_guest_visit.[inserted_date_time]
     , d_crm_ltf_guest_visit.[updated_date_time]
     , d_crm_ltf_guest_visit.[bk_hash]
     , d_crm_ltf_guest_visit.[p_crmcloudsync_ltf_guest_visit_id]
     , d_crm_ltf_guest_visit.[dv_load_date_time]
     , d_crm_ltf_guest_visit.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (batch_info.[l_hash] + batch_info.[s_hash])),2)
  FROM ( SELECT d_crm_ltf_campaign_instance.[campaign_id]
              , d_crm_contact.[contact_id]
              , [created_by_business_unit_id] = d_crm_system_user_created_by.[business_unit_id]
              , [created_by_system_user_id]   = d_crm_system_user_created_by.[system_user_id]
              , d_crm_lead.[lead_id]
              , d_crm_ltf_campaign_instance.[ltf_campaign_instance_id]
              , d_crm_ltf_club.[ltf_club_id]
              , [ltf_guest_visit_id] = d_crm_ltf_guest_visit.[activity_id]
              , d_crm_lead.[opportunity_id]
              , d_crm_lead.[owning_business_unit_id]
              , d_crm_lead.[owning_system_user_id]
              , [dim_crm_campaign_key]              = d_crm_ltf_campaign_instance.[dim_crm_campaign_key]
              , [dim_crm_contact_key]               = d_crm_contact.[dim_crm_contact_key]
              , [dim_crm_lead_key]                  = d_crm_lead.[dim_crm_lead_key]
              , [dim_crm_ltf_campaign_instance_key] = d_crm_ltf_campaign_instance.[dim_crm_ltf_campaign_instance_key]
              , [dim_crm_ltf_club_key]              = d_crm_ltf_club.[dim_crm_ltf_club_key]
              , [dim_crm_ltf_guest_visit_key]       = d_crm_ltf_guest_visit.[dim_crm_ltf_guest_visit_key]
              , [dim_crm_opportunity_key]           = d_crm_lead.[dim_crm_opportunity_key]
              , d_crm_ltf_guest_visit.[activity_type_code]
              , d_crm_ltf_guest_visit.[activity_type_code_name]
              , d_crm_ltf_guest_visit.[created_on]
              , [email_address_1]   = NullIf(NullIf(d_crm_ltf_guest_visit.[ltf_email_address_1],''),'********@*******.***')
              , [first_name]        = NullIf(d_crm_ltf_guest_visit.[ltf_first_name],'')
              , [last_name]         = NullIf(d_crm_ltf_guest_visit.[ltf_last_name],'')
              , [ltf_campaign_name] = NullIf(d_crm_ltf_campaign_instance.[ltf_campaign_name],'')
              , d_crm_contact.[ltf_do_not_email_address_1]
              , d_crm_ltf_guest_visit.[ltf_guest_type]
              , d_crm_ltf_guest_visit.[ltf_guest_type_name]
              , d_crm_lead.[ltf_lead_source]
              , [ltf_lead_source_name] = CASE WHEN (NOT d_crm_lead.[ltf_lead_source_name] Is Null AND d_crm_lead.[ltf_lead_source_name] = 'OMS chat') THEN 'Web Chat' ELSE NullIf(d_crm_lead.[ltf_lead_source_name],'') END --Converting legacy to current for historical reporting consistancies.
              , d_crm_lead.[ltf_lead_type]
              , d_crm_lead.[ltf_lead_type_name]
              , d_crm_ltf_guest_visit.[ltf_line_of_business]
              , d_crm_ltf_guest_visit.[ltf_line_of_business_name]
              , d_crm_ltf_guest_visit.[modified_on]
              , d_crm_lead.[owner_id_type]
              --, d_crm_ltf_guest_visit.[regarding_object_type_code]
              , d_crm_ltf_club.[club_id]
              , [created_by_employee_id] = CASE WHEN (ISNUMERIC(d_crm_system_user_created_by.[employee_id]) = 1 AND CONVERT(bigint, d_crm_system_user_created_by.[employee_id]) <= 2147483647) THEN CONVERT(int, d_crm_system_user_created_by.[employee_id]) ELSE Null END
              --, [ltf_referred_by_employee_id] = CASE WHEN (ISNUMERIC(d_crm_system_user_referred_by.[employee_id]) = 1 AND CONVERT(bigint, d_crm_system_user_referred_by.[employee_id]) <= 2147483647) THEN CONVERT(int, d_crm_system_user_referred_by.[employee_id]) ELSE Null END
              , [ltf_referring_employee_id] = d_mms_employee_referred_by.[employee_id]
              , [ltf_referring_member_id]   = d_mms_member.[member_id]
              , d_crm_lead.[owning_employee_id]
              , [local_created_on]  = DATEADD(hh, -(map_tz_created.[offset]), d_crm_ltf_guest_visit.[created_on])
              , [local_modified_on] = DATEADD(hh, -(map_tz_modified.[offset]), d_crm_ltf_guest_visit.[modified_on])
              , d_crm_ltf_guest_visit.[inserted_date_time]
              , d_crm_ltf_guest_visit.[updated_date_time]
              , d_crm_ltf_guest_visit.[bk_hash]
              , d_crm_ltf_guest_visit.[p_crmcloudsync_ltf_guest_visit_id]
              , d_crm_ltf_guest_visit.[dv_load_date_time]
              , d_crm_ltf_guest_visit.[dv_batch_id]
              , RowRank = RANK() OVER (PARTITION BY d_crm_ltf_guest_visit.[dim_crm_ltf_guest_visit_key] ORDER BY d_crm_ltf_guest_visit.[dv_batch_id] DESC, d_crm_ltf_guest_visit.[inserted_date_time] ASC)
              , RowNumber = ROW_NUMBER() OVER (PARTITION BY d_crm_ltf_guest_visit.[dim_crm_ltf_guest_visit_key] ORDER BY d_crm_ltf_guest_visit.[dv_batch_id] DESC, d_crm_ltf_guest_visit.[inserted_date_time] ASC)
           FROM [dbo].[d_crmcloudsync_ltf_guest_visit] d_crm_ltf_guest_visit
                INNER JOIN [dbo].[d_crmcloudsync_ltf_club] d_crm_ltf_club
                  ON d_crm_ltf_club.[dim_crm_ltf_club_key] = d_crm_ltf_guest_visit.[dim_crm_ltf_club_key]
                INNER JOIN [dbo].[dim_club] dim_club
                  ON dim_club.[dim_club_key] = d_crm_ltf_club.[dim_club_key]
                INNER JOIN [dbo].[d_crmcloudsync_system_user] d_crm_system_user_created_by
                  ON d_crm_system_user_created_by.[dim_crm_system_user_key] = d_crm_ltf_guest_visit.[created_by_dim_crm_system_user_key]
                INNER JOIN [dbo].[d_crmcloudsync_contact] d_crm_contact
                  ON d_crm_contact.[dim_crm_contact_key] = d_crm_ltf_guest_visit.[regarding_object_dim_crm_system_user_key]
                INNER JOIN [dbo].[d_mms_member] d_mms_member
                  ON d_mms_member.[dim_mms_member_key] = d_crm_ltf_guest_visit.[referring_dim_mms_member_key]
                LEFT OUTER JOIN
                  ( SELECT d_crm_lead.[lead_id]
                         , d_crm_lead.[dim_crm_lead_key]
                         , d_crm_lead.[ltf_originating_guest_visit_dim_crm_ltf_guest_visit_key]
                         , d_crm_lead.[qualifying_opportunity_dim_crm_opportunity_key]
                         , d_crm_lead.[ltf_lead_source]
                         , d_crm_lead.[ltf_lead_source_name]
                         , d_crm_lead.[ltf_lead_type]
                         , d_crm_lead.[ltf_lead_type_name]
                         , RowRank = RANK() OVER (PARTITION BY d_crm_lead.[ltf_originating_guest_visit_dim_crm_ltf_guest_visit_key] ORDER BY d_crm_lead.[dv_batch_id] DESC, d_crm_lead.[inserted_date_time] ASC)
                         , RowNumber = ROW_NUMBER() OVER (PARTITION BY d_crm_lead.[ltf_originating_guest_visit_dim_crm_ltf_guest_visit_key] ORDER BY d_crm_lead.[dv_batch_id] DESC, d_crm_lead.[inserted_date_time] ASC)
                         , d_crm_opportunity.[opportunity_id]
                         , d_crm_opportunity.[dim_crm_opportunity_key]
                         , d_crm_opportunity.[originating_lead_dim_crm_lead_key]
                         , d_crm_opportunity_apply.[dim_crm_owner_key]
                         , [owner_id_type] = CASE WHEN d_crm_opportunity.[opportunity_id] Is Null THEN d_crm_lead.[owner_id_type] ELSE d_crm_opportunity.[owner_id_type] END
                         , [owning_business_unit_id] = CASE WHEN d_crm_opportunity.[opportunity_id] Is Null THEN d_crm_lead.[owning_business_unit] ELSE d_crm_opportunity.[owning_business_unit] END
                         , [owning_system_user_id] = CASE WHEN d_crm_opportunity.[opportunity_id] Is Null THEN d_crm_system_user_owning_lead.[system_user_id] ELSE d_crm_system_user_owning_opportunity.[system_user_id] END
                         , [owning_employee_id] = CASE WHEN d_crm_opportunity.[opportunity_id] Is Null
                                                       THEN CASE WHEN (ISNUMERIC(d_crm_system_user_owning_lead.[employee_id]) = 1 AND CONVERT(bigint, d_crm_system_user_owning_lead.[employee_id]) <= 2147483647) THEN CONVERT(int, d_crm_system_user_owning_lead.[employee_id]) ELSE Null END
                                                       ELSE CASE WHEN (ISNUMERIC(d_crm_system_user_owning_opportunity.[employee_id]) = 1 AND CONVERT(bigint, d_crm_system_user_owning_opportunity.[employee_id]) <= 2147483647) THEN CONVERT(int, d_crm_system_user_owning_opportunity.[employee_id]) ELSE Null END
                                                  END
                      FROM [dbo].[d_crmcloudsync_lead] d_crm_lead
                           CROSS APPLY ( SELECT [dim_crm_owner_key] = CASE WHEN d_crm_lead.[owner_id_type] = 'systemuser' THEN d_crm_lead.[dim_crm_owner_key] ELSE d_crm_lead.[owning_user_dim_crm_system_user_key] END ) d_crm_lead_apply
                           INNER JOIN [dbo].[d_crmcloudsync_system_user] d_crm_system_user_owning_lead
                             ON d_crm_system_user_owning_lead.[dim_crm_system_user_key] = d_crm_lead_apply.[dim_crm_owner_key]
                           INNER JOIN [dbo].[d_crmcloudsync_opportunity] d_crm_opportunity
                             ON d_crm_opportunity.[originating_lead_dim_crm_lead_key] = d_crm_lead.[dim_crm_lead_key]
                           CROSS APPLY ( SELECT [dim_crm_owner_key] = CASE WHEN d_crm_opportunity.[owner_id_type] = 'systemuser' THEN d_crm_opportunity.[dim_crm_owner_key] ELSE d_crm_opportunity.[owning_user_dim_crm_system_user_key] END ) d_crm_opportunity_apply
                           INNER JOIN [dbo].[d_crmcloudsync_system_user] d_crm_system_user_owning_opportunity
                             ON d_crm_system_user_owning_opportunity.[dim_crm_system_user_key] = d_crm_opportunity_apply.[dim_crm_owner_key]
                      WHERE d_crm_lead.[ltf_originating_guest_visit_dim_crm_ltf_guest_visit_key] <> '-998'
                  ) d_crm_lead
                  ON d_crm_lead.[ltf_originating_guest_visit_dim_crm_ltf_guest_visit_key] = d_crm_ltf_guest_visit.[dim_crm_ltf_guest_visit_key]
                     AND d_crm_lead.RowRank = 1 AND d_crm_lead.RowNumber = 1
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
                     AND (NOT d_crm_ltf_guest_visit.[created_on] Is Null AND (d_crm_ltf_guest_visit.[created_on] >= map_tz_created.[utc_start_date_time] AND d_crm_ltf_guest_visit.[created_on] < map_tz_created.[utc_end_date_time]))
                LEFT OUTER JOIN [dbo].[map_utc_time_zone_conversion] map_tz_modified
                  ON map_tz_modified.[val_time_zone_id] = dim_club.[val_time_zone_id]
                     AND (NOT d_crm_ltf_guest_visit.[modified_on] Is Null AND (d_crm_ltf_guest_visit.[modified_on] >= map_tz_modified.[utc_start_date_time] AND d_crm_ltf_guest_visit.[modified_on] < map_tz_modified.[utc_end_date_time]))
                LEFT OUTER JOIN [dbo].[d_mms_employee_history] d_mms_employee_referred_by
                  ON d_mms_employee_referred_by.[member_id] = d_mms_member.[member_id]
                     AND DATEADD(hh, -(map_tz_created.[offset]), d_crm_ltf_guest_visit.[created_on]) >= d_mms_employee_referred_by.[effective_date_time]
                     AND DATEADD(hh, -(map_tz_created.[offset]), d_crm_ltf_guest_visit.[created_on]) < d_mms_employee_referred_by.[expiration_date_time]
             WHERE ( ( d_crm_ltf_guest_visit.[bk_hash] IN ('-999','-998','-997') )
                  OR ( d_crm_ltf_guest_visit.[regarding_object_type_code] = 'contact'
                       AND d_crm_ltf_guest_visit.[inserted_date_time] >= DATEADD(YY, -2, DATEADD(YY, DATEDIFF(YY, 0, DATEADD(DD, -1, DATEADD(DD, DATEDIFF(DD, 0, GETDATE()), 0))), 0))
                       AND ( d_crm_ltf_guest_visit.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
                         AND d_crm_ltf_guest_visit.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END ) ) )
       ) d_crm_ltf_guest_visit
       CROSS APPLY
         ( SELECT [l_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar(36), d_crm_ltf_guest_visit.[campaign_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar(36), d_crm_ltf_guest_visit.[contact_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar(36), d_crm_ltf_guest_visit.[created_by_business_unit_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar(36), d_crm_ltf_guest_visit.[created_by_system_user_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar(36), d_crm_ltf_guest_visit.[lead_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar(36), d_crm_ltf_guest_visit.[ltf_campaign_instance_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar(36), d_crm_ltf_guest_visit.[ltf_club_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar(36), d_crm_ltf_guest_visit.[ltf_guest_visit_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar(36), d_crm_ltf_guest_visit.[opportunity_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar(36), d_crm_ltf_guest_visit.[owning_business_unit_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar(36), d_crm_ltf_guest_visit.[owning_system_user_id]),'z#@$k%&P'))),2)

                , [s_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar(36), d_crm_ltf_guest_visit.[ltf_guest_visit_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_ltf_guest_visit.[activity_type_code]),'z#@$k%&P')
                                                                  --+ 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_ltf_guest_visit.[activity_type_code_name]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_ltf_guest_visit.[created_on], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_ltf_guest_visit.[email_address_1]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_ltf_guest_visit.[first_name]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_ltf_guest_visit.[last_name]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_ltf_guest_visit.[ltf_campaign_name]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_ltf_guest_visit.[ltf_do_not_email_address_1]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_ltf_guest_visit.[ltf_guest_type]),'z#@$k%&P')
                                                                  --+ 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_ltf_guest_visit.[ltf_guest_type_name]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_ltf_guest_visit.[ltf_lead_source]),'z#@$k%&P')
                                                                  --+ 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_ltf_guest_visit.[ltf_lead_source_name]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_ltf_guest_visit.[ltf_lead_type]),'z#@$k%&P')
                                                                  --+ 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_ltf_guest_visit.[ltf_lead_type_name]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_ltf_guest_visit.[modified_on], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_ltf_guest_visit.[owner_id_type]),'z#@$k%&P'))),2)
         ) batch_info
  WHERE d_crm_ltf_guest_visit.RowRank = 1
    AND d_crm_ltf_guest_visit.RowNumber = 1
ORDER BY d_crm_ltf_guest_visit.[dv_batch_id] ASC, d_crm_ltf_guest_visit.[dv_load_date_time] ASC, ISNULL(d_crm_ltf_guest_visit.[modified_on],d_crm_ltf_guest_visit.[created_on]) ASC;

END
