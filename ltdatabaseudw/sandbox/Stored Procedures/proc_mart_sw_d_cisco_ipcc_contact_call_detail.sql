CREATE PROC [sandbox].[proc_mart_sw_d_cisco_ipcc_contact_call_detail] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT HUB.[session_id]
     , LNK.[application_task_id]
     , LNK.[application_id]
     , SAT.[originator_dn]
     , SAT.[destination_dn]
     , SAT.[start_date_time]
     , SAT.[end_date_time]
     , SAT.[called_number]
     , SAT.[orig_called_number]
     , SAT.[application_name]
     , SAT.[connect_time]
     , SAT.[custom_variable_1]
     , SAT.[custom_variable_2]
     , SAT.[custom_variable_3]
     , SAT.[custom_variable_4]
     , SAT.[custom_variable_6]
     , SAT.[custom_variable_9]
     , SAT.[custom_variable_10]
     , SAT.[redirect]
     , SAT.[flow_out]
     , [club_id] = CASE WHEN (NOT d_mms_club.club_id Is Null AND NOT NullIf(SAT.[custom_variable_10],'') Is Null) THEN d_mms_club.club_id ELSE -998 END
     , [local_start_date_time] = DATEADD(hh, -(ISNULL(map_tz_start_date_time.[offset],0)), SAT.[start_date_time])
     , [local_end_date_time]   = DATEADD(hh, -(ISNULL(map_tz_end_date_time.[offset],0)), SAT.[end_date_time])
     , HUB.[bk_hash]
     , PIT.[p_ciscoautoattendant_ipcc_contact_call_detail_id]
     , PIT.[dv_load_date_time]
     , PIT.[dv_batch_id]
     , HUB.[dv_deleted]
  FROM [dbo].[h_ciscoautoattendant_ipcc_contact_call_detail] HUB
       INNER JOIN [dbo].[p_ciscoautoattendant_ipcc_contact_call_detail] PIT
         ON PIT.[bk_hash] = HUB.[bk_hash]
       INNER JOIN [dbo].[l_ciscoautoattendant_ipcc_contact_call_detail] LNK
         ON LNK.[bk_hash] = PIT.[bk_hash]
            AND LNK.[l_ciscoautoattendant_ipcc_contact_call_detail_id] = PIT.[l_ciscoautoattendant_ipcc_contact_call_detail_id]
       INNER JOIN [dbo].[s_ciscoautoattendant_ipcc_contact_call_detail] SAT
         ON SAT.[bk_hash] = PIT.[bk_hash]
            AND SAT.[s_ciscoautoattendant_ipcc_contact_call_detail_id] = PIT.[s_ciscoautoattendant_ipcc_contact_call_detail_id]
       LEFT OUTER JOIN [dbo].[d_mms_club] d_mms_club
         ON ( (NOT NullIf(SAT.[custom_variable_10],'') Is Null AND d_mms_club.[domain_name_prefix] = SAT.[custom_variable_10])
           OR (NullIf(SAT.[custom_variable_10],'') Is Null AND d_mms_club.[club_id] = 13) )
       LEFT OUTER JOIN [dbo].[map_utc_time_zone_conversion] map_tz_start_date_time
         ON map_tz_start_date_time.[val_time_zone_id] = d_mms_club.[val_time_zone_id]
            AND (NOT SAT.[start_date_time] Is Null AND (SAT.[start_date_time] >= map_tz_start_date_time.[utc_start_date_time] AND SAT.[start_date_time] < map_tz_start_date_time.[utc_end_date_time]))
       LEFT OUTER JOIN [dbo].[map_utc_time_zone_conversion] map_tz_end_date_time
         ON map_tz_end_date_time.[val_time_zone_id] = d_mms_club.[val_time_zone_id]
            AND (NOT SAT.[end_date_time] Is Null AND (SAT.[end_date_time] >= map_tz_end_date_time.[utc_start_date_time] AND SAT.[end_date_time] < map_tz_end_date_time.[utc_end_date_time]))
  WHERE (NOT SAT.[start_date_time] Is Null AND SAT.[start_date_time] >= DATEADD(YY, -2, DATEADD(YY, DATEDIFF(YY, 0, DATEADD(DD, -1, DATEADD(DD, DATEDIFF(DD, 0, GETDATE()), 0))), 0)))
    AND ( PIT.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
      AND PIT.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END )
    AND (
--Club AA Calls--
        (LNK.[application_id] = 27  --FITNESS_CENTER_AA
         AND NOT NullIf(SAT.[custom_variable_10],'') Is Null
         AND SAT.[custom_variable_3] = 'MM-NM-NewMemberInquiry')
--Club AA Calls--
--External/Corp AA Calls--
     OR (LNK.[application_id] = 7  --MKT_CALLCENTER
         AND (NullIf(SAT.[custom_variable_9],'') Is Null OR NOT SAT.[custom_variable_9] LIKE 'Campaign%'))
--External/Corp AA Calls--
      )
ORDER BY PIT.[dv_batch_id] ASC, PIT.[dv_load_date_time] ASC;

END
