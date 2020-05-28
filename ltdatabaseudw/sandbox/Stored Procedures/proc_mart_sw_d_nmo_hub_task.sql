CREATE PROC [sandbox].[proc_mart_sw_d_nmo_hub_task] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT d_nmo_hub_task.[hub_task_id]
     , [hub_task_department_id] = ISNULL(d_nmo_hub_task_department.[hub_task_department_id],-998)
     , [hub_task_status_id]     = ISNULL(LNK_Task.[hub_task_status_id],-998)
     --, [hub_task_status_id]     = ISNULL(d_nmo_hub_task_status.[hub_task_status_id],-998)
     , [hub_task_type_id]       = ISNULL(d_nmo_hub_task_type.[hub_task_type_id],-998)
     --, [assignee_party_id] = d_nmo_hub_task.[assignee_Party_Id]
     , dim_club.[club_id]
     , d_mms_member.[member_id]
     --, [creator_party_id] = d_nmo_hub_task.[creator_Party_Id]
     , d_nmo_hub_task.[party_id]

     --, [assignee_name] = d_nmo_hub_task.[assignee_Name]
     --, [creator_name] = d_nmo_hub_task.[creator_name]

     , [hub_task_department_title] = d_nmo_hub_task_department.[title]
     , [hub_task_status_title]     = d_nmo_hub_task_status.[title]
     , [hub_task_type_title]       = d_nmo_hub_task_type.[title]

     , d_nmo_hub_task.[created_date]
     , d_nmo_hub_task.[due_date]
     , d_nmo_hub_task.[resolution_date]
     , d_nmo_hub_task.[updated_date]

     --, d_nmo_hub_task.[priority]

     , [dim_nmo_hub_task_department_key] = d_nmo_hub_task.[dim_nmo_hub_task_department_key]
     , [dim_nmo_hub_task_status_key]     = CASE WHEN NOT LNK_Task.[hub_task_status_id] Is Null THEN CONVERT(char(32), HASHBYTES('MD5', ('P%#&z$@k' + CONVERT(varchar, LNK_Task.[hub_task_status_id]))),2) ELSE CONVERT(char(32),'-998',2) END
     --, [dim_nmo_hub_task_status_key]     = d_nmo_hub_task.[dim_nmo_hub_task_status_key]
     , [dim_nmo_hub_task_type_key]       = d_nmo_hub_task.[dim_nmo_hub_task_type_key]
     , d_nmo_hub_task.[dim_club_key]
     , d_mms_member.[dim_mms_member_key]

     , d_nmo_hub_task.[bk_hash]
     , d_nmo_hub_task.[p_nmo_hub_task_id]
     , d_nmo_hub_task_department.[p_nmo_hub_task_department_id]
     , d_nmo_hub_task_status.[p_nmo_hub_task_status_id]
     , d_nmo_hub_task_type.[p_nmo_hub_task_type_id]
     , d_nmo_hub_task.[dv_load_date_time]
     , d_nmo_hub_task.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (batch_info.[l_hash] + batch_info.[s_hash])),2)
     --, d_nmo_hub_task.[deleted_flag]
  FROM [dbo].[d_nmo_hub_task]
       INNER JOIN
         ( SELECT PIT.[p_nmo_hub_task_id]
                , RowRank = RANK() OVER (PARTITION BY ltfeb_member.[dim_mms_member_key] ORDER BY PIT.[created_date] ASC)
                , RowNumber = ROW_NUMBER() OVER (PARTITION BY ltfeb_member.[dim_mms_member_key] ORDER BY PIT.[created_date] ASC)
             FROM [dbo].[d_nmo_hub_task] PIT
                  INNER JOIN [dbo].[map_ltfeb_party_id_dim_mms_member_key] ltfeb_party
                    ON ltfeb_party.[party_id] = PIT.[party_id]
                  INNER JOIN [dbo].[d_mms_member] ltfeb_member
                    ON ltfeb_member.[dim_mms_member_key] = ltfeb_party.[dim_mms_member_key]
             WHERE PIT.[created_date] >= ltfeb_member.[join_date]
               AND ( PIT.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
                 AND PIT.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END )
         ) PITU
         ON PITU.[p_nmo_hub_task_id] = d_nmo_hub_task.[p_nmo_hub_task_id]
            AND PITU.RowRank = 1 AND PITU.RowNumber = 1
       INNER JOIN [dbo].[p_nmo_hub_task] PIT_Task
         ON PIT_Task.[p_nmo_hub_task_id] = PITU.[p_nmo_hub_task_id]
       INNER JOIN [dbo].[l_nmo_hub_task] LNK_Task
         ON LNK_Task.[l_nmo_hub_task_id] = PIT_Task.l_nmo_hub_task_id
       INNER JOIN [dbo].[d_nmo_hub_task_department]
         ON d_nmo_hub_task_department.[bk_hash] = d_nmo_hub_task.[dim_nmo_hub_task_department_key]
       INNER JOIN [dbo].[d_nmo_hub_task_status]
         ON d_nmo_hub_task_status.[hub_task_status_id] = LNK_Task.[hub_task_status_id]
         --ON d_nmo_hub_task_status.[bk_hash] = d_nmo_hub_task.[dim_nmo_hub_task_status_key]
       INNER JOIN [dbo].[d_nmo_hub_task_type]
         ON d_nmo_hub_task_type.[bk_hash] = d_nmo_hub_task.[dim_nmo_hub_task_type_key]
       INNER JOIN [dbo].[dim_club]
         ON dim_club.[dim_club_key] = d_nmo_hub_task.[dim_club_key]
       INNER JOIN [dbo].[map_ltfeb_party_id_dim_mms_member_key]
         ON map_ltfeb_party_id_dim_mms_member_key.[party_id] = d_nmo_hub_task.[party_id]
       INNER JOIN [dbo].[d_mms_member]
         ON d_mms_member.[dim_mms_member_key] = map_ltfeb_party_id_dim_mms_member_key.[dim_mms_member_key]
       CROSS APPLY
         ( SELECT [l_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, d_nmo_hub_task.[hub_task_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_nmo_hub_task_department.[hub_task_department_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_nmo_hub_task_status.[hub_task_status_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_nmo_hub_task_type.[hub_task_type_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_nmo_hub_task.[party_id]),'z#@$k%&P'))),2)

                , [s_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, d_nmo_hub_task.[hub_task_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_nmo_hub_task_department.[title]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_nmo_hub_task_status.[title]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_nmo_hub_task_type.[title]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_nmo_hub_task.[created_date], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_nmo_hub_task.[due_date], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_nmo_hub_task.[resolution_date], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_nmo_hub_task.[updated_date], 120),'z#@$k%&P'))),2)
         ) batch_info
ORDER BY d_nmo_hub_task.[dv_batch_id] ASC, d_nmo_hub_task.[dv_load_date_time] ASC, ISNULL(d_nmo_hub_task.[updated_date], d_nmo_hub_task.[created_date]) ASC;

END
