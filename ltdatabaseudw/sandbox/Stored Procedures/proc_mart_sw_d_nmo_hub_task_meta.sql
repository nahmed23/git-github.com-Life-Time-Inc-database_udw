﻿CREATE PROC [sandbox].[proc_mart_sw_d_nmo_hub_task_meta] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT d_nmo_hub_task_meta.[hub_task_meta_id]
     , d_nmo_hub_task.[hub_task_id]
     , [hub_task_department_id] = CASE WHEN NOT d_nmo_hub_task_department.[hub_task_department_id] Is Null THEN d_nmo_hub_task_department.[hub_task_department_id] ELSE CONVERT(int, d_nmo_hub_task_interest.[d_nmo_hub_task_department_bk_hash]) END
     , [hub_task_interest_id] = CASE WHEN NOT d_nmo_hub_task_interest.[hub_task_interest_id] Is Null THEN d_nmo_hub_task_interest.[hub_task_interest_id] ELSE CONVERT(int, d_nmo_hub_task_interest.[bk_hash]) END
     , dim_club.[club_id]
     , d_mms_member.[member_id]
     , d_nmo_hub_task.[party_id]
     
     , interest_title = d_nmo_hub_task_interest.[title]
     , department_title = d_nmo_hub_task_department.[title]
     
     , d_nmo_hub_task_meta.[created_date]
     , d_nmo_hub_task_meta.[meta_description]
     , d_nmo_hub_task_meta.[meta_key]
     , d_nmo_hub_task_meta.[updated_date]

     , [dim_nmo_hub_task_key] = d_nmo_hub_task_meta.[d_nmo_hub_task_bk_hash]
     , [dim_nmo_hub_task_department_key] = d_nmo_hub_task_interest.[bk_hash]
     , [dim_nmo_hub_task_interest_key] = d_nmo_hub_task_interest.[d_nmo_hub_task_department_bk_hash]
     , d_nmo_hub_task.[dim_club_key]
     , d_mms_member.[dim_mms_member_key]

     , d_nmo_hub_task_meta.[bk_hash]
     , d_nmo_hub_task_meta.[p_nmo_hub_task_meta_id]
     , d_nmo_hub_task_meta.[dv_load_date_time]
     , d_nmo_hub_task_meta.[dv_load_end_date_time]
     , d_nmo_hub_task_meta.[dv_batch_id]
     --, d_nmo_hub_task_meta.[deleted_flag]
  FROM [dbo].[d_nmo_hub_task_meta]
       INNER JOIN [dbo].[d_nmo_hub_task]
         ON d_nmo_hub_task.[bk_hash] = d_nmo_hub_task_meta.[d_nmo_hub_task_bk_hash]
       INNER JOIN [dbo].[d_nmo_hub_task_interest]
         ON d_nmo_hub_task_interest.[title] = d_nmo_hub_task_meta.[meta_description]
            AND d_nmo_hub_task_meta.[meta_key] = 'interest'
       INNER JOIN [dbo].[d_nmo_hub_task_department]
         ON d_nmo_hub_task_department.[bk_hash] = d_nmo_hub_task_interest.[d_nmo_hub_task_department_bk_hash]
       INNER JOIN [dbo].[dim_club]
         ON dim_club.[dim_club_key] = d_nmo_hub_task.[dim_club_key]
       INNER JOIN [dbo].[map_ltfeb_party_id_dim_mms_member_key]
         ON map_ltfeb_party_id_dim_mms_member_key.[party_id] = d_nmo_hub_task.[party_id]
       INNER JOIN [dbo].[d_mms_member]
         ON d_mms_member.[dim_mms_member_key] = map_ltfeb_party_id_dim_mms_member_key.[dim_mms_member_key]
       --CROSS APPLY
       --  ( SELECT [l_hash] = CONVERT(char(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, d_nmo_hub_task.[hub_task_id]),'z#@$k%&P')
       --                                                        + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_nmo_hub_task_department.[hub_task_department_id]),'z#@$k%&P')
       --                                                        + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_nmo_hub_task_status.[hub_task_status_id]),'z#@$k%&P')
       --                                                        + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_nmo_hub_task_type.[id]),'z#@$k%&P')
       --                                                        + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_nmo_hub_task.[party_id]),'z#@$k%&P'))),2)

       --         , [s_hash] = CONVERT(char(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, d_nmo_hub_task.[hub_task_id]),'z#@$k%&P')
       --                                                        + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_nmo_hub_task_department.[title]),'z#@$k%&P')
       --                                                        + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_nmo_hub_task_status.[title]),'z#@$k%&P')
       --                                                        + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_nmo_hub_task_type.[title]),'z#@$k%&P')
       --                                                        + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_nmo_hub_task.[created_date], 120),'z#@$k%&P')
       --                                                        + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_nmo_hub_task.[due_date], 120),'z#@$k%&P')
       --                                                        + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_nmo_hub_task.[resolution_date], 120),'z#@$k%&P')
       --                                                        + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_nmo_hub_task.[updated_date], 120),'z#@$k%&P'))),2)
       --  ) batch_info
  WHERE ( d_nmo_hub_task_meta.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
      AND d_nmo_hub_task_meta.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END );

END
