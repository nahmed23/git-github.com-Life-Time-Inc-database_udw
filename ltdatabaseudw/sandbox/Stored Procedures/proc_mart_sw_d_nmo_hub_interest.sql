CREATE PROC [sandbox].[proc_mart_sw_d_nmo_hub_interest] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT [hub_task_interest_id] = CASE WHEN NOT d_nmo_hub_task_interest.[hub_task_interest_id] Is Null THEN d_nmo_hub_task_interest.[hub_task_interest_id] ELSE CONVERT(int, d_nmo_hub_task_interest.[bk_hash]) END
     , [hub_task_department_id] = CASE WHEN NOT d_nmo_hub_task_department.[hub_task_department_id] Is Null THEN d_nmo_hub_task_department.[hub_task_department_id] ELSE CONVERT(int, d_nmo_hub_task_interest.[d_nmo_hub_task_department_bk_hash]) END
     , interest_title = d_nmo_hub_task_interest.[title]
     , department_title = d_nmo_hub_task_department.[title]
     , d_nmo_hub_task_interest.[activation_date]
     , d_nmo_hub_task_interest.[created_date]
     , d_nmo_hub_task_interest.[expiration_date]
     , d_nmo_hub_task_interest.[updated_date]
     , [dim_nmo_hub_task_department_key] = d_nmo_hub_task_interest.[d_nmo_hub_task_department_bk_hash]
     , d_nmo_hub_task_interest.[bk_hash]
     , d_nmo_hub_task_interest.[p_nmo_hub_task_interest_id]
     , d_nmo_hub_task_department.[p_nmo_hub_task_department_id]
     , d_nmo_hub_task_interest.[dv_load_date_time]
     , d_nmo_hub_task_interest.[dv_batch_id]
     , [dv_hash] = CONVERT(char(32), HASHBYTES('MD5', (batch_info.[l_hash] + batch_info.[s_hash])),2)
     --, d_nmo_hub_task_interest.[deleted_flag]
  FROM [dbo].[d_nmo_hub_task_interest]
       INNER JOIN [dbo].[d_nmo_hub_task_department]
         ON d_nmo_hub_task_department.[bk_hash] = d_nmo_hub_task_interest.[d_nmo_hub_task_department_bk_hash]
       CROSS APPLY
         ( SELECT [l_hash] = CONVERT(char(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, d_nmo_hub_task_interest.[hub_task_interest_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_nmo_hub_task_department.[hub_task_department_id]),'z#@$k%&P'))),2)

                , [s_hash] = CONVERT(char(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, d_nmo_hub_task_interest.[hub_task_interest_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_nmo_hub_task_interest.[title]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_nmo_hub_task_department.[title]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_nmo_hub_task_interest.[activation_date], 120),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_nmo_hub_task_interest.[created_date], 120),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_nmo_hub_task_interest.[expiration_date], 120),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_nmo_hub_task_interest.[updated_date], 120),'z#@$k%&P'))),2)
         ) batch_info
  WHERE ( d_nmo_hub_task_interest.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
      AND d_nmo_hub_task_interest.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END );

END
