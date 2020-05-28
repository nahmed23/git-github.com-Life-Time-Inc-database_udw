CREATE PROC [sandbox].[proc_mart_sw_d_crm_annotation] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT d_crm_annotation.[annotation_id]
     , d_crm_annotation.[object_id]
     , d_crm_annotation.[created_on]
     , d_crm_annotation.[file_name]
     , d_crm_annotation.[file_size]
     , d_crm_annotation.[is_document]
     , d_crm_annotation.[is_document_name]
     , d_crm_annotation.[mime_type]
     , d_crm_annotation.[modified_on]
     , d_crm_annotation.[note_text]
     , d_crm_annotation.[object_id_type_code]
     , d_crm_annotation.[subject]
     , d_crm_annotation.[inserted_date_time]
     , d_crm_annotation.[updated_date_time]
     , d_crm_annotation.[bk_hash]
     , d_crm_annotation.[p_crmcloudsync_annotation_id]
     , d_crm_annotation.[dv_load_date_time]
     , d_crm_annotation.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (batch_info.[l_hash] + batch_info.[s_hash])),2)
  FROM ( SELECT d_crm_annotation.[annotation_id]
              , d_crm_annotation.[object_id]
              , d_crm_annotation.[created_on]
              , [file_name] = NullIf(d_crm_annotation.[file_name],'')
              , [file_size] = NullIf(d_crm_annotation.[file_size],0)
              , d_crm_annotation.[is_document]
              , d_crm_annotation.[is_document_name]
              , [mime_type] = NullIf(d_crm_annotation.[mime_type],'')
              , d_crm_annotation.[modified_on]
              , [note_text] = NullIf(d_crm_annotation.[note_text],'')
              , d_crm_annotation.[object_id_type_code]
              , [subject] = NullIf(d_crm_annotation.[subject],'')
              , [inserted_date_time] = d_crm_annotation.[dv_inserted_date_time]
              , [updated_date_time] = d_crm_annotation.[dv_updated_date_time]
              , d_crm_annotation.[bk_hash]
              , d_crm_annotation.[p_crmcloudsync_annotation_id]
              , d_crm_annotation.[dv_load_date_time]
              , d_crm_annotation.[dv_batch_id]
           FROM [dbo].[d_crmcloudsync_annotation] d_crm_annotation
           WHERE (d_crm_annotation.[is_document] Is Null OR d_crm_annotation.[is_document] = 0)
             AND d_crm_annotation.[created_on] >= DATEADD(YY, -2, DATEADD(YY, DATEDIFF(YY, 0, DATEADD(DD, -1, DATEADD(DD, DATEDIFF(DD, 0, GETDATE()), 0))), 0))
             AND ( d_crm_annotation.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
               AND d_crm_annotation.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END )
       ) d_crm_annotation
       CROSS APPLY
         ( SELECT [l_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar(36), d_crm_annotation.[annotation_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar(36), d_crm_annotation.[object_id]),'z#@$k%&P'))),2)

                , [s_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar(36), d_crm_annotation.[annotation_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_annotation.[created_on], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_annotation.[file_name]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_annotation.[file_size]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_annotation.[is_document]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_annotation.[is_document_name]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_annotation.[mime_type]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_annotation.[modified_on], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_annotation.[object_id_type_code]),'z#@$k%&P'))),2)
         ) batch_info
ORDER BY d_crm_annotation.[dv_batch_id] ASC, d_crm_annotation.[dv_load_date_time] ASC, ISNULL(d_crm_annotation.[modified_on],d_crm_annotation.[created_on]) ASC;

END
