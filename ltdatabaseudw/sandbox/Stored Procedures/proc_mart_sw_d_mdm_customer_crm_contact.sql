CREATE PROC [sandbox].[proc_mart_sw_d_mdm_customer_crm_contact] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT DIM.[dim_mdm_golden_record_customer_id_list_id]
     , DIM.[dim_mdm_golden_record_customer_id_list_key]
     , DIM.[entity_id]
     , [dim_mdm_entity_key] = CASE WHEN NOT DIM.[entity_id] Is Null THEN CONVERT(char(32), HASHBYTES('MD5', ('P%#&z$@k' + CONVERT(varchar, DIM.[entity_id]))),2) ELSE CONVERT(char(32),'-998',2) END
     , [contact_id] = CONVERT(varchar(36), DIM.[id])
     , [dim_crm_contact_key] = CASE WHEN NOT DIM.[id] Is Null THEN CONVERT(char(32), HASHBYTES('MD5', ('P%#&z$@k' + CONVERT(varchar(36), DIM.[id]))),2) ELSE CONVERT(char(32),'-998',2) END
     , DIM.[dv_load_date_time]
     , DIM.[dv_batch_id]
     , [dv_deleted] = CAST(0 AS bit)
  FROM [dbo].[dim_mdm_golden_record_customer_id_list] DIM
       INNER JOIN
         ( SELECT PIT.[dim_mdm_golden_record_customer_id_list_id]
                , RowRank = RANK() OVER (PARTITION BY PIT.[id] ORDER BY PIT.[dv_load_end_date_time] DESC, PIT.[entity_id] DESC)
                , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[id] ORDER BY PIT.[dv_load_end_date_time] DESC, PIT.[entity_id] DESC)
             FROM [dbo].[dim_mdm_golden_record_customer_id_list] PIT
             WHERE PIT.[id_type] = 4  --CRM Contact
               AND ( PIT.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
                 AND PIT.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END )
         ) PITU
         ON PITU.[dim_mdm_golden_record_customer_id_list_id] = DIM.[dim_mdm_golden_record_customer_id_list_id]
            AND PITU.RowRank = 1 AND PITU.RowNumber = 1
ORDER BY DIM.[dv_batch_id] ASC, DIM.[dv_load_date_time] ASC;

END
