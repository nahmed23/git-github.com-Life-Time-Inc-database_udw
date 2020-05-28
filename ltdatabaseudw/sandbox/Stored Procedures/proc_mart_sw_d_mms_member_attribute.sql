CREATE PROC [sandbox].[proc_mart_sw_d_mms_member_attribute] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT d_mms_member_attribute.[member_attribute_id]
     , d_mms_member_attribute.[member_id]
     , d_mms_member_attribute.[val_member_attribute_type_id]
     , d_mms_member_attribute.[attribute_value]
     , d_mms_member_attribute.[effective_from_date_time]
     , d_mms_member_attribute.[effective_thru_date_time]
     , d_mms_member_attribute.[expiration_date]
     , d_mms_member_attribute.[inserted_date_time]
     , d_mms_member_attribute.[updated_date_time]
     , d_mms_member_attribute.[bk_hash]
     , d_mms_member_attribute.[p_mms_member_attribute_id]
     , d_mms_member_attribute.[dv_load_date_time]
     , d_mms_member_attribute.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (batch_info.[l_hash] + batch_info.[s_hash])),2)
     , [dv_deleted] = CAST(0 AS bit)
  FROM [dbo].[d_mms_member_attribute] d_mms_member_attribute
       INNER JOIN
         ( SELECT PIT.[bk_hash]
                , RowRank = RANK() OVER (PARTITION BY PIT.[member_id], PIT.[val_member_attribute_type_id], DATEADD(DD, DATEDIFF(DD, 0, PIT.[inserted_date_time]), 0) ORDER BY PIT.[effective_thru_date_time] DESC, PIT.[effective_from_date_time] ASC)
                , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[member_id], PIT.[val_member_attribute_type_id], DATEADD(DD, DATEDIFF(DD, 0, PIT.[inserted_date_time]), 0) ORDER BY PIT.[effective_thru_date_time] DESC, PIT.[effective_from_date_time] ASC)
             FROM [dbo].[d_mms_member_attribute] PIT
             WHERE ( PIT.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
                 AND PIT.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END )
         ) PITU
         ON PITU.[bk_hash] = d_mms_member_attribute.[bk_hash]
            AND PITU.RowRank = 1 AND PITU.RowNumber = 1
       CROSS APPLY
         ( SELECT [l_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_member_attribute.[member_attribute_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_member_attribute.[member_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_member_attribute.[val_member_attribute_type_id]),'z#@$k%&P'))),2)

                , [s_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_member_attribute.[member_attribute_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(d_mms_member_attribute.[attribute_value],'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_member_attribute.[effective_from_date_time], 120),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_member_attribute.[effective_thru_date_time], 120),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_member_attribute.[expiration_date], 120),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_member_attribute.[inserted_date_time], 120),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_member_attribute.[updated_date_time], 120),'z#@$k%&P'))),2)
         ) batch_info
  WHERE NOT d_mms_member_attribute.[member_attribute_id] Is Null
    AND NOT d_mms_member_attribute.[val_member_attribute_type_id] IN (1,3)
ORDER BY d_mms_member_attribute.[dv_batch_id] ASC, d_mms_member_attribute.[dv_load_date_time] ASC, ISNULL(d_mms_member_attribute.[updated_date_time], d_mms_member_attribute.[inserted_date_time]) ASC;

END
