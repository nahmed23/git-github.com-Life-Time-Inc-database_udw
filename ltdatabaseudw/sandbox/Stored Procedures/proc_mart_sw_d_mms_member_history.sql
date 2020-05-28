CREATE PROC [sandbox].[proc_mart_sw_d_mms_member_history] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT d_mms_member_history.[member_id]
     , d_mms_member_history.[membership_id]
     , d_mms_member_history.[val_member_type_id]
     , d_member_info.[active_flag]
     , d_member_info.[assess_jr_member_dues_flag]
     , d_member_info.[dob]
     , d_mms_member_history.[join_date]
     , d_mms_member_history.[effective_date_time]
     , d_mms_member_history.[bk_hash]
     , d_mms_member_history.[p_mms_member_id]
     , d_mms_member_history.[dv_load_date_time]
     , d_mms_member_history.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (batch_info.[l_hash] + batch_info.[s_hash])),2)
  FROM [dbo].[d_mms_member_history] d_mms_member_history
       INNER JOIN
         ( SELECT PIT.[d_mms_member_history_id]
                , PIT.[p_mms_member_id]
                , PIT.[effective_date_time]
                , RowRank = RANK() OVER (PARTITION BY PIT.[bk_hash], DATEADD(DD, DATEDIFF(DD, 0, PIT.[effective_date_time]), 0) ORDER BY PIT.[effective_date_time] DESC)
                , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[bk_hash], DATEADD(DD, DATEDIFF(DD, 0, PIT.[effective_date_time]), 0) ORDER BY PIT.[effective_date_time] DESC)
             FROM [dbo].[d_mms_member_history] PIT
             WHERE ( PIT.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
                 AND PIT.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END )
         ) PITU
         ON PITU.[d_mms_member_history_id] = d_mms_member_history.[d_mms_member_history_id]
            AND PITU.RowRank = 1 AND PITU.RowNumber = 1
       CROSS APPLY
         ( SELECT [active_flag]                = CONVERT(bit, CASE WHEN ISNULL(d_mms_member_history.[member_active_flag],'Y') = 'Y' THEN 1 ELSE 0 END)
                , [assess_jr_member_dues_flag] = CONVERT(bit, CASE WHEN ISNULL(d_mms_member_history.[assess_junior_member_dues_flag],'Y') = 'Y' THEN 1 ELSE 0 END)
                , [dob] = d_mms_member_history.[date_of_birth]
         ) d_member_info
       CROSS APPLY
         ( SELECT [l_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_member_history.[member_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_member_history.[membership_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_member_history.[val_member_type_id]),'z#@$k%&P'))),2)

                , [s_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_member_history.[member_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + CONVERT(varchar, ISNULL(d_member_info.[active_flag],0))
                                                               + 'P%#&z$@k' + CONVERT(varchar, ISNULL(d_member_info.[assess_jr_member_dues_flag],1))
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_member_history.[date_of_birth], 120),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_member_history.[join_date], 120),'z#@$k%&P'))),2)
         ) batch_info
  WHERE NOT d_mms_member_history.[member_id] Is Null
    --AND ( d_mms_member_history.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
    --  AND d_mms_member_history.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END )
ORDER BY d_mms_member_history.[dv_batch_id] ASC, d_mms_member_history.[effective_date_time] ASC;

END
