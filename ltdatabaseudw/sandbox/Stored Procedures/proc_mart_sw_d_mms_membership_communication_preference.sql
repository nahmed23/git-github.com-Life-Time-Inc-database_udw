CREATE PROC [sandbox].[proc_mart_sw_d_mms_membership_communication_preference] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT [membership_communication_preference_id]
     , [membership_id]
     , [val_communication_preference_id]
     , [active_flag] = CONVERT(bit, CASE WHEN ISNULL([active_flag],'Y') = 'Y' THEN 1 ELSE 0 END)
     , [inserted_date_time]
     , [updated_date_time]
     , [bk_hash]
     , [p_mms_membership_communication_preference_id]
     , [dv_load_date_time]
     , [dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (batch_info.[l_hash] + batch_info.[s_hash])),2)
     , [dv_deleted] = [deleted_flag]
  FROM [dbo].[d_mms_membership_communication_preference]
       CROSS APPLY
         ( SELECT [l_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, [membership_communication_preference_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, [membership_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, [val_communication_preference_id]),'z#@$k%&P'))),2)

                , [s_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, [membership_communication_preference_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, [active_flag]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, [inserted_date_time], 120),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, [updated_date_time], 120),'z#@$k%&P'))),2)
                --, [dv_load_date_time] = ISNULL([updated_date_time],[inserted_date_time])
                --, [dv_batch_id] = CONVERT(bigint, CONVERT(VARCHAR(8), ISNULL([updated_date_time],[inserted_date_time]), 112) + REPLACE(CONVERT(varchar(8), ISNULL([updated_date_time],[inserted_date_time]), 114), ':',''))
                --, [bk_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, [membership_communication_preference_id]),'z#@$k%&P'))),2)
         ) batch_info
  WHERE NOT [membership_communication_preference_id] Is Null
    AND ( [dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
      AND [dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END )
ORDER BY [dv_batch_id] ASC, [dv_load_date_time] ASC, ISNULL([updated_date_time],[inserted_date_time]) ASC;

END
