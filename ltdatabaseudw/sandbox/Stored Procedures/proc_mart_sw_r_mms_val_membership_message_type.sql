CREATE PROC [sandbox].[proc_mart_sw_r_mms_val_membership_message_type] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT [val_membership_message_type_id]
     , [description]
     , [sort_order]
     , [auto_close_flag]
     , [val_message_severity_id]
     , [inserted_date_time]
     , [abbreviation]
     , [eft_single_open_flag]
     , [updated_date_time]
     , [bk_hash]
     , [r_mms_val_membership_message_type_id]
     , [dv_load_date_time]
     , [dv_batch_id]
     , [dv_hash]
  FROM [dbo].[r_mms_val_membership_message_type]
  WHERE [dv_load_end_date_time] = '9999-12-31 00:00:00.000'
    AND NOT [val_membership_message_type_id] Is Null
    AND [dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
    AND [dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END
ORDER BY [dv_batch_id] ASC, ISNULL([updated_date_time], [inserted_date_time]) ASC;

END
