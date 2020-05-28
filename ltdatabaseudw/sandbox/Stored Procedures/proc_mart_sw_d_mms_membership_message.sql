CREATE PROC [sandbox].[proc_mart_sw_d_mms_membership_message] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT PIT.[membership_message_id]
     , LNK.[membership_id]
     , LNK.[open_employee_id]
     , LNK.[close_employee_id]
     , LNK.[val_membership_message_type_id]
     , LNK.[val_message_status_id]
     , LNK.[open_club_id]
     , LNK.[close_club_id]
     , SAT.[open_date_time]
     , SAT.[close_date_time]
     , SAT.[received_date_time]
     , SAT.[comment]
     , SAT.[utc_open_date_time]
     , SAT.[open_date_time_zone]
     , SAT.[utc_close_date_time]
     , SAT.[close_date_time_zone]
     , SAT.[utc_received_date_time]
     , SAT.[received_date_time_zone]
     , SAT.[inserted_date_time]
     , SAT.[updated_date_time]
     , PIT.[bk_hash]
     , PIT.[p_mms_membership_message_id]
     , PIT.[dv_load_date_time]
     , PIT.[dv_batch_id]
     , [dv_hash] = CONVERT(char(32), HASHBYTES('MD5', (LNK.[dv_hash] + SAT.[dv_hash])),2)
     --, [l_hash] = LNK.[dv_hash]
     --, [s_hash] = SAT.[dv_hash]
  FROM [dbo].[p_mms_membership_message] PIT
       INNER JOIN [dbo].[l_mms_membership_message] LNK
         ON LNK.[bk_hash] = PIT.[bk_hash]
            AND LNK.[l_mms_membership_message_id] = PIT.[l_mms_membership_message_id]
       INNER JOIN[dbo].[s_mms_membership_message] SAT
         ON SAT.[bk_hash] = PIT.[bk_hash]
            AND SAT.[s_mms_membership_message_id] = PIT.[s_mms_membership_message_id]
       INNER JOIN
         ( SELECT PIT.[p_mms_membership_message_id]
                , RowRank = RANK() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
                , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
             FROM [dbo].[p_mms_membership_message] PIT
             WHERE ( PIT.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
                 AND PIT.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END )
         ) PITU
         ON PITU.[p_mms_membership_message_id] = PIT.[p_mms_membership_message_id]
            AND PITU.RowRank = 1 AND PITU.RowNumber = 1
  WHERE NOT PIT.[membership_message_id] Is Null
    AND NOT LNK.[membership_id] Is Null
    AND LNK.[val_membership_message_type_id] IN (64,65,82,107,169,177,184,203)
ORDER BY PIT.[dv_batch_id] ASC, PIT.[dv_load_date_time] ASC, ISNULL(SAT.[updated_date_time], SAT.[inserted_date_time]) ASC;

END
