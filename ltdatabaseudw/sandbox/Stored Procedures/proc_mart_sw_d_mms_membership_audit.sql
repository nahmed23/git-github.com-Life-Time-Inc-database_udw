CREATE PROC [sandbox].[proc_mart_sw_d_mms_membership_audit] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT LNK.[membership_audit_id]
     , [membership_id] = LNK.[row_id]
     , SAT.[operation]
     , SAT.[modified_date_time]
     , SAT.[modified_user]
     , SAT.[column_name]
     , SAT.[old_value]
     , SAT.[new_value]
     , [modified_employee_id] = d_mms_employee.[employee_id]
     , [modified_employee_club_id] = d_mms_employee.[club_id]
     , PIT.[bk_hash]
     , PIT.[p_mms_membership_audit_id]
     , PIT.[dv_load_date_time]
     , PIT.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (LNK.[dv_hash] + SAT.[dv_hash])),2)
  FROM [dbo].[p_mms_membership_audit] PIT
       INNER JOIN [dbo].[l_mms_membership_audit] LNK
         ON LNK.[bk_hash] = PIT.[bk_hash]
            AND LNK.[l_mms_membership_audit_id] = PIT.[l_mms_membership_audit_id]
       INNER JOIN[dbo].[s_mms_membership_audit] SAT
         ON SAT.[bk_hash] = PIT.[bk_hash]
            AND SAT.[s_mms_membership_audit_id] = PIT.[s_mms_membership_audit_id]
       INNER JOIN
         ( SELECT PIT.[p_mms_membership_audit_id]
                , RowRank = RANK() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
                , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
             FROM [dbo].[p_mms_membership_audit] PIT
             WHERE ( PIT.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
                 AND PIT.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END )
         ) PITU
         ON PITU.[p_mms_membership_audit_id] = PIT.[p_mms_membership_audit_id]
            AND PITU.RowRank = 1 AND PITU.RowNumber = 1

       OUTER APPLY
         ( SELECT TOP 1
                  d_mms_employee_history.[employee_id]
                , d_mms_club.[club_id]
             FROM [dbo].[d_mms_employee_history]
                  CROSS APPLY
                    ( SELECT [employee_id] = CAST(CASE WHEN (ISNUMERIC(SAT.[modified_user]) = 1 AND CONVERT(bigint, SAT.[modified_user]) <= 2147483647) THEN CONVERT(int, SAT.[modified_user]) ELSE 0 END AS int) ) SAT_Convert
                  INNER JOIN [dbo].[d_mms_club]
                    ON d_mms_employee_history.[dim_club_key] = d_mms_club.[dim_club_key]
             WHERE d_mms_employee_history.[employee_id] = SAT_Convert.[employee_id]
               AND SAT.[modified_date_time] >= d_mms_employee_history.[effective_date_time]
               AND SAT.[modified_date_time] < d_mms_employee_history.[expiration_date_time]
           ORDER BY d_mms_employee_history.[expiration_date_time] DESC
         ) d_mms_employee
  WHERE NOT LNK.[membership_audit_id] Is Null
    AND SAT.[new_value] = '4'
    AND SAT.[old_value] IN ('1','2')
    AND SAT.[column_name] = 'ValMembershipStatusID'
    AND SAT.[operation] = 'Update'
ORDER BY PIT.[dv_batch_id] ASC, PIT.[dv_load_date_time] ASC, SAT.[modified_date_time] ASC;

END
