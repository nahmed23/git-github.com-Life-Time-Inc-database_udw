CREATE PROC [sandbox].[proc_mart_sw_d_mms_member] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT PIT.[member_id]
     , LNK.[membership_id]
     , LNK.[employer_id]
     , LNK.[val_member_type_id]
     , SAT.[first_name]
     , SAT.[middle_name]
     , SAT.[last_name]
     , SAT.[email_address]
     , SAT.[join_date]
     , SAT.[dob]
     , SAT.[active_flag]
     , SAT.[assess_jr_member_dues_flag]
     , SAT.[gender]
     , LNK.[crm_contact_id]
     , LNK.[party_id]
     , LNK.[last_updated_employee_id]
     , SAT.[inserted_date_time]
     , SAT.[updated_date_time]
     , PIT.[bk_hash]
     , PIT.[p_mms_member_id]
     , PIT.[dv_load_date_time]
     , PIT.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (LNK.[dv_hash] + SAT.[dv_hash])),2)
     --, LNK.[val_name_prefix_id]
     --, LNK.[val_name_suffix_id]
     --, LNK.[credit_card_account_id]
     --, LNK.[siebel_row_id]
     --, LNK.[salesforce_prospect_id]
     --, LNK.[salesforce_contact_id]
     --, SAT.[has_message_flag]
     --, SAT.[comment]
     --, SAT.[charge_to_account_flag]
     --, SAT.[cw_medica_number]
     --, SAT.[cw_enrollment_date]
     --, SAT.[cw_program_enrolled_flag]
     --, SAT.[mip_updated_date_time]
     --, SAT.[photo_delete_date_time]
     --, SAT.[member_token]
     --, [l_hash] = LNK.[dv_hash]
     --, [s_hash] = SAT.[dv_hash]
  FROM [dbo].[p_mms_member] PIT
       INNER JOIN [dbo].[l_mms_member] LNK
         ON LNK.[bk_hash] = PIT.[bk_hash]
            AND LNK.[l_mms_member_id] = PIT.[l_mms_member_id]
       INNER JOIN[dbo].[s_mms_member] SAT
         ON SAT.[bk_hash] = PIT.[bk_hash]
            AND SAT.[s_mms_member_id] = PIT.[s_mms_member_id]
       INNER JOIN
         ( SELECT PIT.[p_mms_member_id]
                , RowRank = RANK() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
                , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
             FROM [dbo].[p_mms_member] PIT
             WHERE ( PIT.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
                 AND PIT.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END )
         ) PITU
         ON PITU.[p_mms_member_id] = PIT.[p_mms_member_id]
            AND PITU.RowRank = 1 AND PITU.RowNumber = 1
  WHERE NOT PIT.[member_id] Is Null
ORDER BY PIT.[dv_batch_id] ASC, PIT.[dv_load_date_time] ASC, ISNULL(SAT.[updated_date_time], SAT.[inserted_date_time]) ASC;

END
