CREATE VIEW [sandbox].[v_mart_mms_member_reimbursement]
AS SELECT PIT.[member_reimbursement_id]
     , LNK.[reimbursement_program_id]
     , LNK.[member_id]
     , LNK.[val_reimbursement_termination_reason_id]
     , LNK.[reimbursement_program_identifier_format_id]
     , SAT.[enrollment_date]
     , SAT.[termination_date]
     , SAT.[inserted_date_time]
     , SAT.[updated_date_time]
     , DIM.[dim_mms_reimbursement_program_key]
     , DIM.[dim_mms_member_key]
     , PIT.[bk_hash]
     , PIT.[p_mms_member_reimbursement_id]
     , PIT.[dv_load_date_time]
     , PIT.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (LNK.[dv_hash] + SAT.[dv_hash])),2)
     --, [l_hash] = LNK.[dv_hash]
     --, [s_hash] = SAT.[dv_hash]
  FROM [dbo].[p_mms_member_reimbursement] PIT
       INNER JOIN [dbo].[d_mms_member_reimbursement] DIM
         ON DIM.[bk_hash] = PIT.[bk_hash]
            AND DIM.[p_mms_member_reimbursement_id] = PIT.[p_mms_member_reimbursement_id]
       INNER JOIN [dbo].[l_mms_member_reimbursement] LNK
         ON LNK.[bk_hash] = PIT.[bk_hash]
            AND LNK.[l_mms_member_reimbursement_id] = PIT.[l_mms_member_reimbursement_id]
       INNER JOIN[dbo].[s_mms_member_reimbursement] SAT
         ON SAT.[bk_hash] = PIT.[bk_hash]
            AND SAT.[s_mms_member_reimbursement_id] = PIT.[s_mms_member_reimbursement_id];