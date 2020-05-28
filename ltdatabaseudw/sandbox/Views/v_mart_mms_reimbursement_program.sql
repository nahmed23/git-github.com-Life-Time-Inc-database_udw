CREATE VIEW [sandbox].[v_mart_mms_reimbursement_program]
AS SELECT PIT.[reimbursement_program_id]
     , LNK.[company_id]
     , LNK.[val_reimbursement_program_processing_type_id]
     , LNK.[val_reimbursement_program_type_id]
     , SAT.[active_flag]
     , SAT.[dues_subsidy_amount]
     , SAT.[reimbursement_program_name]
     , SAT.[inserted_date_time]
     , SAT.[updated_date_time]
     , [val_reimbursement_program_processing_type_description] = VRPPT.[description]
     , [val_reimbursement_program_type_description] = VRPT.[description]
     , [dim_mms_reimbursement_program_key] = PIT.[bk_hash]
     , [dim_mms_company_key] = CASE WHEN NOT LNK.[company_id] Is Null THEN CONVERT(char(32), HASHBYTES('MD5', ('P%#&z$@k' + CONVERT(varchar, LNK.[company_id]))),2) ELSE CONVERT(char(32),'-998',2) END
     , PIT.[bk_hash]
     , PIT.[p_mms_reimbursement_program_id]
     , PIT.[dv_load_date_time]
     , PIT.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (LNK.[dv_hash] + SAT.[dv_hash])),2)
  FROM [dbo].[p_mms_reimbursement_program] PIT
       INNER JOIN [dbo].[d_mms_reimbursement_program] DIM
         ON DIM.[bk_hash] = PIT.[bk_hash]
            AND DIM.[p_mms_reimbursement_program_id] = PIT.[p_mms_reimbursement_program_id]
       INNER JOIN [dbo].[l_mms_reimbursement_program] LNK
         ON LNK.[bk_hash] = PIT.[bk_hash]
            AND LNK.[l_mms_reimbursement_program_id] = PIT.[l_mms_reimbursement_program_id]
       INNER JOIN [dbo].[s_mms_reimbursement_program] SAT
         ON SAT.[bk_hash] = PIT.[bk_hash]
            AND SAT.[s_mms_reimbursement_program_id] = PIT.[s_mms_reimbursement_program_id]
       --INNER JOIN
       --  ( SELECT PIT.[p_mms_reimbursement_program_id]
       --         , RowRank = RANK() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
       --         , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
       --      FROM [dbo].[d_mms_reimbursement_program] PIT
       --  ) PITU
       --  ON PITU.[p_mms_reimbursement_program_id] = PIT.[p_mms_reimbursement_program_id]
       --     AND PITU.RowRank = 1 AND PITU.RowNumber = 1
       LEFT OUTER JOIN [dbo].[r_mms_val_reimbursement_program_processing_type] VRPPT
         ON VRPPT.[dv_load_end_date_time] = '9999-12-31 00:00:00.000'
            AND VRPPT.[val_reimbursement_program_processing_type_id] = LNK.[val_reimbursement_program_processing_type_id]
       LEFT OUTER JOIN [dbo].[r_mms_val_reimbursement_program_type] VRPT
         ON VRPT.[dv_load_end_date_time] = '9999-12-31 00:00:00.000'
            AND VRPT.[val_reimbursement_program_type_id] = LNK.[val_reimbursement_program_type_id];