CREATE PROC [sandbox].[proc_mart_sw_d_mms_reimbursement_program] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT PIT.[reimbursement_program_id]
     , LNK.[company_id]
     , LNK.[val_reimbursement_program_processing_type_id]
     , LNK.[val_reimbursement_program_type_id]
     , SAT.[reimbursement_program_name]
     , SAT.[active_flag]
     , SAT.[inserted_date_time]
     , SAT.[updated_date_time]
     , SAT.[dues_subsidy_amount]
     , val_reimbursement_program_processing_type_description = VRPPT.description
     , val_reimbursement_program_type_description = VRPT.description
     , PIT.[bk_hash]
     , PIT.[p_mms_reimbursement_program_id]
     , PIT.[dv_load_date_time]
     , PIT.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (LNK.[dv_hash] + SAT.[dv_hash])),2)
     --, [l_hash] = LNK.[dv_hash]
     --, [s_hash] = SAT.[dv_hash]
  FROM [dbo].[p_mms_reimbursement_program] PIT
       INNER JOIN [dbo].[l_mms_reimbursement_program] LNK
         ON LNK.[bk_hash] = PIT.[bk_hash]
            AND LNK.[l_mms_reimbursement_program_id] = PIT.[l_mms_reimbursement_program_id]
       INNER JOIN [dbo].[s_mms_reimbursement_program] SAT
         ON SAT.[bk_hash] = PIT.[bk_hash]
            AND SAT.[s_mms_reimbursement_program_id] = PIT.[s_mms_reimbursement_program_id]
       INNER JOIN
         ( SELECT PIT.[p_mms_reimbursement_program_id]
                , RowRank = RANK() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
                , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
             FROM [dbo].[p_mms_reimbursement_program] PIT
             WHERE ( PIT.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
                 AND PIT.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END )
         ) PITU
         ON PITU.[p_mms_reimbursement_program_id] = PIT.[p_mms_reimbursement_program_id]
            AND PITU.RowRank = 1 AND PITU.RowNumber = 1
       LEFT OUTER JOIN [dbo].[r_mms_val_reimbursement_program_processing_type] VRPPT
         ON VRPPT.[dv_load_end_date_time] = '9999-12-31 00:00:00.000'
            AND VRPPT.[val_reimbursement_program_processing_type_id] = LNK.[val_reimbursement_program_processing_type_id]
       LEFT OUTER JOIN [dbo].[r_mms_val_reimbursement_program_type] VRPT
         ON VRPT.[dv_load_end_date_time] = '9999-12-31 00:00:00.000'
            AND VRPT.[val_reimbursement_program_type_id] = LNK.[val_reimbursement_program_type_id]
  WHERE NOT PIT.[reimbursement_program_id] Is Null
ORDER BY PIT.[dv_batch_id] ASC, PIT.[dv_load_date_time] ASC, ISNULL(SAT.[updated_date_time], SAT.[inserted_date_time]) ASC;

END
