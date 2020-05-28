CREATE VIEW [sandbox].[v_mart_mms_member]
AS SELECT DIM.[member_id]
     , DIM.[membership_id]
     , LNK.[employer_id]
     , DIM.[val_member_type_id]
     , SAT.[first_name]
     , SAT.[middle_name]
     , SAT.[last_name]
     , DIM.[email_address]
     , DIM.[join_date]
     , SAT.[dob]
     , SAT.[active_flag]
     , SAT.[assess_jr_member_dues_flag]
     , SAT.[gender]
     , LNK.[crm_contact_id]
     , LNK.[party_id]
     , LNK.[last_updated_employee_id]
     , SAT.[inserted_date_time]
     , SAT.[updated_date_time]
     , [dim_mms_member_key] = DIM.[bk_hash]
     , DIM.[dim_mms_membership_key] --= CASE WHEN NOT DIM.[membership_id] Is Null THEN CONVERT(char(32), HASHBYTES('MD5', ('P%#&z$@k' + CONVERT(varchar, DIM.[membership_id]))),2) ELSE CONVERT(char(32),'-998',2) END
     , DIM.[bk_hash]
     , DIM.[p_mms_member_id]
     , DIM.[dv_load_date_time]
     , DIM.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (LNK.[dv_hash] + SAT.[dv_hash])),2)
     , [age]                  = FLOOR(DATEDIFF(DAY, [dob], GETDATE()) / 365.25)
     , [birthday]             = DATEADD(YY, FLOOR(DATEDIFF(DAY, [dob], DATEADD(MM, 1, DATEADD(MM, DATEDIFF(MM, 0, GETDATE()), 0))) / 365.25), [dob])
     , [member_name]          = ISNULL(DIM.[first_name], '') + ' ' + ISNULL(DIM.[last_name], '')
     , [member_l_name_f_name] = ISNULL(DIM.[last_name], '') + ', ' + ISNULL(DIM.[first_name], '')
  FROM [dbo].[p_mms_member] PIT
       INNER JOIN [dbo].[d_mms_member] DIM
         ON DIM.[bk_hash] = PIT.[bk_hash]
            AND DIM.[p_mms_member_id] = PIT.[p_mms_member_id]
       INNER JOIN [dbo].[l_mms_member] LNK
         ON LNK.[bk_hash] = PIT.[bk_hash]
            AND LNK.[l_mms_member_id] = PIT.[l_mms_member_id]
       INNER JOIN[dbo].[s_mms_member] SAT
         ON SAT.[bk_hash] = PIT.[bk_hash]
            AND SAT.[s_mms_member_id] = PIT.[s_mms_member_id];