CREATE VIEW [sandbox].[v_mart_mms_employee]
AS SELECT PIT.[employee_id]
     , LNK.[club_id]
     , LNK.[member_id]
     , SAT.[active_status_flag]
     , SAT.[first_name]
     , SAT.[last_name]
     , SAT.[middle_int]
     , SAT.[hire_date]
     , SAT.[termination_date]
     , SAT.[inserted_date_time]
     , SAT.[updated_date_time]
     , [dim_mms_employee_key] = PIT.[bk_hash]
     , DIM.[dim_club_key] --= CASE WHEN NOT LNK.[club_id] Is Null THEN CONVERT(char(32), HASHBYTES('MD5', ('P%#&z$@k' + CONVERT(varchar, LNK.[club_id]))),2) ELSE CONVERT(char(32),'-998',2) END
     , [dim_mms_member_key] = CASE WHEN NOT LNK.[member_id] Is Null THEN CONVERT(char(32), HASHBYTES('MD5', ('P%#&z$@k' + CONVERT(varchar, LNK.[member_id]))),2) ELSE CONVERT(char(32),'-998',2) END
     , PIT.[bk_hash]
     , PIT.[p_mms_employee_id]
     , PIT.[dv_load_date_time]
     , PIT.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (LNK.[dv_hash] + SAT.[dv_hash])),2)
     , [employee_name]          = SAT.[first_name] + ' ' + SAT.[last_name]
     , [employee_l_name_f_name] = ISNULL(SAT.[last_name], '') + ', ' + ISNULL(SAT.[first_name], '')
  FROM [dbo].[p_mms_employee] PIT
       INNER JOIN [dbo].[d_mms_employee] DIM
         ON DIM.[bk_hash] = PIT.[bk_hash]
            AND DIM.[p_mms_employee_id] = PIT.[p_mms_employee_id]
       INNER JOIN [dbo].[l_mms_employee] LNK
         ON LNK.[bk_hash] = PIT.[bk_hash]
            AND LNK.[l_mms_employee_id] = PIT.[l_mms_employee_id]
       INNER JOIN[dbo].[s_mms_employee] SAT
         ON SAT.[bk_hash] = PIT.[bk_hash]
            AND SAT.[s_mms_employee_id] = PIT.[s_mms_employee_id];