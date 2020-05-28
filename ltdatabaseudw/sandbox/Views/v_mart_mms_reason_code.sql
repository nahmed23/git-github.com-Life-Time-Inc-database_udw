CREATE VIEW [sandbox].[v_mart_mms_reason_code]
AS SELECT PIT.[reason_code_id]
     , SAT.[description]
     , SAT.[name]
     , SAT.[sort_order]
     , SAT.[display_ui_flag]
     , SAT.[inserted_date_time]
     , SAT.[updated_date_time]
     , [dim_mms_reason_code_key] = PIT.[bk_hash]
     , PIT.[bk_hash]
     , PIT.[p_mms_reason_code_id]
     , PIT.[dv_load_date_time]
     , PIT.[dv_batch_id]
     , SAT.[dv_hash]
  FROM [dbo].[p_mms_reason_code] PIT
       INNER JOIN [dbo].[d_mms_reason_code] DIM
         ON DIM.[bk_hash] = PIT.[bk_hash]
            AND DIM.[p_mms_reason_code_id] = PIT.[p_mms_reason_code_id]
       INNER JOIN [dbo].[s_mms_reason_code] SAT
         ON SAT.[bk_hash] = PIT.[bk_hash]
            AND SAT.[s_mms_reason_code_id] = PIT.[s_mms_reason_code_id];