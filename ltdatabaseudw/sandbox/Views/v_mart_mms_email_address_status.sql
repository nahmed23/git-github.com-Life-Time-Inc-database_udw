CREATE VIEW [sandbox].[v_mart_mms_email_address_status]
AS SELECT PIT.[email_address_status_id]
     , LNK.[val_communication_preference_source_id]
     , LNK.[val_communication_preference_status_id]
     , SAT.[email_address]
     , SAT.[status_from_date]
     , SAT.[status_thru_date]
     , SAT.[inserted_date_time]
     , SAT.[updated_date_time]
     , PIT.[bk_hash]
     , PIT.[p_mms_email_address_status_id]
     , PIT.[dv_load_date_time]
     , PIT.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (LNK.[dv_hash] + SAT.[dv_hash])),2)
     --, [l_hash] = LNK.[dv_hash]
     --, [s_hash] = SAT.[dv_hash]
  FROM [dbo].[p_mms_email_address_status] PIT
       INNER JOIN [dbo].[d_mms_email_address_status] DIM
         ON DIM.[bk_hash] = PIT.[bk_hash]
            AND DIM.[p_mms_email_address_status_id] = PIT.[p_mms_email_address_status_id]
       INNER JOIN [dbo].[l_mms_email_address_status] LNK
         ON LNK.[bk_hash] = PIT.[bk_hash]
            AND LNK.[l_mms_email_address_status_id] = PIT.[l_mms_email_address_status_id]
       INNER JOIN[dbo].[s_mms_email_address_status] SAT
         ON SAT.[bk_hash] = PIT.[bk_hash]
            AND SAT.[s_mms_email_address_status_id] = PIT.[s_mms_email_address_status_id];