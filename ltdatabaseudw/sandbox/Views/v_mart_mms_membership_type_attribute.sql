CREATE VIEW [sandbox].[v_mart_mms_membership_type_attribute]
AS SELECT PIT.[membership_type_attribute_id]
     , LNK.[membership_type_id]
     , LNK.[val_membership_type_attribute_id]
     , SAT.[inserted_date_time]
     , SAT.[updated_date_time]
     , DIM.[dim_mms_membership_type_key]
     , PIT.[bk_hash]
     , PIT.[p_mms_membership_type_attribute_id]
     , PIT.[dv_load_date_time]
     , PIT.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (LNK.[dv_hash] + SAT.[dv_hash])),2)
  FROM [dbo].[p_mms_membership_type_attribute] PIT
       INNER JOIN [dbo].[d_mms_membership_type_attribute] DIM
         ON DIM.[bk_hash] = PIT.[bk_hash]
            AND DIM.[p_mms_membership_type_attribute_id] = PIT.[p_mms_membership_type_attribute_id]
       INNER JOIN [dbo].[l_mms_membership_type_attribute] LNK
         ON LNK.[bk_hash] = PIT.[bk_hash]
            AND LNK.[l_mms_membership_type_attribute_id] = PIT.[l_mms_membership_type_attribute_id]
       INNER JOIN[dbo].[s_mms_membership_type_attribute] SAT
         ON SAT.[bk_hash] = PIT.[bk_hash]
            AND SAT.[s_mms_membership_type_attribute_id] = PIT.[s_mms_membership_type_attribute_id];