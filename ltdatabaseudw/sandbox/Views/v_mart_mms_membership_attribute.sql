CREATE VIEW [sandbox].[v_mart_mms_membership_attribute]
AS SELECT PIT.[membership_attribute_id]
     , LNK.[membership_id]
     , LNK.[val_membership_attribute_type_id]
     , SAT.[attribute_value]
     , [effective_from_date_no_time] = DATEADD(DD, DATEDIFF(DD, 0, SAT.[effective_from_date_time]), 0)
     , SAT.[effective_from_date_time]
     , [effective_thru_date_no_time] = DATEADD(DD, DATEDIFF(DD, 0, SAT.[effective_thru_date_time]), 0)
     , SAT.[effective_thru_date_time]
     , SAT.[inserted_date_time]
     , SAT.[updated_date_time]
     , DIM.[dim_mms_membership_key]
     , PIT.[bk_hash]
     , PIT.[p_mms_membership_attribute_id]
     , PIT.[dv_load_date_time]
     , PIT.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (LNK.[dv_hash] + SAT.[dv_hash])),2)
     , DIM.[deleted_flag]
     , [sales_promotion_id] = case when LNK.[val_membership_attribute_type_id]=(3) AND isnumeric(SAT.[attribute_value])=(1) AND CONVERT([decimal],SAT.[attribute_value],(0))<=(2147483647) then CONVERT([int],SAT.[attribute_value],(0)) end
     , [val_termination_reason_id] = case when LNK.[val_membership_attribute_type_id]=(11) AND isnumeric(SAT.[attribute_value])=(1) AND CONVERT([decimal],SAT.[attribute_value],(0))<=(2147483647) then CONVERT([int],SAT.[attribute_value],(0)) end
  FROM [dbo].[p_mms_membership_attribute] PIT
       INNER JOIN [dbo].[d_mms_membership_attribute] DIM
         ON DIM.[bk_hash] = PIT.[bk_hash]
            AND DIM.[p_mms_membership_attribute_id] = PIT.[p_mms_membership_attribute_id]
       INNER JOIN [dbo].[l_mms_membership_attribute] LNK
         ON LNK.[bk_hash] = PIT.[bk_hash]
            AND LNK.[l_mms_membership_attribute_id] = PIT.[l_mms_membership_attribute_id]
       INNER JOIN[dbo].[s_mms_membership_attribute] SAT
         ON SAT.[bk_hash] = PIT.[bk_hash]
            AND SAT.[s_mms_membership_attribute_id] = PIT.[s_mms_membership_attribute_id];