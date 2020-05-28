CREATE VIEW [sandbox].[v_mart_mms_membership_address]
AS SELECT PIT.[membership_address_id]
     , LNK.[membership_id]
     , LNK.[val_address_type_id]
     , LNK.[val_country_id]
     , LNK.[val_state_id]
     , SAT.[address_line_1]
     , SAT.[address_line_2]
     , SAT.[city]
     , SAT.[zip]
     , SAT.[inserted_date_time]
     , SAT.[updated_date_time]
     , [dim_mms_membership_key] = CASE WHEN NOT LNK.[membership_id] Is Null THEN CONVERT(char(32), HASHBYTES('MD5', ('P%#&z$@k' + CONVERT(varchar, LNK.[membership_id]))),2) ELSE CONVERT(char(32),'-998',2) END
     , PIT.[bk_hash]
     , PIT.[p_mms_membership_address_id]
     , PIT.[dv_load_date_time]
     , PIT.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (LNK.[dv_hash] + SAT.[dv_hash])),2)
     --, [l_hash] = LNK.[dv_hash]
     --, [s_hash] = SAT.[dv_hash]
  FROM [dbo].[p_mms_membership_address] PIT
       --INNER JOIN [dbo].[d_mms_membership_address] DIM
       --  ON DIM.[bk_hash] = PIT.[bk_hash]
       --     AND DIM.[p_mms_membership_address_id] = PIT.[p_mms_membership_address_id]
       INNER JOIN [dbo].[l_mms_membership_address] LNK
         ON LNK.[bk_hash] = PIT.[bk_hash]
            AND LNK.[l_mms_membership_address_id] = PIT.[l_mms_membership_address_id]
       INNER JOIN[dbo].[s_mms_membership_address] SAT
         ON SAT.[bk_hash] = PIT.[bk_hash]
            AND SAT.[s_mms_membership_address_id] = PIT.[s_mms_membership_address_id]
       INNER JOIN
         ( SELECT PIT.[bk_hash]
                , PIT.[p_mms_membership_address_id]
                , RowRank = RANK() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
                , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
             FROM [dbo].[p_mms_membership_address] PIT
         ) PITU
         ON PITU.[bk_hash] = PIT.[bk_hash]
            AND PITU.[p_mms_membership_address_id] = PIT.[p_mms_membership_address_id]
            AND PITU.RowRank = 1 AND PITU.RowNumber = 1;