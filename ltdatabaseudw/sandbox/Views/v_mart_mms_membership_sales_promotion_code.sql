CREATE VIEW [sandbox].[v_mart_mms_membership_sales_promotion_code]
AS SELECT PIT.[membership_sales_promotion_code_id]
     , LNK.[membership_id]
     , LNK.[member_id]
     , LNK.[sales_promotion_code_id]
     , LNK.[sales_advisor_employee_id]
     , SAT.[inserted_date_time]
     , SAT.[updated_date_time]
     , DIM.[dim_mms_membership_key]
     , DIM.[dim_mms_member_key]
     , DIM.[dim_mms_sales_promotion_code_key]
     , PIT.[bk_hash]
     , PIT.[p_mms_membership_sales_promotion_code_id]
     , PIT.[dv_load_date_time]
     , PIT.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (LNK.[dv_hash] + SAT.[dv_hash])),2)
  FROM [dbo].[p_mms_membership_sales_promotion_code] PIT
       INNER JOIN [dbo].[d_mms_membership_sales_promotion_code] DIM
         ON DIM.[bk_hash] = PIT.[bk_hash]
            AND DIM.[p_mms_membership_sales_promotion_code_id] = PIT.[p_mms_membership_sales_promotion_code_id]
       INNER JOIN [dbo].[l_mms_membership_sales_promotion_code] LNK
         ON LNK.[bk_hash] = PIT.[bk_hash]
            AND LNK.[l_mms_membership_sales_promotion_code_id] = PIT.[l_mms_membership_sales_promotion_code_id]
       INNER JOIN[dbo].[s_mms_membership_sales_promotion_code] SAT
         ON SAT.[bk_hash] = PIT.[bk_hash]
            AND SAT.[s_mms_membership_sales_promotion_code_id] = PIT.[s_mms_membership_sales_promotion_code_id];