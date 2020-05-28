CREATE VIEW [sandbox].[v_mart_mms_qualified_sales_promotion]
AS SELECT PIT.[qualified_sales_promotion_id]
     , LNK.[sales_promotion_id]
     , LNK.[val_qualified_sales_promotion_type_id]
     , SAT.[description]
     , SAT.[promotion_name]
     , SAT.[inserted_date_time]
     , SAT.[updated_date_time]
     , [dim_mms_qualified_sales_promotion_key] = PIT.[bk_hash]
     , [dim_mms_sales_promotion_key] = CASE WHEN NOT LNK.[sales_promotion_id] Is Null THEN CONVERT(char(32), HASHBYTES('MD5', ('P%#&z$@k' + CONVERT(varchar, LNK.[sales_promotion_id]))),2) ELSE CONVERT(char(32),'-998',2) END
     , [dim_mms_val_qualified_sales_promotion_type_key] = CASE WHEN NOT LNK.[val_qualified_sales_promotion_type_id] Is Null THEN CONVERT(char(32), HASHBYTES('MD5', ('P%#&z$@k' + CONVERT(varchar, LNK.[val_qualified_sales_promotion_type_id]))),2) ELSE CONVERT(char(32),'-998',2) END
     , PIT.[bk_hash]
     , PIT.[p_mms_qualified_sales_promotion_id]
     , PIT.[dv_load_date_time]
     , PIT.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (LNK.[dv_hash] + SAT.[dv_hash])),2)
  FROM [dbo].[p_mms_qualified_sales_promotion] PIT
       INNER JOIN [dbo].[l_mms_qualified_sales_promotion] LNK
         ON LNK.[bk_hash] = PIT.[bk_hash]
            AND LNK.[l_mms_qualified_sales_promotion_id] = PIT.[l_mms_qualified_sales_promotion_id]
       INNER JOIN[dbo].[s_mms_qualified_sales_promotion] SAT
         ON SAT.[bk_hash] = PIT.[bk_hash]
            AND SAT.[s_mms_qualified_sales_promotion_id] = PIT.[s_mms_qualified_sales_promotion_id]
       INNER JOIN
         ( SELECT PIT.[p_mms_qualified_sales_promotion_id]
                , RowRank = RANK() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
                , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
             FROM [dbo].[p_mms_qualified_sales_promotion] PIT
         ) PITU
         ON PITU.[p_mms_qualified_sales_promotion_id] = PIT.[p_mms_qualified_sales_promotion_id]
            AND PITU.RowRank = 1 AND PITU.RowNumber = 1;