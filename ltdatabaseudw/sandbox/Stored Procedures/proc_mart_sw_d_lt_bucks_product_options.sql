CREATE PROC [sandbox].[proc_mart_sw_d_lt_bucks_product_options] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT PIT.[poption_id]
     , LNK.[poption_product]
     , LNK.[poption_mms_id]
     , SAT.[poption_title]
     , SAT.[poption_price]
     , SAT.[poption_active]
     , SAT.[poption_timestamp]
     , SAT.[poption_desc]
     , SAT.[poption_mms_multiplier]
     , SAT.[poption_expiration_days]
     , SAT.[last_modified_timestamp]
     , PIT.[bk_hash]
     , PIT.[p_lt_bucks_product_options_id]
     , PIT.[dv_load_date_time]
     , PIT.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (LNK.[dv_hash] + SAT.[dv_hash])),2)
     --, [l_hash] = LNK.[dv_hash]
     --, [s_hash] = SAT.[dv_hash]
  FROM [dbo].[p_lt_bucks_product_options] PIT
       INNER JOIN [dbo].[l_lt_bucks_product_options] LNK
         ON LNK.[bk_hash] = PIT.[bk_hash]
            AND LNK.[l_lt_bucks_product_options_id] = PIT.[l_lt_bucks_product_options_id]
       INNER JOIN[dbo].[s_lt_bucks_product_options] SAT
         ON SAT.[bk_hash] = PIT.[bk_hash]
            AND SAT.[s_lt_bucks_product_options_id] = PIT.[s_lt_bucks_product_options_id]
       INNER JOIN
         ( SELECT PIT.[p_lt_bucks_product_options_id]
                , RowRank = RANK() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
                , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
             FROM [dbo].[p_lt_bucks_product_options] PIT
             WHERE ( PIT.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
                 AND PIT.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END )
         ) PITU
         ON PITU.[p_lt_bucks_product_options_id] = PIT.[p_lt_bucks_product_options_id]
            AND PITU.RowRank = 1 AND PITU.RowNumber = 1
  WHERE NOT PIT.[poption_id] Is Null
ORDER BY PIT.[dv_batch_id] ASC, PIT.[dv_load_date_time] ASC, SAT.[last_modified_timestamp] ASC;

END
