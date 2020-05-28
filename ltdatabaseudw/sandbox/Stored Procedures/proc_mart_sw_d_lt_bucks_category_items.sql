CREATE PROC [sandbox].[proc_mart_sw_d_lt_bucks_category_items] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT PIT.[citem_id]
     , LNK.[citem_product]
     , LNK.[citem_category]
     , SAT.[citem_active]
     , d_lt_bucks_categories.[category_name]
     , d_lt_bucks_categories.[category_active]
     , d_lt_bucks_categories.[category_isdeleted]
     , PIT.[bk_hash]
     , PIT.[p_lt_bucks_category_items_id]
     , PIT.[dv_load_date_time]
     , PIT.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (LNK.[dv_hash] + SAT.[dv_hash])),2)
     --, [l_hash] = LNK.[dv_hash]
     --, [s_hash] = SAT.[dv_hash]
  FROM [dbo].[p_lt_bucks_category_items] PIT
       INNER JOIN [dbo].[l_lt_bucks_category_items] LNK
         ON LNK.[bk_hash] = PIT.[bk_hash]
            AND LNK.[l_lt_bucks_category_items_id] = PIT.[l_lt_bucks_category_items_id]
       INNER JOIN[dbo].[s_lt_bucks_category_items] SAT
         ON SAT.[bk_hash] = PIT.[bk_hash]
            AND SAT.[s_lt_bucks_category_items_id] = PIT.[s_lt_bucks_category_items_id]
       INNER JOIN
         ( SELECT PIT.[p_lt_bucks_category_items_id]
                , RowRank = RANK() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
                , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
             FROM [dbo].[p_lt_bucks_category_items] PIT
             WHERE ( PIT.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
                 AND PIT.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END )
         ) PITU
         ON PITU.[p_lt_bucks_category_items_id] = PIT.[p_lt_bucks_category_items_id]
            AND PITU.RowRank = 1 AND PITU.RowNumber = 1
       INNER JOIN
         ( SELECT PIT_Child.[category_id]
                , SAT_Child.[category_name]
                , SAT_Child.[category_active]
                , SAT_Child.[category_isdeleted]
             FROM [dbo].[p_lt_bucks_categories] PIT_Child
                  INNER JOIN[dbo].[s_lt_bucks_categories] SAT_Child
                    ON SAT_Child.[bk_hash] = PIT_Child.[bk_hash]
                       AND SAT_Child.[s_lt_bucks_categories_id] = PIT_Child.[s_lt_bucks_categories_id]
             WHERE NOT PIT_Child.[category_id] Is Null
               AND PIT_Child.[dv_load_end_date_time] = '9999-12-31 00:00:00.000'
         ) d_lt_bucks_categories
         ON d_lt_bucks_categories.[category_id] = LNK.[citem_category]
  WHERE NOT PIT.[citem_id] Is Null
ORDER BY PIT.[dv_batch_id] ASC, PIT.[dv_load_date_time] ASC, ISNULL(SAT.[citem_date_modified], SAT.[citem_date_created]) ASC, SAT.[last_modified_timestamp] ASC;

END
