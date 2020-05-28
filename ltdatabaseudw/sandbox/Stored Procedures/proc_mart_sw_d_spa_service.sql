CREATE PROC [sandbox].[proc_mart_sw_d_spa_service] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT [service_id] = CONVERT(int, PIT.[service_id])
     , [category_id] = CONVERT(int, LNK.[dept_cat])
     , [club_id] = CONVERT(int, LNK.[store_number])
     , [service_name] = SAT.[name]
     , [category_name] = CASE WHEN ISNULL(DIM.[category], ISNULL(DIM.[level_2_service_category], DIM.[level_1_service_category])) = '0' THEN null ELSE ISNULL(DIM.[category], ISNULL(DIM.[level_2_service_category], DIM.[level_1_service_category])) END
     , PIT.[bk_hash]
     , PIT.[p_spabiz_service_id]
     , PIT.[dv_load_date_time]
     , PIT.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (LNK.[dv_hash] + SAT.[dv_hash])),2)
     --, [l_hash] = LNK.[dv_hash]
     --, [s_hash] = SAT.[dv_hash]
  FROM [dbo].[p_spabiz_service] PIT
       INNER JOIN [dbo].[l_spabiz_service] LNK
         ON LNK.[bk_hash] = PIT.[bk_hash]
            AND LNK.[l_spabiz_service_id] = PIT.[l_spabiz_service_id]
       INNER JOIN[dbo].[s_spabiz_service] SAT
         ON SAT.[bk_hash] = PIT.[bk_hash]
            AND SAT.[s_spabiz_service_id] = PIT.[s_spabiz_service_id]
       INNER JOIN
         ( SELECT PIT.[p_spabiz_service_id]
                , RowRank = RANK() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
                , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
             FROM [dbo].[p_spabiz_service] PIT
             WHERE ( PIT.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
                 AND PIT.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END )
         ) PITU
         ON PITU.[p_spabiz_service_id] = PIT.[p_spabiz_service_id]
            AND PITU.RowRank = 1 AND PITU.RowNumber = 1
       INNER JOIN [dbo].[dim_spabiz_service] DIM
         ON DIM.[p_spabiz_service_id] = PIT.[p_spabiz_service_id]
  WHERE NOT PIT.[service_id] Is Null
ORDER BY PIT.[dv_batch_id] ASC, PIT.[dv_load_date_time] ASC

END