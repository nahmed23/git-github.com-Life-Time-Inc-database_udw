CREATE PROC [sandbox].[proc_mart_sw_d_lt_bucks_transactions] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT PIT.[transaction_id]
     , LNK.[transaction_user]
     --, LNK.[transaction_session]
     --, LNK.[transaction_promotion]
     , SAT.[transaction_type]
     , SAT.[transaction_amount]
     --, SAT.[transaction_ext_ref]
     --, SAT.[transaction_int_1]
     --, SAT.[transaction_int_2]
     , SAT.[transaction_date_1]
     , SAT.[transaction_timestamp]
     --, SAT.[transaction_int_3]
     --, SAT.[transaction_int_4]
     --, SAT.[transaction_int_5]
     --, SAT.[last_modified_timestamp]
     , d_lt_bucks_users.[member_id]
     --, member_id = (case when isnumeric(LNK.[user_dist_id])=(1) AND convert(bigint,LNK.[user_dist_id])<=(2147483647) then convert([int],LNK.[user_dist_id]) end)
     , PIT.[bk_hash]
     , PIT.[p_lt_bucks_transactions_id]
     , PIT.[dv_load_date_time]
     , PIT.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (LNK.[dv_hash] + SAT.[dv_hash])),2)
     --, [l_hash] = LNK.[dv_hash]
     --, [s_hash] = SAT.[dv_hash]
  FROM [dbo].[p_lt_bucks_transactions] PIT
       INNER JOIN [dbo].[l_lt_bucks_transactions] LNK
         ON LNK.[bk_hash] = PIT.[bk_hash]
            AND LNK.[l_lt_bucks_transactions_id] = PIT.[l_lt_bucks_transactions_id]
       INNER JOIN[dbo].[s_lt_bucks_transactions] SAT
         ON SAT.[bk_hash] = PIT.[bk_hash]
            AND SAT.[s_lt_bucks_transactions_id] = PIT.[s_lt_bucks_transactions_id]
       INNER JOIN
         ( SELECT PIT.[p_lt_bucks_transactions_id]
                , RowRank = RANK() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
                , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
             FROM [dbo].[p_lt_bucks_transactions] PIT
             WHERE ( PIT.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
                 AND PIT.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END )
         ) PITU
         ON PITU.[p_lt_bucks_transactions_id] = PIT.[p_lt_bucks_transactions_id]
            AND PITU.RowRank = 1 AND PITU.RowNumber = 1
       INNER JOIN
         ( SELECT PIT.[user_id]
                , LNK.[user_dist_id]
                , SAT.[user_type]
                , member_id = (case when isnumeric(LNK.[user_dist_id])=(1) AND convert(bigint,LNK.[user_dist_id])<=(2147483647) then convert([int],LNK.[user_dist_id]) end)
             FROM [dbo].[p_lt_bucks_users] PIT
                  INNER JOIN [dbo].[l_lt_bucks_users] LNK
                    ON LNK.[bk_hash] = PIT.[bk_hash]
                       AND LNK.[l_lt_bucks_users_id] = PIT.[l_lt_bucks_users_id]
                  INNER JOIN[dbo].[s_lt_bucks_users] SAT
                    ON SAT.[bk_hash] = PIT.[bk_hash]
                       AND SAT.[s_lt_bucks_users_id] = PIT.[s_lt_bucks_users_id]
             WHERE NOT PIT.[user_id] Is Null
               AND PIT.dv_load_end_date_time = '9999-12-31 00:00:00.000'
             --WHERE SAT.[user_type] = 'member'
         ) d_lt_bucks_users
         ON d_lt_bucks_users.[user_id] = LNK.[transaction_user]
  WHERE NOT PIT.[transaction_id] Is Null
    AND SAT.[transaction_type] = 5
    AND SAT.[transaction_amount] > 0
ORDER BY PIT.[dv_batch_id] ASC, PIT.[dv_load_date_time] ASC;

END
