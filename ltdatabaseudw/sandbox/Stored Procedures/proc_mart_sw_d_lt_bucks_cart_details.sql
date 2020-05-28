CREATE PROC [sandbox].[proc_mart_sw_d_lt_bucks_cart_details] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT d_lt_bucks_shopping_cart.[cart_id]
     , d_lt_bucks_shopping_cart.[cart_session]
     , d_lt_bucks_shopping_cart.[cart_product]
     , d_lt_bucks_shopping_cart.[cart_qty]
     , d_lt_bucks_shopping_cart.[cart_status]
     , d_lt_bucks_shopping_cart.[cart_amount]
     , d_lt_bucks_shopping_cart.[cart_timestamp]
     , d_lt_bucks_shopping_cart.[last_modified_timestamp]
     , d_lt_bucks_cart_details.[cdetail_id]
     , d_lt_bucks_cart_details.[cdetail_poption]
     , d_lt_bucks_cart_details.[cdetail_club]
     , d_lt_bucks_cart_details.[cdetail_transaction_key]
     , d_lt_bucks_cart_details.[cdetail_package]
     , d_lt_bucks_cart_details.[cdetail_expiration_date]
     , cdetail_delivery_date = d_lt_bucks_cart_details.[cdetail_delivery_date]
     , d_lt_bucks_product_options.[poption_product]
     , d_lt_bucks_product_options.[poption_mms_id]
     , d_lt_bucks_transactions.[transaction_id]
     , d_lt_bucks_transactions.[transaction_user]
     , d_lt_bucks_transactions.[transaction_amount]
     --, d_lt_bucks_transactions.[transaction_date_1]
     --, d_lt_bucks_transactions.[transaction_timestamp]
     , d_lt_bucks_transactions.[bucks_amount]
     , d_lt_bucks_users.[member_id]
     , d_mms_package.[package_id]
     , d_mms_package.[tran_item_id]
     , d_mms_package.[val_package_status_id]
     , d_mms_package.[number_of_sessions]
     , d_mms_package.[price_per_session]
     , d_mms_package.[sessions_left]
     , d_mms_package.[balance_amount]
     --, member_id = (case when isnumeric(LNK.[user_dist_id])=(1) AND convert(bigint,LNK.[user_dist_id])<=(2147483647) then convert([int],LNK.[user_dist_id]) end)
     , d_lt_bucks_cart_details.[bk_hash]
     , d_lt_bucks_cart_details.[p_lt_bucks_cart_details_id]
     , d_lt_bucks_cart_details.[dv_load_date_time]
     , d_lt_bucks_cart_details.[dv_batch_id]
     , d_lt_bucks_cart_details.[dv_hash]
  FROM ( SELECT PIT.[cart_id]
              , PIT.[bk_hash]
              , LNK.[cart_session]
              , LNK.[cart_product]
              , SAT.[cart_qty]
              , SAT.[cart_status]
              , SAT.[cart_amount]
              , SAT.[cart_timestamp]
              , SAT.[last_modified_timestamp]
              --, HUB.[h_lt_bucks_shopping_cart_id]
              --, PIT.[p_lt_bucks_shopping_cart_id]
              --, PIT.[dv_load_date_time]
              --, PIT.[dv_batch_id]
              --, SAT.[dv_hash]
           FROM [dbo].[p_lt_bucks_shopping_cart] PIT
                INNER JOIN [dbo].[l_lt_bucks_shopping_cart] LNK
                  ON LNK.[bk_hash] = PIT.[bk_hash]
                     AND LNK.[l_lt_bucks_shopping_cart_id] = PIT.[l_lt_bucks_shopping_cart_id]
                INNER JOIN [dbo].[s_lt_bucks_shopping_cart] SAT
                  ON SAT.[bk_hash] = PIT.[bk_hash]
                     AND SAT.[s_lt_bucks_shopping_cart_id] = PIT.[s_lt_bucks_shopping_cart_id]
                INNER JOIN
                  ( SELECT PIT.[p_lt_bucks_shopping_cart_id]
                         , RowRank = RANK() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
                         , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
                      FROM [dbo].[p_lt_bucks_shopping_cart] PIT
                      WHERE ( PIT.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
                          AND PIT.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END )
                  ) PITU
                  ON PITU.[p_lt_bucks_shopping_cart_id] = PIT.[p_lt_bucks_shopping_cart_id]
                     AND PITU.RowRank = 1 AND PITU.RowNumber = 1
           WHERE NOT PIT.[cart_id] Is Null
             AND PIT.[dv_load_end_date_time] = '9999-12-31 00:00:00.000'
             AND SAT.[cart_status] = 3
             AND SAT.[cart_timestamp] >= '2015-01-01'
       ) d_lt_bucks_shopping_cart
       INNER JOIN
         ( SELECT PIT.[cdetail_id]
                , PIT.[bk_hash]
                , LNK.[cdetail_cart]
                , LNK.[cdetail_poption]
                , LNK.[cdetail_club]
                , LNK.[cdetail_transaction_key]
                , LNK.[cdetail_package]
                , SAT.[cdetail_expiration_date]
                , SAT.[cdetail_delivery_date]
                , PIT.[p_lt_bucks_cart_details_id]
                , PIT.[dv_load_date_time]
                , PIT.[dv_batch_id]
                , SAT.[dv_hash]
             FROM [dbo].[p_lt_bucks_cart_details] PIT
                  INNER JOIN [dbo].[l_lt_bucks_cart_details] LNK
                    ON LNK.[bk_hash] = PIT.[bk_hash]
                       AND LNK.[l_lt_bucks_cart_details_id] = PIT.[l_lt_bucks_cart_details_id]
                  INNER JOIN [dbo].[s_lt_bucks_cart_details] SAT
                    ON SAT.[bk_hash] = PIT.[bk_hash]
                       AND SAT.[s_lt_bucks_cart_details_id] = PIT.[s_lt_bucks_cart_details_id]
                  --INNER JOIN
                  --  ( SELECT PIT.[p_lt_bucks_cart_details_id]
                  --         , RowRank = RANK() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
                  --         , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
                  --      FROM [dbo].[p_lt_bucks_cart_details] PIT
                  --      WHERE PIT.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
                  --        AND PIT.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END + " 
                  --  ) PITU
                  --  ON PITU.[p_lt_bucks_cart_details_id] = PIT.[p_lt_bucks_cart_details_id]
                  --     AND PITU.RowRank = 1 AND PITU.RowNumber = 1
             WHERE NOT PIT.[cdetail_id] Is Null
               AND PIT.[dv_load_end_date_time] = '9999-12-31 00:00:00.000'
         ) d_lt_bucks_cart_details
         ON d_lt_bucks_cart_details.[cdetail_cart] = d_lt_bucks_shopping_cart.[cart_id]
       INNER JOIN
         ( SELECT PIT.[poption_id]
                , PIT.[bk_hash]
                , LNK.[poption_product]
                , LNK.[poption_mms_id]
             FROM [dbo].[p_lt_bucks_product_options] PIT
                  INNER JOIN [dbo].[l_lt_bucks_product_options] LNK
                    ON LNK.[bk_hash] = PIT.[bk_hash]
                       AND LNK.[l_lt_bucks_product_options_id] = PIT.[l_lt_bucks_product_options_id]
             WHERE NOT PIT.[poption_id] Is Null
               AND PIT.[dv_load_end_date_time] = '9999-12-31 00:00:00.000'
         ) d_lt_bucks_product_options
         ON d_lt_bucks_product_options.[poption_id] = d_lt_bucks_cart_details.[cdetail_poption]
       INNER JOIN
         ( SELECT PIT.[transaction_id]
                , LNK.[transaction_user]
                , LNK.[transaction_session]
                , [transaction_amount] = SAT.[transaction_amount]
                , bucks_amount = SUM(d_lt_bucks_transaction_fifo.[tfifo_amount])
             FROM [dbo].[p_lt_bucks_transactions] PIT
                  INNER JOIN [dbo].[l_lt_bucks_transactions] LNK
                    ON LNK.[bk_hash] = PIT.[bk_hash]
                       AND LNK.[l_lt_bucks_transactions_id] = PIT.[l_lt_bucks_transactions_id]
                  INNER JOIN [dbo].[s_lt_bucks_transactions] SAT
                    ON SAT.[bk_hash] = PIT.[bk_hash]
                       AND SAT.[s_lt_bucks_transactions_id] = PIT.[s_lt_bucks_transactions_id]
                  INNER JOIN
                    ( SELECT PIT.[tfifo_id]
                           , PIT.[bk_hash]
                           , LNK.[tfifo_transaction_1]
                           , LNK.[tfifo_transaction_2]
                           , SAT.[tfifo_amount]
                           , SAT.[tfifo_timestamp]
                           , SAT.[last_modified_timestamp]
                        FROM [dbo].[p_lt_bucks_transaction_fifo] PIT
                             INNER JOIN [dbo].[l_lt_bucks_transaction_fifo] LNK
                               ON LNK.[bk_hash] = PIT.[bk_hash]
                                  AND LNK.[l_lt_bucks_transaction_fifo_id] = PIT.[l_lt_bucks_transaction_fifo_id]
                             INNER JOIN [dbo].[s_lt_bucks_transaction_fifo] SAT
                               ON SAT.[bk_hash] = PIT.[bk_hash]
                                  AND SAT.[s_lt_bucks_transaction_fifo_id] = PIT.[s_lt_bucks_transaction_fifo_id]
                        WHERE NOT PIT.[tfifo_id] Is Null
                          AND PIT.[dv_load_end_date_time] = '9999-12-31 00:00:00.000'
                    ) d_lt_bucks_transaction_fifo
                    ON d_lt_bucks_transaction_fifo.[tfifo_transaction_2] = PIT.[transaction_id]
                  --INNER JOIN
                  --  ( SELECT PIT.[p_lt_bucks_transactions_id]
                  --         , RowRank = RANK() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
                  --         , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
                  --      FROM [dbo].[p_lt_bucks_transactions] PIT
                  --      WHERE PIT.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
                  --        AND PIT.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END + " 
                  --  ) PITU
                  --  ON PITU.[p_lt_bucks_transactions_id] = PIT.[p_lt_bucks_transactions_id]
                  --     AND PITU.RowRank = 1 AND PITU.RowNumber = 1
             WHERE NOT PIT.[transaction_id] Is Null
               AND PIT.[dv_load_end_date_time] = '9999-12-31 00:00:00.000'
               AND SAT.[transaction_type] = 1
             GROUP BY PIT.[transaction_id], LNK.[transaction_user], LNK.[transaction_session], SAT.[transaction_amount]
         ) d_lt_bucks_transactions
         ON d_lt_bucks_transactions.[transaction_session] = d_lt_bucks_shopping_cart.[cart_session]
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
               AND PIT.[dv_load_end_date_time] = '9999-12-31 00:00:00.000'
               AND SAT.[user_type] = 1
         ) d_lt_bucks_users
         ON d_lt_bucks_users.[user_id] = d_lt_bucks_transactions.[transaction_user]
       INNER JOIN
         ( SELECT PIT.[package_id]
                --, LNK.[member_id]
                --, LNK.[membership_id]
                --, LNK.[club_id]
                --, LNK.[employee_id]
                , LNK.[val_package_status_id]
                , LNK.[mms_tran_id]
                --, LNK.[product_id]
                , LNK.[tran_item_id]
                , SAT.[number_of_sessions]
                , SAT.[price_per_session]
                , SAT.[created_date_time]
                --, SAT.[utc_created_date_time]
                --, SAT.[created_date_time_zone]
                , SAT.[sessions_left]
                , SAT.[balance_amount]
                --, SAT.[inserted_date_time]
                --, SAT.[updated_date_time]
                --, SAT.[package_edited_flag]
                --, SAT.[package_edit_date_time]
                --, SAT.[utc_package_edit_date_time]
                --, SAT.[package_edit_date_time_zone]
                , SAT.[expiration_date_time]
                , SAT.[unexpire_count]
                , PIT.[bk_hash]
                , PIT.[p_mms_package_id]
                , PIT.[dv_load_date_time]
                , PIT.[dv_batch_id]
                , RowRank = RANK() OVER (PARTITION BY LNK.[mms_tran_id] ORDER BY PIT.[package_id] ASC)
                , RowNumber = ROW_NUMBER() OVER (PARTITION BY LNK.[mms_tran_id] ORDER BY PIT.[package_id] ASC)
             FROM [dbo].[p_mms_package] PIT
                  INNER JOIN [dbo].[l_mms_package] LNK
                    ON LNK.[bk_hash] = PIT.[bk_hash]
                       AND LNK.[l_mms_package_id] = PIT.[l_mms_package_id]
                  INNER JOIN [dbo].[s_mms_package] SAT
                    ON SAT.[bk_hash] = PIT.[bk_hash]
                       AND SAT.[s_mms_package_id] = PIT.[s_mms_package_id]
                  --INNER JOIN
                  --  ( SELECT PIT.[p_mms_package_id]
                  --         , RowRank = RANK() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
                  --         , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
                  --      FROM [dbo].[p_mms_package] PIT
                  --           INNER JOIN [dbo].[h_mms_package] HUB
                  --             ON HUB.[bk_hash] = PIT.[bk_hash]
                  --      WHERE HUB.dv_deleted = 0
                  --      --WHERE PIT.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
                  --      --  AND PIT.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END + " 
                  --  ) PITU
                  --  ON PITU.[p_mms_package_id] = PIT.[p_mms_package_id]
                  --     AND PITU.RowRank = 1 AND PITU.RowNumber = 1
             WHERE NOT PIT.[package_id] Is Null
               AND PIT.[dv_load_end_date_time] = '9999-12-31 00:00:00.000'
         ) d_mms_package
         ON d_mms_package.[mms_tran_id] = d_lt_bucks_cart_details.[cdetail_transaction_key]
  WHERE ( (NOT d_lt_bucks_cart_details.[cdetail_package] Is Null AND d_lt_bucks_cart_details.[cdetail_package] = d_mms_package.[package_id])
       OR (d_lt_bucks_cart_details.[cdetail_package] Is Null AND (d_mms_package.RowRank = 1 AND d_mms_package.RowNumber = 1)) )
ORDER BY d_lt_bucks_cart_details.[dv_batch_id] ASC, d_lt_bucks_cart_details.[dv_load_date_time] ASC;

END
