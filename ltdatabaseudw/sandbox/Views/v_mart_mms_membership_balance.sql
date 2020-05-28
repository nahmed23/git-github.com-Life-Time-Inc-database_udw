CREATE VIEW [sandbox].[v_mart_mms_membership_balance]
AS SELECT d_mms_membership_balance.[membership_balance_id]
     , d_mms_membership_balance.[membership_id]
     , d_mms_membership_balance.[current_balance]
     , d_mms_membership_balance.[eft_amount]
     , d_mms_membership_balance.[statement_balance]
     , d_mms_membership_balance.[assessed_date_time]
     , d_mms_membership_balance.[statement_date_time]
     , d_mms_membership_balance.[previous_statement_balance]
     , d_mms_membership_balance.[previous_statement_datetime]
     , d_mms_membership_balance.[committed_balance]
     , d_mms_membership_balance.[inserted_date_time]
     , d_mms_membership_balance.[updated_date_time]
     , d_mms_membership_balance.[resubmit_collect_from_bank_account_flag]
     , d_mms_membership_balance.[committed_balance_products]
     , d_mms_membership_balance.[current_balance_products]
     , d_mms_membership_balance.[eft_amount_products]
     , d_mms_membership_balance.[bk_hash]
     , d_mms_membership_balance.[p_mms_membership_balance_id]
     , d_mms_membership_balance.[dv_load_date_time]
     , d_mms_membership_balance.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (batch_info.[l_hash] + batch_info.[s_hash])),2)
     , d_mms_membership_balance.[dv_deleted]
  FROM ( SELECT LNK.[membership_balance_id]
              , LNK.[membership_id]
              , SAT.[current_balance]
              , SAT.[eft_amount]
              , SAT.[statement_balance]
              , SAT.[assessed_date_time]
              , SAT.[statement_date_time]
              , SAT.[previous_statement_balance]
              , SAT.[previous_statement_datetime]
              , SAT.[committed_balance]
              , SAT.[inserted_date_time]
              , SAT.[updated_date_time]
              , SAT.[resubmit_collect_from_bank_account_flag]
              , SAT.[committed_balance_products]
              , SAT.[current_balance_products]
              , SAT.[eft_amount_products]
              , [dim_mms_membership_key] = CASE WHEN NOT LNK.[membership_id] Is Null THEN CONVERT(char(32), HASHBYTES('MD5', ('P%#&z$@k' + CONVERT(varchar, LNK.[membership_id]))),2) ELSE CONVERT(char(32),'-998',2) END
              , PIT.[bk_hash]
              , PIT.[p_mms_membership_balance_id]
              , PIT.[dv_load_date_time]
              , PIT.[dv_batch_id]
              , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (LNK.[dv_hash] + SAT.[dv_hash])),2)
              --, [l_hash] = LNK.[dv_hash]
              --, [s_hash] = SAT.[dv_hash]
              , [dv_deleted] = CAST(CASE WHEN d_mms_membership.[val_membership_status_id] = 1 AND (NOT d_mms_membership.[membership_expiration_date] Is Null AND d_mms_membership.[membership_expiration_date] < DATEADD(mm, -6, DATEADD(mm, DATEDIFF(mm, 0, GETDATE()), 0))) THEN 1 ELSE 0 END AS bit)
           FROM [dbo].[p_mms_membership_balance] PIT
                INNER JOIN [dbo].[l_mms_membership_balance] LNK
                  ON LNK.[bk_hash] = PIT.[bk_hash]
                     AND LNK.[l_mms_membership_balance_id] = PIT.[l_mms_membership_balance_id]
                INNER JOIN[dbo].[s_mms_membership_balance] SAT
                  ON SAT.[bk_hash] = PIT.[bk_hash]
                     AND SAT.[s_mms_membership_balance_id] = PIT.[s_mms_membership_balance_id]
                INNER JOIN
                  ( SELECT PIT.[bk_hash]
                         , PIT.[p_mms_membership_balance_id]
                         , RowRank = RANK() OVER (PARTITION BY PIT.[membership_id] ORDER BY PIT.[dv_load_end_date_time] DESC)
                         , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[membership_id] ORDER BY PIT.[dv_load_end_date_time] DESC)
                      FROM [dbo].[p_mms_membership_balance] PIT
                  ) PITU
                  ON PITU.[bk_hash] = PIT.[bk_hash]
                     AND PITU.[p_mms_membership_balance_id] = PIT.[p_mms_membership_balance_id]
                     AND PITU.RowRank = 1 AND PITU.RowNumber = 1
                INNER JOIN [dbo].[d_mms_membership] d_mms_membership
                  ON d_mms_membership.[membership_id] = LNK.[membership_id]
           WHERE NOT LNK.[membership_balance_id] Is Null
       ) d_mms_membership_balance
       CROSS APPLY
         ( SELECT [l_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_membership_balance.[membership_balance_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_membership_balance.[membership_id]),'z#@$k%&P'))),2)

                , [s_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_membership_balance.[membership_balance_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_membership_balance.[current_balance]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_membership_balance.[eft_amount]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_membership_balance.[statement_balance]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_membership_balance.[assessed_date_time], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_membership_balance.[statement_date_time], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_membership_balance.[previous_statement_balance]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_membership_balance.[previous_statement_datetime], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_membership_balance.[committed_balance]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_membership_balance.[inserted_date_time], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_membership_balance.[updated_date_time], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_membership_balance.[resubmit_collect_from_bank_account_flag]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_membership_balance.[committed_balance_products]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_membership_balance.[current_balance_products]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_membership_balance.[eft_amount_products]),'z#@$k%&P'))),2)

                --  [dv_load_date_time] = ISNULL(d_mms_membership_balance.[updated_date_time],d_mms_membership_balance.[inserted_date_time])
                --, [dv_batch_id] = CONVERT(bigint, CONVERT(VARCHAR(8), ISNULL(d_mms_membership_balance.[updated_date_time],d_mms_membership_balance.[inserted_date_time]), 112) + REPLACE(CONVERT(varchar(8), ISNULL(d_mms_membership_balance.[updated_date_time],d_mms_membership_balance.[inserted_date_time]), 114), ':',''))
                --, [bk_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_membership_balance.[membership_balance_id]),'z#@$k%&P'))),2)
                
                
         ) batch_info;