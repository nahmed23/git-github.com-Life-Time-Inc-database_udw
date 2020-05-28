CREATE PROC [sandbox].[proc_mart_sw_d_mms_mms_tran_refund] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT d_mms_mms_tran.[mms_tran_id]
     , d_mms_mms_tran.[club_id]
     , d_mms_mms_tran.[membership_id]
     , d_mms_mms_tran.[member_id]
     , d_mms_mms_tran.[reason_code_id]
     , d_mms_mms_tran.[val_tran_type_id]
     , d_mms_mms_tran.[tran_employee_id]
     , d_mms_mms_tran.[val_currency_code_id]
     , d_mms_mms_tran.[receipt_comment]
     , d_mms_mms_tran.[post_date_time]
     , d_mms_mms_tran.[tran_date]
     , d_mms_mms_tran.[tran_amount]
     , d_mms_mms_tran.[currency_code]
     , d_mms_mms_tran.[tran_item_quantity]
     , d_mms_mms_tran.[tran_item_amount]
     , d_mms_mms_tran.[tran_item_sales_tax]
     , d_mms_mms_tran.[inserted_date_time]
     , d_mms_mms_tran.[updated_date_time]
     , d_mms_mms_tran.[bk_hash]
     , d_mms_mms_tran.[p_mms_mms_tran_id]
     , d_mms_mms_tran.[dv_load_date_time]
     , d_mms_mms_tran.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (batch_info.[l_hash] + batch_info.[s_hash])),2)
     , d_mms_mms_tran.[dv_deleted]
  FROM ( SELECT [mms_tran_id]          = d_mms_mms_tran.[mms_tran_id]
              , [club_id]              = d_mms_mms_tran.[club_id]
              , [membership_id]        = d_mms_mms_tran.[membership_id]
              , [member_id]            = d_mms_member.[member_id]
              , [reason_code_id]       = d_mms_reason_code.[reason_code_id]
              , [val_tran_type_id]     = d_mms_mms_tran.[val_tran_type_id]
              , [tran_employee_id]     = d_mms_mms_tran.[employee_id]
              , [val_currency_code_id] = ISNULL(d_mms_mms_tran.[val_currency_code_id],1)
              , [receipt_comment]      = NullIf(d_mms_mms_tran.[receipt_comment],'')
              , [post_date_time]       = d_mms_mms_tran.[post_date_time]
              , [tran_date]            = d_mms_mms_tran.[tran_date]
              , [tran_amount]          = d_mms_mms_tran.[tran_amount]
              , [currency_code]        = ISNULL(d_mms_mms_tran.original_currency_code,'USD')
              , [tran_item_quantity]   = ISNULL(d_mms_tran_item_summary.[sales_quantity],0)
              , [tran_item_amount]     = ISNULL(d_mms_tran_item_summary.[sales_dollar_amount],0)
              , [tran_item_sales_tax]  = ISNULL(d_mms_tran_item_summary.[sales_tax_amount],0)
              , [inserted_date_time]   = SAT.[inserted_date_time]  --d_mms_mms_tran.[post_date_time]
              , [updated_date_time]    = d_mms_mms_tran.[updated_date_time]
              , [dv_deleted]           = CAST(CASE WHEN (NOT d_mms_mms_tran.[voided_flag] Is Null AND d_mms_mms_tran.[voided_flag] = 'Y') THEN 1 ELSE 0 END AS bit)
              , d_mms_mms_tran.[bk_hash]
              , d_mms_mms_tran.[p_mms_mms_tran_id]
              , d_mms_mms_tran.[dv_load_date_time]
              , d_mms_mms_tran.[dv_batch_id]
           FROM [dbo].[d_mms_mms_tran] d_mms_mms_tran
                INNER JOIN [dbo].[p_mms_mms_tran] PIT
                  ON PIT.[bk_hash] = d_mms_mms_tran.[bk_hash]
                     AND PIT.[p_mms_mms_tran_id] = d_mms_mms_tran.[p_mms_mms_tran_id]
                INNER JOIN [dbo].[s_mms_mms_tran] SAT
                  ON SAT.[bk_hash] = PIT.[bk_hash]
                     AND SAT.[s_mms_mms_tran_id] = PIT.[s_mms_mms_tran_id]
                INNER JOIN [dbo].[d_mms_reason_code] d_mms_reason_code
                  ON d_mms_reason_code.[dim_mms_transaction_reason_key] = d_mms_mms_tran.[dim_mms_transaction_reason_key]
                --INNER JOIN [dbo].[d_mms_product] d_mms_product
                --  ON d_mms_product.[dim_mms_product_key] = d_mms_tran_item.[dim_mms_product_key]
                INNER JOIN [dbo].[d_mms_membership] d_mms_membership
                  ON d_mms_membership.[dim_mms_membership_key] = d_mms_mms_tran.[dim_mms_membership_key]
                INNER JOIN [dbo].[d_mms_member] d_mms_member
                  ON d_mms_member.[dim_mms_member_key] = d_mms_mms_tran.[dim_mms_member_key]
                LEFT OUTER JOIN
                  ( SELECT d_mms_tran_item.[fact_mms_sales_transaction_key]
                         , [sales_quantity]      = SUM(d_mms_tran_item.[sales_quantity])
                         , [sales_dollar_amount] = SUM(d_mms_tran_item.[sales_dollar_amount])
                         , [sales_tax_amount]    = SUM(d_mms_tran_item.[sales_tax_amount])
                      FROM [dbo].[d_mms_tran_item] d_mms_tran_item
                      --( SELECT d_mms_tran_item.[fact_mms_sales_transaction_key]
                      --            , d_mms_tran_item.[sales_quantity]
                      --            , d_mms_tran_item.[sales_dollar_amount]
                      --            , d_mms_tran_item.[sales_tax_amount]
                      --            , RowRank = RANK() OVER (PARTITION BY d_mms_tran_item.[fact_mms_sales_transaction_item_key] ORDER BY d_mms_tran_item.[tran_item_id] ASC)
                      --            , RowNumber = ROW_NUMBER() OVER (PARTITION BY d_mms_tran_item.[fact_mms_sales_transaction_item_key] ORDER BY d_mms_tran_item.[tran_item_id] ASC)
                      --         FROM [dbo].[d_mms_tran_item] d_mms_tran_item
                      --     ) d_mms_tran_item
                      --WHERE (d_mms_tran_item.RowRank = 1 AND d_mms_tran_item.RowNumber = 1)
                      GROUP BY d_mms_tran_item.[fact_mms_sales_transaction_key]
                  ) d_mms_tran_item_summary
                  ON d_mms_tran_item_summary.[fact_mms_sales_transaction_key] = d_mms_mms_tran.[fact_mms_sales_transaction_key]
           WHERE d_mms_membership.[membership_type_id] <> 134  --House Account
             AND d_mms_mms_tran.[val_tran_type_id] = 5
             AND (d_mms_mms_tran.[employee_id] Is Null OR NOT d_mms_mms_tran.[employee_id] = -5)
             AND d_mms_mms_tran.[post_date_time] >= '2017-01-01'
             AND ( d_mms_mms_tran.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
               AND d_mms_mms_tran.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END )
       ) d_mms_mms_tran
       CROSS APPLY
         ( SELECT [l_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mms_tran.[mms_tran_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mms_tran.[club_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mms_tran.[membership_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mms_tran.[member_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mms_tran.[reason_code_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mms_tran.[val_tran_type_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mms_tran.[tran_employee_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mms_tran.[val_currency_code_id]),'z#@$k%&P'))),2)

                , [s_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mms_tran.[mms_tran_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(d_mms_mms_tran.[receipt_comment],'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mms_tran.[post_date_time], 120),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mms_tran.[tran_date], 120),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mms_tran.[tran_amount]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(d_mms_mms_tran.[currency_code],'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mms_tran.[tran_item_quantity]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mms_tran.[tran_item_amount]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mms_tran.[tran_item_sales_tax]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mms_tran.[inserted_date_time], 120),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mms_tran.[updated_date_time], 120),'z#@$k%&P'))),2)

                --, [dv_load_date_time] = ISNULL(d_mms_mms_tran.[updated_date_time],d_mms_mms_tran.[inserted_date_time])
                --, [dv_batch_id] = CONVERT(bigint, CONVERT(VARCHAR(8), ISNULL(d_mms_mms_tran.[updated_date_time],d_mms_mms_tran.[inserted_date_time]), 112) + REPLACE(CONVERT(varchar(8), ISNULL(d_mms_mms_tran.[updated_date_time],d_mms_mms_tran.[inserted_date_time]), 114), ':',''))
                --, [bk_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mms_tran.[mms_tran_id]),'z#@$k%&P'))),2)
         ) batch_info
ORDER BY d_mms_mms_tran.[dv_batch_id] ASC, d_mms_mms_tran.[dv_load_date_time] ASC, ISNULL(d_mms_mms_tran.[updated_date_time], d_mms_mms_tran.[inserted_date_time]) ASC;

END
