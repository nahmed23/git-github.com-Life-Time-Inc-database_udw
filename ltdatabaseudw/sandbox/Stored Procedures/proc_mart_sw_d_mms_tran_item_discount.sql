CREATE PROC [sandbox].[proc_mart_sw_d_mms_tran_item_discount] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT d_mms_tran_item_discount.[tran_item_discount_id]
     , d_mms_tran_item_discount.[pricing_discount_id]
     , d_mms_tran_item_discount.[val_discount_reason_id]
     , d_mms_tran_item_discount.[val_discount_type_id]
     , d_mms_tran_item_discount.[sales_promotion_id]
     , d_mms_tran_item_discount.[tran_item_id]
     , d_mms_tran_item_discount.[product_id]
     , d_mms_tran_item_discount.[bundle_product_id]
     , d_mms_tran_item_discount.[mms_tran_id]
     , d_mms_tran_item_discount.[club_id]
     , d_mms_tran_item_discount.[membership_id]
     , d_mms_tran_item_discount.[member_id]
     , d_mms_tran_item_discount.[reason_code_id]
     , d_mms_tran_item_discount.[val_tran_type_id]
     , d_mms_tran_item_discount.[tran_employee_id]
     , d_mms_tran_item_discount.[val_currency_code_id]
     , d_mms_tran_item_discount.[original_tran_item_id]
     , d_mms_tran_item_discount.[original_mms_tran_id]
     , d_mms_tran_item_discount.[domain_name]
     , d_mms_tran_item_discount.[post_date_time]
     , d_mms_tran_item_discount.[tran_date]
     --, d_mms_tran_item_discount.[tran_amount]
     , d_mms_tran_item_discount.[currency_code]
     , d_mms_tran_item_discount.[tran_item_quantity]
     , d_mms_tran_item_discount.[tran_item_amount]
     , d_mms_tran_item_discount.[tran_item_sales_tax]
     , d_mms_tran_item_discount.[tran_item_discount_amount]
     , d_mms_tran_item_discount.[promotion_code]
     , d_mms_tran_item_discount.[inserted_date_time]
     , d_mms_tran_item_discount.[updated_date_time]
     , d_mms_tran_item_discount.[bk_hash]
     , d_mms_tran_item_discount.[p_mms_tran_item_discount_id]
     , d_mms_tran_item_discount.[dv_load_date_time]
     , d_mms_tran_item_discount.[dv_batch_id]
     , [dv_hash]    = CONVERT(varchar(32), HASHBYTES('MD5', (batch_info.[l_hash] + batch_info.[s_hash])),2)
     , d_mms_tran_item_discount.[dv_deleted]
  FROM ( SELECT [tran_item_discount_id]     = d_mms_tran_item_discount.[tran_item_discount_id]
              , [pricing_discount_id]       = d_mms_tran_item_discount.[pricing_discount_id]
              , [val_discount_reason_id]    = d_mms_tran_item_discount.[val_discount_reason_id]
              , [val_discount_type_id]      = CONVERT(int, d_mms_pricing_discount.[val_discount_type_id])
              , [sales_promotion_id]        = CONVERT(int, d_mms_pricing_discount.[sales_promotion_id])
              , [tran_item_id]              = d_mms_tran_item.[tran_item_id]
              , [product_id]                = d_mms_tran_item.[product_id]
              , [bundle_product_id]         = d_mms_tran_item.[bundle_product_id]
              , [mms_tran_id]               = d_mms_mms_tran.[mms_tran_id]
              , [club_id]                   = d_mms_mms_tran.[club_id]
              , [membership_id]             = d_mms_mms_tran.[membership_id]
              , [member_id]                 = d_mms_member.[member_id]
              , [reason_code_id]            = d_mms_reason_code.[reason_code_id]
              , [val_tran_type_id]          = d_mms_mms_tran.[val_tran_type_id]
              , [tran_employee_id]          = d_mms_mms_tran.[employee_id]
              , [val_currency_code_id]      = ISNULL(d_mms_mms_tran.[val_currency_code_id],1)
              , [original_tran_item_id]     = d_mms_tran_item_refund.[original_tran_item_id]
              , [original_mms_tran_id]      = d_mms_tran_item_refund.[original_mms_tran_id]
              , [domain_name]               = d_mms_mms_tran.[domain_name]
              , [post_date_time]            = d_mms_mms_tran.[post_date_time]
              , [tran_date]                 = d_mms_mms_tran.[tran_date]
              --, [tran_amount]               = ISNULL(d_mms_mms_tran.[tran_amount],0)
              , [currency_code]             = ISNULL(d_mms_mms_tran.[original_currency_code],'USD')
              , [tran_item_quantity]        = ISNULL(d_mms_tran_item.[sales_quantity],0)
              , [tran_item_amount]          = ISNULL(d_mms_tran_item.[sales_dollar_amount],0)
              , [tran_item_sales_tax]       = ISNULL(d_mms_tran_item.[sales_tax_amount],0)
              , [tran_item_discount_amount] = ISNULL(d_mms_tran_item.[sales_discount_dollar_amount],0)
              , [promotion_code]            = d_mms_tran_item_discount.[promotion_code]
              , [inserted_date_time]        = d_mms_tran_item_discount.[inserted_date_time]
              , [updated_date_time]         = d_mms_mms_tran.[updated_date_time]
              , [dv_deleted]                = CAST(CASE WHEN (NOT d_mms_mms_tran.[voided_flag] Is Null AND d_mms_mms_tran.[voided_flag] = 'Y') THEN 1 ELSE 0 END AS bit)
              , d_mms_tran_item_discount.[bk_hash]
              , d_mms_tran_item_discount.[p_mms_tran_item_discount_id]
              , [dv_load_date_time] = CASE WHEN d_mms_tran_item_discount.[dv_load_date_time] > d_mms_tran_item.[dv_load_date_time] AND d_mms_tran_item_discount.[dv_load_date_time] > d_mms_mms_tran.[dv_load_date_time] THEN d_mms_tran_item_discount.[dv_load_date_time]
                                           WHEN d_mms_tran_item.[dv_load_date_time] > d_mms_tran_item_discount.[dv_load_date_time] AND d_mms_tran_item.[dv_load_date_time] > d_mms_mms_tran.[dv_load_date_time] THEN d_mms_tran_item.[dv_load_date_time]
                                           ELSE d_mms_mms_tran.[dv_load_date_time] END
              , [dv_batch_id] = CASE WHEN d_mms_tran_item_discount.[dv_batch_id] > d_mms_tran_item.[dv_batch_id] AND d_mms_tran_item_discount.[dv_batch_id] > d_mms_mms_tran.[dv_batch_id] THEN d_mms_tran_item_discount.[dv_batch_id]
                                     WHEN d_mms_tran_item.[dv_batch_id] > d_mms_tran_item_discount.[dv_batch_id] AND d_mms_tran_item.[dv_batch_id] > d_mms_mms_tran.[dv_batch_id] THEN d_mms_tran_item.[dv_batch_id]
                                     ELSE d_mms_mms_tran.[dv_batch_id] END
           FROM [dbo].[d_mms_mms_tran] d_mms_mms_tran
                INNER JOIN [dbo].[d_mms_reason_code] d_mms_reason_code
                  ON d_mms_reason_code.[dim_mms_transaction_reason_key] = d_mms_mms_tran.[dim_mms_transaction_reason_key]
                --INNER JOIN [dbo].[d_mms_tran_item] d_mms_tran_item
                --  ON d_mms_tran_item.[fact_mms_sales_transaction_key] = d_mms_mms_tran.[fact_mms_sales_transaction_key]
                INNER JOIN [dbo].[d_mms_tran_item] d_mms_tran_item
                  --( SELECT d_mms_tran_item.*
                  --       , RowRank = RANK() OVER (PARTITION BY d_mms_tran_item.[fact_mms_sales_transaction_item_key] ORDER BY d_mms_tran_item.[tran_item_id] ASC)
                  --       , RowNumber = ROW_NUMBER() OVER (PARTITION BY d_mms_tran_item.[fact_mms_sales_transaction_item_key] ORDER BY d_mms_tran_item.[tran_item_id] ASC)
                  --    FROM [dbo].[d_mms_tran_item] d_mms_tran_item
                  --) d_mms_tran_item
                  ON d_mms_tran_item.[fact_mms_sales_transaction_key] = d_mms_mms_tran.[fact_mms_sales_transaction_key]
                     --AND (d_mms_tran_item.RowRank = 1 AND d_mms_tran_item.RowNumber = 1)
                --INNER JOIN [dbo].[d_mms_product] d_mms_product
                --  ON d_mms_product.[dim_mms_product_key] = d_mms_tran_item.[dim_mms_product_key]
                INNER JOIN [dbo].[d_mms_membership] d_mms_membership
                  ON d_mms_membership.[dim_mms_membership_key] = d_mms_mms_tran.[dim_mms_membership_key]
                INNER JOIN [dbo].[d_mms_member] d_mms_member
                  ON d_mms_member.[dim_mms_member_key] = d_mms_mms_tran.[dim_mms_member_key]
                INNER JOIN [dbo].[d_mms_tran_item_discount] d_mms_tran_item_discount
                  ON d_mms_tran_item_discount.[tran_item_id] = d_mms_tran_item.[tran_item_id]
                INNER JOIN [dbo].[d_mms_pricing_discount] d_mms_pricing_discount
                  ON d_mms_pricing_discount.[pricing_discount_id] = d_mms_tran_item_discount.[pricing_discount_id]
                LEFT OUTER JOIN
                  ( SELECT d_mms_tran_item_refund.[tran_item_id]
                         , d_mms_tran_item_refund.[original_tran_item_id]
                         , [original_mms_tran_id] = d_mms_tran_item.[mms_tran_id]
                         --, RowRank = RANK() OVER (PARTITION BY d_mms_tran_item.[fact_mms_sales_transaction_item_key] ORDER BY d_mms_tran_item.[tran_item_id] ASC)
                         --, RowNumber = ROW_NUMBER() OVER (PARTITION BY d_mms_tran_item.[fact_mms_sales_transaction_item_key] ORDER BY d_mms_tran_item.[tran_item_id] ASC)
                      FROM [dbo].[d_mms_tran_item_refund] d_mms_tran_item_refund
                           INNER JOIN [dbo].[d_mms_tran_item] d_mms_tran_item
                             ON d_mms_tran_item.[tran_item_id] = d_mms_tran_item_refund.[original_tran_item_id]
                  ) d_mms_tran_item_refund
                  ON d_mms_tran_item_refund.[tran_item_id] = d_mms_tran_item.[tran_item_id]
                     --AND (d_mms_tran_item_refund.RowRank = 1 AND d_mms_tran_item_refund.RowNumber = 1)
             WHERE d_mms_membership.[membership_type_id] <> 134  --House Account
               AND ( d_mms_mms_tran.[val_tran_type_id] = 5
                  OR ( d_mms_mms_tran.[val_tran_type_id] = 1
                       AND d_mms_reason_code.[reason_code_id] IN (37,114) )
                  OR ( d_mms_mms_tran.[val_tran_type_id] = 3
                       AND d_mms_reason_code.[reason_code_id] IN (35,107)
                       AND (d_mms_mms_tran.[transaction_edited_flag] Is Null OR d_mms_mms_tran.[transaction_edited_flag] = 'N')  --Only Applies to val_tran_type_id = 3--Sale
                       AND (d_mms_mms_tran.[reversal_flag] Is Null OR d_mms_mms_tran.[reversal_flag] = 'N')  --Only Applies to val_tran_type_id = 3--Sale
                       AND d_mms_mms_tran.[original_mms_tran_id] Is Null ) )  --Only Applies to val_tran_type_id = 3--Sale
               AND d_mms_mms_tran.[post_date_time] >= '2017-01-01'
               AND ( ( d_mms_mms_tran.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
                   AND d_mms_mms_tran.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END )
                  OR ( d_mms_tran_item.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
                   AND d_mms_tran_item.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END )
                  OR ( d_mms_tran_item_discount.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
                   AND d_mms_tran_item_discount.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END ) )
       ) d_mms_tran_item_discount
       CROSS APPLY
         ( SELECT [l_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_tran_item_discount.[tran_item_discount_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_tran_item_discount.[pricing_discount_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_tran_item_discount.[val_discount_reason_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_tran_item_discount.[val_discount_type_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_tran_item_discount.[sales_promotion_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_tran_item_discount.[tran_item_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_tran_item_discount.[product_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_tran_item_discount.[bundle_product_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_tran_item_discount.[mms_tran_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_tran_item_discount.[club_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_tran_item_discount.[membership_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_tran_item_discount.[member_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_tran_item_discount.[reason_code_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_tran_item_discount.[val_tran_type_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_tran_item_discount.[tran_employee_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_tran_item_discount.[val_currency_code_id]),'z#@$k%&P'))),2)

                , [s_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_tran_item_discount.[tran_item_discount_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_tran_item_discount.[domain_name]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_tran_item_discount.[post_date_time], 120),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_tran_item_discount.[tran_date], 120),'z#@$k%&P')
                                                               --+ 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_tran_item_discount.[tran_amount]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_tran_item_discount.[tran_item_quantity]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_tran_item_discount.[tran_item_amount]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_tran_item_discount.[tran_item_sales_tax]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_tran_item_discount.[tran_item_discount_amount]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_tran_item_discount.[promotion_code]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_tran_item_discount.[inserted_date_time], 120),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_tran_item_discount.[updated_date_time], 120),'z#@$k%&P'))),2)

                --, [dv_load_date_time] = ISNULL(d_mms_tran_item_discount.[updated_date_time],d_mms_tran_item_discount.[inserted_date_time])
                --, [dv_batch_id] = CONVERT(bigint, CONVERT(VARCHAR(8), ISNULL(d_mms_tran_item_discount.[updated_date_time],d_mms_tran_item_discount.[inserted_date_time]), 112) + REPLACE(CONVERT(varchar(8), ISNULL(d_mms_tran_item_discount.[updated_date_time],d_mms_tran_item_discount.[inserted_date_time]), 114), ':',''))
                --, [bk_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_tran_item_discount.[tran_item_discount_id]),'z#@$k%&P'))),2)
         ) batch_info
ORDER BY d_mms_tran_item_discount.[dv_batch_id] ASC, d_mms_tran_item_discount.[dv_load_date_time] ASC, ISNULL(d_mms_tran_item_discount.[updated_date_time], d_mms_tran_item_discount.[inserted_date_time]) ASC;

END
