CREATE PROC [sandbox].[proc_mart_sw_d_mms_tran_item_fee] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT d_mms_tran_item_fee.[tran_item_id]
     , d_mms_tran_item_fee.[product_id]
     , d_mms_tran_item_fee.[bundle_product_id]
     , d_mms_tran_item_fee.[mms_tran_id]
     , d_mms_tran_item_fee.[club_id]
     , d_mms_tran_item_fee.[membership_id]
     , d_mms_tran_item_fee.[member_id]
     , d_mms_tran_item_fee.[reason_code_id]
     , d_mms_tran_item_fee.[val_tran_type_id]
     , d_mms_tran_item_fee.[tran_employee_id]
     , d_mms_tran_item_fee.[val_currency_code_id]
     , d_mms_tran_item_fee.[commission_employee_id]
     , d_mms_tran_item_fee.[original_tran_item_id]
     , d_mms_tran_item_fee.[original_mms_tran_id]
     , d_mms_tran_item_fee.[post_date_time]
     , d_mms_tran_item_fee.[tran_date]
     --, d_mms_tran_item_fee.[tran_amount]
     , d_mms_tran_item_fee.[currency_code]
     , d_mms_tran_item_fee.[ip_address]
     , d_mms_tran_item_fee.[tran_item_quantity]
     , d_mms_tran_item_fee.[tran_item_amount]
     , d_mms_tran_item_fee.[tran_item_sales_tax]
     , d_mms_tran_item_fee.[tran_item_discount_amount]
     , d_mms_tran_item_fee.[inserted_date_time]
     , d_mms_tran_item_fee.[updated_date_time]
     , d_mms_tran_item_fee.[bk_hash]
     , d_mms_tran_item_fee.[p_mms_tran_item_id]
     , d_mms_tran_item_fee.[dv_load_date_time]
     , d_mms_tran_item_fee.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (batch_info.[l_hash] + batch_info.[s_hash])),2)
     , d_mms_tran_item_fee.[dv_deleted]
  FROM ( SELECT [tran_item_id]              = d_mms_tran_item.[tran_item_id]
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
              , [commission_employee_id]    = d_mms_sale_commission.[employee_id]
              , [original_tran_item_id]     = d_mms_tran_item_refund.[original_tran_item_id]
              , [original_mms_tran_id]      = d_mms_tran_item_refund.[original_mms_tran_id]
              , [post_date_time]            = d_mms_mms_tran.[post_date_time]
              , [tran_date]                 = d_mms_mms_tran.[tran_date]
              --, [tran_amount]               = ISNULL(d_mms_mms_tran.[tran_amount],0)
              , [currency_code]             = ISNULL(d_mms_mms_tran.[original_currency_code],'USD')
              , [ip_address]                = SAT.[ip_address]
              , [tran_item_quantity]        = ISNULL(d_mms_tran_item.[sales_quantity],0)
              , [tran_item_amount]          = ISNULL(d_mms_tran_item.[sales_dollar_amount],0)
              , [tran_item_sales_tax]       = ISNULL(d_mms_tran_item.[sales_tax_amount],0)
              , [tran_item_discount_amount] = ISNULL(d_mms_tran_item.[sales_discount_dollar_amount],0)
              , [inserted_date_time]        = d_mms_tran_item.[inserted_date_time]
              , [updated_date_time]         = d_mms_mms_tran.[updated_date_time]
              , [dv_deleted]                = CAST(CASE WHEN (NOT d_mms_mms_tran.[voided_flag] Is Null AND d_mms_mms_tran.[voided_flag] = 'Y') THEN 1 ELSE 0 END AS bit)
              , d_mms_tran_item.[bk_hash]
              , d_mms_tran_item.[p_mms_tran_item_id]
              , [dv_load_date_time] = CASE WHEN d_mms_tran_item.[dv_load_date_time] > d_mms_mms_tran.[dv_load_date_time] THEN d_mms_tran_item.[dv_load_date_time] ELSE d_mms_mms_tran.[dv_load_date_time] END
              , [dv_batch_id]       = CASE WHEN d_mms_tran_item.[dv_batch_id] > d_mms_mms_tran.[dv_batch_id] THEN d_mms_tran_item.[dv_batch_id] ELSE d_mms_mms_tran.[dv_batch_id] END
           FROM [dbo].[d_mms_mms_tran] d_mms_mms_tran
                INNER JOIN [dbo].[p_mms_mms_tran] PIT
                  ON PIT.[bk_hash] = d_mms_mms_tran.[bk_hash]
                     AND PIT.[p_mms_mms_tran_id] = d_mms_mms_tran.[p_mms_mms_tran_id]
                INNER JOIN [dbo].[s_mms_mms_tran] SAT
                  ON SAT.[bk_hash] = PIT.[bk_hash]
                     AND SAT.[s_mms_mms_tran_id] = PIT.[s_mms_mms_tran_id]
                INNER JOIN [dbo].[d_mms_reason_code] d_mms_reason_code
                  ON d_mms_reason_code.[dim_mms_transaction_reason_key] = d_mms_mms_tran.[dim_mms_transaction_reason_key]
                INNER JOIN [dbo].[d_mms_tran_item] d_mms_tran_item
                  --( SELECT d_mms_tran_item.*
                  --       , RowRank = RANK() OVER (PARTITION BY d_mms_tran_item.[fact_mms_sales_transaction_item_key] ORDER BY d_mms_tran_item.[tran_item_id] ASC)
                  --       , RowNumber = ROW_NUMBER() OVER (PARTITION BY d_mms_tran_item.[fact_mms_sales_transaction_item_key] ORDER BY d_mms_tran_item.[tran_item_id] ASC)
                  --    FROM [dbo].[d_mms_tran_item] d_mms_tran_item
                  --) d_mms_tran_item
                  ON d_mms_tran_item.[fact_mms_sales_transaction_key] = d_mms_mms_tran.[fact_mms_sales_transaction_key]
                     --AND (d_mms_tran_item.RowNumber = 1 AND d_mms_tran_item.RowRank = 1)
                --INNER JOIN [dbo].[d_mms_tran_item] d_mms_tran_item
                --  ON d_mms_tran_item.[fact_mms_sales_transaction_key] = d_mms_mms_tran.[fact_mms_sales_transaction_key]
                INNER JOIN [dbo].[d_mms_product] d_mms_product
                  ON d_mms_product.[dim_mms_product_key] = d_mms_tran_item.[dim_mms_product_key]
                INNER JOIN [dbo].[d_mms_membership] d_mms_membership
                  ON d_mms_membership.[dim_mms_membership_key] = d_mms_mms_tran.[dim_mms_membership_key]
                INNER JOIN [dbo].[d_mms_member] d_mms_member
                  ON d_mms_member.[dim_mms_member_key] = d_mms_mms_tran.[dim_mms_member_key]
                LEFT OUTER JOIN
                  ( SELECT d_mms_sale_commission.[fact_mms_sales_transaction_item_key]
                         , d_mms_employee.[employee_id]
                         , RowRank = RANK() OVER (PARTITION BY d_mms_sale_commission.[fact_mms_sales_transaction_item_key] ORDER BY d_mms_sale_commission.[sale_commission_id] ASC)
                         , RowNumber = ROW_NUMBER() OVER (PARTITION BY d_mms_sale_commission.[fact_mms_sales_transaction_item_key] ORDER BY d_mms_sale_commission.[sale_commission_id] ASC)
                      FROM [dbo].[d_mms_sale_commission] d_mms_sale_commission
                           INNER JOIN [dbo].[d_mms_employee] d_mms_employee
                             ON d_mms_employee.[dim_employee_key] = d_mms_sale_commission.[dim_employee_key]
                  ) d_mms_sale_commission
                  ON d_mms_sale_commission.[fact_mms_sales_transaction_item_key] = d_mms_tran_item.[fact_mms_sales_transaction_item_key]
                     AND d_mms_sale_commission.RowRank = 1 AND d_mms_sale_commission.RowNumber = 1
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
           WHERE ( ( d_mms_mms_tran.[val_tran_type_id] = 1
                     AND (NOT d_mms_product.[revenue_category] Is Null AND d_mms_product.[revenue_category] IN ('RC50025','RC50027','RC50031','RC50161'))
                     AND ( ( d_mms_reason_code.[reason_code_id] = 37 AND d_mms_product.[assess_as_dues_flag] = 'N' AND d_mms_product.[junior_member_dues_flag] = 'N'
                             AND ( (d_mms_tran_item.[product_id] = 88 AND (d_mms_tran_item.[sales_dollar_amount] = 0 OR d_mms_sale_commission.[employee_id] Is Null))
                                OR (d_mms_tran_item.[sales_dollar_amount] <> 0 OR NOT d_mms_sale_commission.[employee_id] Is Null) ) )
                        OR ( d_mms_reason_code.[reason_code_id] IN (28,114,238,257,264,265,266) ) ) )
                OR ( d_mms_mms_tran.[val_tran_type_id] = 3
                     AND (NOT d_mms_product.[revenue_category] Is Null AND d_mms_product.[revenue_category] IN ('RC50025','RC50027','RC50031'))
                     AND d_mms_reason_code.[reason_code_id] IN (35,292)
                     AND (NOT d_mms_product.[gl_account_number] Is Null AND d_mms_product.[gl_account_number] IN ('4006','4010','4015','4016','4032','4995'))
                     AND d_mms_mms_tran.[transaction_edited_flag] = 'N'  --Only Applies to val_tran_type_id = 3--Sale
                     AND d_mms_mms_tran.[reversal_flag] = 'N'  --Only Applies to val_tran_type_id = 3--Sale
                     AND d_mms_mms_tran.[original_mms_tran_id] Is Null )  --Only Applies to val_tran_type_id = 3--Sale
                OR ( d_mms_mms_tran.[val_tran_type_id] IN (4,5)
                     AND (NOT d_mms_product.[revenue_category] Is Null AND d_mms_product.[revenue_category] IN ('RC50025','RC50027','RC50031','RC50092','RC50161')) ) )
             AND d_mms_mms_tran.[post_date_time] >= '2018-01-01'

             AND ( ( d_mms_mms_tran.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
                 AND d_mms_mms_tran.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END )
                OR ( d_mms_tran_item.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
                 AND d_mms_tran_item.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END ) )
       ) d_mms_tran_item_fee
       CROSS APPLY
         ( SELECT [l_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_tran_item_fee.[tran_item_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_tran_item_fee.[product_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_tran_item_fee.[bundle_product_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_tran_item_fee.[mms_tran_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_tran_item_fee.[club_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_tran_item_fee.[membership_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_tran_item_fee.[member_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_tran_item_fee.[reason_code_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_tran_item_fee.[val_tran_type_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_tran_item_fee.[tran_employee_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_tran_item_fee.[val_currency_code_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_tran_item_fee.[commission_employee_id]),'z#@$k%&P'))),2)

                , [s_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_tran_item_fee.[tran_item_id]),'z#@$k%&P')
                                                                  --+ 'P%#&z$@k' + ISNULL(d_mms_tran_item_fee.[domain_name],'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_tran_item_fee.[post_date_time], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_tran_item_fee.[tran_date], 120),'z#@$k%&P')
                                                                  --+ 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_tran_item_fee.[tran_amount]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_tran_item_fee.[tran_item_quantity]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_tran_item_fee.[tran_item_amount]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_tran_item_fee.[tran_item_sales_tax]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_tran_item_fee.[tran_item_discount_amount]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_tran_item_fee.[inserted_date_time], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_tran_item_fee.[updated_date_time], 120),'z#@$k%&P'))),2)

                --, [dv_load_date_time] = ISNULL(d_mms_tran_item_fee.[updated_date_time],d_mms_tran_item_fee.[inserted_date_time])
                --, [dv_batch_id] = CONVERT(bigint, CONVERT(VARCHAR(8), ISNULL(d_mms_tran_item_fee.[updated_date_time],d_mms_tran_item_fee.[inserted_date_time]), 112) + REPLACE(CONVERT(varchar(8), ISNULL(d_mms_tran_item_fee.[updated_date_time],d_mms_tran_item_fee.[inserted_date_time]), 114), ':',''))
                --, [bk_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_tran_item_fee.[tran_item_id]),'z#@$k%&P'))),2)
         ) batch_info
ORDER BY d_mms_tran_item_fee.[dv_batch_id] ASC, d_mms_tran_item_fee.[dv_load_date_time] ASC, ISNULL(d_mms_tran_item_fee.[updated_date_time], d_mms_tran_item_fee.[inserted_date_time]) ASC;

END
