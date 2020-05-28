CREATE PROC [sandbox].[proc_mart_sw_d_mms_tran_item_package] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT d_mms_mms_tran.[package_id]
     , d_mms_mms_tran.[val_package_status_id]
     , d_mms_mms_tran.[tran_item_id]
     , d_mms_mms_tran.[product_id]
     , d_mms_mms_tran.[bundle_product_id]
     , d_mms_mms_tran.[mms_tran_id]
     , d_mms_mms_tran.[club_id]
     , d_mms_mms_tran.[membership_id]
     , d_mms_mms_tran.[member_id]
     , d_mms_mms_tran.[reason_code_id]
     , d_mms_mms_tran.[val_tran_type_id]
     , d_mms_mms_tran.[tran_employee_id]
     , d_mms_mms_tran.[val_currency_code_id]
     , d_mms_mms_tran.[commission_employee_id]
     , d_mms_mms_tran.[original_tran_item_id]
     , d_mms_mms_tran.[original_mms_tran_id]
     , d_mms_mms_tran.[post_date_time]
     , d_mms_mms_tran.[tran_date]
     --, d_mms_mms_tran.[tran_amount]
     , d_mms_mms_tran.[currency_code]
     , d_mms_mms_tran.[tran_item_quantity]
     , d_mms_mms_tran.[tran_item_amount]
     , d_mms_mms_tran.[tran_item_sales_tax]
     , d_mms_mms_tran.[tran_item_discount_amount]
     , d_mms_mms_tran.[tran_item_lt_bucks_amount]
     , d_mms_mms_tran.[created_date_time]
     , d_mms_mms_tran.[inserted_date_time]
     , d_mms_mms_tran.[updated_date_time]
     , d_mms_mms_tran.[lt_bucks_amount]
     , d_mms_mms_tran.[number_of_sessions]
     , d_mms_mms_tran.[sessions_left]
     , d_mms_mms_tran.[sessions_redeemed]
     , d_mms_mms_tran.[sessions_adjusted]
     , d_mms_mms_tran.[session_club_id]
     , d_mms_mms_tran.[session_created_employee_id]
     , d_mms_mms_tran.[session_delivered_employee_id]
     , d_mms_mms_tran.[session_delivered_date_time]
     , d_mms_mms_tran.[bk_hash]
     , d_mms_mms_tran.[p_mms_tran_item_id]
     , d_mms_mms_tran.[dv_load_date_time]
     , d_mms_mms_tran.[dv_batch_id]
     , [dv_hash]    = CONVERT(varchar(32), HASHBYTES('MD5', (batch_info.[l_hash] + batch_info.[s_hash])),2)
     , d_mms_mms_tran.[dv_deleted]
  FROM ( SELECT [package_id]                    = d_mms_package.[package_id]
              , [val_package_status_id]         = d_mms_package.[val_package_status_id]
              , [tran_item_id]                  = d_mms_tran_item.[tran_item_id]
              , [product_id]                    = d_mms_tran_item.[product_id]
              , [bundle_product_id]             = d_mms_tran_item.[bundle_product_id]
              , [mms_tran_id]                   = d_mms_tran_item.[mms_tran_id]
              , [club_id]                       = d_mms_tran_item.[club_id]
              , [membership_id]                 = d_mms_tran_item.[membership_id]
              , [member_id]                     = ISNULL(d_mms_package.[member_id], d_mms_tran_item.[member_id])
              , [reason_code_id]                = d_mms_tran_item.[reason_code_id]
              , [val_tran_type_id]              = d_mms_tran_item.[val_tran_type_id]
              , [tran_employee_id]              = d_mms_tran_item.[tran_employee_id]
              , [val_currency_code_id]          = d_mms_tran_item.[val_currency_code_id]
              , [commission_employee_id]        = d_mms_tran_item.[commission_employee_id]
              , [original_tran_item_id]         = d_mms_tran_item_refund.[original_tran_item_id]
              , [original_mms_tran_id]          = d_mms_tran_item_refund.[original_mms_tran_id]
              , [post_date_time]                = d_mms_tran_item.[post_date_time]
              , [tran_date]                     = d_mms_tran_item.[tran_date]
              --, [tran_amount]                   = d_mms_tran_item.[tran_amount]
              , [currency_code]                 = d_mms_tran_item.[currency_code]
              , [tran_item_quantity]            = (d_mms_tran_item.[tran_item_quantity] + ISNULL(d_mms_tran_item_refund.[tran_item_quantity], 0))
              , [tran_item_amount]              = (d_mms_tran_item.[tran_item_amount] + ISNULL(d_mms_tran_item_refund.[tran_item_amount], 0))
              , [tran_item_sales_tax]           = (d_mms_tran_item.[tran_item_sales_tax] + ISNULL(d_mms_tran_item_refund.[tran_item_sales_tax], 0))
              , [tran_item_discount_amount]     = (d_mms_tran_item.[tran_item_discount_amount] + ISNULL(d_mms_tran_item_refund.[tran_item_discount_amount], 0))
              , [tran_item_lt_bucks_amount]     = (d_mms_tran_item.[tran_item_lt_bucks_amount] + ISNULL(d_mms_tran_item_refund.[tran_item_lt_bucks_amount], 0))
              , [created_date_time]             = d_mms_package.[created_date_time]
              , [inserted_date_time]            = CASE WHEN d_mms_package.[package_id] Is Null THEN d_mms_tran_item.[inserted_date_time] ELSE d_mms_package.[inserted_date_time] END
              , [updated_date_time]             = CASE WHEN d_mms_package.[package_id] Is Null THEN d_mms_tran_item.[updated_date_time] ELSE d_mms_package.[updated_date_time] END
              , d_mms_tran_item.[dv_deleted]
              , [lt_bucks_amount]               = (d_mms_tran_item.[tran_item_lt_bucks_amount] + ISNULL(d_mms_tran_item_refund.[tran_item_lt_bucks_amount], 0))
              , [number_of_sessions]            = ISNULL(d_mms_package.[number_of_sessions],1)
              , [sessions_left]                 = ISNULL(d_mms_package.[sessions_remaining],0)
              , [sessions_redeemed]             = ISNULL(d_mms_package.[number_of_sessions],1) - ISNULL(d_mms_package.[sessions_remaining],0) - ISNULL(d_mms_package.[number_of_sessions_adjusted],0)
              , [sessions_adjusted]             = ISNULL(d_mms_package.[number_of_sessions_adjusted],0)
              , [session_club_id]               = d_mms_package.[club_id]
              , [session_created_employee_id]   = d_mms_package.[created_employee_id]
              , [session_delivered_employee_id] = d_mms_package.[delivered_employee_id]
              , [session_delivered_date_time]   = d_mms_package.[delivered_date_time]
              , d_mms_tran_item.[fact_mms_sales_transaction_item_key]
              , d_mms_tran_item.[bk_hash]
              , d_mms_tran_item.[p_mms_tran_item_id]
              , [dv_load_date_time] = CASE WHEN ISNULL(d_mms_package.[dv_batch_id],-1) > d_mms_tran_item.[dv_batch_id] THEN ISNULL(d_mms_package.[dv_load_date_time],-1) ELSE d_mms_tran_item.[dv_load_date_time] END
              , [dv_batch_id]       = CASE WHEN ISNULL(d_mms_package.[dv_batch_id],-1) > d_mms_tran_item.[dv_batch_id] THEN ISNULL(d_mms_package.[dv_batch_id],-1) ELSE d_mms_tran_item.[dv_batch_id] END
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
                       , [post_date_time]            = d_mms_mms_tran.[post_date_time]
                       , [tran_date]                 = d_mms_mms_tran.[tran_date]
                       --, [tran_amount]               = ISNULL(d_mms_mms_tran.[tran_amount],0)
                       , [currency_code]             = ISNULL(d_mms_mms_tran.[original_currency_code],'USD')
                       , [tran_item_quantity]        = ISNULL(d_mms_tran_item.[sales_quantity],0)
                       , [tran_item_amount]          = ISNULL(d_mms_tran_item.[sales_dollar_amount],0)
                       , [tran_item_sales_tax]       = ISNULL(d_mms_tran_item.[sales_tax_amount],0)
                       , [tran_item_discount_amount] = ISNULL(d_mms_tran_item.[sales_discount_dollar_amount],0)
                       , [tran_item_lt_bucks_amount] = ISNULL(d_mms_tran_item.[item_lt_bucks_amount],0)
                       , [inserted_date_time]        = d_mms_tran_item.[inserted_date_time]
                       , [updated_date_time]         = d_mms_mms_tran.[updated_date_time]
                       , [dv_deleted]                = CAST(CASE WHEN (NOT d_mms_mms_tran.[voided_flag] Is Null AND d_mms_mms_tran.[voided_flag] = 'Y') THEN 1 ELSE 0 END AS bit)
                       , d_mms_tran_item.[fact_mms_sales_transaction_item_key]
                       , d_mms_tran_item.[bk_hash]
                       , d_mms_tran_item.[p_mms_tran_item_id]
                       , [dv_load_date_time] = CASE WHEN d_mms_tran_item.[dv_load_date_time] > d_mms_mms_tran.[dv_load_date_time] THEN d_mms_tran_item.[dv_load_date_time] ELSE d_mms_mms_tran.[dv_load_date_time] END
                       , [dv_batch_id]       = CASE WHEN d_mms_tran_item.[dv_batch_id] > d_mms_mms_tran.[dv_batch_id] THEN d_mms_tran_item.[dv_batch_id] ELSE d_mms_mms_tran.[dv_batch_id] END
                       , d_mms_member.[val_member_type_id]
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
                              --AND (d_mms_tran_item.RowNumber = 1 AND d_mms_tran_item.RowRank = 1)
                         --INNER JOIN [dbo].[d_mms_product] d_mms_product
                         --  ON d_mms_product.[dim_mms_product_key] = d_mms_tran_item.[dim_mms_product_key]
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
                    WHERE d_mms_membership.[membership_type_id] <> 134  --House Account
                      AND NOT d_mms_mms_tran.[val_tran_type_id] IN (4,5)
                      AND (d_mms_mms_tran.[transaction_edited_flag] Is Null OR d_mms_mms_tran.[transaction_edited_flag] = 'N')
                      AND (d_mms_mms_tran.[reversal_flag] Is Null OR d_mms_mms_tran.[reversal_flag] = 'N')
                      AND d_mms_mms_tran.[post_date_time] >= '2019-01-01'

                      AND ( ( d_mms_mms_tran.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
                          AND d_mms_mms_tran.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END )
                         OR ( d_mms_tran_item.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
                          AND d_mms_tran_item.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END )
                         OR (EXISTS
                              ( SELECT *
                                  FROM [dbo].[d_mms_package] d_mms_package
                                  WHERE d_mms_package.[fact_mms_sales_transaction_item_key] = d_mms_tran_item.[fact_mms_sales_transaction_item_key]
                                    AND ( d_mms_package.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
                                      AND d_mms_package.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END ) ))
                         OR (EXISTS
                              ( SELECT *
                                  FROM [dbo].[d_mms_package_session] d_mms_package_session
                                       INNER JOIN [dbo].[d_mms_package] d_mms_package
                                         ON d_mms_package.[fact_mms_package_key] = d_mms_package_session.[fact_mms_package_key]
                                  WHERE d_mms_package.[fact_mms_sales_transaction_item_key] = d_mms_tran_item.[fact_mms_sales_transaction_item_key]
                                    AND ( d_mms_package_session.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
                                      AND d_mms_package_session.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END ) ))
                         OR (EXISTS
                              ( SELECT *
                                  FROM [dbo].[d_mms_package_adjustment] d_mms_package_adjustment
                                       INNER JOIN [dbo].[d_mms_package] d_mms_package
                                         ON d_mms_package.[fact_mms_package_key] = d_mms_package_adjustment.[fact_mms_package_key]
                                  WHERE d_mms_package.[fact_mms_sales_transaction_item_key] = d_mms_tran_item.[fact_mms_sales_transaction_item_key]
                                    AND ( d_mms_package_adjustment.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
                                      AND d_mms_package_adjustment.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END ) )) )

                      AND NOT EXISTS
                        ( SELECT d_mms_mms_tran_refund.*
                            FROM [dbo].[d_mms_mms_tran_refund] d_mms_mms_tran_refund
                            WHERE d_mms_mms_tran_refund.[fact_mms_sales_transaction_key] = d_mms_mms_tran.[fact_mms_sales_transaction_key]
                              AND NOT d_mms_mms_tran_refund.[mms_tran_refund_id] Is Null ) --Not a Refund
                ) d_mms_tran_item
                LEFT OUTER JOIN
                  ( SELECT d_mms_tran_item_refund.[original_tran_item_id]
                         , [original_mms_tran_id]         = d_mms_tran_item.[mms_tran_id]
                         , [tran_item_quantity]           = SUM(ISNULL(d_mms_ti_refund.[sales_quantity],0)) * -1
                         , [tran_item_amount]             = SUM(ISNULL(d_mms_ti_refund.[sales_dollar_amount],0))
                         , [tran_item_sales_tax]          = SUM(ISNULL(d_mms_ti_refund.[sales_tax_amount],0))
                         , [tran_item_discount_amount]    = SUM(ISNULL(d_mms_ti_refund.[sales_discount_dollar_amount],0))
                         , [tran_item_lt_bucks_amount]    = SUM(ISNULL(d_mms_ti_refund.[item_lt_bucks_amount],0))
                         , [tran_item_lt_bucks_sales_tax] = SUM(ISNULL(d_mms_ti_refund.[item_lt_bucks_sales_tax],0))
                      FROM [dbo].[d_mms_tran_item_refund] d_mms_tran_item_refund
                           --INNER JOIN [dbo].[d_mms_tran_item] d_mms_ti_refund
                           --  ON d_mms_ti_refund.[fact_mms_sales_transaction_item_key] = d_mms_tran_item_refund.[fact_mms_sales_transaction_item_key]
                           INNER JOIN [dbo].[d_mms_tran_item] d_mms_ti_refund
                             --( SELECT d_mms_tran_item.*
                             --       , RowRank = RANK() OVER (PARTITION BY d_mms_tran_item.[fact_mms_sales_transaction_item_key] ORDER BY d_mms_tran_item.[tran_item_id] ASC)
                             --       , RowNumber = ROW_NUMBER() OVER (PARTITION BY d_mms_tran_item.[fact_mms_sales_transaction_item_key] ORDER BY d_mms_tran_item.[tran_item_id] ASC)
                             --    FROM [dbo].[d_mms_tran_item] d_mms_tran_item
                             --) d_mms_ti_refund
                             ON d_mms_ti_refund.[fact_mms_sales_transaction_item_key] = d_mms_tran_item_refund.[fact_mms_sales_transaction_item_key]
                                --AND (d_mms_ti_refund.RowNumber = 1 AND d_mms_ti_refund.RowRank = 1)
                           INNER JOIN [dbo].[d_mms_mms_tran] d_mms_mms_t_refund
                             ON d_mms_mms_t_refund.[fact_mms_sales_transaction_key] = d_mms_ti_refund.[fact_mms_sales_transaction_key]
                           --INNER JOIN [dbo].[d_mms_tran_item] d_mms_tran_item
                           --  ON d_mms_tran_item.[tran_item_id] = d_mms_tran_item_refund.[original_tran_item_id]
                           INNER JOIN [dbo].[d_mms_tran_item] d_mms_tran_item
                             --( SELECT d_mms_tran_item.[fact_mms_sales_transaction_item_key]
                             --       , d_mms_tran_item.[fact_mms_sales_transaction_key]
                             --       , d_mms_tran_item.[tran_item_id]
                             --       , d_mms_tran_item.[mms_tran_id]
                             --       , RowRank = RANK() OVER (PARTITION BY d_mms_tran_item.[fact_mms_sales_transaction_item_key] ORDER BY d_mms_tran_item.[tran_item_id] ASC)
                             --       , RowNumber = ROW_NUMBER() OVER (PARTITION BY d_mms_tran_item.[fact_mms_sales_transaction_item_key] ORDER BY d_mms_tran_item.[tran_item_id] ASC)
                             --    FROM [dbo].[d_mms_tran_item] d_mms_tran_item
                             --) d_mms_tran_item
                             ON d_mms_tran_item.[tran_item_id] = d_mms_tran_item_refund.[original_tran_item_id]
                                --AND (d_mms_tran_item.RowNumber = 1 AND d_mms_tran_item.RowRank = 1)
                      WHERE d_mms_mms_t_refund.[val_tran_type_id] = 5
                        AND (d_mms_mms_t_refund.[voided_flag] Is Null OR d_mms_mms_t_refund.[voided_flag] = 'N')
                        AND (d_mms_mms_t_refund.[transaction_edited_flag] Is Null OR d_mms_mms_t_refund.[transaction_edited_flag] = 'N')
                        AND (d_mms_mms_t_refund.[reversal_flag] Is Null OR d_mms_mms_t_refund.[reversal_flag] = 'N')
                        --AND (TI2.MMSTranID > (@MinMMSTranID-1) AND TI2.MMSTranID < (@MaxMMSTranID+1))
                        --AND TI2.MMSTranID BETWEEN @MinMMSTranID AND @MaxMMSTranID
                      GROUP BY d_mms_tran_item_refund.[original_tran_item_id], d_mms_tran_item.[mms_tran_id]
                  ) d_mms_tran_item_refund
                  ON d_mms_tran_item_refund.[original_tran_item_id] = d_mms_tran_item.[tran_item_id]
                LEFT OUTER JOIN
                  ( SELECT d_mms_package.[package_id]
                         , d_mms_package.[fact_mms_sales_transaction_item_key]
                         , d_mms_package.[dim_mms_member_key]
                         , d_mms_member.[member_id]
                         , d_mms_package.[club_id]
                         , d_mms_package.[employee_id]
                         , d_mms_package.[val_package_status_id]
                         , d_mms_package.[created_date_time]
                         , d_mms_package.[inserted_date_time]
                         , d_mms_package.[updated_date_time]
                         , d_mms_package.[package_edit_date_time]
                         , d_mms_package.[number_of_sessions]
                         , d_mms_package.[sessions_remaining]
                         , d_mms_package_adjustment.[number_of_sessions_adjusted]
                         , d_mms_package_session.[created_employee_id]
                         , d_mms_package_session.[delivered_employee_id]
                         , d_mms_package_session.[delivered_date_time]
                         , [dv_load_date_time] = CASE d_timestamp.[dv_source] WHEN 'd_mms_package_session' THEN d_mms_package_session.[dv_load_date_time]
                                                                              WHEN 'd_mms_package_adjustment' THEN d_mms_package_adjustment.[dv_load_date_time]
                                                                              ELSE d_mms_package.[dv_load_date_time] END
                         , [dv_batch_id] = CASE d_timestamp.[dv_source] WHEN 'd_mms_package_session' THEN d_mms_package_session.[dv_batch_id]
                                                                        WHEN 'd_mms_package_adjustment' THEN d_mms_package_adjustment.[dv_batch_id]
                                                                        ELSE d_mms_package.[dv_batch_id] END
                         , RowRank = RANK() OVER (PARTITION BY d_mms_package.[fact_mms_sales_transaction_item_key] ORDER BY d_mms_package.[package_id] ASC)
                         , RowNumber = ROW_NUMBER() OVER (PARTITION BY d_mms_package.[fact_mms_sales_transaction_item_key] ORDER BY d_mms_package.[package_id] ASC)
                      FROM [dbo].[d_mms_package] d_mms_package
                           INNER JOIN [dbo].[d_mms_member] d_mms_member
                             ON d_mms_member.[dim_mms_member_key] = d_mms_package.[dim_mms_member_key]
                           LEFT OUTER JOIN
                             ( SELECT d_mms_package_adjustment.[fact_mms_package_key]
                                    , [number_of_sessions_adjusted] = SUM(ISNULL(d_mms_package_adjustment.[number_of_sessions_adjusted],0))
                                    , [package_adjustment_amount] = SUM(ISNULL(d_mms_package_adjustment.[package_adjustment_amount],0))
                                    , [dv_load_date_time] = MAX(d_mms_package_adjustment.[dv_load_date_time])
                                    , [dv_batch_id] = MAX(d_mms_package_adjustment.[dv_batch_id])
                                 FROM [dbo].[d_mms_package_adjustment] d_mms_package_adjustment
                                 GROUP BY d_mms_package_adjustment.[fact_mms_package_key]
                             ) d_mms_package_adjustment
                             ON d_mms_package_adjustment.[fact_mms_package_key] = d_mms_package.[fact_mms_package_key]
                           LEFT OUTER JOIN
                             ( SELECT d_mms_package_session.[fact_mms_package_key]
                                    , d_mms_package_session.[delivered_dim_club_key]  --ClubID
                                    --*$dpw$*--missing column
                                    , LNK.[created_employee_id]
                                    , [delivered_employee_id] = d_mms_employee_delivered.[employee_id]
                                    --*$dpw$*--missing column
                                    , SAT.[delivered_date_time]
                                    , d_mms_package_session.[dv_load_date_time]
                                    , d_mms_package_session.[dv_batch_id]
                                    , RowRank = RANK() OVER (PARTITION BY d_mms_package_session.[fact_mms_package_key] ORDER BY SAT.[delivered_date_time] ASC, SAT.[inserted_date_time] ASC)
                                    , RowNumber = ROW_NUMBER() OVER (PARTITION BY d_mms_package_session.[fact_mms_package_key] ORDER BY SAT.[delivered_date_time] ASC, SAT.[inserted_date_time] ASC)
                                 FROM [dbo].[d_mms_package_session] d_mms_package_session
                                      INNER JOIN [dbo].[p_mms_package_session] PIT
                                        ON PIT.[p_mms_package_session_id] = d_mms_package_session.[p_mms_package_session_id]
                                      INNER JOIN [dbo].[l_mms_package_session] LNK
                                        ON LNK.[bk_hash] = PIT.[bk_hash]
                                           AND LNK.[l_mms_package_session_id] = PIT.[l_mms_package_session_id]
                                      INNER JOIN [dbo].[s_mms_package_session] SAT
                                        ON SAT.[bk_hash] = PIT.[bk_hash]
                                           AND SAT.[s_mms_package_session_id] = PIT.[s_mms_package_session_id]
                                      INNER JOIN [dbo].[d_mms_employee] d_mms_employee_delivered
                                        ON d_mms_employee_delivered.[dim_employee_key] = d_mms_package_session.[delivered_dim_employee_key]
                             ) d_mms_package_session 
                             ON d_mms_package_session.[fact_mms_package_key] = d_mms_package.[fact_mms_package_key]
                                AND d_mms_package_session.RowRank = 1 AND d_mms_package_session.RowNumber = 1
                           CROSS APPLY
                             ( SELECT dv_source = CASE WHEN ( (ISNULL(d_mms_package_session.[dv_batch_id],-1) > ISNULL(d_mms_package_adjustment.[dv_batch_id],-1))
                                                          AND (ISNULL(d_mms_package_session.[dv_batch_id],-1) > ISNULL(d_mms_package.[dv_batch_id],-1)) )
                                                            THEN 'd_mms_package_session'
                                                       WHEN ( (ISNULL(d_mms_package_adjustment.[dv_batch_id],-1) > ISNULL(d_mms_package_session.[dv_batch_id],-1))
                                                          AND (ISNULL(d_mms_package_adjustment.[dv_batch_id],-1) > ISNULL(d_mms_package.[dv_batch_id],-1)) )
                                                            THEN 'd_mms_package_adjustment'
                                                       ELSE 'd_mms_package' END
                             ) d_timestamp
                  ) d_mms_package
                  ON d_mms_package.[fact_mms_sales_transaction_item_key] = d_mms_tran_item.[fact_mms_sales_transaction_item_key]
                     AND d_mms_package.RowRank = 1 AND d_mms_package.RowNumber = 1
           WHERE ( ( d_mms_tran_item.[val_member_type_id] <> 4
                     AND d_mms_tran_item.[tran_employee_id] <> -5
                     AND (NOT d_mms_package.[package_id] Is Null AND d_mms_package.[val_package_status_id] <> 4) -- Voided
                     AND ( d_mms_tran_item.[product_id] IN (2785,3436,14058,14288,3232,3965,15808) --Onboarding Session, FitPoint, UO Know It, myLT Buck$ - Custom Know It, Comp PT Session, Equipment Orientation, Tennis - Free Orientation Session
                        OR d_mms_tran_item.[product_id] = 12805 --Know It Session
                        OR d_mms_tran_item.[product_id] IN (7198,7444,7893,9748,11589,11786,11788,11789,11790,14059,14072) --myHealthScore Products
                        OR d_mms_tran_item.[product_id] IN (13295,1644,1868,259)
                        OR d_mms_tran_item.[product_id] IN (16720,16721)  --Spa Onboarding Package
                        OR d_mms_tran_item.[product_id] IN (11335) ) )
                OR ( d_mms_tran_item.[tran_employee_id] = -5
                     AND (d_mms_package.[package_id] Is Null OR d_mms_package.[val_package_status_id] <> 4) ) -- Voided
                OR ( d_mms_tran_item.[tran_employee_id] <> -5
                     AND (d_mms_tran_item.[tran_item_quantity] + ISNULL(d_mms_tran_item_refund.[tran_item_quantity], 0)) > 0
                     AND (d_mms_tran_item.[tran_item_amount] + ISNULL(d_mms_tran_item_refund.[tran_item_amount], 0)) >= 0
                     AND (d_mms_package.[package_id] Is Null OR d_mms_package.[val_package_status_id] <> 4) -- Voided
                     AND EXISTS
                       ( SELECT d_udw_product_master_history.[product_id]
                           FROM [dbo].[d_udwcloudsync_product_master_history] d_udw_product_master_history
                           WHERE d_udw_product_master_history.source_system = 'MMS'
                             AND ( d_udw_product_master_history.[reporting_division] IN ('PT Division', 'Personal Training')
                                OR d_udw_product_master_history.[reporting_department] IN ('Aquatics','Cycle','Leagues','Racquetball','Run','Squash','Tri','Ultimate Hoops','Yoga','Massage','Spa','Tennis','Kids Activities') )
                             AND d_udw_product_master_history.[product_id] = d_mms_tran_item.[product_id]
                             AND d_mms_tran_item.[post_date_time] >= d_udw_product_master_history.[effective_date_time]
                             AND d_mms_tran_item.[post_date_time] < d_udw_product_master_history.[expiration_date_time] ) ) )
       ) d_mms_mms_tran
       CROSS APPLY
         ( SELECT [l_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mms_tran.[tran_item_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mms_tran.[product_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mms_tran.[bundle_product_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mms_tran.[mms_tran_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mms_tran.[club_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mms_tran.[membership_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mms_tran.[member_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mms_tran.[reason_code_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mms_tran.[val_tran_type_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mms_tran.[tran_employee_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mms_tran.[val_currency_code_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mms_tran.[commission_employee_id]),'z#@$k%&P'))),2)

                , [s_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mms_tran.[tran_item_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mms_tran.[post_date_time], 120),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mms_tran.[tran_date], 120),'z#@$k%&P')
                                                               --+ 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mms_tran.[tran_amount]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mms_tran.[tran_item_quantity]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mms_tran.[tran_item_amount]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mms_tran.[tran_item_sales_tax]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mms_tran.[tran_item_discount_amount]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mms_tran.[inserted_date_time], 120),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mms_tran.[updated_date_time], 120),'z#@$k%&P'))),2)

                --, [dv_load_date_time] = ISNULL(d_mms_mms_tran.[updated_date_time],d_mms_mms_tran.[inserted_date_time])
                --, [dv_batch_id] = CONVERT(bigint, CONVERT(VARCHAR(8), ISNULL(d_mms_mms_tran.[updated_date_time],d_mms_mms_tran.[inserted_date_time]), 112) + REPLACE(CONVERT(varchar(8), ISNULL(d_mms_mms_tran.[updated_date_time],d_mms_mms_tran.[inserted_date_time]), 114), ':',''))
                --, [bk_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mms_tran.[tran_item_id]),'z#@$k%&P'))),2)
         ) batch_info
ORDER BY d_mms_mms_tran.[dv_batch_id] ASC, d_mms_mms_tran.[dv_load_date_time] ASC, ISNULL(d_mms_mms_tran.[updated_date_time], d_mms_mms_tran.[inserted_date_time]) ASC;

END
