CREATE PROC [sandbox].[proc_mart_sw_d_udw_product_master_history] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT [d_udwcloudsync_product_master_history_id]
     , [product_id]
     , [source_system]
     , [effective_date_time]
     , [expiration_date_time]
     , [dim_mms_product_key]
     , [dim_reporting_hierarchy_key]
     , [product_description]
     , [reporting_department]
     , [reporting_division]
     , [reporting_product_group]
     , [reporting_product_group_gl_account]
     , [reporting_product_group_sort_order]
     , [reporting_region_type]
     , [reporting_sub_division]
     , [revenue_product_group_discount_gl_account]
     , [revenue_product_group_refund_gl_account]
     , [sales_category_description]
     , [inserted_date_time]
     , [updated_date_time]
     , [bk_hash]
     , [p_udwcloudsync_product_master_id]
     , [dv_load_date_time]
     , [dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (batch_info.[l_hash] + batch_info.[s_hash])),2)
     , [dv_deleted] = CAST(deleted_flag AS bit)
  FROM ( SELECT [d_udwcloudsync_product_master_history_id]
              , [product_id]
              , [source_system]
              , [effective_date_time]
              , [expiration_date_time]
              , [dim_mms_product_key]
              , [dim_reporting_hierarchy_key]
              , [product_description]
              , [reporting_department]
              , [reporting_division]
              , [reporting_product_group]
              , [reporting_product_group_gl_account]
              , [reporting_product_group_sort_order]
              , [reporting_region_type]
              , [reporting_sub_division]
              , [revenue_product_group_discount_gl_account]
              , [revenue_product_group_refund_gl_account]
              , [sales_category_description]
              , [inserted_date_time] = [dv_inserted_date_time]
              , [updated_date_time]  = [dv_updated_date_time]
              , [bk_hash]
              , [p_udwcloudsync_product_master_id]
              , [dv_load_date_time]
              , [dv_batch_id]
              , [deleted_flag]
              , [dv_deleted] = CAST(deleted_flag AS bit)
              , RowRank = RANK() OVER (PARTITION BY [dim_mms_product_key], [dim_reporting_hierarchy_key], [effective_date_time] ORDER BY [p_udwcloudsync_product_master_id] ASC)
              , RowNumber = ROW_NUMBER() OVER (PARTITION BY [dim_mms_product_key], [dim_reporting_hierarchy_key], [effective_date_time] ORDER BY [p_udwcloudsync_product_master_id] ASC)
           FROM [dbo].[d_udwcloudsync_product_master_history] UDW
           WHERE UDW.[source_system] = 'MMS'
             AND ( UDW.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
               AND UDW.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END )
       ) d_udw_product_master_history
       CROSS APPLY
         ( SELECT [l_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, d_udw_product_master_history.[d_udwcloudsync_product_master_history_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_udw_product_master_history.[product_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(d_udw_product_master_history.[dim_mms_product_key],'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(d_udw_product_master_history.[dim_reporting_hierarchy_key],'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(d_udw_product_master_history.[bk_hash],'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_udw_product_master_history.[p_udwcloudsync_product_master_id]),'z#@$k%&P'))),2)

                , [s_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, d_udw_product_master_history.[d_udwcloudsync_product_master_history_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(d_udw_product_master_history.[source_system],'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_udw_product_master_history.[effective_date_time], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_udw_product_master_history.[expiration_date_time], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(d_udw_product_master_history.[product_description],'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(d_udw_product_master_history.[reporting_department],'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(d_udw_product_master_history.[reporting_division],'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(d_udw_product_master_history.[reporting_product_group],'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_udw_product_master_history.[reporting_product_group_gl_account]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_udw_product_master_history.[reporting_product_group_sort_order]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(d_udw_product_master_history.[reporting_region_type],'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(d_udw_product_master_history.[reporting_sub_division],'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(d_udw_product_master_history.[revenue_product_group_discount_gl_account],'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(d_udw_product_master_history.[revenue_product_group_refund_gl_account],'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(d_udw_product_master_history.[sales_category_description],'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_udw_product_master_history.[inserted_date_time], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_udw_product_master_history.[updated_date_time], 120),'z#@$k%&P'))),2)
         ) batch_info
  WHERE d_udw_product_master_history.RowRank = 1
    AND d_udw_product_master_history.RowNumber = 1
ORDER BY d_udw_product_master_history.[dv_batch_id] ASC, d_udw_product_master_history.[dv_load_date_time] ASC, ISNULL(d_udw_product_master_history.[updated_date_time], d_udw_product_master_history.[inserted_date_time]) ASC;

END
