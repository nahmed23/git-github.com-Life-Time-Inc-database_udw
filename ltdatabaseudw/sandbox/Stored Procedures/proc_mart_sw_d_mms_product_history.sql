CREATE PROC [sandbox].[proc_mart_sw_d_mms_product_history] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT [product_id] = CASE WHEN NOT PIT.[product_id] Is Null THEN PIT.[product_id] ELSE CONVERT(int, PIT.[bk_hash]) END
     , d_mms_product_history.[department_id]
     , d_mms_product_history.[gl_over_ride_club_id]
     , LNK.[val_assessment_day_id]
     , LNK.[val_employee_level_type_id]
     , LNK.[val_gl_group_id]
     , LNK.[val_product_status_id]
     , LNK.[val_recurrent_product_type_id]
     , SAT.[description]
     , SAT.[gl_account_number]
     , d_mms_product_history.[gl_department_code]
     , d_mms_product_history.[gl_product_code]
     , SAT.[gl_sub_account_number]
     , d_mms_product_history.[lt_buck_cost_percent]
     , SAT.[name]
     , d_mms_product_history.[pay_component]
     , d_mms_product_history.[revenue_category]
     , d_mms_product_history.[sku]
     , SAT.[sort_order]
     , d_mms_product_history.[spend_category]
     , d_mms_product_history.[workday_account]
     , d_mms_product_history.[workday_cost_center]
     , d_mms_product_history.[workday_offering]
     , d_mms_product_history.[workday_over_ride_region]
     , d_mms_product_history.[workday_revenue_product_group_account]
     , d_mms_product_history.[assess_as_dues_flag]
     , d_mms_product_history.[access_by_price_paid_flag]
     , SAT.[allow_zero_dollar_flag]
     , SAT.[bundle_product_flag]
     , SAT.[complete_package_flag]
     , SAT.[confirm_member_data_flag]
     , d_mms_product_history.[deferred_revenue_flag]
     , d_mms_product_history.[display_ui_flag]
     , SAT.[eligible_for_hold_flag]
     , SAT.[jr_member_dues_flag]
     , d_mms_product_history.[lt_buck_eligible_flag]
     , SAT.[medical_product_flag]
     , d_mms_product_history.[package_product_flag]
     , d_mms_product_history.[price_locked_flag]
     , SAT.[sold_not_serviced_flag]
     , d_mms_product_history.[tip_allowed_flag]
     , SAT.[start_date]
     , SAT.[end_date]
     , SAT.[turn_off_date_time]
     , SAT.[inserted_date_time]
     , SAT.[updated_date_time]
     , d_mms_product_history.[bk_hash]
     , d_mms_product_history.[p_mms_product_id]
     , d_mms_product_history.[dv_load_date_time]
     , d_mms_product_history.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (batch_info.[l_hash] + batch_info.[s_hash])),2)
  FROM [dbo].[d_mms_product_history] d_mms_product_history
       INNER JOIN
         ( SELECT PIT.[d_mms_product_history_id]
                , PIT.[effective_date_time]
                , RowRank = RANK() OVER (PARTITION BY PIT.[bk_hash], DATEADD(DD, DATEDIFF(DD, 0, PIT.[effective_date_time]), 0) ORDER BY PIT.[effective_date_time] DESC)
                , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[bk_hash], DATEADD(DD, DATEDIFF(DD, 0, PIT.[effective_date_time]), 0) ORDER BY PIT.[effective_date_time] DESC)
             FROM [dbo].[d_mms_product_history] PIT
             WHERE ( PIT.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
                 AND PIT.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END )
         ) PITU
         ON PITU.[d_mms_product_history_id] = d_mms_product_history.[d_mms_product_history_id]
            AND PITU.RowRank = 1 AND PITU.RowNumber = 1
       INNER JOIN [dbo].[p_mms_product] PIT
         ON PIT.[p_mms_product_id] = d_mms_product_history.[p_mms_product_id]
       INNER JOIN [dbo].[l_mms_product] LNK
         ON LNK.[bk_hash] = PIT.[bk_hash]
            AND LNK.[l_mms_product_id] = PIT.[l_mms_product_id]
       INNER JOIN [dbo].[s_mms_product] SAT
         ON SAT.[bk_hash] = PIT.[bk_hash]
            AND SAT.[s_mms_product_id] = PIT.[s_mms_product_id]
       --INNER JOIN [dbo].[s_mms_product_1] SAT_1
       --  ON SAT_1.[bk_hash] = PIT.[bk_hash]
       --     AND SAT_1.[s_mms_product_1_id] = PIT.[s_mms_product_1_id]
       --INNER JOIN [dbo].[s_mms_product_2] SAT_2
       --  ON SAT_2.[bk_hash] = PIT.[bk_hash]
       --     AND SAT_2.[s_mms_product_2_id] = PIT.[s_mms_product_2_id]
       CROSS APPLY
         ( SELECT [l_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_product_history.[product_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_product_history.[department_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_product_history.[gl_over_ride_club_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, LNK.[val_assessment_day_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, LNK.[val_employee_level_type_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, LNK.[val_gl_group_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, LNK.[val_product_status_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, LNK.[val_recurrent_product_type_id]),'z#@$k%&P'))),2)

                , [s_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_product_history.[product_id]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(SAT.[description],'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(d_mms_product_history.[gl_account_number],'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(d_mms_product_history.[gl_department_code],'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(d_mms_product_history.[gl_product_code],'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(SAT.[gl_sub_account_number],'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(SAT.[name],'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(d_mms_product_history.[pay_component],'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(d_mms_product_history.[revenue_category],'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(d_mms_product_history.[sku],'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(d_mms_product_history.[spend_category],'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(d_mms_product_history.[workday_account],'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(d_mms_product_history.[workday_cost_center],'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(d_mms_product_history.[workday_offering],'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(d_mms_product_history.[workday_over_ride_region],'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(d_mms_product_history.[workday_revenue_product_group_account],'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, SAT.[sort_order]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, SAT.[allow_zero_dollar_flag]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_product_history.[assess_as_dues_flag]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_product_history.[access_by_price_paid_flag]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, SAT.[bundle_product_flag]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, SAT.[complete_package_flag]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, SAT.[confirm_member_data_flag]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_product_history.[deferred_revenue_flag]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_product_history.[display_ui_flag]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, SAT.[eligible_for_hold_flag]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, SAT.[jr_member_dues_flag]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_product_history.[lt_buck_eligible_flag]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_product_history.[lt_buck_cost_percent]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, SAT.[medical_product_flag]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_product_history.[package_product_flag]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_product_history.[price_locked_flag]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, SAT.[sold_not_serviced_flag]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_product_history.[tip_allowed_flag]),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, SAT.[start_date], 120),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, SAT.[end_date], 120),'z#@$k%&P')
                                                               + 'P%#&z$@k' + ISNULL(CONVERT(varchar, SAT.[turn_off_date_time], 120),'z#@$k%&P'))),2)
         ) batch_info
ORDER BY PIT.[dv_batch_id] ASC, PIT.[dv_load_date_time] ASC, ISNULL(SAT.[updated_date_time], SAT.[inserted_date_time]) ASC;

END
