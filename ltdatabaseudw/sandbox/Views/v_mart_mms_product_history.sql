CREATE VIEW [sandbox].[v_mart_mms_product_history]
AS SELECT [product_id] = CASE WHEN NOT PIT.[product_id] Is Null THEN PIT.[product_id] ELSE CONVERT(int, PIT.[bk_hash]) END
     , DIM.[department_id]
     , DIM.[gl_over_ride_club_id]
     , LNK.[val_assessment_day_id]
     , LNK.[val_employee_level_type_id]
     , LNK.[val_gl_group_id]
     , LNK.[val_product_status_id]
     , LNK.[val_recurrent_product_type_id]
     , SAT.[description]
     , SAT.[gl_account_number]
     , DIM.[gl_department_code]
     , DIM.[gl_product_code]
     , SAT.[gl_sub_account_number]
     , DIM.[lt_buck_cost_percent]
     , SAT.[name]
     , DIM.[pay_component]
     , DIM.[revenue_category]
     , DIM.[sku]
     , SAT.[sort_order]
     , DIM.[spend_category]
     , DIM.[workday_account]
     , DIM.[workday_cost_center]
     , DIM.[workday_offering]
     , DIM.[workday_over_ride_region]
     , DIM.[workday_revenue_product_group_account]
     , SAT_2.[access_by_price_paid_flag]
     , SAT.[allow_zero_dollar_flag]
     , SAT.[assess_as_dues_flag]
     , SAT.[bundle_product_flag]
     , SAT.[complete_package_flag]
     , SAT.[confirm_member_data_flag]
     , SAT.[deferred_revenue_flag]
     , SAT.[display_ui_flag]
     , SAT.[eligible_for_hold_flag]
     , SAT.[jr_member_dues_flag]
     , SAT_1.[lt_buck_eligible]
     , SAT.[medical_product_flag]
     , SAT.[package_product_flag]
     , SAT.[price_locked_flag]
     , SAT.[sold_not_serviced_flag]
     , SAT.[tip_allowed_flag]
     , SAT.[start_date]
     , SAT.[end_date]
     , SAT.[turn_off_date_time]
     , SAT.[inserted_date_time]
     , SAT.[updated_date_time]
     , [effective_date_no_time] = DATEADD(DD, DATEDIFF(DD, 0, DIM.[effective_date_time]), 0)
     , DIM.[effective_date_time]
     , [dim_product_key] = DIM.[bk_hash]
     , DIM.[bk_hash]
     , DIM.[p_mms_product_id]
     , DIM.[dv_load_date_time]
     , DIM.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (batch_info.[l_hash] + batch_info.[s_hash])),2)
  FROM [dbo].[p_mms_product] PIT
       INNER JOIN [dbo].[d_mms_product_history] DIM
         ON DIM.[bk_hash] = PIT.[bk_hash]
            AND DIM.[p_mms_product_id] = PIT.[p_mms_product_id]
       INNER JOIN [dbo].[l_mms_product] LNK
         ON LNK.[bk_hash] = PIT.[bk_hash]
            AND LNK.[l_mms_product_id] = PIT.[l_mms_product_id]
       INNER JOIN [dbo].[s_mms_product] SAT
         ON SAT.[bk_hash] = PIT.[bk_hash]
            AND SAT.[s_mms_product_id] = PIT.[s_mms_product_id]
       INNER JOIN [dbo].[s_mms_product_1] SAT_1
         ON SAT_1.[bk_hash] = PIT.[bk_hash]
            AND SAT_1.[s_mms_product_1_id] = PIT.[s_mms_product_1_id]
       INNER JOIN [dbo].[s_mms_product_2] SAT_2
         ON SAT_2.[bk_hash] = PIT.[bk_hash]
            AND SAT_2.[s_mms_product_2_id] = PIT.[s_mms_product_2_id]
       INNER JOIN
         ( SELECT PIT.[d_mms_product_history_id]
                , PIT.[effective_date_time]
                , RowRank = RANK() OVER (PARTITION BY PIT.[bk_hash], DATEADD(DD, DATEDIFF(DD, 0, PIT.[effective_date_time]), 0) ORDER BY PIT.[effective_date_time] DESC)
                , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[bk_hash], DATEADD(DD, DATEDIFF(DD, 0, PIT.[effective_date_time]), 0) ORDER BY PIT.[effective_date_time] DESC)
             FROM [dbo].[d_mms_product_history] PIT
         ) PITU
         ON PITU.[d_mms_product_history_id] = DIM.[d_mms_product_history_id]
            AND PITU.RowRank = 1 AND PITU.RowNumber = 1
        --INNER JOIN [dbo].[s_mms_product_1] SAT_1
        --  ON SAT_1.[bk_hash] = PIT.[bk_hash]
        --     AND SAT_1.[s_mms_product_1_id] = PIT.[s_mms_product_1_id]
        --INNER JOIN [dbo].[s_mms_product_2] SAT_2
        --  ON SAT_2.[bk_hash] = PIT.[bk_hash]
        --     AND SAT_2.[s_mms_product_2_id] = PIT.[s_mms_product_2_id]
       CROSS APPLY
         ( SELECT [l_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[product_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[department_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[gl_over_ride_club_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, LNK.[val_assessment_day_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, LNK.[val_employee_level_type_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, LNK.[val_gl_group_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, LNK.[val_product_status_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, LNK.[val_recurrent_product_type_id]),'z#@$k%&P'))),2)

                , [s_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[product_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(SAT.[description],'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(DIM.[gl_account_number],'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(DIM.[gl_department_code],'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(DIM.[gl_product_code],'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(SAT.[gl_sub_account_number],'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(SAT.[name],'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(DIM.[pay_component],'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(DIM.[revenue_category],'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(DIM.[sku],'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(DIM.[spend_category],'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(DIM.[workday_account],'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(DIM.[workday_cost_center],'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(DIM.[workday_offering],'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(DIM.[workday_over_ride_region],'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(DIM.[workday_revenue_product_group_account],'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, SAT.[sort_order]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, SAT.[allow_zero_dollar_flag]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[assess_as_dues_flag]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[access_by_price_paid_flag]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, SAT.[bundle_product_flag]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, SAT.[complete_package_flag]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, SAT.[confirm_member_data_flag]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[deferred_revenue_flag]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[display_ui_flag]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, SAT.[eligible_for_hold_flag]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, SAT.[jr_member_dues_flag]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[lt_buck_eligible_flag]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[lt_buck_cost_percent]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, SAT.[medical_product_flag]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[package_product_flag]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[price_locked_flag]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, SAT.[sold_not_serviced_flag]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[tip_allowed_flag]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, SAT.[start_date], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, SAT.[end_date], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, SAT.[turn_off_date_time], 120),'z#@$k%&P'))),2)
            ) batch_info;