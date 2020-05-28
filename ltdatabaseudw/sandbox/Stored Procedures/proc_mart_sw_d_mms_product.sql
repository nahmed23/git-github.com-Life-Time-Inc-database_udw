CREATE PROC [sandbox].[proc_mart_sw_d_mms_product] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT [product_id] = CASE WHEN NOT PIT.[product_id] Is Null THEN PIT.[product_id] ELSE CONVERT(int, PIT.[bk_hash]) END
     , LNK.[department_id]
     , LNK.[gl_over_ride_club_id]
     , LNK.[val_gl_group_id]
     , LNK.[val_recurrent_product_type_id]
     , LNK.[val_product_status_id]
     , LNK.[val_assessment_day_id]
     , LNK.[workday_account]
     , LNK.[workday_cost_center]
     , LNK.[workday_offering]
     , LNK.[workday_over_ride_region]
     , LNK.[workday_revenue_product_group_account]
     , LNK.[revenue_category]
     , LNK.[spend_category]
     , LNK.[pay_component]
     , LNK.[val_employee_level_type_id]
     , SAT.[name]
     , SAT.[description]
     , SAT.[display_ui_flag]
     , SAT.[sort_order]
     , SAT.[inserted_date_time]
     , SAT.[turn_off_date_time]
     , SAT.[start_date]
     , SAT.[end_date]
     , SAT.[gl_account_number]
     , d_mms_product.[gl_department_code]
     , d_mms_product.[gl_product_code]
     , SAT.[gl_sub_account_number]
     , SAT.[complete_package_flag]
     , SAT.[allow_zero_dollar_flag]
     , SAT.[package_product_flag]
     , SAT.[sold_not_serviced_flag]
     , SAT.[updated_date_time]
     , SAT.[tip_allowed_flag]
     , SAT.[jr_member_dues_flag]
     , SAT.[eligible_for_hold_flag]
     , SAT.[confirm_member_data_flag]
     , SAT.[medical_product_flag]
     , SAT.[bundle_product_flag]
     , SAT.[deferred_revenue_flag]
     , SAT.[price_locked_flag]
     , SAT.[assess_as_dues_flag]
     , SAT.[sku]
     , SAT_1.[lt_buck_eligible]
     , SAT_1.[lt_buck_cost_percent]
     , SAT_2.[access_by_price_paid_flag]
     , PIT.[bk_hash]
     , PIT.[p_mms_product_id]
     , PIT.[dv_load_date_time]
     , PIT.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (LNK.[dv_hash] + SAT.[dv_hash])),2)
     --, [l_hash] = LNK.[dv_hash]
     --, [s_hash] = SAT.[dv_hash]
  FROM [dbo].[d_mms_product] d_mms_product
       INNER JOIN [dbo].[p_mms_product] PIT
         ON PIT.[p_mms_product_id] = d_mms_product.[p_mms_product_id]
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
       --INNER JOIN
       --  ( SELECT PIT.[p_mms_product_id]
       --         , RowRank = RANK() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
       --         , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
       --      FROM [dbo].[p_mms_product] PIT
       --      WHERE PIT.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
       --        AND PIT.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END + " 
       --  ) PITU
       --  ON PITU.[p_mms_product_id] = PIT.[p_mms_product_id]
       --     AND PITU.RowRank = 1 AND PITU.RowNumber = 1
  --WHERE ( NOT PIT.[product_id] Is Null
  --     OR (ISNUMERIC(PIT.[bk_hash]) = 1 AND CONVERT(bigint, PIT.[bk_hash]) <= 2147483647) )
ORDER BY PIT.[dv_batch_id] ASC, PIT.[dv_load_date_time] ASC, ISNULL(SAT.[updated_date_time], SAT.[inserted_date_time]) ASC;

END
