CREATE VIEW [sandbox].[v_mart_mms_membership_history]
AS SELECT d_mms_mbrs_history.[membership_id]
     , d_mms_mbrs_history.[club_id]
     , d_mms_mbrs_history.[advisor_employee_id]
     , d_mms_mbrs_history.[company_id]
     , LNK.[jr_member_dues_product_id]
     , d_mms_mbrs_history.[membership_type_id]
     , d_mms_mbrs_history.[val_enrollment_type_id]
     , d_mms_mbrs_history.[val_membership_source_id]
     , d_mms_mbrs_history.[val_membership_status_id]
     , d_mms_mbrs_history.[val_termination_reason_id]
     , d_mms_mbrs_history.[val_termination_reason_club_type_id]

     --, [product_id]                           = d_membership_price.[product_id]
     --, [val_card_level_id]                    = r_mms_val_membership_type_group.[val_card_level_id]
     --, [val_check_in_group_id]                = d_membership_price.[val_check_in_group_id]
     --, [val_membership_type_family_status_id] = d_membership_price.[val_membership_type_family_status_id]
     --, [val_membership_type_group_id]         = d_membership_price.[val_membership_type_group_id]
     --, [val_pricing_method_id]                = d_membership_price.[val_pricing_method_id]
     --, [val_pricing_rule_id]                  = d_membership_price.[val_pricing_rule_id]

     , d_membership_price.[current_price]
     , d_mms_mbrs_history.[undiscounted_price]

     , [activation_date_no_time]           = DATEADD(DD, DATEDIFF(DD, 0, d_mms_mbrs_history.[membership_activation_date]), 0)
     , [activation_date]                   = d_mms_mbrs_history.[membership_activation_date]
     , [cancellation_request_date_no_time] = DATEADD(DD, DATEDIFF(DD, 0, d_mms_mbrs_history.[membership_cancellation_request_date]), 0)
     , [cancellation_request_date]         = d_mms_mbrs_history.[membership_cancellation_request_date]
     , [created_date_no_time]              = DATEADD(DD, DATEDIFF(DD, 0, d_mms_mbrs_history.[membership_created_date_time]), 0)
     , [created_date_time]                 = d_mms_mbrs_history.[membership_created_date_time]
     , [expiration_date_no_time]           = DATEADD(DD, DATEDIFF(DD, 0, d_mms_mbrs_history.[membership_expiration_date]), 0)
     , [expiration_date]                   = d_mms_mbrs_history.[membership_expiration_date]

     --, d_membership_price.[access_by_price_paid_flag]
     --, d_membership_price.[membership_type_assess_jr_member_dues_flag]
     , d_membership_price.[club_assess_jr_member_dues_flag]
  
     , [effective_date_no_time] = DATEADD(DD, DATEDIFF(DD, 0, d_mms_mbrs_history.[effective_date_time]), 0)
     , d_mms_mbrs_history.[effective_date_time]
     
     , d_mms_mbrs_history.[dim_mms_membership_key]
     , [dim_advisor_employee_key] = CASE WHEN NOT d_mms_mbrs_history.[advisor_employee_id] Is Null THEN CONVERT(char(32), HASHBYTES('MD5', ('P%#&z$@k' + CONVERT(varchar, d_mms_mbrs_history.[advisor_employee_id]))),2) ELSE CONVERT(char(32),'-998',2) END
     , [dim_club_key] = d_mms_membership.[home_dim_club_key]
     , d_mms_membership.[dim_mms_company_key]
     , d_mms_membership.[dim_mms_membership_type_key]
     , [bk_hash] = d_mms_mbrs_history.[dim_mms_membership_key]
     , d_mms_mbrs_history.[dv_load_date_time]
     , d_mms_mbrs_history.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (batch_info.[l_hash] + batch_info.[s_hash])),2)

     --, [dim_club_key] = d_mms_mbrs_history.[home_dim_club_key]
     --, d_mms_mbrs_history.[dim_mms_membership_type_key]
     --, d_mms_mbrs_type_history.[dim_mms_product_key]
  FROM [dbo].[dim_mms_membership_history] d_mms_mbrs_history
       INNER JOIN
         ( SELECT PIT.[dim_mms_membership_history_id]
                , PIT.[effective_date_time]
                , RowRank = RANK() OVER (PARTITION BY PIT.[dim_mms_membership_key], DATEADD(DD, DATEDIFF(DD, 0, PIT.[effective_date_time]), 0) ORDER BY PIT.[effective_date_time] DESC)
                , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[dim_mms_membership_key], DATEADD(DD, DATEDIFF(DD, 0, PIT.[effective_date_time]), 0) ORDER BY PIT.[effective_date_time] DESC)
                FROM [dbo].[dim_mms_membership_history] PIT
         ) PITU
         ON PITU.[dim_mms_membership_history_id] = d_mms_mbrs_history.[dim_mms_membership_history_id]
            AND PITU.RowRank = 1 AND PITU.RowNumber = 1

       INNER JOIN [dbo].[d_mms_membership] d_mms_membership
         ON d_mms_membership.[dim_mms_membership_key] = d_mms_mbrs_history.[dim_mms_membership_key]
       INNER JOIN [dbo].[p_mms_membership] PIT
         ON PIT.[bk_hash] = d_mms_membership.[bk_hash]
            AND PIT.[p_mms_membership_id] = d_mms_membership.[p_mms_membership_id]
       INNER JOIN [dbo].[l_mms_membership] LNK
         ON LNK.[bk_hash] = PIT.[bk_hash]
            AND LNK.[l_mms_membership_id] = PIT.[l_mms_membership_id]

       INNER JOIN [dbo].[d_mms_membership_type] d_mms_membership_type
         ON d_mms_mbrs_history.[dim_mms_membership_type_key] = d_mms_membership_type.[dim_mms_membership_type_key]

        --OUTER APPLY
        --  ( SELECT d_mms_mbrs_type_history.[dim_mms_membership_type_key]
        --         , d_mms_club_history.[d_mms_membership_type_history_id]
        --         , RowRank = RANK() OVER (PARTITION BY d_mms_mbrs_type_history.[dim_mms_membership_type_key], d_mms_mbrs_type_history.[effective_date_time] ORDER BY d_mms_mbrs_type_history.[d_mms_membership_type_history_id] DESC)
        --         , RowNumber = ROW_NUMBER() OVER (PARTITION BY d_mms_mbrs_type_history.[dim_mms_membership_type_key], d_mms_mbrs_type_history.[effective_date_time] ORDER BY d_mms_mbrs_type_history.[d_mms_membership_type_history_id] DESC)
        --      FROM [dbo].[d_mms_membership_type_history] d_mms_mbrs_type_history
        --      WHERE d_mms_mbrs_type_history.[dim_club_key] = d_mms_mbrs_history.[home_dim_club_key]
        --        AND d_mms_mbrs_history.effective_date_time >= d_mms_mbrs_type_history.[effective_date_time]
        --        AND d_mms_mbrs_history.effective_date_time < d_mms_mbrs_type_history.[expiration_date_time]
        --  ) d_mms_mbrs_type_history

        --LEFT OUTER JOIN [dbo].[d_mms_membership_type_history] d_mms_mbrs_type_history
        --  ON d_mms_mbrs_history.[dim_mms_membership_type_key] = d_mms_mbrs_type_history.[dim_mms_membership_type_key]
        --     AND d_mms_mbrs_history.effective_date_time >= d_mms_mbrs_type_history.[effective_date_time]
        --     AND d_mms_mbrs_history.effective_date_time < d_mms_mbrs_type_history.[expiration_date_time]

        --INNER JOIN [dbo].[d_mms_product] d_mms_product
        --  ON d_mms_membership_type.[dim_mms_product_key] = d_mms_product.[dim_mms_product_key]

        --LEFT OUTER JOIN [dbo].[d_mms_product_history] d_mms_product_history
        --  ON d_mms_mbrs_type_history.[dim_mms_product_key] = d_mms_product_history.[dim_mms_product_key]
        --     AND d_mms_mbrs_history.effective_date_time >= d_mms_product_history.[effective_date_time]
        --     AND d_mms_mbrs_history.effective_date_time < d_mms_product_history.[expiration_date_time]

       INNER JOIN [dbo].[d_mms_club] d_mms_club
         ON d_mms_club.[dim_club_key] = d_mms_mbrs_history.[home_dim_club_key]

        --OUTER APPLY
        --  ( SELECT d_mms_club_history.[dim_club_key]
        --         , d_mms_club_history.[assess_junior_member_dues_flag]
        --         , d_mms_club_history.[d_mms_club_history_id]
        --         , RowRank = RANK() OVER (PARTITION BY d_mms_club_history.[dim_club_key], d_mms_club_history.[effective_date_time] ORDER BY d_mms_club_history.[d_mms_club_history_id] DESC)
        --         , RowNumber = ROW_NUMBER() OVER (PARTITION BY d_mms_club_history.[dim_club_key], d_mms_club_history.[effective_date_time] ORDER BY d_mms_club_history.[d_mms_club_history_id] DESC)
        --      FROM [dbo].[d_mms_club_history] d_mms_club_history
        --      WHERE d_mms_club_history.[dim_club_key] = d_mms_mbrs_history.[home_dim_club_key]
        --        AND d_mms_mbrs_history.effective_date_time >= d_mms_club_history.[effective_date_time]
        --        AND d_mms_mbrs_history.effective_date_time < d_mms_club_history.[expiration_date_time]
        --  ) d_mms_club_history

       LEFT OUTER JOIN [dbo].[d_mms_club_history] d_mms_club_history
         ON d_mms_club_history.[dim_club_key] = d_mms_mbrs_history.[home_dim_club_key]
            AND d_mms_mbrs_history.[effective_date_time] >= d_mms_club_history.[effective_date_time]
            AND d_mms_mbrs_history.[effective_date_time] < d_mms_club_history.[expiration_date_time]

       OUTER APPLY
         ( SELECT d_mms_club_prod_history.[price]
                , d_mms_club_prod_history.[d_mms_club_product_history_id]
                , RowRank = RANK() OVER (PARTITION BY d_mms_club_prod_history.[dim_club_key], d_mms_club_prod_history.[dim_mms_product_key] ORDER BY d_mms_club_prod_history.[effective_date_time] DESC)
                , RowNumber = ROW_NUMBER() OVER (PARTITION BY d_mms_club_prod_history.[dim_club_key], d_mms_club_prod_history.[dim_mms_product_key] ORDER BY d_mms_club_prod_history.[effective_date_time] DESC)
             FROM [dbo].[d_mms_club_product_history] d_mms_club_prod_history
             WHERE d_mms_membership_type.[dim_mms_product_key] = d_mms_club_prod_history.[dim_mms_product_key]
               AND d_mms_mbrs_history.[home_dim_club_key] = d_mms_club_prod_history.[dim_club_key]
               AND d_mms_mbrs_history.[effective_date_time] >= d_mms_club_prod_history.[effective_date_time]
               AND d_mms_mbrs_history.[effective_date_time] < d_mms_club_prod_history.[expiration_date_time]
         ) d_mms_club_prod_history

        --LEFT OUTER JOIN [dbo].[d_mms_club_product_history] d_mms_club_prod_history
        --  ON d_mms_mbrs_type_history.[dim_mms_product_key] = d_mms_club_prod_history.[dim_mms_product_key]
        --     AND d_mms_mbrs_history.[home_dim_club_key] = d_mms_club_prod_history.[dim_club_key]
        --     AND d_mms_mbrs_history.[effective_date_time] >= d_mms_club_prod_history.[effective_date_time]
        --     AND d_mms_mbrs_history.[effective_date_time] < d_mms_club_prod_history.[expiration_date_time]

       CROSS APPLY
         ( SELECT [current_price] = CASE WHEN ( (d_mms_mbrs_history.[club_id] IN (286,287) AND d_mms_mbrs_history.[membership_type_id] = 15702)
                                                 OR (d_mms_mbrs_history.[club_id] = 277 AND d_mms_mbrs_history.[val_termination_reason_id] = 26) )
                                              THEN ISNULL(d_mms_mbrs_history.[current_price],0)
                                         WHEN (d_mms_mbrs_history.[current_price] Is Null OR d_mms_mbrs_history.[current_price] = 0)
                                              THEN ISNULL(d_mms_club_prod_history.[price],0)
                                         ELSE ISNULL(d_mms_mbrs_history.[current_price],0)
                                    END

                --, [access_by_price_paid_flag]                  = CONVERT(bit, CASE WHEN ISNULL(d_mms_product_history.[access_by_price_paid_flag],ISNULL(d_mms_product.[access_by_price_paid_flag],'N')) = 'Y' THEN 1 ELSE 0 END)
                --, [membership_type_assess_jr_member_dues_flag] = CONVERT(bit, CASE WHEN ISNULL(d_mms_mbrs_type_history.[assess_junior_member_dues_flag],ISNULL(d_mms_membership_type.[assess_junior_member_dues_flag],'Y')) = 'Y' THEN 1 ELSE 0 END)
                , [club_assess_jr_member_dues_flag]            = CONVERT(bit, CASE WHEN ISNULL(d_mms_club_history.[assess_junior_member_dues_flag],ISNULL(d_mms_club.[assess_junior_member_dues_flag],'Y')) = 'Y' THEN 1 ELSE 0 END)

                --, [product_id]                           = ISNULL(d_mms_mbrs_type_history.[product_id],d_mms_membership_type.[product_id])
                --, [val_check_in_group_id]                = ISNULL(d_mms_mbrs_type_history.[val_check_in_group_id],d_mms_membership_type.[val_check_in_group_id])
                --, [val_membership_type_family_status_id] = ISNULL(d_mms_mbrs_type_history.[val_membership_type_family_status_id],d_mms_membership_type.[val_membership_type_family_status_id])
                --, [val_membership_type_group_id]         = ISNULL(d_mms_mbrs_type_history.[val_membership_type_group_id],d_mms_membership_type.[val_membership_type_group_id])
                --, [val_pricing_method_id]                = ISNULL(d_mms_mbrs_type_history.[val_pricing_method_id],d_mms_membership_type.[val_pricing_method_id])
                --, [val_pricing_rule_id]                  = ISNULL(d_mms_mbrs_type_history.[val_pricing_rule_id],d_mms_membership_type.[val_pricing_rule_id])
         ) d_membership_price

        --LEFT OUTER JOIN [dbo].[r_mms_val_membership_type_group] r_mms_val_membership_type_group
        --  ON r_mms_val_membership_type_group.[val_membership_type_group_id] = d_membership_price.[val_membership_type_group_id]

       CROSS APPLY
         ( SELECT [l_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_history.[membership_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_history.[club_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_history.[advisor_employee_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_history.[company_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_history.[membership_type_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_history.[val_enrollment_type_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_history.[val_membership_source_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_history.[val_membership_status_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_history.[val_termination_reason_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_history.[val_termination_reason_club_type_id]),'z#@$k%&P'))),2)
                                                              --+ 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_membership_price.[product_id]),'z#@$k%&P')
                                                              --+ 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_membership_price.[val_check_in_group_id]),'z#@$k%&P')
                                                              --+ 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_membership_price.[val_membership_type_family_status_id]),'z#@$k%&P')
                                                              --+ 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_membership_price.[val_membership_type_group_id]),'z#@$k%&P')
                                                              --+ 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_membership_price.[val_pricing_method_id]),'z#@$k%&P')
                                                              --+ 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_membership_price.[val_pricing_rule_id]),'z#@$k%&P')

                , [s_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_history.[membership_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_membership_price.[current_price]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_history.[membership_activation_date], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_history.[membership_expiration_date], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_history.[membership_cancellation_request_date], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_mms_mbrs_history.[membership_created_date_time], 120),'z#@$k%&P')
                                                                  --+ 'P%#&z$@k' + CONVERT(varchar, ISNULL(d_membership_price.[access_by_price_paid_flag],0))
                                                                  --+ 'P%#&z$@k' + CONVERT(varchar, ISNULL(d_membership_price.[membership_type_assess_jr_member_dues_flag],1))
                                                                  + 'P%#&z$@k' + CONVERT(varchar, ISNULL(d_membership_price.[club_assess_jr_member_dues_flag],1)))),2)
         ) batch_info
   WHERE (d_mms_club_prod_history.[d_mms_club_product_history_id] Is Null OR (d_mms_club_prod_history.RowRank = 1 AND d_mms_club_prod_history.RowNumber = 1));