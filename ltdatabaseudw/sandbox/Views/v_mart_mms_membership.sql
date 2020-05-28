CREATE VIEW [sandbox].[v_mart_mms_membership]
AS SELECT PIT.[membership_id]
     , LNK.[advisor_employee_id]
     , LNK.[club_id]
     , LNK.[company_id]
     , LNK.[jr_member_dues_product_id]
     , LNK.[membership_type_id]
     , LNK.[promotion_id]
     , LNK.[purchaser_id]
     , LNK.[qualified_sales_promotion_id]
     , LNK.[val_eft_option_id]
     , [val_eft_option_product_id] = LNK_1.[val_eft_option_product_id]
     , LNK.[val_enrollment_type_id]
     , LNK.[val_membership_source_id]
     , LNK.[val_membership_status_id]
     , LNK.[val_termination_reason_id]
     , LNK.[val_termination_reason_club_type_id]
     , [product_tier_price] = CONVERT(decimal(26,6), ISNULL(d_mms_membership_attribute.[price],ISNULL(d_mms_membership_product_tier.[price],0)))
     , [current_price] = ISNULL(d_batch_mms_membership.[current_price],0)  --CASE WHEN SAT.[current_price] Is Null THEN d_mms_club_product.[price] ELSE SAT.[current_price] END, 0)
     , [undiscounted_price] = SAT_1.[undiscounted_price]
     , SAT.[join_fee_paid]
     , SAT.[prior_plus_price]
     , [prior_plus_undiscounted_price] = SAT_1.[prior_plus_undiscounted_price]
     , LNK.[prior_plus_membership_type_id]
     , [overdue_recurrent_product_balance_flag] = SAT_1.[overdue_recurrent_product_balance_flag]
     , [activation_date_no_time]           = DATEADD(DD, DATEDIFF(DD, 0, SAT.[activation_date]), 0)
     , SAT.[activation_date]
     , [cancellation_request_date_no_time] = DATEADD(DD, DATEDIFF(DD, 0, SAT.[cancellation_request_date]), 0)
     , SAT.[cancellation_request_date]
     , [created_date_no_time]              = DATEADD(DD, DATEDIFF(DD, 0, SAT.[created_date_time]), 0)
     , SAT.[created_date_time]
     , [expiration_date_no_time]           = DATEADD(DD, DATEDIFF(DD, 0, SAT.[expiration_date]), 0)
     , SAT.[expiration_date]
     , SAT.[utc_created_date_time]
     , SAT.[created_date_time_zone]
     , LNK.[crm_opportunity_id]
     , LNK.[last_updated_employee_id]
     , d_batch_mms_membership.[inserted_date_time]
     , d_batch_mms_membership.[updated_date_time]
     , d_mms_membership.[dim_mms_membership_key]
     , [dim_advisor_employee_key] = CASE WHEN NOT d_mms_membership.[advisor_employee_id] Is Null THEN CONVERT(char(32), HASHBYTES('MD5', ('P%#&z$@k' + CONVERT(varchar, d_mms_membership.[advisor_employee_id]))),2) ELSE CONVERT(char(32),'-998',2) END
     , [dim_club_key] = d_mms_membership.[home_dim_club_key]
     , d_mms_membership.[dim_mms_company_key]
     , d_mms_membership.[dim_mms_membership_type_key]
     , PIT.[bk_hash]
     , PIT.[p_mms_membership_id]
     , d_batch_mms_membership.[dv_load_date_time]
     , d_batch_mms_membership.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (LNK.[dv_hash] + SAT.[dv_hash])),2)
  FROM [dbo].[d_mms_membership] d_mms_membership
       INNER JOIN [dbo].[p_mms_membership] PIT
         ON PIT.[bk_hash] = d_mms_membership.[bk_hash]
            AND PIT.[p_mms_membership_id] = d_mms_membership.[p_mms_membership_id]
       INNER JOIN [dbo].[l_mms_membership] LNK
         ON LNK.[bk_hash] = PIT.[bk_hash]
            AND LNK.[l_mms_membership_id] = PIT.[l_mms_membership_id]
       INNER JOIN [dbo].[l_mms_membership_1] LNK_1
         ON LNK_1.[bk_hash] = PIT.[bk_hash]
            AND LNK_1.[l_mms_membership_1_id] = PIT.[l_mms_membership_1_id]
       INNER JOIN[dbo].[s_mms_membership] SAT
         ON SAT.[bk_hash] = PIT.[bk_hash]
            AND SAT.[s_mms_membership_id] = PIT.[s_mms_membership_id]
       INNER JOIN[dbo].[s_mms_membership_1] SAT_1
         ON SAT_1.[bk_hash] = PIT.[bk_hash]
            AND SAT_1.[s_mms_membership_1_id] = PIT.[s_mms_membership_1_id]
       INNER JOIN [dbo].[d_mms_membership_type]
         ON d_mms_membership_type.[dim_mms_membership_type_key] = d_mms_membership.[dim_mms_membership_type_key]

       CROSS APPLY
         ( SELECT SAT.[inserted_date_time]
                , SAT.[updated_date_time]
                , [effective_date_time] = ISNULL(SAT.[updated_date_time], SAT.[inserted_date_time])
                , [dv_load_date_time]   = ISNULL(SAT.[updated_date_time], SAT.[inserted_date_time])
                , [dv_batch_id]         = CONVERT(bigint, CONVERT(VARCHAR(8), ISNULL(SAT.[updated_date_time], SAT.[inserted_date_time]), 112) + REPLACE(CONVERT(varchar(8), ISNULL(SAT.[updated_date_time], SAT.[inserted_date_time]), 114), ':',''))
         ) b_mms_membership

       LEFT OUTER JOIN
         ( SELECT d_mms_club_product.[dim_club_key]
                , d_mms_club_product.[dim_mms_product_key]
                , d_mms_club_product.[price]
                , RowRank = RANK() OVER (PARTITION BY d_mms_club_product.[dim_club_key], d_mms_club_product.[dim_mms_product_key] ORDER BY d_mms_club_product.[club_product_id] DESC)
                , RowNumber = ROW_NUMBER() OVER (PARTITION BY d_mms_club_product.[dim_club_key], d_mms_club_product.[dim_mms_product_key] ORDER BY d_mms_club_product.[club_product_id] DESC)
                FROM [dbo].[d_mms_club_product]
         ) d_mms_club_product
         ON d_mms_club_product.[dim_club_key] = d_mms_membership.[home_dim_club_key]
            AND d_mms_club_product.[dim_mms_product_key] = d_mms_membership_type.[dim_mms_product_key]
            AND d_mms_club_product.RowRank = 1 AND d_mms_club_product.RowNumber = 1

       LEFT OUTER JOIN
         ( SELECT d_mms_membership_attribute.[dim_mms_membership_key]
                , [price]               = CONVERT(decimal(26,6), ISNULL(d_mms_membership_attribute.[membership_attribute_value],0))
                , s_mms_membership_attribute.[inserted_date_time]
                , s_mms_membership_attribute.[updated_date_time]
                , [effective_date_time] = ISNULL(s_mms_membership_attribute.[updated_date_time],s_mms_membership_attribute.[inserted_date_time])
                , [dv_load_date_time]   = ISNULL(s_mms_membership_attribute.[updated_date_time],s_mms_membership_attribute.[inserted_date_time])
                , [dv_batch_id]         = CONVERT(bigint, CONVERT(VARCHAR(8), ISNULL(s_mms_membership_attribute.[updated_date_time],s_mms_membership_attribute.[inserted_date_time]), 112) + REPLACE(CONVERT(varchar(8), ISNULL(s_mms_membership_attribute.[updated_date_time],s_mms_membership_attribute.[inserted_date_time]), 114), ':',''))
             FROM [dbo].[d_mms_membership_attribute]
                  INNER JOIN [dbo].[p_mms_membership_attribute]
                    ON p_mms_membership_attribute.[bk_hash] = d_mms_membership_attribute.[bk_hash]
                       AND p_mms_membership_attribute.[p_mms_membership_attribute_id] = d_mms_membership_attribute.[p_mms_membership_attribute_id]
                  INNER JOIN [dbo].[s_mms_membership_attribute]
                    ON s_mms_membership_attribute.[bk_hash] = p_mms_membership_attribute.[bk_hash]
                       AND s_mms_membership_attribute.[s_mms_membership_attribute_id] = p_mms_membership_attribute.[s_mms_membership_attribute_id]
             WHERE d_mms_membership_attribute.[val_membership_attribute_type_id] = 15
               AND (d_mms_membership_attribute.[effective_thru_date_time] Is Null OR d_mms_membership_attribute.[effective_thru_date_time] > GETDATE())
               AND d_mms_membership_attribute.[effective_from_date_time] <= GETDATE()
         ) d_mms_membership_attribute
         ON d_mms_membership_attribute.[dim_mms_membership_key] = d_mms_membership.[dim_mms_membership_key]

       LEFT OUTER JOIN  --d_mms_membership_product_tier
         ( SELECT d_mms_membership_product_tier.[dim_mms_membership_key]
                , d_mms_product_tier_price.[price]
                , SAT.[inserted_date_time]
                , SAT.[updated_date_time]
                , d_timestamp.[effective_date_time]
                , [dv_load_date_time]   = d_timestamp.[effective_date_time]
                , [dv_batch_id]         = CONVERT(bigint, CONVERT(VARCHAR(8), d_timestamp.[effective_date_time], 112) + REPLACE(CONVERT(varchar(8), d_timestamp.[effective_date_time], 114), ':',''))
                , RowRank = RANK() OVER (PARTITION BY d_mms_membership_product_tier.[dim_mms_membership_key] ORDER BY d_mms_membership_product_tier.[membership_product_tier_id] DESC)
                , RowNumber = ROW_NUMBER() OVER (PARTITION BY d_mms_membership_product_tier.[dim_mms_membership_key] ORDER BY d_mms_membership_product_tier.[membership_product_tier_id] DESC)
             FROM [dbo].[d_mms_membership_product_tier]
                  INNER JOIN [dbo].[p_mms_membership_product_tier] PIT
                    ON PIT.[bk_hash] = d_mms_membership_product_tier.[bk_hash]
                       AND PIT.[p_mms_membership_product_tier_id] = d_mms_membership_product_tier.[p_mms_membership_product_tier_id]
                  INNER JOIN[dbo].[s_mms_membership_product_tier] SAT
                    ON SAT.[bk_hash] = PIT.[bk_hash]
                       AND SAT.[s_mms_membership_product_tier_id] = PIT.[s_mms_membership_product_tier_id]
                  INNER JOIN [dbo].[d_mms_product_tier]
                    ON d_mms_product_tier.[bk_hash] = d_mms_membership_product_tier.[dim_mms_product_tier_key]
                  INNER JOIN [dbo].[d_mms_product]
                    ON d_mms_product.[dim_mms_product_key] = d_mms_product_tier.[dim_mms_product_key]
                  INNER JOIN [dbo].[d_mms_member]
                    ON d_mms_member.[member_active_flag] = 'Y' AND d_mms_member.[val_member_type_id] = 1 AND d_mms_member.[dim_mms_membership_key] = d_mms_membership_product_tier.[dim_mms_membership_key]
                  INNER JOIN [dbo].[p_mms_member]
                    ON p_mms_member.[bk_hash] = d_mms_member.[bk_hash]
                       AND p_mms_member.[p_mms_member_id] = d_mms_member.[p_mms_member_id]
                  INNER JOIN [dbo].[s_mms_member]
                    ON s_mms_member.[bk_hash] = p_mms_member.[bk_hash]
                       AND s_mms_member.[s_mms_member_id] = p_mms_member.[s_mms_member_id]
                  CROSS APPLY
                    ( SELECT [mpt_effective_date_time]    = ISNULL(SAT.[updated_date_time], SAT.[inserted_date_time])
                           , [member_effective_date_time] = ISNULL(s_mms_member.[updated_date_time], s_mms_member.[inserted_date_time])
                    ) d_batch_timestamp
                  INNER JOIN [dbo].[d_mms_member_attribute]
                    ON d_mms_member_attribute.val_member_attribute_type_id = 4
                       AND d_mms_member_attribute.[dim_mms_member_key] = d_mms_member.[dim_mms_member_key]
                       AND (d_mms_member_attribute.[effective_thru_date_time] Is Null OR d_mms_member_attribute.[effective_thru_date_time] > GETDATE())
                       AND d_mms_member_attribute.[effective_from_date_time] <= GETDATE()
                  INNER JOIN [dbo].[d_mms_product_tier_price]
                    ON d_mms_product_tier_price.[dim_mms_product_tier_key] = d_mms_membership_product_tier.[dim_mms_product_tier_key]
                       AND ( (d_mms_product_tier.[val_product_tier_type_id] = 2 AND d_mms_product_tier_price.[val_card_level_id] = CAST(d_mms_member_attribute.[attribute_value] AS int) ) )
                        --(d_mms_product_tier.[val_product_tier_type_id] <> 2 AND d_mms_product_tier_price.[val_membership_type_group_id] = d_mms_membership_type.[val_membership_type_group_id])
                  CROSS APPLY
                    ( SELECT effective_date_time = CASE WHEN (d_mms_member_attribute.[effective_from_date_time] > d_batch_timestamp.[mpt_effective_date_time] AND d_mms_member_attribute.[effective_from_date_time] > d_batch_timestamp.[member_effective_date_time]) THEN d_mms_member_attribute.[effective_from_date_time]
                                                        WHEN (d_batch_timestamp.[member_effective_date_time] > d_batch_timestamp.[mpt_effective_date_time] AND d_batch_timestamp.[member_effective_date_time] > d_mms_member_attribute.[effective_from_date_time]) THEN d_batch_timestamp.[member_effective_date_time]
                                                        ELSE d_batch_timestamp.[mpt_effective_date_time]
                                                   END
                    ) d_timestamp
         ) d_mms_membership_product_tier
         ON d_mms_membership_product_tier.[dim_mms_membership_key] = d_mms_membership.[dim_mms_membership_key]
            AND d_mms_membership_product_tier.RowRank = 1 AND d_mms_membership_product_tier.RowNumber = 1

        --CROSS APPLY
        --  ( SELECT product_tier_price = CONVERT(decimal(26,6), ISNULL(d_mms_membership_attribute.[price], ISNULL(d_mms_membership_product_tier.[price],0)))
        --         , effective_date_time = CASE WHEN (NOT d_mms_membership_attribute.[effective_from_date_time] Is Null AND d_mms_membership_attribute.[effective_from_date_time] > b_mms_membership.[effective_date_time]) THEN d_mms_membership_attribute.[effective_from_date_time]
        --                                      WHEN (NOT d_mms_membership_product_tier.[effective_date_time] Is Null AND d_mms_membership_product_tier.[effective_date_time] > b_mms_membership.[effective_date_time]) THEN d_mms_membership_product_tier.[effective_date_time]
        --                                      ELSE b_mms_membership.[effective_date_time]
        --                                 END
        --  ) Member_Tier

       CROSS APPLY
         ( SELECT [current_price] = CASE WHEN ( (LNK.[club_id] IN (286,287) AND LNK.[membership_type_id] = 15702)
                                                 OR (LNK.[club_id] = 277 AND LNK.[val_termination_reason_id] = 26) )
                                              THEN ISNULL(SAT.[current_price],0)
                                         WHEN (SAT.[current_price] Is Null OR SAT.[current_price] = 0)
                                              THEN ISNULL(d_mms_club_product.[price],0)
                                         ELSE ISNULL(SAT.[current_price],0)
                                    END
                , [dv_load_date_time] = CASE WHEN ( (ISNULL(d_mms_membership_product_tier.[dv_batch_id],-1) > ISNULL(d_mms_membership_attribute.[dv_batch_id],-1))
                                                AND (ISNULL(d_mms_membership_product_tier.[dv_batch_id],-1) > ISNULL(b_mms_membership.[dv_batch_id],-1)) )
                                                    THEN d_mms_membership_product_tier.[dv_load_date_time]
                                                WHEN ( (ISNULL(d_mms_membership_attribute.[dv_batch_id],-1) > ISNULL(d_mms_membership_product_tier.[dv_batch_id],-1))
                                                AND (ISNULL(d_mms_membership_attribute.[dv_batch_id],-1) > ISNULL(b_mms_membership.[dv_batch_id],-1)) )
                                                    THEN d_mms_membership_attribute.[dv_load_date_time]
                                                ELSE b_mms_membership.[dv_load_date_time] END
                , [dv_batch_id] = CASE WHEN ( (ISNULL(d_mms_membership_product_tier.[dv_batch_id],-1) > ISNULL(d_mms_membership_attribute.[dv_batch_id],-1))
                                            AND (ISNULL(d_mms_membership_product_tier.[dv_batch_id],-1) > ISNULL(b_mms_membership.[dv_batch_id],-1)) )
                                            THEN d_mms_membership_product_tier.[dv_batch_id]
                                        WHEN ( (ISNULL(d_mms_membership_attribute.[dv_batch_id],-1) > ISNULL(d_mms_membership_product_tier.[dv_batch_id],-1))
                                            AND (ISNULL(d_mms_membership_attribute.[dv_batch_id],-1) > ISNULL(b_mms_membership.[dv_batch_id],-1)) )
                                            THEN d_mms_membership_attribute.[dv_batch_id]
                                        ELSE b_mms_membership.[dv_batch_id] END
                , [inserted_date_time] = CASE WHEN ( (ISNULL(d_mms_membership_product_tier.[dv_batch_id],-1) > ISNULL(d_mms_membership_attribute.[dv_batch_id],-1))
                                                    AND (ISNULL(d_mms_membership_product_tier.[dv_batch_id],-1) > ISNULL(b_mms_membership.[dv_batch_id],-1)) )
                                                    THEN d_mms_membership_product_tier.[inserted_date_time]
                                                WHEN ( (ISNULL(d_mms_membership_attribute.[dv_batch_id],-1) > ISNULL(d_mms_membership_product_tier.[dv_batch_id],-1))
                                                    AND (ISNULL(d_mms_membership_attribute.[dv_batch_id],-1) > ISNULL(b_mms_membership.[dv_batch_id],-1)) )
                                                    THEN d_mms_membership_attribute.[inserted_date_time]
                                                ELSE b_mms_membership.[inserted_date_time] END
                , [updated_date_time] = CASE WHEN ( (ISNULL(d_mms_membership_product_tier.[dv_batch_id],-1) > ISNULL(d_mms_membership_attribute.[dv_batch_id],-1))
                                                AND (ISNULL(d_mms_membership_product_tier.[dv_batch_id],-1) > ISNULL(b_mms_membership.[dv_batch_id],-1)) )
                                                    THEN d_mms_membership_product_tier.[updated_date_time]
                                                WHEN ( (ISNULL(d_mms_membership_attribute.[dv_batch_id],-1) > ISNULL(d_mms_membership_product_tier.[dv_batch_id],-1))
                                                AND (ISNULL(d_mms_membership_attribute.[dv_batch_id],-1) > ISNULL(b_mms_membership.[dv_batch_id],-1)) )
                                                    THEN d_mms_membership_attribute.[updated_date_time]
                                                ELSE b_mms_membership.[updated_date_time] END
         ) d_batch_mms_membership;