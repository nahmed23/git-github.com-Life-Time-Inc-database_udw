CREATE VIEW [sandbox].[v_mart_mms_membership_type_history]
AS SELECT DIM.[membership_type_id]
     , DIM.[product_id]
     , LNK.[member_card_design_id]
     , DIM.[val_check_in_group_id]
     , DIM.[val_enrollment_type_id]
     , d_batch_mms_membership_type.[val_membership_type_family_status_id]
     , DIM.[val_membership_type_group_id]
     , d_batch_mms_membership_type.[val_pricing_method_id]
     , d_batch_mms_membership_type.[val_pricing_rule_id]
     , DIM.[val_restricted_group_id]
     , DIM.[val_unit_type_id]
     , DIM.[val_welcome_kit_type_id]
     , [display_name] = ISNULL(DIM.[display_name],'')
     , SAT.[assess_due_flag]
     , d_batch_mms_membership_type.[assess_jr_member_dues_flag]
     , SAT.[express_membership_flag]
     , d_batch_mms_membership_type.[min_primary_age]
     , d_batch_mms_membership_type.[min_unit_type]
     , d_batch_mms_membership_type.[max_unit_type]
     , SAT.[short_term_membership_flag]
     , SAT.[suppress_membership_card_flag]
     , SAT.[waive_admin_fee_flag]
     , SAT.[waive_enrollment_fee_flag]
     , d_batch_mms_membership_type.[waive_late_fee_flag]
     , SAT.[inserted_date_time]
     , SAT.[updated_date_time]
     , MaxJuniorAge = CAST(CASE d_batch_mms_membership_type.[val_pricing_method_id] WHEN 1 THEN 11 WHEN 2 THEN 11 WHEN 3 THEN 13 ELSE 0 END AS int)
     , [effective_date_no_time] = DATEADD(DD, DATEDIFF(DD, 0, PITU.[effective_date_time]), 0)
     , PITU.[effective_date_time]
     , [dim_mms_membership_type_key] = PIT.[bk_hash]
     , DIM.[dim_mms_product_key] --= CASE WHEN NOT LNK.[product_id] Is Null THEN CONVERT(char(32), HASHBYTES('MD5', ('P%#&z$@k' + CONVERT(varchar, LNK.[product_id]))),2) ELSE CONVERT(char(32),'-998',2) END
     , [dim_mms_val_membership_type_family_status_key] = CASE WHEN NOT LNK.[val_membership_type_family_status_id] Is Null THEN CONVERT(char(32), HASHBYTES('MD5', ('P%#&z$@k' + CONVERT(varchar, LNK.[val_membership_type_family_status_id]))),2) ELSE CONVERT(char(32),'-998',2) END
     , PIT.[bk_hash]
     , PIT.[p_mms_membership_type_id]
     , PIT.[dv_load_date_time]
     , PIT.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (batch_info.[l_hash] + batch_info.[s_hash])),2)
  FROM [dbo].[p_mms_membership_type] PIT
       INNER JOIN [dbo].[d_mms_membership_type_history] DIM
         ON PIT.[bk_hash] = DIM.[bk_hash]
            AND PIT.[p_mms_membership_type_id] = DIM.[p_mms_membership_type_id]
       INNER JOIN [dbo].[l_mms_membership_type] LNK
         ON LNK.[bk_hash] = PIT.[bk_hash]
            AND LNK.[l_mms_membership_type_id] = PIT.[l_mms_membership_type_id]
       INNER JOIN [dbo].[s_mms_membership_type] SAT
         ON SAT.[bk_hash] = PIT.[bk_hash]
            AND SAT.[s_mms_membership_type_id] = PIT.[s_mms_membership_type_id]
       INNER JOIN
         ( SELECT PIT.[bk_hash]
                , PIT.[p_mms_membership_type_id]
                , PIT.[d_mms_membership_type_history_id]
                , PIT.[effective_date_time]
                , RowRank = RANK() OVER (PARTITION BY PIT.[bk_hash], DATEADD(DD, DATEDIFF(DD, 0, PIT.[effective_date_time]), 0) ORDER BY PIT.[effective_date_time] DESC)
                , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[bk_hash], DATEADD(DD, DATEDIFF(DD, 0, PIT.[effective_date_time]), 0) ORDER BY PIT.[effective_date_time] DESC)
             FROM [dbo].[d_mms_membership_type_history] PIT
         ) PITU
         ON PITU.[bk_hash] = DIM.[bk_hash]
            AND PITU.[d_mms_membership_type_history_id] = DIM.[d_mms_membership_type_history_id]
            AND PITU.RowRank = 1 AND PITU.RowNumber = 1
       LEFT OUTER JOIN
         ( SELECT LNK.[bk_hash]
                , LNK.[val_pricing_method_id]
                , RowRank = RANK() OVER (PARTITION BY LNK.[bk_hash] ORDER BY LNK.[dv_load_date_time] ASC)
                , RowNumber = ROW_NUMBER() OVER (PARTITION BY LNK.[bk_hash] ORDER BY LNK.[dv_load_date_time] ASC)
             FROM [dbo].[l_mms_membership_type] LNK
             WHERE NOT LNK.[val_pricing_method_id] Is Null
         ) l_pricing_method
         ON l_pricing_method.[bk_hash] = DIM.[bk_hash]
            AND l_pricing_method.RowRank = 1 AND l_pricing_method.RowNumber = 1
       LEFT OUTER JOIN
         ( SELECT LNK.[bk_hash]
                , LNK.[val_pricing_rule_id]
                , RowRank = RANK() OVER (PARTITION BY LNK.[bk_hash] ORDER BY LNK.[dv_load_date_time] ASC)
                , RowNumber = ROW_NUMBER() OVER (PARTITION BY LNK.[bk_hash] ORDER BY LNK.[dv_load_date_time] ASC)
             FROM [dbo].[l_mms_membership_type] LNK
             WHERE NOT LNK.[val_pricing_rule_id] Is Null
         ) l_pricing_rule
         ON l_pricing_rule.[bk_hash] = DIM.[bk_hash]
            AND l_pricing_rule.RowRank = 1 AND l_pricing_rule.RowNumber = 1
       LEFT OUTER JOIN
         ( SELECT LNK.[bk_hash]
                , LNK.[val_membership_type_family_status_id]
                , RowRank = RANK() OVER (PARTITION BY LNK.[bk_hash] ORDER BY LNK.[dv_load_date_time] ASC)
                , RowNumber = ROW_NUMBER() OVER (PARTITION BY LNK.[bk_hash] ORDER BY LNK.[dv_load_date_time] ASC)
             FROM [dbo].[l_mms_membership_type] LNK
             WHERE NOT LNK.[val_membership_type_family_status_id] Is Null
         ) l_family_status
         ON l_family_status.[bk_hash] = DIM.[bk_hash]
            AND l_family_status.RowRank = 1 AND l_family_status.RowNumber = 1
       LEFT OUTER JOIN
         ( SELECT SAT.[bk_hash]
                , SAT.[min_primary_age]
                , RowRank = RANK() OVER (PARTITION BY SAT.[bk_hash] ORDER BY SAT.[dv_load_date_time] ASC)
                , RowNumber = ROW_NUMBER() OVER (PARTITION BY SAT.[bk_hash] ORDER BY SAT.[dv_load_date_time] ASC)
             FROM [dbo].[s_mms_membership_type] SAT
             WHERE NOT SAT.[min_primary_age] Is Null
         ) s_min_primary_age
         ON s_min_primary_age.[bk_hash] = DIM.[bk_hash]
            AND s_min_primary_age.RowRank = 1 AND s_min_primary_age.RowNumber = 1
       LEFT OUTER JOIN
         ( SELECT SAT.[bk_hash]
                , SAT.[assess_jr_member_dues_flag]
                , RowRank = RANK() OVER (PARTITION BY SAT.[bk_hash] ORDER BY SAT.[dv_load_date_time] ASC)
                , RowNumber = ROW_NUMBER() OVER (PARTITION BY SAT.[bk_hash] ORDER BY SAT.[dv_load_date_time] ASC)
             FROM [dbo].[s_mms_membership_type] SAT
             WHERE NOT SAT.[assess_jr_member_dues_flag] Is Null
         ) s_assess_jr_member_dues_flag
         ON s_assess_jr_member_dues_flag.[bk_hash] = DIM.[bk_hash]
            AND s_assess_jr_member_dues_flag.RowRank = 1 AND s_assess_jr_member_dues_flag.RowNumber = 1
       CROSS APPLY
         ( SELECT [val_membership_type_family_status_id] = ISNULL(DIM.[val_membership_type_family_status_id],l_family_status.[val_membership_type_family_status_id])
                , [val_pricing_method_id] = ISNULL(DIM.[val_pricing_method_id],l_pricing_method.[val_pricing_method_id])
                , [val_pricing_rule_id] = ISNULL(DIM.[val_pricing_rule_id],l_pricing_rule.[val_pricing_rule_id])
                , [assess_jr_member_dues_flag] = ISNULL(SAT.[assess_jr_member_dues_flag],ISNULL(s_assess_jr_member_dues_flag.[assess_jr_member_dues_flag],1))
                , [min_primary_age] = ISNULL(SAT.[min_primary_age],ISNULL(s_min_primary_age.[min_primary_age],0))
                , [min_unit_type] = ISNULL(SAT.[min_unit_type],0)
                , [max_unit_type] = ISNULL(SAT.[max_unit_type],0)
                , [waive_late_fee_flag] = ISNULL(SAT.[waive_late_fee_flag],0)
         ) d_batch_mms_membership_type
       CROSS APPLY
         ( SELECT [l_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[membership_type_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[product_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, LNK.[member_card_design_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[val_check_in_group_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[val_enrollment_type_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_batch_mms_membership_type.[val_membership_type_family_status_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[val_membership_type_group_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_batch_mms_membership_type.[val_pricing_method_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_batch_mms_membership_type.[val_pricing_rule_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[val_restricted_group_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[val_unit_type_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[val_welcome_kit_type_id]),'z#@$k%&P'))),2)

                , [s_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, PIT.[membership_type_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[display_name]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, SAT.[assess_due_flag]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_batch_mms_membership_type.[assess_jr_member_dues_flag]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[express_membership_flag]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_batch_mms_membership_type.[min_primary_age]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_batch_mms_membership_type.[min_unit_type]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_batch_mms_membership_type.[max_unit_type]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[short_term_membership_flag]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[suppress_membership_card_flag]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[waive_admin_fee_flag]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, SAT.[waive_enrollment_fee_flag]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_batch_mms_membership_type.[waive_late_fee_flag]),'z#@$k%&P'))),2)
            ) batch_info;