CREATE VIEW [sandbox].[v_mart_mms_membership_type]
AS SELECT PIT.[membership_type_id]
     , LNK.[product_id]
     , LNK.[member_card_design_id]
     , LNK.[val_check_in_group_id]
     , LNK.[val_enrollment_type_id]
     , LNK.[val_membership_type_family_status_id]
     , LNK.[val_membership_type_group_id]
     , LNK.[val_pricing_method_id]
     , LNK.[val_pricing_rule_id]
     , LNK.[val_restricted_group_id]
     , LNK.[val_unit_type_id]
     , LNK.[val_welcome_kit_type_id]
     , [display_name] = ISNULL(SAT.[display_name],'')
     , SAT.[assess_due_flag]
     , [assess_jr_member_dues_flag] = ISNULL(SAT.[assess_jr_member_dues_flag],1)
     , SAT.[express_membership_flag]
     , SAT.[min_primary_age]
     , [min_unit_type] = ISNULL(SAT.[min_unit_type],0)
     , [max_unit_type] = ISNULL(SAT.[max_unit_type],0)
     , SAT.[short_term_membership_flag]
     , SAT.[suppress_membership_card_flag]
     , SAT.[waive_admin_fee_flag]
     , SAT.[waive_enrollment_fee_flag]
     , SAT.[waive_late_fee_flag]
     , SAT.[inserted_date_time]
     , SAT.[updated_date_time]
     , [dim_mms_membership_type_key] = PIT.[bk_hash]
     , DIM.[dim_mms_product_key] --= CASE WHEN NOT LNK.[product_id] Is Null THEN CONVERT(char(32), HASHBYTES('MD5', ('P%#&z$@k' + CONVERT(varchar, LNK.[product_id]))),2) ELSE CONVERT(char(32),'-998',2) END
     , [dim_mms_val_membership_type_family_status_key] = CASE WHEN NOT LNK.[val_membership_type_family_status_id] Is Null THEN CONVERT(char(32), HASHBYTES('MD5', ('P%#&z$@k' + CONVERT(varchar, LNK.[val_membership_type_family_status_id]))),2) ELSE CONVERT(char(32),'-998',2) END
     , PIT.[bk_hash]
     , PIT.[p_mms_membership_type_id]
     , PIT.[dv_load_date_time]
     , PIT.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (LNK.[dv_hash] + SAT.[dv_hash])),2)
  FROM [dbo].[p_mms_membership_type] PIT
       INNER JOIN [dbo].[d_mms_membership_type] DIM
         ON DIM.[bk_hash] = PIT.[bk_hash]
            AND DIM.[p_mms_membership_type_id] = PIT.[p_mms_membership_type_id]
       INNER JOIN [dbo].[l_mms_membership_type] LNK
         ON LNK.[bk_hash] = PIT.[bk_hash]
            AND LNK.[l_mms_membership_type_id] = PIT.[l_mms_membership_type_id]
       INNER JOIN [dbo].[s_mms_membership_type] SAT
         ON SAT.[bk_hash] = PIT.[bk_hash]
            AND SAT.[s_mms_membership_type_id] = PIT.[s_mms_membership_type_id];