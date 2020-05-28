CREATE PROC [sandbox].[proc_mart_sw_d_mms_membership_type] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT PIT.[membership_type_id]
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
     , PIT.[bk_hash]
     , PIT.[p_mms_membership_type_id]
     , PIT.[dv_load_date_time]
     , PIT.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (LNK.[dv_hash] + SAT.[dv_hash])),2)
     --, [l_hash] = LNK.[dv_hash]
     --, [s_hash] = SAT.[dv_hash]
  FROM [dbo].[p_mms_membership_type] PIT
       INNER JOIN [dbo].[l_mms_membership_type] LNK
         ON LNK.[bk_hash] = PIT.[bk_hash]
            AND LNK.[l_mms_membership_type_id] = PIT.[l_mms_membership_type_id]
       INNER JOIN [dbo].[s_mms_membership_type] SAT
         ON SAT.[bk_hash] = PIT.[bk_hash]
            AND SAT.[s_mms_membership_type_id] = PIT.[s_mms_membership_type_id]
       INNER JOIN
         ( SELECT PIT.[p_mms_membership_type_id]
                , RowRank = RANK() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
                , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
             FROM [dbo].[p_mms_membership_type] PIT
             WHERE ( PIT.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
                 AND PIT.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END )
         ) PITU
         ON PITU.[p_mms_membership_type_id] = PIT.[p_mms_membership_type_id]
            AND PITU.RowRank = 1 AND PITU.RowNumber = 1
  WHERE NOT PIT.[membership_type_id] Is Null
ORDER BY PIT.[dv_batch_id] ASC, PIT.[dv_load_date_time] ASC, ISNULL(SAT.[updated_date_time], SAT.[inserted_date_time]) ASC;

END
