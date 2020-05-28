CREATE VIEW [marketing].[v_dim_mms_membership_guest_privilege_rule]
AS select d_mms_guest_privilege_rule.dim_mms_membership_guest_privilege_rule_key dim_mms_membership_guest_privilege_rule_key,
       d_mms_guest_privilege_rule.guest_privilege_rule_id guest_privilege_rule_id,
       d_mms_guest_privilege_rule.card_level_dim_description_key card_level_dim_description_key,
       d_mms_guest_privilege_rule.earliest_membership_created_dim_date_key earliest_membership_created_dim_date_key,
       d_mms_guest_privilege_rule.high_membership_type_check_in_group_level high_membership_type_check_in_group_level,
       d_mms_guest_privilege_rule.latest_membership_created_dim_date_key latest_membership_created_dim_date_key,
       d_mms_guest_privilege_rule.low_membership_type_check_in_group_level low_membership_type_check_in_group_level,
       d_mms_guest_privilege_rule.max_number_of_guests max_number_of_guests,
       d_mms_guest_privilege_rule.month_flag month_flag,
       d_mms_guest_privilege_rule.val_card_level_id val_card_level_id,
       d_mms_guest_privilege_rule.year_flag year_flag
  from dbo.d_mms_guest_privilege_rule;