CREATE VIEW [sandbox].[v_mart_sw_membership_attribute_termination_reason]
AS SELECT d_mms_membership_attribute.[membership_attribute_id]
       , d_mms_membership_attribute.[membership_id]
       , d_mms_membership_attribute.[val_membership_attribute_type_id]
       , d_mms_membership_attribute.[val_termination_reason_id]
       , d_mms_membership_attribute.[effective_from_date_time]
       , d_mms_membership_attribute.[effective_thru_date_time]
       , [termination_reason_description]  = r_mms_val_termination_reason.[description]
       , r_mms_val_termination_reason.[additional_reason_display_ui_flag]
       , d_mms_membership_attribute.[dim_mms_membership_key]
   FROM [sandbox].[v_mart_mms_membership_attribute] d_mms_membership_attribute
        INNER JOIN [sandbox].[v_mart_mms_val_termination_reason] r_mms_val_termination_reason
          ON r_mms_val_termination_reason.[val_termination_reason_id] = d_mms_membership_attribute.[val_termination_reason_id]
   WHERE d_mms_membership_attribute.[val_membership_attribute_type_id] = 11
     AND (NOT d_mms_membership_attribute.[val_termination_reason_id] Is Null AND d_mms_membership_attribute.[val_termination_reason_id] > 0)
     AND d_mms_membership_attribute.[effective_thru_date_time] Is Null;