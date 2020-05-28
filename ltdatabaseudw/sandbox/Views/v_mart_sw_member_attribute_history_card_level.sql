CREATE VIEW [sandbox].[v_mart_sw_member_attribute_history_card_level]
AS SELECT d_mms_member_attribute.[member_attribute_id]
       , d_mms_member_attribute.[member_id]
       , d_mms_member_attribute.[val_member_attribute_type_id]
       , d_mms_member_attribute.[val_card_level_id]
       , d_mms_member_attribute.[effective_from_date_time]
       , d_mms_member_attribute.[effective_thru_date_time]
       , d_mms_member_attribute.[expiration_date]
       , [card_level_description] = r_mms_val_card_level.[description]
       , d_mms_member_attribute.[effective_from_date_no_time]
       , d_mms_member_attribute.[effective_thru_date_no_time]
       , d_mms_member_attribute.[dim_mms_member_key]
   FROM [sandbox].[v_mart_mms_member_attribute] d_mms_member_attribute
        INNER JOIN [sandbox].[v_mart_mms_val_card_level] r_mms_val_card_level
          ON r_mms_val_card_level.[val_card_level_id] = d_mms_member_attribute.[val_card_level_id]
   WHERE d_mms_member_attribute.[val_member_attribute_type_id] = 4
     AND (NOT d_mms_member_attribute.[effective_thru_date_time] Is Null)
     AND (NOT d_mms_member_attribute.[val_card_level_id] Is Null AND d_mms_member_attribute.[val_card_level_id] > 0);