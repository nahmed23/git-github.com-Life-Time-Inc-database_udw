CREATE VIEW [sandbox].[v_mart_sw_membership_address]
AS SELECT d_mms_membership.[membership_id]
       , [address_line_1] = ISNULL(d_mms_membership_communication_preference.[description], ISNULL(d_mms_membership_address.[address_line_1], ''))
       , [address_line_2] = ISNULL(d_mms_membership_communication_preference.[description], ISNULL(d_mms_membership_address.[address_line_2], ''))
       , [city] = ISNULL(d_mms_membership_communication_preference.[description], ISNULL(d_mms_membership_address.[city], ''))
       , [state] = ISNULL(d_mms_membership_communication_preference.[description], ISNULL(d_mms_membership_address.[abbreviation], ''))
       , [postal_code] = ISNULL(d_mms_membership_communication_preference.[description], ISNULL(d_mms_membership_address.[zip], ''))
       , d_mms_membership_address.[membership_address_id]
       , [val_communication_preference_id] = ISNULL(d_mms_membership_communication_preference.[val_communication_preference_id], 1)
       , [active_flag] = ISNULL(d_mms_membership_communication_preference.[active_flag], 0)
       , [communication_preference_description] = ISNULL(d_mms_membership_communication_preference.[description], '')
       , [contact_address_line_1] = ISNULL(d_mms_membership_address.[address_line_1], '')
       , [contact_address_line_2] = ISNULL(d_mms_membership_address.[address_line_2], '')
       , [contact_city] = ISNULL(d_mms_membership_address.[city], '')
       , [contact_state] = ISNULL(d_mms_membership_address.[abbreviation], '')
       , [contact_postal_code] = ISNULL(d_mms_membership_address.[zip], '')
       , d_mms_membership.[dim_mms_membership_key]
    FROM [sandbox].[v_mart_mms_membership] d_mms_membership
         LEFT OUTER JOIN
           ( SELECT d_mms_membership_address.[membership_address_id], d_mms_membership_address.[membership_id]
                  , d_mms_membership_address.[address_line_1], d_mms_membership_address.[address_line_2]
                  , d_mms_membership_address.[city], d_mms_membership_address.[zip], r_mms_val_state.[abbreviation]
               FROM [sandbox].[v_mart_mms_membership_address] d_mms_membership_address
                    INNER JOIN [sandbox].[v_mart_mms_val_state] r_mms_val_state
                      ON r_mms_val_state.[val_state_id] = d_mms_membership_address.[val_state_id]
           ) d_mms_membership_address
           ON d_mms_membership_address.[membership_id] = d_mms_membership.[membership_id]

         LEFT OUTER JOIN [sandbox].[v_mart_sw_membership_communication_preference] d_mms_membership_communication_preference
           ON d_mms_membership_communication_preference.[active_flag] = 1 AND d_mms_membership_communication_preference.[val_communication_preference_id] = 1 AND d_mms_membership_communication_preference.[membership_id] = d_mms_membership.[membership_id];