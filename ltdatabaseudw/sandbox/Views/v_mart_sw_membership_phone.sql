CREATE VIEW [sandbox].[v_mart_sw_membership_phone]
AS SELECT d_mms_membership.[membership_id]
         , d_mms_membership.[club_id]
         , d_mms_membership.[val_membership_status_id]
         , [membership_telephone] = ISNULL(d_mms_membership_communication_preference.[description], ISNULL(MbrsPhone.[telephone], ''))
         , MbrsPhone.[membership_phone_id]
         , [val_communication_preference_id] = ISNULL(d_mms_membership_communication_preference.[val_communication_preference_id], 1)
         , [active_flag] = ISNULL(d_mms_membership_communication_preference.[active_flag], 0)
         , [communication_preference_description] = ISNULL(d_mms_membership_communication_preference.[description], '')
         , [contact_telephone] = ISNULL(MbrsPhone.[telephone], '')
         , [do_not_contact] = CAST(CASE WHEN d_mms_membership_communication_preference.[val_communication_preference_id] Is Null THEN 0 ELSE 1 END AS bit)
         , d_mms_membership.[dim_mms_membership_key]
         , d_mms_membership.[dim_club_key]
      FROM [sandbox].[v_mart_mms_membership] d_mms_membership
           LEFT OUTER JOIN ( SELECT MbrsPhoneOuter.[membership_phone_id], MbrsPhoneOuter.[membership_id]
                                  , MbrsPhoneOuter.[area_code], MbrsPhoneOuter.[number]
                                  , [telephone] = MbrsPhoneOuter.[area_code] + MbrsPhoneOuter.[number]
                               FROM [sandbox].[v_mart_mms_membership_phone] MbrsPhoneOuter
                               WHERE MbrsPhoneOuter.[membership_phone_id] = ( SELECT TOP 1 MbrsPhoneInner.[membership_phone_id]
                                                                                FROM [sandbox].[v_mart_mms_membership_phone] MbrsPhoneInner
                                                                                WHERE NOT (MbrsPhoneInner.[area_code] Is Null OR MbrsPhoneInner.[number] Is Null)
                                                                                  AND MbrsPhoneInner.[membership_id] = MbrsPhoneOuter.[membership_id]
                                                                              ORDER BY MbrsPhoneInner.[val_phone_type_id] )
                           ) MbrsPhone
                           ON MbrsPhone.[membership_id] = d_mms_membership.[membership_id]

           LEFT OUTER JOIN [sandbox].[v_mart_sw_membership_communication_preference] d_mms_membership_communication_preference
             ON d_mms_membership_communication_preference.[active_flag] = 1 AND d_mms_membership_communication_preference.[val_communication_preference_id] = 2 AND d_mms_membership_communication_preference.[membership_id] = d_mms_membership.[membership_id];