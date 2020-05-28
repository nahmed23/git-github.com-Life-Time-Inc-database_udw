CREATE VIEW [sandbox].[v_mart_dim_club]
AS SELECT d_mms_club.[club_id]
         , d_mms_club.[val_club_type_id]
         , d_mms_club.[val_currency_code_id]
         , d_mms_club.[val_pre_sale_id]
         , d_mms_club.[val_region_id]
         , d_mms_club.[val_sales_area_id]
         , d_mms_club.[val_time_zone_id]
         , d_mms_club.[allow_junior_check_in_flag]
         , d_mms_club.[allow_multiple_currency_flag]
         , d_mms_club.[assess_jr_member_dues_flag]
         , d_mms_club.[check_in_group_level]
         , d_mms_club.[club_activation_date]
         , d_mms_club.[club_code]
         , d_mms_club.[club_deactivation_date]
         , d_mms_club.[club_name]
         , d_mms_club.[crm_division_code]
         , d_mms_club.[currency_code]
         , d_mms_club.[domain_name_prefix]
         , d_mms_club.[dst_offset]
         , d_mms_club.[formal_club_name]
         , d_mms_club.[health_mms_club_identifier]
         , d_mms_club.[marketing_club_level]
         , d_mms_club.[marketing_map_region]
         , d_mms_club.[marketing_map_xml_state_name]
         , d_mms_club.[max_child_center_checkin_age]
         , d_mms_club.[max_junior_age]
         , d_mms_club.[max_secondary_age]
         , d_mms_club.[min_front_desk_checkin_age]
         , d_mms_club.[sell_junior_member_dues_flag]
         , d_mms_club.[st_offset]
         , d_mms_club.[workday_region]
         , d_mms_club.[inserted_date_time]
         , d_mms_club.[updated_date_time]
         , d_mms_club_address.[val_country_id]
         , d_mms_club_address.[val_state_id]
         , d_mms_club_address.[address_line_1]
         , d_mms_club_address.[address_line_2]
         , d_mms_club_address.[city]
         , d_mms_club_address.[state_abbreviation]
         , d_mms_club_address.[state_name]
         , d_mms_club_address.[postal_code]
         , d_mms_club_address.[country_abbreviation]
         , d_mms_club_address.[country_name]
         , d_mms_club_address.[latitude]
         , d_mms_club_address.[longitude]
         , d_mms_club_phone.[phone_number]
         , d_mms_club_fax.[fax_number]
         , d_crm_ltf_club.[five_letter_club_code]
         , d_crm_ltf_club.[ltf_club_id]
         , d_crm_ltf_club.[dim_crm_ltf_club_key]
         , [area_director_dim_crm_system_user_key]
         , [area_director_crm_system_user_id]
         , [area_director_dim_mms_employee_key]
         , [area_director_mms_employee_id]
         , [general_manager_dim_crm_system_user_key]
         , [general_manager_crm_system_user_id]
         , [general_manager_dim_mms_employee_key]
         , [general_manager_mms_employee_id]
         , [regional_manager_dim_crm_system_user_key]
         , [regional_manager_crm_system_user_id]
         , [regional_manager_dim_mms_employee_key]
         , [regional_manager_mms_employee_id]
         , [regional_sales_lead_dim_crm_system_user_key]
         , [regional_sales_lead_crm_system_user_id]
         , [regional_sales_lead_dim_mms_employee_key]
         , [regional_sales_lead_mms_employee_id]
         , [regional_vice_president_dim_crm_system_user_key]
         , [regional_vice_president_crm_system_user_id]
         , [regional_vice_president_dim_mms_employee_key]
         , [regional_vice_president_mms_employee_id]
         , [dim_club_key] = d_mms_club.[bk_hash]
         , d_mms_club.[bk_hash]
         , d_mms_club.[p_mms_club_id]
         , batch_info.[dv_load_date_time]
         , batch_info.[dv_batch_id]
         , d_mms_club.[dv_hash]
      FROM ( SELECT [club_id] = CASE WHEN NOT PIT.[club_id] Is Null THEN PIT.[club_id] ELSE CONVERT(int, PIT.[bk_hash]) END
                  , LNK.[val_club_type_id]
                  , LNK.[val_currency_code_id]
                  , LNK.[val_pre_sale_id]
                  , LNK.[val_region_id]
                  , LNK.[val_sales_area_id]
                  , LNK.[val_time_zone_id]
                  , SAT.[allow_junior_check_in_flag]
                  , SAT.[allow_multiple_currency_flag]
                  , [assess_jr_member_dues_flag] = SAT.[assess_junior_member_dues_flag]
                  , SAT.[check_in_group_level]
                  , SAT.[club_activation_date]
                  , SAT.[club_code]
                  , SAT.[club_deactivation_date]
                  , SAT.[club_name]
                  , SAT.[crm_division_code]
                  , SAT.[domain_name_prefix]
                  , SAT.[formal_club_name]
                  , SAT.[health_mms_club_identifier]
                  , SAT.[marketing_club_level]
                  , SAT.[marketing_map_region]
                  , SAT.[marketing_map_xml_state_name]
                  , SAT.[max_child_center_checkin_age]
                  , SAT.[max_junior_age]
                  , SAT.[max_secondary_age]
                  , SAT.[min_front_desk_checkin_age]
                  , SAT.[sell_junior_member_dues_flag]
                  , SAT.[workday_region]
                  , SAT.[inserted_date_time]
                  , SAT.[updated_date_time]
                  , PIT.[bk_hash]
                  , PIT.[p_mms_club_id]
                  , PIT.[dv_load_date_time]
                  , PIT.[dv_batch_id]
                  , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (LNK.[dv_hash] + SAT.[dv_hash])),2)
                  , r_mms_val_currency_code.[currency_code]
                  , r_mms_val_time_zone.[dst_offset]
                  , r_mms_val_time_zone.[st_offset]
                    --, [l_hash] = LNK.[dv_hash]
                    --, [s_hash] = SAT.[dv_hash]
               FROM [dbo].[d_mms_club] d_mms_club
                    INNER JOIN [dbo].[p_mms_club] PIT
                      ON PIT.[p_mms_club_id] = d_mms_club.[p_mms_club_id]
                    INNER JOIN [dbo].[l_mms_club] LNK
                      ON LNK.[bk_hash] = PIT.[bk_hash]
                         AND LNK.[l_mms_club_id] = PIT.[l_mms_club_id]
                    INNER JOIN [dbo].[s_mms_club] SAT
                      ON SAT.[bk_hash] = PIT.[bk_hash]
                         AND SAT.[s_mms_club_id] = PIT.[s_mms_club_id]
                    --INNER JOIN
                    --  ( SELECT PIT.[p_mms_club_id]
                    --         , RowRank = RANK() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
                    --         , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
                    --      FROM [dbo].[p_mms_club] PIT
                    --  ) PITU
                    --  ON PITU.[p_mms_club_id] = PIT.[p_mms_club_id]
                    --     AND PITU.RowRank = 1 AND PITU.RowNumber = 1
                    LEFT OUTER JOIN [dbo].[r_mms_val_currency_code] r_mms_val_currency_code
                      ON r_mms_val_currency_code.[dv_load_end_date_time] = '9999-12-31 00:00:00.000'
                         AND r_mms_val_currency_code.[val_currency_code_id] = LNK.[val_currency_code_id]
                    LEFT OUTER JOIN [dbo].[r_mms_val_time_zone] r_mms_val_time_zone
                      ON r_mms_val_time_zone.[dv_load_end_date_time] = '9999-12-31 00:00:00.000'
                         AND r_mms_val_time_zone.val_time_zone_id = LNK.[val_time_zone_id]
               WHERE NOT PIT.[club_id] Is Null
                  OR (ISNUMERIC(PIT.[bk_hash]) = 1 AND CONVERT(bigint, PIT.[bk_hash]) <= 2147483647)
           ) d_mms_club
           --INNER JOIN [dbo].[dim_club] dim_club
           --  ON dim_club.[dim_club_key] = d_mms_club.[bk_hash]
           LEFT OUTER JOIN
             ( SELECT PIT.[club_address_id]
                    , LNK.[club_id]
                    , LNK.[val_country_id]
                    , LNK.[val_state_id]
                    , [address_line_1] = SAT.[address_line1]
                    , [address_line_2] = SAT.[address_line2]
                    , SAT.[city]
                    , [postal_code] = SAT.[zip_code]
                    , SAT.[latitude]
                    , SAT.[longitude]
                    , SAT.[inserted_date_time]
                    , SAT.[updated_date_time]
                    , PIT.[bk_hash]
                    , PIT.[p_mms_club_address_id]
                    , PIT.[dv_load_date_time]
                    , PIT.[dv_batch_id]
                    , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (LNK.[dv_hash] + SAT.[dv_hash])),2)
                    , [state_abbreviation] = r_mms_val_state.[abbreviation]
                    , [state_name] = r_mms_val_state.[description]
                    , [country_abbreviation] = r_mms_val_country.[abbreviation]
                    , [country_name] = r_mms_val_country.[description]
                    --, [l_hash] = LNK.[dv_hash]
                    --, [s_hash] = SAT.[dv_hash]
                 FROM [dbo].[d_mms_club_address] d_mms_club_address
                      INNER JOIN [dbo].[p_mms_club_address] PIT
                        ON PIT.[p_mms_club_address_id] = d_mms_club_address.[p_mms_club_address_id]
                      INNER JOIN [dbo].[l_mms_club_address] LNK
                        ON LNK.[bk_hash] = PIT.[bk_hash]
                           AND LNK.[l_mms_club_address_id] = PIT.[l_mms_club_address_id]
                      INNER JOIN [dbo].[s_mms_club_address] SAT
                        ON SAT.[bk_hash] = PIT.[bk_hash]
                           AND SAT.[s_mms_club_address_id] = PIT.[s_mms_club_address_id]
                      --INNER JOIN
                      --  ( SELECT PIT.[p_mms_club_address_id]
                      --         , RowRank = RANK() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
                      --         , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
                      --      FROM [dbo].[p_mms_club_address] PIT
                      --  ) PITU
                      --  ON PITU.[p_mms_club_address_id] = PIT.[p_mms_club_address_id]
                      --     AND PITU.RowRank = 1 AND PITU.RowNumber = 1
                      LEFT OUTER JOIN [dbo].[r_mms_val_country] r_mms_val_country
                        ON r_mms_val_country.[dv_load_end_date_time] = '9999-12-31 00:00:00.000'
                           AND r_mms_val_country.[val_country_id] = LNK.[val_country_id]
                      LEFT OUTER JOIN [dbo].[r_mms_val_state] r_mms_val_state
                        ON r_mms_val_state.[dv_load_end_date_time] = '9999-12-31 00:00:00.000'
                           AND r_mms_val_state.[val_state_id] = LNK.[val_state_id]
                 WHERE NOT PIT.[club_address_id] Is Null
                   AND LNK.[val_address_type_id] = 5
             ) d_mms_club_address
             ON d_mms_club_address.[club_id] = d_mms_club.[club_id]
           LEFT OUTER JOIN
             ( SELECT PIT.[club_phone_id]
                    , LNK.[club_id]
                    , [phone_number] = ISNULL(SAT.[area_code],'') + ISNULL(SAT.[number],'')
                    , SAT.[inserted_date_time]
                    , SAT.[updated_date_time]
                    , PIT.[bk_hash]
                    , PIT.[p_mms_club_phone_id]
                    , PIT.[dv_load_date_time]
                    , PIT.[dv_batch_id]
                    , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (LNK.[dv_hash] + SAT.[dv_hash])),2)
                    --, [l_hash] = LNK.[dv_hash]
                    --, [s_hash] = SAT.[dv_hash]
                 FROM [dbo].[d_mms_club_phone] d_mms_club_phone
                      INNER JOIN [dbo].[p_mms_club_phone] PIT
                        ON PIT.[p_mms_club_phone_id] = d_mms_club_phone.[p_mms_club_phone_id]
                      INNER JOIN [dbo].[l_mms_club_phone] LNK
                        ON LNK.[bk_hash] = PIT.[bk_hash]
                           AND LNK.[l_mms_club_phone_id] = PIT.[l_mms_club_phone_id]
                      INNER JOIN [dbo].[s_mms_club_phone] SAT
                        ON SAT.[bk_hash] = PIT.[bk_hash]
                           AND SAT.[s_mms_club_phone_id] = PIT.[s_mms_club_phone_id]
                      --INNER JOIN
                      --  ( SELECT PIT.[p_mms_club_phone_id]
                      --         , RowRank = RANK() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
                      --         , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
                      --      FROM [dbo].[p_mms_club_phone] PIT
                      --  ) PITU
                      --  ON PITU.[p_mms_club_phone_id] = PIT.[p_mms_club_phone_id]
                      --     AND PITU.RowRank = 1 AND PITU.RowNumber = 1
                 WHERE NOT PIT.[club_phone_id] Is Null
                   AND LNK.[val_phone_type_id] = 5
             ) d_mms_club_phone
             ON d_mms_club_phone.[club_id] = d_mms_club.[club_id]
           LEFT OUTER JOIN
             ( SELECT PIT.[club_phone_id]
                    , LNK.[club_id]
                    , [fax_number] = ISNULL(SAT.[area_code],'') + ISNULL(SAT.[number],'')
                    , SAT.[inserted_date_time]
                    , SAT.[updated_date_time]
                    , PIT.[bk_hash]
                    , PIT.[p_mms_club_phone_id]
                    , PIT.[dv_load_date_time]
                    , PIT.[dv_batch_id]
                    , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (LNK.[dv_hash] + SAT.[dv_hash])),2)
                    --, [l_hash] = LNK.[dv_hash]
                    --, [s_hash] = SAT.[dv_hash]
                 FROM [dbo].[d_mms_club_phone] d_mms_club_phone
                      INNER JOIN [dbo].[p_mms_club_phone] PIT
                        ON PIT.[p_mms_club_phone_id] = d_mms_club_phone.[p_mms_club_phone_id]
                      INNER JOIN [dbo].[l_mms_club_phone] LNK
                        ON LNK.[bk_hash] = PIT.[bk_hash]
                           AND LNK.[l_mms_club_phone_id] = PIT.[l_mms_club_phone_id]
                      INNER JOIN [dbo].[s_mms_club_phone] SAT
                        ON SAT.[bk_hash] = PIT.[bk_hash]
                           AND SAT.[s_mms_club_phone_id] = PIT.[s_mms_club_phone_id]
                      --INNER JOIN
                      --  ( SELECT PIT.[p_mms_club_phone_id]
                      --         , RowRank = RANK() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
                      --         , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
                      --      FROM [dbo].[p_mms_club_phone] PIT
                      --  ) PITU
                      --  ON PITU.[p_mms_club_phone_id] = PIT.[p_mms_club_phone_id]
                      --     AND PITU.RowRank = 1 AND PITU.RowNumber = 1
                 WHERE NOT PIT.[club_phone_id] Is Null
                   AND LNK.[val_phone_type_id] = 11
             ) d_mms_club_fax
             ON d_mms_club_fax.[club_id] = d_mms_club.[club_id]
           LEFT OUTER JOIN
             ( SELECT LNK.[ltf_club_id]
                    , [five_letter_club_code] = SAT.[ltf_five_letter_club_code]
                    , [four_letter_club_code] = SAT.[ltf_four_letter_club_code]
                    , [dim_crm_ltf_club_key] = PIT.[bk_hash]
                    , [dim_club_key] = CASE WHEN NOT LNK.[ltf_mms_club_id] Is Null THEN CONVERT(char(32), HASHBYTES('MD5', ('P%#&z$@k' + CONVERT(varchar, LNK.[ltf_mms_club_id]))),2) ELSE CONVERT(char(32),'-998',2) END
                    , [area_director_dim_crm_system_user_key] = d_crm_system_user_area_director.[dim_crm_system_user_key]
                    , [area_director_crm_system_user_id] = d_crm_system_user_area_director.[system_user_id]
                    , [area_director_mms_employee_id] = d_mms_employee_area_director.[employee_id]
                    , [area_director_dim_mms_employee_key] = d_mms_employee_area_director.[dim_employee_key]
                    , [general_manager_dim_crm_system_user_key] = d_crm_system_user_general_manager.[dim_crm_system_user_key]
                    , [general_manager_crm_system_user_id] = d_crm_system_user_general_manager.[system_user_id]
                    , [general_manager_mms_employee_id] = d_crm_system_user_general_manager.[employee_id]
                    , [general_manager_dim_mms_employee_key] = d_mms_employee_general_manager.[dim_employee_key]
                    , [regional_manager_dim_crm_system_user_key] = d_crm_system_user_regional_manager.[dim_crm_system_user_key]
                    , [regional_manager_crm_system_user_id] = d_crm_system_user_regional_manager.[system_user_id]
                    , [regional_manager_mms_employee_id] = d_mms_employee_regional_manager.[employee_id]
                    , [regional_manager_dim_mms_employee_key] = d_mms_employee_regional_manager.[dim_employee_key]
                    , [regional_sales_lead_dim_crm_system_user_key] = d_crm_system_user_regional_sales_lead.[dim_crm_system_user_key]
                    , [regional_sales_lead_crm_system_user_id] = d_crm_system_user_regional_sales_lead.[system_user_id]
                    , [regional_sales_lead_mms_employee_id] = d_mms_employee_regional_sales_lead.[employee_id]
                    , [regional_sales_lead_dim_mms_employee_key] = d_mms_employee_regional_sales_lead.[dim_employee_key]
                    , [regional_vice_president_dim_crm_system_user_key] = d_crm_system_user_regional_vice_president.[dim_crm_system_user_key]
                    , [regional_vice_president_crm_system_user_id] = d_crm_system_user_regional_vice_president.[system_user_id]
                    , [regional_vice_president_mms_employee_id] = d_crm_system_user_regional_vice_president.[employee_id]
                    , [regional_vice_president_dim_mms_employee_key] = d_mms_employee_regional_vice_president.[dim_employee_key]
                    , PIT.[dv_load_date_time]
                    , PIT.[dv_batch_id]
                 FROM [dbo].[p_crmcloudsync_ltf_club] PIT
                      INNER JOIN [dbo].[l_crmcloudsync_ltf_club] LNK
                        ON LNK.[bk_hash] = PIT.[bk_hash]
                           AND LNK.[l_crmcloudsync_ltf_club_id] = PIT.[l_crmcloudsync_ltf_club_id]
                      INNER JOIN [dbo].[s_crmcloudsync_ltf_club] SAT
                        ON SAT.[bk_hash] = PIT.[bk_hash]
                           AND SAT.[s_crmcloudsync_ltf_club_id] = PIT.[s_crmcloudsync_ltf_club_id]
                      INNER JOIN
                        ( SELECT PIT.[p_crmcloudsync_ltf_club_id]
                               , RowRank = RANK() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
                               , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
                            FROM [dbo].[p_crmcloudsync_ltf_club] PIT
                        ) PITU
                        ON PITU.[p_crmcloudsync_ltf_club_id] = PIT.[p_crmcloudsync_ltf_club_id]
                           AND PITU.RowRank = 1 AND PITU.RowNumber = 1
                      LEFT OUTER JOIN [dbo].[d_crmcloudsync_system_user] d_crm_system_user_area_director
                        ON d_crm_system_user_area_director.[system_user_id] = LNK.[ltf_area_director]
                      LEFT OUTER JOIN [dbo].[d_mms_employee] d_mms_employee_area_director
                        ON d_mms_employee_area_director.[dim_employee_key] = d_crm_system_user_area_director.[dim_mms_employee_key]
                      LEFT OUTER JOIN [dbo].[d_crmcloudsync_system_user] d_crm_system_user_general_manager
                        ON d_crm_system_user_general_manager.[system_user_id] = LNK.[ltf_general_manager]
                      LEFT OUTER JOIN [dbo].[d_mms_employee] d_mms_employee_general_manager
                        ON d_mms_employee_general_manager.[dim_employee_key] = d_crm_system_user_general_manager.[dim_mms_employee_key]
                      LEFT OUTER JOIN [dbo].[d_crmcloudsync_system_user] d_crm_system_user_regional_manager
                        ON d_crm_system_user_regional_manager.[system_user_id] = LNK.[ltf_club_regional_manager]
                      LEFT OUTER JOIN [dbo].[d_mms_employee] d_mms_employee_regional_manager
                        ON d_mms_employee_regional_manager.[dim_employee_key] = d_crm_system_user_regional_manager.[dim_mms_employee_key]
                      LEFT OUTER JOIN [dbo].[d_crmcloudsync_system_user] d_crm_system_user_regional_sales_lead
                        ON d_crm_system_user_regional_sales_lead.[system_user_id] = LNK.[ltf_regional_sales_lead]
                      LEFT OUTER JOIN [dbo].[d_mms_employee] d_mms_employee_regional_sales_lead
                        ON d_mms_employee_regional_sales_lead.[dim_employee_key] = d_crm_system_user_regional_sales_lead.[dim_mms_employee_key]
                      LEFT OUTER JOIN [dbo].[d_crmcloudsync_system_user] d_crm_system_user_regional_vice_president
                        ON d_crm_system_user_regional_vice_president.[system_user_id] = LNK.[ltf_regional_vice_president]
                      LEFT OUTER JOIN [dbo].[d_mms_employee] d_mms_employee_regional_vice_president
                        ON d_mms_employee_regional_vice_president.[dim_employee_key] = d_crm_system_user_regional_vice_president.[dim_mms_employee_key]
                 WHERE NOT PIT.[ltf_club_id] Is Null
             ) d_crm_ltf_club
             ON d_crm_ltf_club.[dim_club_key] = d_mms_club.[bk_hash]
                AND d_crm_ltf_club.[four_letter_club_code] = d_mms_club.[domain_name_prefix]
           CROSS APPLY
             ( SELECT [dv_load_date_time] = CASE WHEN d_mms_club.[dv_load_date_time] > ISNULL(d_crm_ltf_club.[dv_load_date_time],CONVERT(DATETIME, -53690))
                                                  AND d_mms_club.[dv_load_date_time] > ISNULL(d_mms_club_address.[dv_load_date_time],CONVERT(DATETIME, -53690))
                                                  AND d_mms_club.[dv_load_date_time] > ISNULL(d_mms_club_phone.[dv_load_date_time],CONVERT(DATETIME, -53690))
                                                  AND d_mms_club.[dv_load_date_time] > ISNULL(d_mms_club_fax.[dv_load_date_time],CONVERT(DATETIME, -53690))
                                                      THEN d_mms_club.[dv_load_date_time]
                                                 WHEN d_crm_ltf_club.[dv_load_date_time] > ISNULL(d_mms_club.[dv_load_date_time],CONVERT(DATETIME, -53690))
                                                  AND d_crm_ltf_club.[dv_load_date_time] > ISNULL(d_mms_club_address.[dv_load_date_time],CONVERT(DATETIME, -53690))
                                                  AND d_crm_ltf_club.[dv_load_date_time] > ISNULL(d_mms_club_phone.[dv_load_date_time],CONVERT(DATETIME, -53690))
                                                  AND d_crm_ltf_club.[dv_load_date_time] > ISNULL(d_mms_club_fax.[dv_load_date_time],CONVERT(DATETIME, -53690))
                                                      THEN d_crm_ltf_club.[dv_load_date_time]
                                                 WHEN d_mms_club_address.[dv_load_date_time] > ISNULL(d_mms_club.[dv_load_date_time],CONVERT(DATETIME, -53690))
                                                  AND d_mms_club_address.[dv_load_date_time] > ISNULL(d_crm_ltf_club.[dv_load_date_time],CONVERT(DATETIME, -53690))
                                                  AND d_mms_club_address.[dv_load_date_time] > ISNULL(d_mms_club_phone.[dv_load_date_time],CONVERT(DATETIME, -53690))
                                                  AND d_mms_club_address.[dv_load_date_time] > ISNULL(d_mms_club_fax.[dv_load_date_time],CONVERT(DATETIME, -53690))
                                                      THEN d_mms_club_address.[dv_load_date_time]
                                                 WHEN d_mms_club_phone.[dv_load_date_time] > ISNULL(d_mms_club.[dv_load_date_time],CONVERT(DATETIME, -53690))
                                                  AND d_mms_club_phone.[dv_load_date_time] > ISNULL(d_crm_ltf_club.[dv_load_date_time],CONVERT(DATETIME, -53690))
                                                  AND d_mms_club_phone.[dv_load_date_time] > ISNULL(d_mms_club_address.[dv_load_date_time],CONVERT(DATETIME, -53690))
                                                  AND d_mms_club_phone.[dv_load_date_time] > ISNULL(d_mms_club_fax.[dv_load_date_time],CONVERT(DATETIME, -53690))
                                                      THEN d_mms_club_phone.[dv_load_date_time]
                                                 WHEN d_mms_club_fax.[dv_load_date_time] > ISNULL(d_mms_club.[dv_load_date_time],CONVERT(DATETIME, -53690))
                                                  AND d_mms_club_fax.[dv_load_date_time] > ISNULL(d_crm_ltf_club.[dv_load_date_time],CONVERT(DATETIME, -53690))
                                                  AND d_mms_club_fax.[dv_load_date_time] > ISNULL(d_mms_club_address.[dv_load_date_time],CONVERT(DATETIME, -53690))
                                                  AND d_mms_club_fax.[dv_load_date_time] > ISNULL(d_mms_club_phone.[dv_load_date_time],CONVERT(DATETIME, -53690))
                                                      THEN d_mms_club_fax.[dv_load_date_time]
                                                 ELSE CONVERT(DATETIME, -53690)
                                            END
                    , [dv_batch_id] = CASE WHEN d_mms_club.[dv_batch_id] > ISNULL(d_crm_ltf_club.[dv_batch_id],-1)
                                            AND d_mms_club.[dv_batch_id] > ISNULL(d_mms_club_address.[dv_batch_id],-1)
                                            AND d_mms_club.[dv_batch_id] > ISNULL(d_mms_club_phone.[dv_batch_id],-1)
                                            AND d_mms_club.[dv_batch_id] > ISNULL(d_mms_club_fax.[dv_batch_id],-1)
                                                THEN d_mms_club.[dv_batch_id]
                                           WHEN d_crm_ltf_club.[dv_batch_id] > ISNULL(d_mms_club.[dv_batch_id],-1)
                                            AND d_crm_ltf_club.[dv_batch_id] > ISNULL(d_mms_club_address.[dv_batch_id],-1)
                                            AND d_crm_ltf_club.[dv_batch_id] > ISNULL(d_mms_club_phone.[dv_batch_id],-1)
                                            AND d_crm_ltf_club.[dv_batch_id] > ISNULL(d_mms_club_fax.[dv_batch_id],-1)
                                                THEN d_crm_ltf_club.[dv_batch_id]
                                           WHEN d_mms_club_address.[dv_batch_id] > ISNULL(d_mms_club.[dv_batch_id],-1)
                                            AND d_mms_club_address.[dv_batch_id] > ISNULL(d_crm_ltf_club.[dv_batch_id],-1)
                                            AND d_mms_club_address.[dv_batch_id] > ISNULL(d_mms_club_phone.[dv_batch_id],-1)
                                            AND d_mms_club_address.[dv_batch_id] > ISNULL(d_mms_club_fax.[dv_batch_id],-1)
                                                THEN d_mms_club_address.[dv_batch_id]
                                           WHEN d_mms_club_phone.[dv_batch_id] > ISNULL(d_mms_club.[dv_batch_id],-1)
                                            AND d_mms_club_phone.[dv_batch_id] > ISNULL(d_crm_ltf_club.[dv_batch_id],-1)
                                            AND d_mms_club_phone.[dv_batch_id] > ISNULL(d_mms_club_address.[dv_batch_id],-1)
                                            AND d_mms_club_phone.[dv_batch_id] > ISNULL(d_mms_club_fax.[dv_batch_id],-1)
                                                THEN d_mms_club_phone.[dv_batch_id]
                                           WHEN d_mms_club_fax.[dv_batch_id] > ISNULL(d_mms_club.[dv_batch_id],-1)
                                            AND d_mms_club_fax.[dv_batch_id] > ISNULL(d_crm_ltf_club.[dv_batch_id],-1)
                                            AND d_mms_club_fax.[dv_batch_id] > ISNULL(d_mms_club_address.[dv_batch_id],-1)
                                            AND d_mms_club_fax.[dv_batch_id] > ISNULL(d_mms_club_phone.[dv_batch_id],-1)
                                                THEN d_mms_club_fax.[dv_batch_id]
                                           ELSE -1
                                      END
             ) batch_info;