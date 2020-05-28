CREATE VIEW [sandbox].[v_mart_mms_club_history]
AS SELECT [club_id] = CASE WHEN NOT PIT.[club_id] Is Null THEN PIT.[club_id] ELSE CONVERT(int, PIT.[bk_hash]) END
     , LNK.[val_club_type_id]
     , LNK.[val_currency_code_id]
     , LNK.[val_pre_sale_id]
     , LNK.[val_region_id]
     , LNK.[val_sales_area_id]
     , LNK.[val_time_zone_id]
     , SAT.[allow_junior_check_in_flag]
     , SAT.[allow_multiple_currency_flag]
     , assess_jr_member_dues_flag = SAT.[assess_junior_member_dues_flag]
     , SAT.[check_in_group_level]
     , SAT.[club_activation_date]
     , SAT.[club_code]
     , SAT.[club_name]
     , SAT.[club_deactivation_date]
     , SAT.[crm_division_code]
     , r_mms_val_currency_code.[currency_code]
     , SAT.[domain_name_prefix]
     , r_mms_val_time_zone.[dst_offset]
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
     , r_mms_val_time_zone.[st_offset]
     , SAT.[workday_region]
     , SAT.[inserted_date_time]
     , SAT.[updated_date_time]
     , DIM.[effective_date_time]
     , [dim_club_key] = PIT.[bk_hash]
     , PIT.[bk_hash]
     , PIT.[p_mms_club_id]
     , PIT.[dv_load_date_time]
     , PIT.[dv_batch_id]
     , [dv_hash] = CONVERT(char(32), HASHBYTES('MD5', (LNK.[dv_hash] + SAT.[dv_hash])),2)
  FROM [dbo].[p_mms_club] PIT
       INNER JOIN [dbo].[d_mms_club_history] DIM
         ON DIM.[bk_hash] = PIT.[bk_hash]
            AND DIM.[p_mms_club_id] = PIT.[p_mms_club_id]
       INNER JOIN [dbo].[l_mms_club] LNK
         ON LNK.[bk_hash] = PIT.[bk_hash]
            AND LNK.[l_mms_club_id] = PIT.[l_mms_club_id]
       INNER JOIN [dbo].[s_mms_club] SAT
         ON SAT.[bk_hash] = PIT.[bk_hash]
            AND SAT.[s_mms_club_id] = PIT.[s_mms_club_id]
       --INNER JOIN
       --  ( SELECT PIT.[p_mms_club_id]
       --         , PIT.[effective_date_time]
       --         , RowRank = RANK() OVER (PARTITION BY PIT.[bk_hash], DATEADD(DD, DATEDIFF(DD, 0, PIT.[effective_date_time]), 0) ORDER BY PIT.[dv_load_end_date_time] DESC)
       --         , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[bk_hash], DATEADD(DD, DATEDIFF(DD, 0, PIT.[effective_date_time]), 0) ORDER BY PIT.[dv_load_end_date_time] DESC)
       --      FROM [dbo].[d_mms_club_history] PIT
       --  ) PITU
       --  ON PITU.[p_mms_club_id] = PIT.[p_mms_club_id]
       --     AND PITU.RowRank = 1 AND PITU.RowNumber = 1
       LEFT OUTER JOIN [dbo].[r_mms_val_currency_code] r_mms_val_currency_code
         ON r_mms_val_currency_code.[dv_load_end_date_time] = '9999-12-31 00:00:00.000'
            AND r_mms_val_currency_code.[val_currency_code_id] = LNK.[val_currency_code_id]
       LEFT OUTER JOIN [dbo].[r_mms_val_time_zone] r_mms_val_time_zone
         ON r_mms_val_time_zone.[dv_load_end_date_time] = '9999-12-31 00:00:00.000'
            AND r_mms_val_time_zone.val_time_zone_id = LNK.[val_time_zone_id];