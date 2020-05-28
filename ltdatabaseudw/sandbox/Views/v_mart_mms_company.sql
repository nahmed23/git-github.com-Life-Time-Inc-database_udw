CREATE VIEW [sandbox].[v_mart_mms_company]
AS SELECT PIT.[company_id]
         , SAT.[account_rep_initials]
         , SAT.[company_name]
         , SAT.[print_usage_report_flag]
         , SAT.[corporate_code]
         , SAT.[inserted_date_time]
         , SAT.[start_date]
         , SAT.[end_date]
         , SAT.[account_rep_name]
         , SAT.[initiation_fee]
         , SAT.[updated_date_time]
         , SAT.[enrollment_disc_percentage]
         , SAT.[mac_enrollment_disc_percentage]
         , SAT.[invoice_flag]
         , SAT.[dollar_discount]
         , SAT.[admin_fee]
         , SAT.[override_percentage]
         , SAT.[eft_account_number]
         , SAT.[usage_report_flag]
         , SAT.[report_to_email_address]
         , SAT.[usage_report_member_type]
         , SAT.[small_business_flag]
         , SAT.[account_owner]
         , SAT.[subsidy_measurement]
         , SAT.[opportunity_record_type]
         , PIT.[bk_hash]
         , PIT.[p_mms_company_id]
         , PIT.[dv_load_date_time]
         , PIT.[dv_batch_id]
         , SAT.[dv_hash]
      FROM [dbo].[p_mms_company] PIT
           INNER JOIN [dbo].[s_mms_company] SAT
             ON SAT.[bk_hash] = PIT.[bk_hash]
                AND SAT.[s_mms_company_id] = PIT.[s_mms_company_id]
           INNER JOIN
             ( SELECT PIT.[p_mms_company_id]
                    , RowRank = RANK() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
                    , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
                 FROM [dbo].[d_mms_company] PIT
             ) PITU
             ON PITU.[p_mms_company_id] = PIT.[p_mms_company_id]
                AND PITU.RowRank = 1 AND PITU.RowNumber = 1;