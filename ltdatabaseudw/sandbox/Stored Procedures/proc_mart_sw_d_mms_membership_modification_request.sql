CREATE PROC [sandbox].[proc_mart_sw_d_mms_membership_modification_request] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT PIT.[membership_modification_request_id]
     , LNK.[membership_id]
     , LNK.[member_id]
     , LNK.[val_membership_modification_request_type_id]
     , LNK.[val_flex_reason_id]
     , LNK.[membership_type_id]
     , LNK.[val_membership_modification_request_status_id]
     , LNK.[employee_id]
     , LNK.[val_membership_upgrade_date_range_id]
     , LNK.[club_id]
     , LNK.[commisioned_employee_id]
     , LNK.[member_agreement_staging_id]
     , LNK.[previous_membership_type_id]
     , SAT.[request_date_time]
     , SAT.[utc_request_date_time]
     , SAT.[request_date_time_zone]
     , SAT.[effective_date]
     , SAT.[inserted_date_time]
     , SAT.[updated_date_time]
     , SAT.[status_changed_date_time]
     , SAT.[last_eft_month]
     , SAT.[future_membership_upgrade_flag]
     , SAT.[first_months_dues]
     , SAT.[total_monthly_amount]
     , SAT.[membership_upgrade_month_year]
     , SAT.[agreement_price]
     , SAT.[waive_service_fee_flag]
     , SAT.[full_access_date_extension_flag]
     , SAT.[new_members]
     , SAT.[add_on_fee]
     , SAT.[service_fee]
     , SAT.[diamond_fee]
     , SAT.[pro_rated_dues]
     , SAT.[deactivated_members]
     , SAT.[juniors_assessed]
     , SAT.[member_freeze_flag]
     , PIT.[bk_hash]
     , PIT.[p_mms_membership_modification_request_id]
     , PIT.[dv_load_date_time]
     , PIT.[dv_batch_id]
     , [dv_hash] = CONVERT(char(32), HASHBYTES('MD5', (LNK.[dv_hash] + SAT.[dv_hash])),2)
     --, [l_hash] = LNK.[dv_hash]
     --, [s_hash] = SAT.[dv_hash]
  FROM [dbo].[p_mms_membership_modification_request] PIT
       INNER JOIN [dbo].[l_mms_membership_modification_request] LNK
         ON LNK.[bk_hash] = PIT.[bk_hash]
            AND LNK.[l_mms_membership_modification_request_id] = PIT.[l_mms_membership_modification_request_id]
       INNER JOIN[dbo].[s_mms_membership_modification_request] SAT
         ON SAT.[bk_hash] = PIT.[bk_hash]
            AND SAT.[s_mms_membership_modification_request_id] = PIT.[s_mms_membership_modification_request_id]
       INNER JOIN
         ( SELECT PIT.[p_mms_membership_modification_request_id]
                , RowRank = RANK() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
                , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[bk_hash] ORDER BY PIT.[dv_load_end_date_time] DESC)
             FROM [dbo].[p_mms_membership_modification_request] PIT
             WHERE ( PIT.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
                 AND PIT.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END )
         ) PITU
         ON PITU.[p_mms_membership_modification_request_id] = PIT.[p_mms_membership_modification_request_id]
            AND PITU.RowRank = 1 AND PITU.RowNumber = 1
  WHERE NOT PIT.[membership_modification_request_id] Is Null
ORDER BY PIT.[dv_batch_id] ASC, PIT.[dv_load_date_time] ASC, ISNULL(SAT.[updated_date_time], SAT.[inserted_date_time]) ASC;

END
