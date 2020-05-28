CREATE VIEW [sandbox].[v_mart_mms_member_history]
AS SELECT DIM.[member_id]
     , DIM.[membership_id]
     , DIM.[val_member_type_id]
     , d_member_info.[active_flag]
     , d_member_info.[assess_jr_member_dues_flag]
     , d_member_info.[dob]
     , DIM.[join_date]
     , [effective_date_no_time] = DATEADD(DD, DATEDIFF(DD, 0, DIM.[effective_date_time]), 0)
     , DIM.[effective_date_time]
     , DIM.[dim_mms_member_key]
     , DIM.[dim_mms_membership_key]
     , DIM.[bk_hash]
     , DIM.[p_mms_member_id]
     , DIM.[dv_load_date_time]
     , DIM.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (batch_info.[l_hash] + batch_info.[s_hash])),2)
  FROM [dbo].[p_mms_member] PIT
       INNER JOIN [dbo].[d_mms_member_history] DIM
         ON DIM.[bk_hash] = PIT.[bk_hash]
            AND DIM.[p_mms_member_id] = PIT.[p_mms_member_id]
       INNER JOIN
         ( SELECT PIT.[bk_hash]
                , PIT.[p_mms_member_id]
                , PIT.[d_mms_member_history_id]
                , PIT.[effective_date_time]
                , RowRank = RANK() OVER (PARTITION BY PIT.[bk_hash], DATEADD(DD, DATEDIFF(DD, 0, PIT.[effective_date_time]), 0) ORDER BY PIT.[effective_date_time] DESC)
                , RowNumber = ROW_NUMBER() OVER (PARTITION BY PIT.[bk_hash], DATEADD(DD, DATEDIFF(DD, 0, PIT.[effective_date_time]), 0) ORDER BY PIT.[effective_date_time] DESC)
             FROM [dbo].[d_mms_member_history] PIT
         ) PITU
         ON PITU.[bk_hash] = DIM.[bk_hash]
            AND PITU.[d_mms_member_history_id] = DIM.[d_mms_member_history_id]
            AND PITU.RowRank = 1 AND PITU.RowNumber = 1
       CROSS APPLY
         ( SELECT [active_flag]                = CONVERT(bit, CASE WHEN ISNULL(DIM.[member_active_flag],'Y') = 'Y' THEN 1 ELSE 0 END)
                , [assess_jr_member_dues_flag] = CONVERT(bit, CASE WHEN ISNULL(DIM.[assess_junior_member_dues_flag],'Y') = 'Y' THEN 1 ELSE 0 END)
                , [dob] = DIM.[date_of_birth]
         ) d_member_info
       CROSS APPLY
         ( SELECT [l_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[member_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[membership_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[val_member_type_id]),'z#@$k%&P'))),2)

                , [s_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[member_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + CONVERT(varchar, ISNULL(d_member_info.[active_flag],0))
                                                                  + 'P%#&z$@k' + CONVERT(varchar, ISNULL(d_member_info.[assess_jr_member_dues_flag],1))
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[date_of_birth], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, DIM.[join_date], 120),'z#@$k%&P'))),2)
         ) batch_info;