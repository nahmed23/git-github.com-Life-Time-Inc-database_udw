CREATE PROC [sandbox].[proc_mart_sw_d_exerp_participation] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT d_exerp_participation.[participation_id]
     , d_exerp_participation.[activity_id]
     , d_exerp_participation.[booking_id]
     , d_exerp_participation.[employee_id]
     , d_exerp_participation.[member_id]
     , d_exerp_participation.[product_id]
     , d_exerp_participation.[dim_exerp_activity_key]
     , d_exerp_participation.[dim_exerp_booking_key]
     , d_exerp_participation.[dim_employee_key]
     , d_exerp_participation.[dim_mms_member_key]
     , d_exerp_participation.[dim_mms_product_key]
     , d_exerp_participation.[activity_name]
     , d_exerp_participation.[booking_state]
     , d_exerp_participation.[participation_state]
     , d_exerp_participation.[staff_usage_state]
     , d_exerp_participation.[set_date]
     , d_exerp_participation.[schedule_date]
     , d_exerp_participation.[complete_date]
     , d_exerp_participation.[RowRank]
     , d_exerp_participation.[RowNumber]
     , d_exerp_participation.[bk_hash]
     , d_exerp_participation.[p_exerp_participation_id]
     , d_exerp_participation.[dv_load_date_time]
     , d_exerp_participation.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (batch_info.[l_hash] + batch_info.[s_hash])),2)
     , [dv_deleted] = CAST(0 AS bit)
  FROM ( SELECT [participation_id] = SUBSTRING(d_exerp_participation.[participation_id], 1, 20)
              , dim_exerp_activity.[activity_id]
              , [booking_id] = SUBSTRING(dim_exerp_booking.[booking_id], 1, 20)
              , dim_employee.[employee_id]
              , d_mms_member.[member_id]
              , dim_mms_product.[product_id]
              , [dim_exerp_activity_key] = CONVERT(varchar(32), dim_exerp_activity.[dim_exerp_activity_key])
              , [dim_exerp_booking_key]  = CONVERT(varchar(32), d_exerp_participation.[dim_exerp_booking_key])
              , [dim_employee_key]       = CONVERT(varchar(32), dim_exerp_staff_usage.[dim_employee_key])
              , [dim_mms_member_key]     = CONVERT(varchar(32), d_exerp_participation.[dim_mms_member_key])
              , [dim_mms_product_key]    = CONVERT(varchar(32), dim_exerp_activity.[dim_mms_product_key])
              , [activity_name]          = SUBSTRING(dim_exerp_activity.[activity_name], 1, 50)
              , [booking_state]          = SUBSTRING(dim_exerp_booking.[booking_state], 1, 20)
              , [participation_state]    = SUBSTRING(d_exerp_participation.[participation_state], 1, 20)
              , [staff_usage_state]      = SUBSTRING(dim_exerp_staff_usage.[staff_usage_state], 1, 20)
              , d_batch_detail.[set_date]
              , d_batch_detail.[schedule_date]
              , d_batch_detail.[complete_date]
              , RowRank = RANK() OVER (PARTITION BY d_exerp_participation.[dim_mms_member_key] ORDER BY d_batch_detail.[set_date] ASC, d_batch_detail.[schedule_date] ASC, d_batch_detail.[complete_date] ASC)
              , RowNumber = ROW_NUMBER() OVER (PARTITION BY d_exerp_participation.[dim_mms_member_key] ORDER BY d_batch_detail.[set_date] ASC, d_batch_detail.[schedule_date] ASC, d_batch_detail.[complete_date] ASC)
              , d_exerp_participation.[bk_hash]
              , d_exerp_participation.[p_exerp_participation_id]
              , d_batch_detail.[dv_load_date_time]
              , d_batch_detail.[dv_batch_id]
           FROM [dbo].[dim_exerp_activity]
                INNER JOIN [dbo].[dim_mms_product]
                  ON dim_mms_product.[dim_mms_product_key] = dim_exerp_activity.[dim_mms_product_key]
                INNER JOIN [dbo].[dim_exerp_booking]
                  ON dim_exerp_booking.[dim_exerp_activity_key] = dim_exerp_activity.[dim_exerp_activity_key]
                INNER JOIN [dbo].[d_exerp_participation]
                  ON d_exerp_participation.[dim_exerp_booking_key] = dim_exerp_booking.[dim_exerp_booking_key]
                INNER JOIN [dbo].[d_mms_member]
                  ON d_mms_member.[dim_mms_member_key] = d_exerp_participation.[dim_mms_member_key]
                INNER JOIN [dbo].[dim_exerp_staff_usage]
                  ON dim_exerp_staff_usage.[dim_exerp_booking_key] = dim_exerp_booking.[dim_exerp_booking_key]
                INNER JOIN [dbo].[dim_employee]
                  ON dim_employee.[dim_employee_key] = dim_exerp_staff_usage.[dim_employee_key]
                INNER JOIN [dbo].[dim_date] dd_created
                  ON dd_created.[dim_date_key] = d_exerp_participation.[creation_dim_date_key]
                INNER JOIN [dbo].[dim_date] dd_scheduled
                  ON dd_scheduled.[dim_date_key] = dim_exerp_booking.[start_dim_date_key]
                INNER JOIN [dbo].[dim_date] dd_completed
                  ON dd_completed.[dim_date_key] = d_exerp_participation.[show_up_dim_date_key]
                CROSS APPLY
                  ( SELECT set_date = CASE WHEN dd_created.calendar_date = '1899-12-30 00:00:00.000' THEN Null ELSE dd_created.calendar_date END
                         , schedule_date = CASE WHEN dd_scheduled.calendar_date = '1899-12-30 00:00:00.000' THEN Null ELSE dd_scheduled.calendar_date END
                         , complete_date = CASE WHEN dd_completed.calendar_date = '1899-12-30 00:00:00.000' THEN Null ELSE dd_completed.calendar_date END
                         , [dv_load_date_time] = CASE WHEN ( (ISNULL(dim_exerp_staff_usage.[dv_batch_id],-1) > ISNULL(d_exerp_participation.[dv_batch_id],-1))
                                                         AND (ISNULL(dim_exerp_staff_usage.[dv_batch_id],-1) > ISNULL(dim_exerp_booking.[dv_batch_id],-1)) )
                                                           THEN dim_exerp_staff_usage.[dv_load_date_time]
                                                      WHEN ( (ISNULL(dim_exerp_booking.[dv_batch_id],-1) > ISNULL(d_exerp_participation.[dv_batch_id],-1))
                                                         AND (ISNULL(dim_exerp_booking.[dv_batch_id],-1) > ISNULL(dim_exerp_staff_usage.[dv_batch_id],-1)) )
                                                           THEN dim_exerp_booking.[dv_load_date_time]
                                                      ELSE d_exerp_participation.[dv_load_date_time] END
                         , [dv_batch_id] = CASE WHEN ( (ISNULL(dim_exerp_staff_usage.[dv_batch_id],-1) > ISNULL(d_exerp_participation.[dv_batch_id],-1))
                                                   AND (ISNULL(dim_exerp_staff_usage.[dv_batch_id],-1) > ISNULL(dim_exerp_booking.[dv_batch_id],-1)) )
                                                     THEN dim_exerp_staff_usage.[dv_batch_id]
                                                WHEN ( (ISNULL(dim_exerp_booking.[dv_batch_id],-1) > ISNULL(d_exerp_participation.[dv_batch_id],-1))
                                                   AND (ISNULL(dim_exerp_booking.[dv_batch_id],-1) > ISNULL(dim_exerp_staff_usage.[dv_batch_id],-1)) )
                                                     THEN dim_exerp_booking.[dv_batch_id]
                                                ELSE d_exerp_participation.[dv_batch_id] END
                  ) d_batch_detail
           WHERE dim_exerp_activity.[external_id] = '701592826589'
             --AND dim_exerp_activity.[activity_type] = 'CLASS_BOOKING'  --'STAFF_BOOKING' is obsolete
             AND d_exerp_participation.[dim_mms_member_key] <> '-998'
             AND d_exerp_participation.[participation_state] <> 'CANCELLED'
             AND d_exerp_participation.[cancel_dim_date_key] = '-998'
             AND dim_exerp_staff_usage.[staff_usage_state] <> 'CANCELLED'
             AND ( d_batch_detail.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
               AND d_batch_detail.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END )
       ) d_exerp_participation
       CROSS APPLY
         ( SELECT [l_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, d_exerp_participation.[participation_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_exerp_participation.[activity_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_exerp_participation.[booking_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_exerp_participation.[employee_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_exerp_participation.[member_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_exerp_participation.[product_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(d_exerp_participation.[dim_exerp_activity_key],'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(d_exerp_participation.[dim_exerp_booking_key],'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(d_exerp_participation.[dim_employee_key],'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(d_exerp_participation.[dim_mms_member_key],'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(d_exerp_participation.[dim_mms_product_key],'z#@$k%&P'))),2)

                , [s_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, d_exerp_participation.[participation_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_exerp_participation.[activity_name]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_exerp_participation.[booking_state]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_exerp_participation.[participation_state]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_exerp_participation.[staff_usage_state]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_exerp_participation.[set_date], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_exerp_participation.[schedule_date], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_exerp_participation.[complete_date], 120),'z#@$k%&P'))),2)
         ) batch_info
  WHERE d_exerp_participation.RowRank = 1
    AND d_exerp_participation.RowNumber = 1
ORDER BY d_exerp_participation.[dv_batch_id] ASC, d_exerp_participation.[dv_load_date_time] ASC, d_exerp_participation.[member_id], d_exerp_participation.set_date, d_exerp_participation.schedule_date, d_exerp_participation.complete_date;

END
