CREATE PROC [sandbox].[proc_mart_sw_d_spa_ticket_data] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT d_spabiz_ticket_data.ticket_data_id
     , d_spabiz_ticket_data.ticket_id
     , d_spabiz_ticket_data.item_id
     , d_spabiz_ticket_data.member_id
     , d_spabiz_ticket_data.employee_id
     , d_spabiz_ticket_data.club_id
     , d_spabiz_ticket_data.ticket_item_date_time
     , d_spabiz_ticket_data.status
     , d_spabiz_ticket_data.service_amount
     , d_spabiz_ticket_data.service_quantity
     , d_spabiz_ticket_data.service_name
     , d_spabiz_ticket_data.category_name
     , d_spabiz_ticket_data.first_name
     , d_spabiz_ticket_data.last_name
     , d_spabiz_ticket_data.middle_name
     , d_spabiz_ticket_data.email
     , d_spabiz_ticket_data.address_line_1
     , d_spabiz_ticket_data.address_line_2
     , d_spabiz_ticket_data.city
     , d_spabiz_ticket_data.state
     , d_spabiz_ticket_data.zip
     , d_spabiz_ticket_data.bk_hash
     , d_spabiz_ticket_data.p_spabiz_ticket_data_id
     , d_spabiz_ticket_data.dv_load_date_time
     , d_spabiz_ticket_data.dv_batch_id
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (batch_info.[l_hash] + batch_info.[s_hash])),2)
  FROM ( SELECT [ticket_data_id]        = CONVERT(int, d_spabiz_ticket_data.[ticket_data_id])
              , [ticket_id]             = CONVERT(int, d_spabiz_ticket_data.[ticket_id])
              , [item_id]               = CONVERT(int, d_spabiz_ticket_data.[l_spabiz_ticket_data_item_id])
              , [member_id]             = (CASE WHEN (ISNUMERIC(d_spabiz_customer.[member_id]) = 1 AND CONVERT(bigint, d_spabiz_customer.[member_id]) <= 2147483647) THEN CONVERT(int, d_spabiz_customer.[member_id]) ELSE Null END)
              , [employee_id]           = (CASE WHEN (ISNUMERIC(d_spabiz_staff.[neill_id]) = 1 AND CONVERT(bigint, d_spabiz_staff.[neill_id]) <= 2147483647) THEN CONVERT(int, d_spabiz_staff.[neill_id]) ELSE Null END)
              , [club_id]               = (CASE WHEN (ISNUMERIC(d_spabiz_staff.[store_number]) = 1 AND CONVERT(bigint, d_spabiz_staff.[store_number]) <= 2147483647) THEN CONVERT(int, d_spabiz_staff.[store_number]) ELSE Null END)
              , [ticket_item_date_time] = d_spabiz_ticket_data.[ticket_item_date_time]
              , [status]                = CONVERT(int, d_spabiz_ticket_data.[status_id])
              , [service_amount]        = d_spabiz_ticket_data.[service_amount]
              , [service_quantity]      = d_spabiz_ticket_data.[service_quantity]
              , [service_name]          = dim_spabiz_service.[service_name]
              , [category_name]         = CASE WHEN ISNULL(dim_spabiz_service.[category], ISNULL(dim_spabiz_service.[level_2_service_category], dim_spabiz_service.[level_1_service_category])) = '0' THEN null ELSE ISNULL(dim_spabiz_service.[category], ISNULL(dim_spabiz_service.[level_2_service_category], dim_spabiz_service.[level_1_service_category])) END
              , [first_name]            = d_spabiz_customer.[first_name]
              , [last_name]             = d_spabiz_customer.[last_name]
              , [middle_name]           = d_spabiz_customer.[middle_name]
              --, d_spabiz_customer.[gender]
              , [email]                 = d_spabiz_customer.[email]
              , [address_line_1]        = SUBSTRING(d_spabiz_customer.[address_line_1], 1, 150)
              , [address_line_2]        = SUBSTRING(d_spabiz_customer.[address_line_2], 1, 150)
              , [city]                  = d_spabiz_customer.[address_city]
              , [state]                 = d_spabiz_customer.[address_state_or_province]
              , [zip]                   = d_spabiz_customer.[address_postal_code]
              , d_spabiz_ticket_data.[bk_hash]
              , d_spabiz_ticket_data.[p_spabiz_ticket_data_id]
              , d_spabiz_ticket_data.[dv_load_date_time]
              , d_spabiz_ticket_data.[dv_batch_id]
           FROM [dbo].[d_spabiz_ticket_data] d_spabiz_ticket_data
                INNER JOIN [dbo].[d_spabiz_customer] d_spabiz_customer
                  ON d_spabiz_customer.[customer_id] = d_spabiz_ticket_data.[l_spabiz_ticket_data_cust_id]
                     AND d_spabiz_customer.[store_number] = d_spabiz_ticket_data.[store_number]
                INNER JOIN [dbo].[d_spabiz_staff] d_spabiz_staff
                  ON d_spabiz_staff.[staff_id] = d_spabiz_ticket_data.[l_spabiz_ticket_data_staff_id_1]
                INNER JOIN [dbo].[dim_spabiz_service] dim_spabiz_service
                  ON dim_spabiz_service.service_id = d_spabiz_ticket_data.[l_spabiz_ticket_data_item_id]
           WHERE d_spabiz_ticket_data.[l_spabiz_ticket_data_data_type] = 5
             AND d_spabiz_ticket_data.[status_id] = '1'
             AND d_spabiz_ticket_data.[ticket_item_date_time] >= '2010-01-01'
             AND ( d_spabiz_ticket_data.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
               AND d_spabiz_ticket_data.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END )
       ) d_spabiz_ticket_data
       CROSS APPLY
         ( SELECT [l_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, d_spabiz_ticket_data.[ticket_data_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_spabiz_ticket_data.[ticket_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_spabiz_ticket_data.[item_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_spabiz_ticket_data.[club_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_spabiz_ticket_data.[member_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_spabiz_ticket_data.[employee_id]),'z#@$k%&P'))),2)

                , [s_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, d_spabiz_ticket_data.[ticket_data_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_spabiz_ticket_data.[ticket_item_date_time], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_spabiz_ticket_data.[status]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_spabiz_ticket_data.[service_amount]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_spabiz_ticket_data.[service_quantity]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(d_spabiz_ticket_data.[service_name],'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(d_spabiz_ticket_data.[first_name],'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(d_spabiz_ticket_data.[last_name],'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(d_spabiz_ticket_data.[middle_name],'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(d_spabiz_ticket_data.[email],'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(d_spabiz_ticket_data.[address_line_1],'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(d_spabiz_ticket_data.[address_line_2],'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(d_spabiz_ticket_data.[city],'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(d_spabiz_ticket_data.[state],'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(d_spabiz_ticket_data.[zip],'z#@$k%&P'))),2)
         ) batch_info
ORDER BY d_spabiz_ticket_data.[dv_batch_id] ASC, d_spabiz_ticket_data.[dv_load_date_time] ASC, d_spabiz_ticket_data.[ticket_item_date_time];

END
