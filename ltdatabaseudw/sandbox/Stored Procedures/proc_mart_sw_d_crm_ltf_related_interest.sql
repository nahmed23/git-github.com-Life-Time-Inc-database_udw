CREATE PROC [sandbox].[proc_mart_sw_d_crm_ltf_related_interest] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT d_crm_ltf_related_interest.[ltf_related_interest_id]
     , d_crm_ltf_related_interest.[contact_id]
     , d_crm_ltf_related_interest.[ltf_interest_id]
     , d_crm_ltf_related_interest.[ltf_subscriber_id]
     , d_crm_ltf_related_interest.[dim_crm_contact_key]
     , d_crm_ltf_related_interest.[dim_crm_ltf_interest_key]
     , d_crm_ltf_related_interest.[dim_crm_ltf_related_interest_key]
     , d_crm_ltf_related_interest.[dim_crm_ltf_subscriber_key]
     , d_crm_ltf_related_interest.[dim_mms_member_key]
     , d_crm_ltf_related_interest.[created_on]
     , d_crm_ltf_related_interest.[ltf_add_date]
     , d_crm_ltf_related_interest.[ltf_add_source]
     , d_crm_ltf_related_interest.[ltf_interest_id_name]
     , d_crm_ltf_related_interest.[ltf_primary_interest]
     , d_crm_ltf_related_interest.[ltf_remove_date]
     , d_crm_ltf_related_interest.[ltf_remove_source]
     , d_crm_ltf_related_interest.[modified_on]
     , d_crm_ltf_related_interest.[state_code]
     , d_crm_ltf_related_interest.[status_code]
     , d_crm_ltf_related_interest.[member_id]
     , d_crm_ltf_related_interest.[inserted_date_time]
     , d_crm_ltf_related_interest.[updated_date_time]
     , d_crm_ltf_related_interest.[bk_hash]
     , d_crm_ltf_related_interest.[p_crmcloudsync_ltf_related_interest_id]
     , d_crm_ltf_related_interest.[dv_load_date_time]
     , d_crm_ltf_related_interest.[dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (batch_info.[l_hash] + batch_info.[s_hash])),2)
     , d_crm_ltf_related_interest.[dv_deleted]
  FROM ( SELECT d_crm_ltf_related_interest.[ltf_related_interest_id]
              , d_crm_contact.[contact_id]
              , d_crm_ltf_related_interest.[ltf_interest_id]
              , d_crm_ltf_subscriber.[ltf_subscriber_id]
              , d_crm_ltf_related_interest.[dim_crm_contact_key]
              , [dim_crm_ltf_interest_key]         = d_crm_ltf_related_interest.[dim_crm_interest_key]
              , [dim_crm_ltf_related_interest_key] = d_crm_ltf_related_interest.[dim_crm_related_interest_key]
              , [dim_crm_ltf_subscriber_key]       = d_crm_ltf_subscriber.[dim_crm_ltf_subscriber_key]
              , [dim_mms_member_key]               = d_crm_ltf_subscriber.[dim_mms_member_key]
              , d_crm_ltf_related_interest.[created_on]
              , d_crm_ltf_related_interest.[ltf_add_date]
              , d_crm_ltf_related_interest.[ltf_add_source]
              , d_crm_ltf_related_interest.[ltf_interest_id_name]
              , [ltf_primary_interest] = CAST(CASE WHEN ISNULL(d_crm_ltf_related_interest.[primary_interest_flag], 'N') = 'N' THEN 0 ELSE 1 END AS bit)
              , d_crm_ltf_related_interest.[ltf_remove_date]
              , d_crm_ltf_related_interest.[ltf_remove_source]
              , d_crm_ltf_related_interest.[modified_on]
              , d_crm_ltf_related_interest.[state_code]
              , d_crm_ltf_related_interest.[status_code]
              , d_crm_ltf_subscriber.[member_id]
              , [inserted_date_time] = d_crm_ltf_related_interest.[dv_inserted_date_time]
              , [updated_date_time]  = d_crm_ltf_related_interest.[dv_updated_date_time]
              , d_crm_ltf_related_interest.[bk_hash]
              , d_crm_ltf_related_interest.[p_crmcloudsync_ltf_related_interest_id]
              , d_crm_ltf_related_interest.[dv_load_date_time]
              , d_crm_ltf_related_interest.[dv_batch_id]
              , [dv_deleted] = CAST(CASE WHEN d_crm_ltf_subscriber.[ltf_subscriber_id] Is Null THEN 1 WHEN d_crm_ltf_related_interest.[status_code] = 2 THEN 1 ELSE ISNULL(d_crm_ltf_related_interest.[deleted_flag],0) END AS bit)
           FROM [dbo].[d_crmcloudsync_ltf_related_interest] d_crm_ltf_related_interest
                INNER JOIN [dbo].[d_crmcloudsync_contact] d_crm_contact
                  ON d_crm_contact.[dim_crm_contact_key] = d_crm_ltf_related_interest.[dim_crm_contact_key]
                LEFT OUTER JOIN
                  ( SELECT d_crm_ltf_subscriber.[dim_crm_ltf_subscriber_key]
                         , d_crm_ltf_subscriber.[ltf_subscriber_id]
                         --, d_crm_ltf_subscriber.[dim_crm_ltf_subscription_key]
                         --, d_crm_ltf_subscription.[ltf_subscription_id]
                         , [dim_crm_contact_key] = d_crm_ltf_subscriber.[ltf_contact_dim_crm_contact_key]
                         , [dim_mms_member_key] = d_crm_ltf_subscriber.[dim_mms_member_key]
                         , [member_id] = CASE WHEN (ISNUMERIC(d_crm_ltf_subscriber.[ltf_name]) = 1 AND CONVERT(bigint, d_crm_ltf_subscriber.[ltf_name]) <= 2147483647) THEN CONVERT(int, d_crm_ltf_subscriber.[ltf_name]) ELSE Null END
                         , RowRank = RANK() OVER (PARTITION BY d_crm_ltf_subscriber.[ltf_contact_dim_crm_contact_key] ORDER BY d_crm_ltf_subscriber.[created_on] DESC, d_crm_ltf_subscriber.[dv_load_date_time] DESC)
                         , RowNumber = ROW_NUMBER() OVER (PARTITION BY d_crm_ltf_subscriber.[ltf_contact_dim_crm_contact_key] ORDER BY d_crm_ltf_subscriber.[created_on] DESC, d_crm_ltf_subscriber.[dv_load_date_time] DESC)
                      FROM [dbo].[d_crmcloudsync_ltf_subscriber] d_crm_ltf_subscriber
                           INNER JOIN [dbo].[d_crmcloudsync_ltf_subscription] d_crm_ltf_subscription
                             ON d_crm_ltf_subscription.[dim_crm_ltf_subscription_key] = d_crm_ltf_subscriber.[dim_crm_ltf_subscription_key]
                      WHERE d_crm_ltf_subscription.[status_code] <> 2
                        AND d_crm_ltf_subscriber.[status_code] = 1
                  ) d_crm_ltf_subscriber
                  ON d_crm_ltf_subscriber.[dim_crm_contact_key] = d_crm_ltf_related_interest.[dim_crm_contact_key]
                    AND d_crm_ltf_subscriber.RowRank = 1 AND d_crm_ltf_subscriber.RowNumber = 1
           WHERE d_crm_ltf_related_interest.[inserted_date_time] >= DATEADD(YY, -2, DATEADD(YY, DATEDIFF(YY, 0, DATEADD(DD, -1, DATEADD(DD, DATEDIFF(DD, 0, GETDATE()), 0))), 0))
             AND ( d_crm_ltf_related_interest.[dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
               AND d_crm_ltf_related_interest.[dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END )
       ) d_crm_ltf_related_interest
       CROSS APPLY
         ( SELECT [l_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar(36), d_crm_ltf_related_interest.[ltf_related_interest_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar(36), d_crm_ltf_related_interest.[contact_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar(36), d_crm_ltf_related_interest.[ltf_interest_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_ltf_related_interest.[member_id]),'z#@$k%&P'))),2)

                , [s_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar(36), d_crm_ltf_related_interest.[ltf_related_interest_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_ltf_related_interest.[created_on], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_ltf_related_interest.[ltf_interest_id_name]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_ltf_related_interest.[modified_on], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_ltf_related_interest.[state_code]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, d_crm_ltf_related_interest.[status_code]),'z#@$k%&P'))),2)
         ) batch_info
ORDER BY d_crm_ltf_related_interest.[dv_batch_id] ASC, d_crm_ltf_related_interest.[dv_load_date_time] ASC, ISNULL(d_crm_ltf_related_interest.[modified_on],d_crm_ltf_related_interest.[created_on]) ASC;

END
