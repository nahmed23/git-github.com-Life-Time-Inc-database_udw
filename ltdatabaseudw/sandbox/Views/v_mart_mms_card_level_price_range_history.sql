CREATE VIEW [sandbox].[v_mart_mms_card_level_price_range_history]
AS SELECT [card_level_price_range_id]
     , [val_card_level_id]
     , [product_id]
     , [starting_price]
     , [ending_price]
     , [inserted_date_time]
     , [updated_date_time]
     , [effective_date_no_time] = DATEADD(DD, DATEDIFF(DD, 0, [effective_date_time]), 0)
     , [effective_date_time]
     , [expiration_date_no_time] = DATEADD(DD, DATEDIFF(DD, 0, [expiration_date_time]), 0)
     , [expiration_date_time]
     , [bk_hash]
     , [p_mms_card_level_price_range_id]
     , [dv_load_date_time]
     , [dv_batch_id]
     , [dv_hash] = CONVERT(varchar(32), HASHBYTES('MD5', (batch_info.[l_hash] + batch_info.[s_hash])),2)
  FROM [dbo].[d_mms_card_level_price_range_history]
       CROSS APPLY
         ( SELECT [l_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, [card_level_price_range_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, [val_card_level_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, [product_id]),'z#@$k%&P'))),2)

                , [s_hash] = CONVERT(varchar(32), HASHBYTES('MD5', ('P%#&z$@k' + ISNULL(CONVERT(varchar, [card_level_price_range_id]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, [starting_price]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, [ending_price]),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, [effective_date_time], 120),'z#@$k%&P')
                                                                  + 'P%#&z$@k' + ISNULL(CONVERT(varchar, [expiration_date_time], 120),'z#@$k%&P'))),2)
         ) batch_info;