CREATE PROC [sandbox].[proc_mart_sw_d_mms_card_level_price_range_history] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT [card_level_price_range_id]
     , [val_card_level_id]
     , [product_id]
     , [starting_price]
     , [ending_price]
     , [inserted_date_time]
     , [updated_date_time]
     , [effective_date_time]
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

         ) batch_info
  WHERE NOT [card_level_price_range_id] Is Null
    AND ( [dv_batch_id] >= CASE WHEN @min_batch_id = -998 THEN -1 ELSE @min_batch_id END
      AND [dv_batch_id] <= CASE WHEN @max_batch_id = -998 THEN 9223372036854775807 ELSE @max_batch_id END )
ORDER BY [dv_batch_id] ASC, [dv_load_date_time] ASC, ISNULL([updated_date_time], [inserted_date_time]) ASC;

END
