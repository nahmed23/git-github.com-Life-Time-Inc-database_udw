CREATE PROC [sandbox].[proc_dim_exchange_rate] @min_batch_id [bigint],@max_batch_id [bigint] AS

BEGIN

SELECT [dim_exchange_rate_id]
     , [dim_exchange_rate_key]
     , [effective_date]
     , [from_currency_code]
     , [to_currency_code]
     , [exchange_rate_type_description]
     , [effective_dim_date_key]
     , [exchange_rate]
     , [source_daily_average_date]
     , [dv_load_date_time]
     , [dv_batch_id]
  FROM [dbo].[dim_exchange_rate]
ORDER BY [dim_exchange_rate_key];

END
