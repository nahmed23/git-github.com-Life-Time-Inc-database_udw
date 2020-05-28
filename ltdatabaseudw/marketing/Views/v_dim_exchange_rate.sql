CREATE VIEW [marketing].[v_dim_exchange_rate] AS select dim_exchange_rate.dim_exchange_rate_key dim_exchange_rate_key,
       dim_exchange_rate.effective_date effective_date,
       dim_exchange_rate.from_currency_code from_currency_code,
       dim_exchange_rate.to_currency_code to_currency_code,
       dim_exchange_rate.exchange_rate_type_description exchange_rate_type_description,
       dim_exchange_rate.exchange_rate exchange_rate,
       dim_exchange_rate.source_daily_average_date source_daily_average_date
  from dbo.dim_exchange_rate;