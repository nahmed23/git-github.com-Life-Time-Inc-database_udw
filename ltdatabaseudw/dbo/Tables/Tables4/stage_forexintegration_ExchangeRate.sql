CREATE TABLE [dbo].[stage_forexintegration_ExchangeRate] (
    [stage_forexintegration_ExchangeRate_id] BIGINT         NOT NULL,
    [exchange_rate_id]                       INT            NULL,
    [from_exchange_rate_iso_code]            VARCHAR (3)    NULL,
    [to_exchange_rate_iso_code]              VARCHAR (3)    NULL,
    [rate_type]                              VARCHAR (50)   NULL,
    [effective_date]                         DATETIME       NULL,
    [daily_average_date]                     DATETIME       NULL,
    [currency_rate]                          NUMERIC (5, 4) NULL,
    [source]                                 VARCHAR (50)   NULL,
    [inserted_date_time]                     DATETIME       NULL,
    [updated_date_time]                      DATETIME       NULL,
    [dv_batch_id]                            BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

