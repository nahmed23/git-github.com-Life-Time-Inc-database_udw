CREATE TABLE [dbo].[stage_hash_forexintegration_ExchangeRate] (
    [stage_hash_forexintegration_ExchangeRate_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                     CHAR (32)      NOT NULL,
    [exchange_rate_id]                            INT            NULL,
    [from_exchange_rate_iso_code]                 VARCHAR (3)    NULL,
    [to_exchange_rate_iso_code]                   VARCHAR (3)    NULL,
    [rate_type]                                   VARCHAR (50)   NULL,
    [effective_date]                              DATETIME       NULL,
    [daily_average_date]                          DATETIME       NULL,
    [currency_rate]                               NUMERIC (5, 4) NULL,
    [source]                                      VARCHAR (50)   NULL,
    [inserted_date_time]                          DATETIME       NULL,
    [updated_date_time]                           DATETIME       NULL,
    [dv_load_date_time]                           DATETIME       NOT NULL,
    [dv_inserted_date_time]                       DATETIME       NOT NULL,
    [dv_insert_user]                              VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]                        DATETIME       NULL,
    [dv_update_user]                              VARCHAR (50)   NULL,
    [dv_batch_id]                                 BIGINT         NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

