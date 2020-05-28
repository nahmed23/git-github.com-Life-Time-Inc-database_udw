CREATE TABLE [dbo].[d_forexintegration_exchange_rate] (
    [d_forexintegration_exchange_rate_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                             CHAR (32)       NOT NULL,
    [exchange_rate_id]                    INT             NULL,
    [effective_date]                      DATETIME        NULL,
    [effective_dim_date_key]              CHAR (8)        NULL,
    [exchange_rate]                       DECIMAL (26, 6) NULL,
    [exchange_rate_type_description]      VARCHAR (50)    NULL,
    [from_currency_code]                  VARCHAR (3)     NULL,
    [source_daily_average_date]           DATETIME        NULL,
    [to_currency_code]                    VARCHAR (3)     NULL,
    [p_forexintegration_exchange_rate_id] BIGINT          NOT NULL,
    [deleted_flag]                        INT             NULL,
    [dv_load_date_time]                   DATETIME        NULL,
    [dv_load_end_date_time]               DATETIME        NULL,
    [dv_batch_id]                         BIGINT          NOT NULL,
    [dv_inserted_date_time]               DATETIME        NOT NULL,
    [dv_insert_user]                      VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                DATETIME        NULL,
    [dv_update_user]                      VARCHAR (50)    NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_forexintegration_exchange_rate]([dv_batch_id] ASC);

