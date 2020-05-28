CREATE TABLE [dbo].[s_fdw_dim_period_exchange_rate] (
    [s_fdw_dim_period_exchange_rate_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                           CHAR (32)       NOT NULL,
    [dim_period_exchange_rate_key]      INT             NULL,
    [dim_accounting_period_key]         INT             NULL,
    [from_currency_code]                VARCHAR (10)    NULL,
    [to_currency_code]                  VARCHAR (10)    NULL,
    [budget_rate]                       DECIMAL (14, 4) NULL,
    [plan_rate]                         DECIMAL (14, 4) NULL,
    [inserted_date_time]                DATETIME        NULL,
    [insert_user]                       VARCHAR (50)    NULL,
    [updated_date_time]                 DATETIME        NULL,
    [updated_user]                      VARCHAR (50)    NULL,
    [batch_id]                          BIGINT          NULL,
    [dv_load_date_time]                 DATETIME        NOT NULL,
    [dv_batch_id]                       BIGINT          NOT NULL,
    [dv_r_load_source_id]               BIGINT          NOT NULL,
    [dv_inserted_date_time]             DATETIME        NOT NULL,
    [dv_insert_user]                    VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]              DATETIME        NULL,
    [dv_update_user]                    VARCHAR (50)    NULL,
    [dv_hash]                           CHAR (32)       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_fdw_dim_period_exchange_rate]
    ON [dbo].[s_fdw_dim_period_exchange_rate]([bk_hash] ASC, [s_fdw_dim_period_exchange_rate_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_fdw_dim_period_exchange_rate]([dv_batch_id] ASC);

