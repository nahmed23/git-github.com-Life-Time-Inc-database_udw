CREATE TABLE [dbo].[h_fdw_dim_period_exchange_rate] (
    [h_fdw_dim_period_exchange_rate_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                           CHAR (32)    NOT NULL,
    [dim_accounting_period_key]         INT          NULL,
    [from_currency_code]                VARCHAR (10) NULL,
    [to_currency_code]                  VARCHAR (10) NULL,
    [dv_load_date_time]                 DATETIME     NOT NULL,
    [dv_batch_id]                       BIGINT       NOT NULL,
    [dv_r_load_source_id]               BIGINT       NOT NULL,
    [dv_inserted_date_time]             DATETIME     NOT NULL,
    [dv_insert_user]                    VARCHAR (50) NOT NULL,
    [dv_updated_date_time]              DATETIME     NULL,
    [dv_update_user]                    VARCHAR (50) NULL,
    [dv_deleted]                        BIT          DEFAULT ((0)) NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_h_fdw_dim_period_exchange_rate]
    ON [dbo].[h_fdw_dim_period_exchange_rate]([bk_hash] ASC, [h_fdw_dim_period_exchange_rate_id] ASC);

