CREATE TABLE [dbo].[dim_plan_exchange_rate] (
    [dim_plan_exchange_rate_id]  BIGINT          IDENTITY (1, 1) NOT NULL,
    [dim_plan_exchange_rate_key] CHAR (32)       NULL,
    [from_currency_code]         VARCHAR (3)     NULL,
    [to_currency_code]           VARCHAR (3)     NULL,
    [plan_rate]                  DECIMAL (26, 6) NULL,
    [dv_load_date_time]          DATETIME        NULL,
    [dv_load_end_date_time]      DATETIME        NULL,
    [dv_batch_id]                BIGINT          NULL,
    [dv_inserted_date_time]      DATETIME        NOT NULL,
    [dv_insert_user]             VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]       DATETIME        NULL,
    [dv_update_user]             VARCHAR (50)    NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dim_plan_exchange_rate_key]));

