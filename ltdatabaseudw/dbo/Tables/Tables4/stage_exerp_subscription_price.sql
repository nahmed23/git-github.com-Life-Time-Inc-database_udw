CREATE TABLE [dbo].[stage_exerp_subscription_price] (
    [stage_exerp_subscription_price_id] BIGINT          NOT NULL,
    [id]                                INT             NULL,
    [subscription_id]                   VARCHAR (4000)  NULL,
    [type]                              VARCHAR (4000)  NULL,
    [entry_datetime]                    DATETIME        NULL,
    [from_date]                         DATETIME        NULL,
    [to_date]                           DATETIME        NULL,
    [price]                             DECIMAL (26, 6) NULL,
    [cancelled]                         BIT             NULL,
    [cancel_datetime]                   DATETIME        NULL,
    [center_id]                         INT             NULL,
    [ets]                               BIGINT          NULL,
    [dummy_modified_date_time]          DATETIME        NULL,
    [dv_batch_id]                       BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

