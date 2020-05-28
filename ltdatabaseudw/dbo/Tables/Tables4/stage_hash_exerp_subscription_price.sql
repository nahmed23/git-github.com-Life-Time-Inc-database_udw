CREATE TABLE [dbo].[stage_hash_exerp_subscription_price] (
    [stage_hash_exerp_subscription_price_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                CHAR (32)       NOT NULL,
    [id]                                     INT             NULL,
    [subscription_id]                        VARCHAR (4000)  NULL,
    [type]                                   VARCHAR (4000)  NULL,
    [entry_datetime]                         DATETIME        NULL,
    [from_date]                              DATETIME        NULL,
    [to_date]                                DATETIME        NULL,
    [price]                                  DECIMAL (26, 6) NULL,
    [cancelled]                              BIT             NULL,
    [cancel_datetime]                        DATETIME        NULL,
    [center_id]                              INT             NULL,
    [ets]                                    BIGINT          NULL,
    [dummy_modified_date_time]               DATETIME        NULL,
    [dv_load_date_time]                      DATETIME        NOT NULL,
    [dv_updated_date_time]                   DATETIME        NULL,
    [dv_update_user]                         VARCHAR (50)    NULL,
    [dv_batch_id]                            BIGINT          NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

