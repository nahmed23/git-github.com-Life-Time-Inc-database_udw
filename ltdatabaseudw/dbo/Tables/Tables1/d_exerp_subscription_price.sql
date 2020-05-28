CREATE TABLE [dbo].[d_exerp_subscription_price] (
    [d_exerp_subscription_price_id]    BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                          CHAR (32)       NOT NULL,
    [dim_exerp_subscription_price_key] VARCHAR (32)    NULL,
    [subscription_price_id]            INT             NULL,
    [cancel_dim_date_key]              CHAR (8)        NULL,
    [cancelled_flag]                   CHAR (1)        NULL,
    [dim_club_key]                     VARCHAR (32)    NULL,
    [dim_exerp_subscription_key]       VARCHAR (32)    NULL,
    [entry_dim_date_key]               CHAR (8)        NULL,
    [entry_dim_time_key]               CHAR (8)        NULL,
    [ets]                              BIGINT          NULL,
    [from_dim_date_key]                CHAR (8)        NULL,
    [price]                            DECIMAL (26, 6) NULL,
    [subscription_price_type]          VARCHAR (4000)  NULL,
    [to_dim_date_key]                  CHAR (8)        NULL,
    [p_exerp_subscription_price_id]    BIGINT          NOT NULL,
    [deleted_flag]                     INT             NULL,
    [dv_load_date_time]                DATETIME        NULL,
    [dv_load_end_date_time]            DATETIME        NULL,
    [dv_batch_id]                      BIGINT          NOT NULL,
    [dv_inserted_date_time]            DATETIME        NOT NULL,
    [dv_insert_user]                   VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]             DATETIME        NULL,
    [dv_update_user]                   VARCHAR (50)    NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

