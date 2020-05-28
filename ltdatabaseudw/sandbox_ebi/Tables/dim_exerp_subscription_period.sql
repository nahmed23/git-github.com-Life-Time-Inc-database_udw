CREATE TABLE [sandbox_ebi].[dim_exerp_subscription_period] (
    [dim_exerp_subscription_period_key]          CHAR (32)        NULL,
    [subscription_period_id]                     VARCHAR (4000)   NULL,
    [dim_exerp_subscription_key]                 CHAR (32)        NULL,
    [fact_exerp_transaction_log_key]             VARCHAR (4000)   NULL,
    [dim_club_key]                               CHAR (32)        NULL,
    [subscription_period_state]                  VARCHAR (4000)   NULL,
    [subscription_period_type]                   VARCHAR (4000)   NULL,
    [from_dim_date_key]                          VARCHAR (8)      NULL,
    [to_dim_date_key]                            CHAR (8)         NULL,
    [dim_mms_member_key]                         VARCHAR (32)     NULL,
    [dim_exerp_product_key]                      CHAR (32)        NULL,
    [net_amount]                                 DECIMAL (26, 6)  NOT NULL,
    [number_of_bookings]                         INT              NOT NULL,
    [price_per_booking]                          DECIMAL (37, 17) NOT NULL,
    [lt_bucks_amount]                            DECIMAL (26, 6)  NOT NULL,
    [price_per_booking_less_lt_bucks]            DECIMAL (38, 17) NOT NULL,
    [dv_load_date_time]                          DATETIME         NOT NULL,
    [dv_load_end_date_time]                      VARCHAR (12)     NOT NULL,
    [dv_batch_id]                                BIGINT           NOT NULL,
    [dv_inserted_date_time]                      DATETIME         NOT NULL,
    [dv_insert_user]                             NVARCHAR (128)   NULL,
    [refunded_dim_exerp_subscription_period_key] CHAR (32)        NOT NULL,
    [refund_adjusted_flag]                       VARCHAR (1)      NOT NULL,
    [refund_amount]                              DECIMAL (27, 6)  NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([dim_exerp_subscription_period_key]));

