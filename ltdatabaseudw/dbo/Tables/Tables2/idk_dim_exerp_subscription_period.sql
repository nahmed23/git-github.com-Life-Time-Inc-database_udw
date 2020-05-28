CREATE TABLE [dbo].[idk_dim_exerp_subscription_period] (
    [idk_dim_exerp_subscription_period_id]  BIGINT          IDENTITY (1, 1) NOT NULL,
    [dim_exerp_subscription_period_key]     VARCHAR (32)    NULL,
    [number_of_bookings]                    INT             NULL,
    [price_per_booking]                     DECIMAL (26, 6) NULL,
    [lt_bucks_amount]                       DECIMAL (26, 6) NULL,
    [price_per_booking_less_lt_bucks]       DECIMAL (26, 6) NULL,
    [dv_batch_id]                           BIGINT          NULL,
    [dv_load_date_time]                     DATETIME        NOT NULL,
    [dv_inserted_date_time]                 DATETIME        NOT NULL,
    [dv_insert_user]                        VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                  DATETIME        NULL,
    [dv_update_user]                        VARCHAR (50)    NULL,
    [recurrence_main_dim_exerp_booking_key] VARCHAR (32)    NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([dim_exerp_subscription_period_key]));

