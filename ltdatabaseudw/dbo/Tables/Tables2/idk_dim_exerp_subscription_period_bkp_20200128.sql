﻿CREATE TABLE [dbo].[idk_dim_exerp_subscription_period_bkp_20200128] (
    [dim_exerp_subscription_period_id]  BIGINT          IDENTITY (1, 1) NOT NULL,
    [dim_club_key]                      VARCHAR (32)    NULL,
    [dim_exerp_product_key]             VARCHAR (32)    NULL,
    [dim_exerp_subscription_key]        VARCHAR (32)    NULL,
    [dim_exerp_subscription_period_key] VARCHAR (32)    NULL,
    [dim_mms_member_key]                VARCHAR (32)    NULL,
    [fact_exerp_transaction_log_key]    VARCHAR (32)    NULL,
    [from_dim_date_key]                 VARCHAR (8)     NULL,
    [lt_bucks_amount]                   DECIMAL (26, 6) NULL,
    [net_amount]                        DECIMAL (26, 6) NULL,
    [number_of_bookings]                INT             NULL,
    [price_per_booking]                 DECIMAL (26, 6) NULL,
    [price_per_booking_less_lt_bucks]   DECIMAL (26, 6) NULL,
    [subscription_period_id]            VARCHAR (4000)  NULL,
    [subscription_period_state]         VARCHAR (4000)  NULL,
    [subscription_period_type]          VARCHAR (4000)  NULL,
    [to_dim_date_key]                   VARCHAR (8)     NULL,
    [dv_load_date_time]                 DATETIME        NULL,
    [dv_load_end_date_time]             DATETIME        NULL,
    [dv_batch_id]                       BIGINT          NOT NULL,
    [dv_inserted_date_time]             DATETIME        NOT NULL,
    [dv_insert_user]                    VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]              DATETIME        NULL,
    [dv_update_user]                    VARCHAR (50)    NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([dim_exerp_subscription_period_key]));

