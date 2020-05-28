﻿CREATE TABLE [dbo].[d_exerp_subscription] (
    [d_exerp_subscription_id]              BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                              CHAR (32)       NOT NULL,
    [dim_exerp_subscription_key]           CHAR (32)       NULL,
    [subscription_id]                      VARCHAR (4000)  NULL,
    [billed_until_dim_date_key]            CHAR (8)        NULL,
    [binding_end_dim_date_key]             CHAR (8)        NULL,
    [binding_price]                        DECIMAL (26, 6) NULL,
    [creation_dim_date_key]                CHAR (8)        NULL,
    [creation_dim_time_key]                CHAR (8)        NULL,
    [dim_club_key]                         CHAR (32)       NULL,
    [dim_exerp_product_key]                CHAR (32)       NULL,
    [dim_mms_member_key]                   VARCHAR (32)    NULL,
    [end_dim_date_key]                     CHAR (8)        NULL,
    [ets]                                  BIGINT          NULL,
    [extension_dim_exerp_subscription_key] CHAR (32)       NULL,
    [freeze_period_dim_exerp_product_key]  CHAR (32)       NULL,
    [period_count]                         INT             NULL,
    [period_unit]                          VARCHAR (4000)  NULL,
    [price]                                DECIMAL (26, 6) NULL,
    [price_update_excluded_flag]           CHAR (1)        NULL,
    [reassign_dim_exerp_subscription_key]  CHAR (32)       NULL,
    [renewal_type]                         VARCHAR (4000)  NULL,
    [requires_main_flag]                   CHAR (1)        NULL,
    [start_dim_date_key]                   CHAR (8)        NULL,
    [stop_cancel_dim_date_key]             CHAR (8)        NULL,
    [stop_cancel_dim_time_key]             CHAR (8)        NULL,
    [stop_dim_date_key]                    CHAR (8)        NULL,
    [stop_dim_employee_key]                VARCHAR (32)    NULL,
    [stop_dim_time_key]                    CHAR (8)        NULL,
    [sub_state]                            VARCHAR (4000)  NULL,
    [subscription_state]                   VARCHAR (4000)  NULL,
    [transfer_dim_exerp_subscription_key]  CHAR (32)       NULL,
    [type_price_update_excluded_flag]      CHAR (1)        NULL,
    [p_exerp_subscription_id]              BIGINT          NOT NULL,
    [deleted_flag]                         INT             NULL,
    [dv_load_date_time]                    DATETIME        NULL,
    [dv_load_end_date_time]                DATETIME        NULL,
    [dv_batch_id]                          BIGINT          NOT NULL,
    [dv_inserted_date_time]                DATETIME        NOT NULL,
    [dv_insert_user]                       VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                 DATETIME        NULL,
    [dv_update_user]                       VARCHAR (50)    NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

