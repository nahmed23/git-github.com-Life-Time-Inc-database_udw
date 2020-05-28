﻿CREATE TABLE [dbo].[d_exerp_subscription_period] (
    [d_exerp_subscription_period_id]    BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                           CHAR (32)      NOT NULL,
    [dim_exerp_subscription_period_key] CHAR (32)      NULL,
    [subscription_period_id]            VARCHAR (4000) NULL,
    [dim_club_key]                      CHAR (32)      NULL,
    [dim_exerp_subscription_key]        CHAR (32)      NULL,
    [ets]                               BIGINT         NULL,
    [fact_exerp_transaction_log_key]    VARCHAR (4000) NULL,
    [from_dim_date_key]                 CHAR (8)       NULL,
    [subscription_period_state]         VARCHAR (4000) NULL,
    [subscription_period_type]          VARCHAR (4000) NULL,
    [to_dim_date_key]                   CHAR (8)       NULL,
    [p_exerp_subscription_period_id]    BIGINT         NOT NULL,
    [deleted_flag]                      INT            NULL,
    [dv_load_date_time]                 DATETIME       NULL,
    [dv_load_end_date_time]             DATETIME       NULL,
    [dv_batch_id]                       BIGINT         NOT NULL,
    [dv_inserted_date_time]             DATETIME       NOT NULL,
    [dv_insert_user]                    VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]              DATETIME       NULL,
    [dv_update_user]                    VARCHAR (50)   NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

