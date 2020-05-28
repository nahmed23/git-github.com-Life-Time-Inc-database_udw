CREATE TABLE [dbo].[d_exerp_subscription_state_log] (
    [d_exerp_subscription_state_log_id]    BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                              CHAR (32)      NOT NULL,
    [dim_exerp_subscription_state_log_key] VARCHAR (32)   NULL,
    [subscription_state_log_id]            INT            NULL,
    [dim_club_key]                         VARCHAR (32)   NULL,
    [dim_exerp_subscription_key]           VARCHAR (32)   NULL,
    [entry_start_dim_date_key]             CHAR (8)       NULL,
    [entry_start_dim_time_key]             CHAR (8)       NULL,
    [ets]                                  BIGINT         NULL,
    [sub_state]                            VARCHAR (4000) NULL,
    [subscription_state_log_state]         VARCHAR (4000) NULL,
    [p_exerp_subscription_state_log_id]    BIGINT         NOT NULL,
    [deleted_flag]                         INT            NULL,
    [dv_load_date_time]                    DATETIME       NULL,
    [dv_load_end_date_time]                DATETIME       NULL,
    [dv_batch_id]                          BIGINT         NOT NULL,
    [dv_inserted_date_time]                DATETIME       NOT NULL,
    [dv_insert_user]                       VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]                 DATETIME       NULL,
    [dv_update_user]                       VARCHAR (50)   NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

