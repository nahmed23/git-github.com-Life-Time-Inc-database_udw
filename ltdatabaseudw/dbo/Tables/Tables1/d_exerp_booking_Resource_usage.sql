CREATE TABLE [dbo].[d_exerp_booking_Resource_usage] (
    [d_exerp_booking_Resource_usage_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                           CHAR (32)      NOT NULL,
    [resource_id]                       VARCHAR (4000) NULL,
    [booking_id]                        VARCHAR (4000) NULL,
    [booking_resource_usage_state]      VARCHAR (4000) NULL,
    [booking_start_dim_date_key]        VARCHAR (8)    NULL,
    [booking_start_dim_time_key]        INT            NULL,
    [booking_stop_dim_date_key]         VARCHAR (8)    NULL,
    [booking_stop_dim_time_key]         INT            NULL,
    [d_exerp_booking_bk_hash]           VARCHAR (32)   NULL,
    [d_exerp_resource_bk_hash]          VARCHAR (32)   NULL,
    [dim_club_key]                      VARCHAR (32)   NULL,
    [ets]                               BIGINT         NULL,
    [p_exerp_booking_Resource_usage_id] BIGINT         NOT NULL,
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

