CREATE TABLE [dbo].[dim_exerp_booking_resource_usage] (
    [dim_exerp_booking_resource_usage_id]  BIGINT         IDENTITY (1, 1) NOT NULL,
    [booking_id]                           VARCHAR (4000) NULL,
    [booking_resource_usage_state]         VARCHAR (4000) NULL,
    [booking_start_dim_date_key]           VARCHAR (8)    NULL,
    [booking_start_dim_time_key]           INT            NULL,
    [booking_stop_dim_date_key]            VARCHAR (8)    NULL,
    [booking_stop_dim_time_key]            INT            NULL,
    [dim_club_key]                         VARCHAR (32)   NULL,
    [dim_exerp_booking_key]                VARCHAR (32)   NULL,
    [dim_exerp_booking_resource_usage_key] VARCHAR (32)   NULL,
    [resource_access_group_id]             VARCHAR (4000) NULL,
    [resource_access_group_name]           VARCHAR (4000) NULL,
    [resource_comment]                     VARCHAR (4000) NULL,
    [resource_external_id]                 VARCHAR (4000) NULL,
    [resource_id]                          VARCHAR (4000) NULL,
    [resource_name]                        VARCHAR (4000) NULL,
    [resource_type]                        VARCHAR (4000) NULL,
    [show_calendar_flag]                   CHAR (1)       NULL,
    [dv_load_date_time]                    DATETIME       NULL,
    [dv_load_end_date_time]                DATETIME       NULL,
    [dv_batch_id]                          BIGINT         NOT NULL,
    [dv_inserted_date_time]                DATETIME       NOT NULL,
    [dv_insert_user]                       VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]                 DATETIME       NULL,
    [dv_update_user]                       VARCHAR (50)   NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([dim_exerp_booking_resource_usage_key]));

