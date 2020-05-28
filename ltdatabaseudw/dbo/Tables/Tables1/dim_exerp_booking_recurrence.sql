CREATE TABLE [dbo].[dim_exerp_booking_recurrence] (
    [dim_exerp_booking_recurrence_id]  BIGINT         IDENTITY (1, 1) NOT NULL,
    [booking_name]                     VARCHAR (4000) NULL,
    [class_capacity]                   INT            NULL,
    [color]                            VARCHAR (4000) NULL,
    [comment]                          VARCHAR (4000) NULL,
    [description]                      VARCHAR (4000) NULL,
    [dim_club_key]                     CHAR (32)      NULL,
    [dim_exerp_activity_key]           CHAR (32)      NULL,
    [dim_exerp_booking_recurrence_key] CHAR (32)      NULL,
    [main_booking_id]                  VARCHAR (4000) NULL,
    [recurrence]                       VARCHAR (4000) NULL,
    [recurrence_end_dim_date_key]      CHAR (8)       NULL,
    [recurrence_end_dim_time_key]      CHAR (8)       NULL,
    [recurrence_start_dim_date_key]    CHAR (8)       NULL,
    [recurrence_start_dim_time_key]    CHAR (8)       NULL,
    [recurrence_type]                  VARCHAR (4000) NULL,
    [dv_load_date_time]                DATETIME       NULL,
    [dv_load_end_date_time]            DATETIME       NULL,
    [dv_batch_id]                      BIGINT         NOT NULL,
    [dv_inserted_date_time]            DATETIME       NOT NULL,
    [dv_insert_user]                   VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]             DATETIME       NULL,
    [dv_update_user]                   VARCHAR (50)   NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dim_exerp_booking_recurrence_key]));

