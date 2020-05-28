CREATE TABLE [dbo].[d_exerp_booking_recurrence] (
    [d_exerp_booking_recurrence_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                       CHAR (32)      NOT NULL,
    [main_booking_id]               VARCHAR (4000) NULL,
    [center_id]                     INT            NULL,
    [d_exerp_center_bk_hash]        CHAR (32)      NULL,
    [recurrence]                    VARCHAR (4000) NULL,
    [recurrence_end_dim_date_key]   CHAR (8)       NULL,
    [recurrence_start_dim_date_key] CHAR (8)       NULL,
    [recurrence_start_dim_time_key] CHAR (8)       NULL,
    [recurrence_type]               VARCHAR (4000) NULL,
    [p_exerp_booking_recurrence_id] BIGINT         NOT NULL,
    [deleted_flag]                  INT            NULL,
    [dv_load_date_time]             DATETIME       NULL,
    [dv_load_end_date_time]         DATETIME       NULL,
    [dv_batch_id]                   BIGINT         NOT NULL,
    [dv_inserted_date_time]         DATETIME       NOT NULL,
    [dv_insert_user]                VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]          DATETIME       NULL,
    [dv_update_user]                VARCHAR (50)   NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_exerp_booking_recurrence]([dv_batch_id] ASC);

