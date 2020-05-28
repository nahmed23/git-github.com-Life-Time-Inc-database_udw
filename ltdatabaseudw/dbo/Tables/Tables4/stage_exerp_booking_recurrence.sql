CREATE TABLE [dbo].[stage_exerp_booking_recurrence] (
    [stage_exerp_booking_recurrence_id] BIGINT         NOT NULL,
    [main_booking_id]                   VARCHAR (4000) NULL,
    [recurrence_type]                   VARCHAR (4000) NULL,
    [recurrence]                        VARCHAR (4000) NULL,
    [recurrence_start_datetime]         DATETIME       NULL,
    [recurrence_end]                    DATETIME       NULL,
    [center_id]                         INT            NULL,
    [dummy_modified_date_time]          DATETIME       NULL,
    [dv_batch_id]                       BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

