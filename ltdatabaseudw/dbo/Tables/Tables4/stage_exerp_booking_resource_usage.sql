CREATE TABLE [dbo].[stage_exerp_booking_resource_usage] (
    [stage_exerp_booking_resource_usage_id] BIGINT         NOT NULL,
    [resource_id]                           VARCHAR (4000) NULL,
    [booking_id]                            VARCHAR (4000) NULL,
    [state]                                 VARCHAR (4000) NULL,
    [booking_start_datetime]                DATETIME       NULL,
    [booking_stop_datetime]                 DATETIME       NULL,
    [center_id]                             INT            NULL,
    [ets]                                   BIGINT         NULL,
    [dummy_modified_date_time]              DATETIME       NULL,
    [dv_batch_id]                           BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

