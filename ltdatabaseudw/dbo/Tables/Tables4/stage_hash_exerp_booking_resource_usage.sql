CREATE TABLE [dbo].[stage_hash_exerp_booking_resource_usage] (
    [stage_hash_exerp_booking_resource_usage_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                    CHAR (32)      NOT NULL,
    [resource_id]                                VARCHAR (4000) NULL,
    [booking_id]                                 VARCHAR (4000) NULL,
    [state]                                      VARCHAR (4000) NULL,
    [booking_start_datetime]                     DATETIME       NULL,
    [booking_stop_datetime]                      DATETIME       NULL,
    [center_id]                                  INT            NULL,
    [ets]                                        BIGINT         NULL,
    [dummy_modified_date_time]                   DATETIME       NULL,
    [dv_load_date_time]                          DATETIME       NOT NULL,
    [dv_batch_id]                                BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

