CREATE TABLE [dbo].[stage_hash_exerp_staff_usage] (
    [stage_hash_exerp_staff_usage_id] BIGINT           IDENTITY (1, 1) NOT NULL,
    [bk_hash]                         CHAR (32)        NOT NULL,
    [id]                              INT              NULL,
    [booking_id]                      VARCHAR (4000)   NULL,
    [center_id]                       INT              NULL,
    [person_id]                       VARCHAR (4000)   NULL,
    [state]                           VARCHAR (4000)   NULL,
    [start_datetime]                  DATETIME         NULL,
    [stop_datetime]                   DATETIME         NULL,
    [salary]                          NUMERIC (18, 10) NULL,
    [ets]                             BIGINT           NULL,
    [substitute_of_person_id]         VARCHAR (4000)   NULL,
    [dummy_modified_date_time]        DATETIME         NULL,
    [dv_load_date_time]               DATETIME         NOT NULL,
    [dv_batch_id]                     BIGINT           NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

