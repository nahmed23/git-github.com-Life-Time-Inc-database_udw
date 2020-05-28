CREATE TABLE [dbo].[stage_exerp_person_status_log] (
    [stage_exerp_person_status_log_id] BIGINT         NOT NULL,
    [id]                               INT            NULL,
    [person_id]                        VARCHAR (4000) NULL,
    [person_status]                    VARCHAR (4000) NULL,
    [from_datetime]                    DATETIME       NULL,
    [center_id]                        INT            NULL,
    [ets]                              BIGINT         NULL,
    [dummy_modified_date_time]         DATETIME       NULL,
    [dv_batch_id]                      BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

