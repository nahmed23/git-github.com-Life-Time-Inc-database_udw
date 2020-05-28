CREATE TABLE [dbo].[stage_exerp_person_attribute_log] (
    [stage_exerp_person_attribute_log_id] BIGINT         NOT NULL,
    [id]                                  INT            NULL,
    [person_id]                           VARCHAR (4000) NULL,
    [attribute]                           VARCHAR (4000) NULL,
    [value]                               VARCHAR (4000) NULL,
    [from_datetime]                       DATETIME       NULL,
    [center_id]                           INT            NULL,
    [ets]                                 BIGINT         NULL,
    [dummy_modified_date_time]            DATETIME       NULL,
    [dv_batch_id]                         BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

