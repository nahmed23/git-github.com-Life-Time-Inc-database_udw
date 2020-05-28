CREATE TABLE [dbo].[stage_exerp_person_ext_attr] (
    [stage_exerp_person_ext_attr_id] BIGINT         NOT NULL,
    [person_id]                      VARCHAR (4000) NULL,
    [name]                           VARCHAR (4000) NULL,
    [value]                          VARCHAR (4000) NULL,
    [center_id]                      INT            NULL,
    [ets]                            BIGINT         NULL,
    [dummy_modified_date_time]       DATETIME       NULL,
    [dv_batch_id]                    BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

