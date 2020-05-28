CREATE TABLE [dbo].[stage_exerp_privilege_grant] (
    [stage_exerp_privilege_grant_id] BIGINT         NOT NULL,
    [id]                             INT            NULL,
    [source_type]                    VARCHAR (4000) NULL,
    [source_id]                      VARCHAR (4000) NULL,
    [privilege_set_id]               INT            NULL,
    [dummy_modified_date_time]       DATETIME       NULL,
    [dv_batch_id]                    BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

