CREATE TABLE [dbo].[stage_exerp_resource_group] (
    [stage_exerp_resource_group_id] BIGINT         NOT NULL,
    [id]                            INT            NULL,
    [name]                          VARCHAR (4000) NULL,
    [state]                         VARCHAR (4000) NULL,
    [dummy_modified_date_time]      DATETIME       NULL,
    [dv_batch_id]                   BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

