CREATE TABLE [dbo].[stage_exerp_resource_resource_group] (
    [stage_exerp_resource_resource_group_id] BIGINT         NOT NULL,
    [resource_id]                            VARCHAR (4000) NULL,
    [resource_group_id]                      INT            NULL,
    [center_id]                              INT            NULL,
    [dummy_modified_date_time]               DATETIME       NULL,
    [dv_batch_id]                            BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

