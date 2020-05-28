CREATE TABLE [dbo].[stage_exerp_resource_availability] (
    [stage_exerp_resource_availability_id] BIGINT         NOT NULL,
    [resource_id]                          VARCHAR (4000) NULL,
    [resource_group_id]                    INT            NULL,
    [availability_type]                    VARCHAR (4000) NULL,
    [value]                                VARCHAR (4000) NULL,
    [from_time]                            VARCHAR (4000) NULL,
    [to_time]                              VARCHAR (4000) NULL,
    [dummy_modified_date_time]             DATETIME       NULL,
    [dv_batch_id]                          BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

