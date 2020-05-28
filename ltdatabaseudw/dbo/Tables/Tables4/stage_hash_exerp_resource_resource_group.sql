CREATE TABLE [dbo].[stage_hash_exerp_resource_resource_group] (
    [stage_hash_exerp_resource_resource_group_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                     CHAR (32)      NOT NULL,
    [resource_id]                                 VARCHAR (4000) NULL,
    [resource_group_id]                           INT            NULL,
    [center_id]                                   INT            NULL,
    [dummy_modified_date_time]                    DATETIME       NULL,
    [dv_load_date_time]                           DATETIME       NOT NULL,
    [dv_batch_id]                                 BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

