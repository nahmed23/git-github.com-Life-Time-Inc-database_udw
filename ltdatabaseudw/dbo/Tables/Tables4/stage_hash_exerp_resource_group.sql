CREATE TABLE [dbo].[stage_hash_exerp_resource_group] (
    [stage_hash_exerp_resource_group_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                            CHAR (32)      NOT NULL,
    [id]                                 INT            NULL,
    [name]                               VARCHAR (4000) NULL,
    [state]                              VARCHAR (4000) NULL,
    [dummy_modified_date_time]           DATETIME       NULL,
    [dv_load_date_time]                  DATETIME       NOT NULL,
    [dv_batch_id]                        BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

