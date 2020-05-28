CREATE TABLE [dbo].[stage_exerp_area] (
    [stage_exerp_area_id]      BIGINT         NOT NULL,
    [id]                       INT            NULL,
    [parent_area_id]           INT            NULL,
    [name]                     VARCHAR (4000) NULL,
    [tree_name]                VARCHAR (4000) NULL,
    [blocked]                  BIT            NULL,
    [dummy_modified_date_time] DATETIME       NULL,
    [dv_batch_id]              BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

