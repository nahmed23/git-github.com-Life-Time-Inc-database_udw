CREATE TABLE [dbo].[stage_exerp_product_group] (
    [stage_exerp_product_group_id] BIGINT         NOT NULL,
    [id]                           INT            NULL,
    [name]                         VARCHAR (4000) NULL,
    [external_id]                  VARCHAR (4000) NULL,
    [parent_product_group_id]      INT            NULL,
    [dimension_product_group_id]   INT            NULL,
    [dummy_modified_date_time]     DATETIME       NULL,
    [dv_batch_id]                  BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

