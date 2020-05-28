CREATE TABLE [dbo].[stage_exerp_product_product_group] (
    [stage_exerp_product_product_group_id] BIGINT         NOT NULL,
    [product_id]                           VARCHAR (4000) NULL,
    [product_group_id]                     INT            NULL,
    [ets]                                  BIGINT         NULL,
    [dummy_modified_date_time]             DATETIME       NULL,
    [dv_batch_id]                          BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

