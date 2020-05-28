CREATE TABLE [dbo].[stage_exerp_master_product] (
    [stage_exerp_master_product_id] BIGINT         NOT NULL,
    [id]                            INT            NULL,
    [name]                          VARCHAR (4000) NULL,
    [state]                         VARCHAR (4000) NULL,
    [globalid]                      VARCHAR (4000) NULL,
    [dummy_modified_date_time]      DATETIME       NULL,
    [dv_batch_id]                   BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

