CREATE TABLE [dbo].[stage_exerp_area_center] (
    [stage_exerp_area_center_id] BIGINT         NOT NULL,
    [center_id]                  INT            NULL,
    [area_id]                    INT            NULL,
    [tree_name]                  VARCHAR (4000) NULL,
    [dummy_modified_date_time]   DATETIME       NULL,
    [dv_batch_id]                BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

