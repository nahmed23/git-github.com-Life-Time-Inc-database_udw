CREATE TABLE [dbo].[stage_mart_dim_seg_member_lifecycle] (
    [stage_mart_dim_seg_member_lifecycle_id] BIGINT          NOT NULL,
    [dim_seg_member_lifecycle_id]            INT             NULL,
    [lifecycle_segment]                      DECIMAL (12, 4) NULL,
    [lifecycle]                              CHAR (20)       NULL,
    [row_add_date]                           DATETIME        NULL,
    [active_flag]                            INT             NULL,
    [dv_batch_id]                            BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

