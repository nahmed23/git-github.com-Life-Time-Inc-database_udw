CREATE TABLE [dbo].[stage_mart_dim_seg_member_primary_activity] (
    [stage_mart_dim_seg_member_primary_activity_id] BIGINT          NOT NULL,
    [dim_seg_member_primary_activity_id]            INT             NULL,
    [primary_activity_segment]                      DECIMAL (12, 4) NULL,
    [primary_activity]                              CHAR (30)       NULL,
    [row_add_date]                                  DATETIME        NULL,
    [active_flag]                                   INT             NULL,
    [dv_batch_id]                                   BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

