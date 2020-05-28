CREATE TABLE [dbo].[stage_mart_dim_seg_member_goal] (
    [stage_mart_dim_seg_member_goal_id] BIGINT          NOT NULL,
    [dim_seg_member_goal_id]            INT             NULL,
    [goal_segment]                      DECIMAL (12, 4) NULL,
    [goal]                              CHAR (20)       NULL,
    [row_add_date]                      DATETIME        NULL,
    [active_flag]                       INT             NULL,
    [dv_batch_id]                       BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

