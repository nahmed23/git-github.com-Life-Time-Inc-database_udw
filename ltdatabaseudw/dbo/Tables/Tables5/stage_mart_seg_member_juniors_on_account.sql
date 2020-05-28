CREATE TABLE [dbo].[stage_mart_seg_member_juniors_on_account] (
    [stage_mart_seg_member_juniors_on_account_id] BIGINT     NOT NULL,
    [juniors_on_account_segment_id]               INT        NULL,
    [juniors_on_account]                          CHAR (255) NULL,
    [dummy_modified_date_time]                    DATETIME   NULL,
    [dv_batch_id]                                 BIGINT     NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

