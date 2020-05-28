CREATE TABLE [dbo].[stage_mart_fact_seg_member_primary_activity] (
    [stage_mart_fact_seg_member_primary_activity_id] BIGINT   NOT NULL,
    [fact_seg_member_primary_activity_id]            INT      NULL,
    [member_id]                                      INT      NULL,
    [primary_activity_segment]                       INT      NULL,
    [row_add_date]                                   DATETIME NULL,
    [active_flag]                                    INT      NULL,
    [row_deactivation_date]                          DATETIME NULL,
    [confidence_score]                               INT      NULL,
    [dv_batch_id]                                    BIGINT   NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

