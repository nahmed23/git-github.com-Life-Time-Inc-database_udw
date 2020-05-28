CREATE TABLE [dbo].[stage_mart_fact_seg_membership_term_risk] (
    [stage_mart_fact_seg_membership_term_risk_id] BIGINT   NOT NULL,
    [fact_seg_membership_term_risk]               INT      NULL,
    [membership_id]                               INT      NULL,
    [term_risk_segment]                           INT      NULL,
    [row_add_date]                                DATETIME NULL,
    [active_flag]                                 INT      NULL,
    [row_deactivation_date]                       DATETIME NULL,
    [dv_batch_id]                                 BIGINT   NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

