CREATE TABLE [dbo].[stage_mart_fact_member_interests] (
    [stage_mart_fact_member_interests_id] BIGINT          NOT NULL,
    [fact_member_interests_id]            INT             NULL,
    [member_id]                           INT             NULL,
    [interest_id]                         INT             NULL,
    [interest_confidence]                 DECIMAL (26, 6) NULL,
    [row_add_date]                        DATETIME        NULL,
    [active_flag]                         INT             NULL,
    [row_deactivation_date]               DATETIME        NULL,
    [dv_batch_id]                         BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

