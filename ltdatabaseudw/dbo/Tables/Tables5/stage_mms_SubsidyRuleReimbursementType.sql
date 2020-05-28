CREATE TABLE [dbo].[stage_mms_SubsidyRuleReimbursementType] (
    [stage_mms_SubsidyRuleReimbursementType_id] BIGINT         NOT NULL,
    [SubsidyRuleReimbursementTypeID]            INT            NULL,
    [SubsidyRuleID]                             INT            NULL,
    [ValReimbursementTypeID]                    TINYINT        NULL,
    [ReimbursementAmount]                       DECIMAL (6, 2) NULL,
    [ReimbursementPercentage]                   DECIMAL (6, 2) NULL,
    [IncludeTaxFlag]                            BIT            NULL,
    [InsertedDateTime]                          DATETIME       NULL,
    [UpdatedDateTime]                           DATETIME       NULL,
    [dv_batch_id]                               BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

