CREATE TABLE [dbo].[stage_mms_SubsidyRule] (
    [stage_mms_SubsidyRule_id]                BIGINT         NOT NULL,
    [SubsidyRuleID]                           INT            NULL,
    [SubsidyCompanyReimbursementProgramID]    INT            NULL,
    [ValReimbursementUsageTypeID]             TINYINT        NULL,
    [Description]                             VARCHAR (255)  NULL,
    [UsageMinimum]                            SMALLINT       NULL,
    [MaxVisitsPerDay]                         SMALLINT       NULL,
    [ReimbursementAmountPerUsage]             NUMERIC (6, 2) NULL,
    [IgnoreUsageMinimumFirstMonthFlag]        BIT            NULL,
    [IncludeTaxUsageTierFlag]                 BIT            NULL,
    [InsertedDateTime]                        DATETIME       NULL,
    [UpdatedDateTime]                         DATETIME       NULL,
    [IgnoreUsageMinimumPreviousNonAccessFlag] BIT            NULL,
    [ApplyUsageCreditsPreviousAccessFlag]     BIT            NULL,
    [dv_batch_id]                             BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

